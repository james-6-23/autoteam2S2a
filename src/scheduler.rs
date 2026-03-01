use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chrono::NaiveTime;
use tokio::sync::Mutex;

use crate::config::ScheduleConfig;
use crate::db::RunHistoryDb;
use crate::server::AppState;

// ─── Scheduler state ─────────────────────────────────────────────────────────

/// 跟踪当前活跃的定时计划（正在执行批次循环的计划）
pub struct SchedulerState {
    active: Mutex<HashMap<String, Arc<AtomicBool>>>,
}

impl SchedulerState {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
        }
    }

    /// 尝试启动一个计划。如果已在运行则返回 None。
    pub async fn start(&self, name: &str) -> Option<Arc<AtomicBool>> {
        let mut active = self.active.lock().await;
        if active.contains_key(name) {
            return None;
        }
        let flag = Arc::new(AtomicBool::new(false));
        active.insert(name.to_string(), flag.clone());
        Some(flag)
    }

    /// 停止一个正在运行的计划
    pub async fn stop(&self, name: &str) -> bool {
        let active = self.active.lock().await;
        if let Some(flag) = active.get(name) {
            flag.store(true, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// 从活跃列表移除（批次循环结束时调用）
    pub async fn remove(&self, name: &str) {
        self.active.lock().await.remove(name);
    }

    /// 检查计划是否正在运行
    pub async fn is_active(&self, name: &str) -> bool {
        self.active.lock().await.contains_key(name)
    }

    /// 获取所有活跃计划名
    pub async fn active_names(&self) -> Vec<String> {
        self.active.lock().await.keys().cloned().collect()
    }
}

// ─── Time window check ──────────────────────────────────────────────────────

/// 判断当前本地时间是否在 [start, end) 窗口内。支持跨日窗口（如 22:00-06:00）。
pub fn is_in_window(start_time: &str, end_time: &str) -> bool {
    let start = match NaiveTime::parse_from_str(start_time, "%H:%M") {
        Ok(t) => t,
        Err(_) => return false,
    };
    let end = match NaiveTime::parse_from_str(end_time, "%H:%M") {
        Ok(t) => t,
        Err(_) => return false,
    };
    let now = chrono::Local::now().time();

    if start <= end {
        // 同日窗口: 10:00-14:00
        now >= start && now < end
    } else {
        // 跨日窗口: 22:00-06:00
        now >= start || now < end
    }
}

/// 校验时间格式 HH:MM
pub fn validate_time(time_str: &str) -> Result<(), String> {
    NaiveTime::parse_from_str(time_str, "%H:%M")
        .map(|_| ())
        .map_err(|_| format!("无效的时间格式 (需要 HH:MM): {time_str}"))
}

// ─── Scheduler loop ─────────────────────────────────────────────────────────

/// 启动后台调度循环
pub fn start_scheduler(state: AppState, db: Arc<RunHistoryDb>) {
    tokio::spawn(async move {
        scheduler_loop(state, db).await;
    });
}

async fn scheduler_loop(state: AppState, db: Arc<RunHistoryDb>) {
    println!("[调度器] 后台调度器已启动，每 30 秒检查时间窗口");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;

        let cfg = state.config.read().await;
        let schedules = cfg.schedule.clone();
        drop(cfg);

        for schedule_cfg in &schedules {
            if !schedule_cfg.enabled {
                continue;
            }

            let in_window = is_in_window(&schedule_cfg.start_time, &schedule_cfg.end_time);
            let is_active = state.scheduler_state.is_active(&schedule_cfg.name).await;

            if in_window && !is_active {
                // 进入时间窗口且未运行 → 启动批次循环
                if let Some(cancel_flag) =
                    state.scheduler_state.start(&schedule_cfg.name).await
                {
                    println!(
                        "[调度器] 进入时间窗口，启动计划: {} ({}-{})",
                        schedule_cfg.name, schedule_cfg.start_time, schedule_cfg.end_time
                    );

                    let state_clone = state.clone();
                    let db_clone = db.clone();
                    let schedule_clone = schedule_cfg.clone();

                    tokio::spawn(async move {
                        run_batch_loop(state_clone, db_clone, schedule_clone, cancel_flag).await;
                    });
                }
            }
        }
    }
}

// ─── Batch loop ─────────────────────────────────────────────────────────────

async fn run_batch_loop(
    state: AppState,
    db: Arc<RunHistoryDb>,
    schedule: ScheduleConfig,
    cancel_flag: Arc<AtomicBool>,
) {
    let mut batch_num = 0u64;

    println!(
        "[调度器] {} 批次循环开始 (每批 {} 个, 间隔 {} 分钟)",
        schedule.name, schedule.target_count, schedule.batch_interval_mins
    );

    loop {
        // 检查取消
        if cancel_flag.load(Ordering::Relaxed) {
            println!("[调度器] {} 已停止（手动取消）", schedule.name);
            break;
        }

        // 检查时间窗口
        if !is_in_window(&schedule.start_time, &schedule.end_time) {
            println!("[调度器] {} 时间窗口结束", schedule.name);
            break;
        }

        batch_num += 1;
        println!(
            "[调度器] {} 开始第 {} 批次（每批 {} 个）",
            schedule.name, batch_num, schedule.target_count
        );

        // 每批次重新读取配置（允许运行期间修改参数）
        let cfg = state.config.read().await.clone();

        // 构建 runner
        let runner =
            match crate::server::build_workflow_runner(&cfg, state.proxy_file.as_deref()).await {
                Ok(r) => r,
                Err(e) => {
                    println!(
                        "[调度器] 构建 runner 失败 ({}): {e}",
                        schedule.name
                    );
                    // 短暂等待后重试
                    tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                    continue;
                }
            };

        // 执行一个批次
        match crate::distribution::run_distribution(
            &runner,
            &cfg,
            &schedule,
            &db,
            "scheduled",
            cancel_flag.clone(),
        )
        .await
        {
            Ok(report) => {
                println!(
                    "[调度器] {} 第 {} 批完成 | 注册: {} | RT: {} | S2A: {} | 耗时: {:.1}s",
                    schedule.name,
                    batch_num,
                    report.registered_ok,
                    report.rt_ok,
                    report.total_s2a_ok,
                    report.elapsed_secs
                );
            }
            Err(e) => {
                println!(
                    "[调度器] {} 第 {} 批失败: {e:#}",
                    schedule.name, batch_num
                );
                // 如果是被取消的，直接退出
                if cancel_flag.load(Ordering::Relaxed) {
                    println!("[调度器] {} 已停止（运行中取消）", schedule.name);
                    break;
                }
            }
        }

        // 批次间等待，期间持续检查取消信号和时间窗口
        let total_wait = std::time::Duration::from_secs(schedule.batch_interval_mins * 60);
        let check_interval = tokio::time::Duration::from_secs(5);
        let mut elapsed = std::time::Duration::ZERO;

        println!(
            "[调度器] {} 等待 {} 分钟后执行下一批...",
            schedule.name, schedule.batch_interval_mins
        );

        while elapsed < total_wait {
            if cancel_flag.load(Ordering::Relaxed) {
                println!("[调度器] {} 已停止（等待期间取消）", schedule.name);
                break;
            }
            if !is_in_window(&schedule.start_time, &schedule.end_time) {
                println!("[调度器] {} 等待期间时间窗口结束", schedule.name);
                break;
            }
            tokio::time::sleep(check_interval).await;
            elapsed += std::time::Duration::from_secs(5);
        }

        // 再次检查退出条件
        if cancel_flag.load(Ordering::Relaxed)
            || !is_in_window(&schedule.start_time, &schedule.end_time)
        {
            break;
        }
    }

    // 清理活跃状态
    state.scheduler_state.remove(&schedule.name).await;
    println!(
        "[调度器] {} 批次循环结束（共执行 {} 批）",
        schedule.name, batch_num
    );
}
