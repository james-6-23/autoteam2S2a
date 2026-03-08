use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::NaiveTime;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::config::ScheduleConfig;
use crate::db::RunHistoryDb;
use crate::log_broadcast::broadcast_log;
use crate::server::AppState;

// ─── Scheduler state ─────────────────────────────────────────────────────────

struct ActiveEntry {
    cancel_flag: Arc<AtomicBool>,
    batch_num: Arc<AtomicU64>,
    /// 下一批次开始时间的 unix timestamp (0 = 执行中)
    next_batch_ts: Arc<AtomicU64>,
}

#[derive(Serialize, Clone)]
pub struct ScheduleRunInfo {
    pub batch_num: u64,
    pub next_batch_at: Option<String>,
}

/// 跟踪当前活跃的定时计划（正在执行批次循环的计划）
pub struct SchedulerState {
    active: Mutex<HashMap<String, ActiveEntry>>,
}

impl SchedulerState {
    pub fn new() -> Self {
        Self {
            active: Mutex::new(HashMap::new()),
        }
    }

    /// 尝试启动一个计划。如果已在运行则返回 None。
    pub async fn start(
        &self,
        name: &str,
    ) -> Option<(Arc<AtomicBool>, Arc<AtomicU64>, Arc<AtomicU64>)> {
        let mut active = self.active.lock().await;
        if active.contains_key(name) {
            return None;
        }
        let flag = Arc::new(AtomicBool::new(false));
        let batch_num = Arc::new(AtomicU64::new(0));
        let next_batch_ts = Arc::new(AtomicU64::new(0));
        active.insert(
            name.to_string(),
            ActiveEntry {
                cancel_flag: flag.clone(),
                batch_num: batch_num.clone(),
                next_batch_ts: next_batch_ts.clone(),
            },
        );
        Some((flag, batch_num, next_batch_ts))
    }

    /// 停止一个正在运行的计划
    pub async fn stop(&self, name: &str) -> bool {
        let active = self.active.lock().await;
        if let Some(entry) = active.get(name) {
            entry.cancel_flag.store(true, Ordering::SeqCst);
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

    /// 获取所有运行中计划的批次信息快照（单次加锁）
    pub async fn snapshot(&self) -> HashMap<String, ScheduleRunInfo> {
        let active = self.active.lock().await;
        active
            .iter()
            .map(|(name, entry)| (name.clone(), Self::build_run_info(entry)))
            .collect()
    }

    fn build_run_info(entry: &ActiveEntry) -> ScheduleRunInfo {
        let ts = entry.next_batch_ts.load(Ordering::Relaxed);
        let next = if ts > 0 {
            chrono::DateTime::from_timestamp(ts as i64, 0).map(|dt| {
                let tz = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
                dt.with_timezone(&tz).format("%H:%M:%S").to_string()
            })
        } else {
            None
        };
        ScheduleRunInfo {
            batch_num: entry.batch_num.load(Ordering::Relaxed),
            next_batch_at: next,
        }
    }
}

// ─── Time window check ──────────────────────────────────────────────────────

/// 判断当前北京时间是否在 [start, end) 窗口内。支持跨日窗口（如 22:00-06:00）。
pub fn is_in_window(start_time: &str, end_time: &str) -> bool {
    let start = match NaiveTime::parse_from_str(start_time, "%H:%M") {
        Ok(t) => t,
        Err(_) => return false,
    };
    let end = match NaiveTime::parse_from_str(end_time, "%H:%M") {
        Ok(t) => t,
        Err(_) => return false,
    };
    let now = crate::util::beijing_now().time();

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

pub async fn load_schedule_config(state: &AppState, name: &str) -> Option<ScheduleConfig> {
    let cfg = state.config.read().await;
    cfg.schedule.iter().find(|s| s.name == name).cloned()
}

// ─── Scheduler loop ─────────────────────────────────────────────────────────

/// 启动后台调度循环
pub fn start_scheduler(state: AppState, db: Arc<RunHistoryDb>) {
    tokio::spawn(async move {
        scheduler_loop(state, db).await;
    });
}

async fn scheduler_loop(state: AppState, db: Arc<RunHistoryDb>) {
    broadcast_log("[调度器] 后台调度器已启动，每 30 秒检查时间窗口");

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
                if let Some((cancel_flag, batch_num, next_ts)) =
                    state.scheduler_state.start(&schedule_cfg.name).await
                {
                    broadcast_log(&format!(
                        "[调度器] 进入时间窗口，启动计划: {} ({}-{})",
                        schedule_cfg.name, schedule_cfg.start_time, schedule_cfg.end_time
                    ));

                    let state_clone = state.clone();
                    let db_clone = db.clone();
                    let schedule_name = schedule_cfg.name.clone();

                    tokio::spawn(async move {
                        run_batch_loop(
                            state_clone,
                            db_clone,
                            schedule_name,
                            cancel_flag,
                            batch_num,
                            next_ts,
                        )
                        .await;
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
    schedule_name: String,
    cancel_flag: Arc<AtomicBool>,
    batch_counter: Arc<AtomicU64>,
    next_batch_ts: Arc<AtomicU64>,
) {
    let Some(initial_schedule) = load_schedule_config(&state, &schedule_name).await else {
        broadcast_log(&format!(
            "[调度器] {} 批次循环未启动：计划不存在",
            schedule_name
        ));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    };

    broadcast_log(&format!(
        "[调度器] {} 批次循环开始 (每批 RT 成功目标 {} 个, 间隔 {} 分钟)",
        schedule_name, initial_schedule.target_count, initial_schedule.batch_interval_mins
    ));

    // 恢复逻辑：检查上次执行完成时间，避免重启后立即重复执行
    let last_finished = tokio::task::spawn_blocking({
        let db = db.clone();
        let schedule_name = schedule_name.clone();
        move || db.last_finished_at(&schedule_name)
    })
    .await;
    if let Ok(Ok(Some(last_ts))) = last_finished {
        if let Ok(last_dt) = chrono::DateTime::parse_from_rfc3339(&last_ts) {
            let now = chrono::Utc::now();
            let since = now.signed_duration_since(last_dt);
            let interval = chrono::Duration::seconds(
                initial_schedule.batch_interval_mins.saturating_mul(60) as i64,
            );
            let remaining = interval - since;

            if remaining > chrono::Duration::zero() {
                let wait_secs = remaining.num_seconds() as u64;
                broadcast_log(&format!(
                    "[调度器] {} 上次完成于 {:.0} 秒前，还需等待 {} 秒",
                    schedule_name,
                    since.num_seconds(),
                    wait_secs
                ));

                let next_ts_val = chrono::Utc::now().timestamp() as u64 + wait_secs;
                next_batch_ts.store(next_ts_val, Ordering::Relaxed);

                let check_interval = tokio::time::Duration::from_secs(5);
                let mut elapsed = std::time::Duration::ZERO;
                let total_wait = std::time::Duration::from_secs(wait_secs);

                while elapsed < total_wait {
                    if cancel_flag.load(Ordering::Relaxed) {
                        broadcast_log(&format!(
                            "[调度器] {} 已停止（恢复等待期间取消）",
                            schedule_name
                        ));
                        state.scheduler_state.remove(&schedule_name).await;
                        return;
                    }
                    let Some(latest_schedule) = load_schedule_config(&state, &schedule_name).await
                    else {
                        broadcast_log(&format!(
                            "[调度器] {} 已停止（恢复等待期间计划已删除）",
                            schedule_name
                        ));
                        state.scheduler_state.remove(&schedule_name).await;
                        return;
                    };
                    if !latest_schedule.enabled {
                        broadcast_log(&format!(
                            "[调度器] {} 已停止（恢复等待期间计划已禁用）",
                            schedule_name
                        ));
                        state.scheduler_state.remove(&schedule_name).await;
                        return;
                    }
                    if !is_in_window(&latest_schedule.start_time, &latest_schedule.end_time) {
                        broadcast_log(&format!(
                            "[调度器] {} 恢复等待期间时间窗口结束",
                            schedule_name
                        ));
                        state.scheduler_state.remove(&schedule_name).await;
                        return;
                    }
                    tokio::time::sleep(check_interval).await;
                    elapsed += std::time::Duration::from_secs(5);
                }
            }
        }
    }

    loop {
        // 检查取消
        if cancel_flag.load(Ordering::Relaxed) {
            broadcast_log(&format!("[调度器] {} 已停止（手动取消）", schedule_name));
            break;
        }

        let cfg = state.config.read().await.clone();
        let Some(schedule) = cfg
            .schedule
            .iter()
            .find(|s| s.name == schedule_name)
            .cloned()
        else {
            broadcast_log(&format!("[调度器] {} 已停止（计划已删除）", schedule_name));
            break;
        };
        if !schedule.enabled {
            broadcast_log(&format!("[调度器] {} 已停止（计划已禁用）", schedule_name));
            break;
        }
        if !is_in_window(&schedule.start_time, &schedule.end_time) {
            broadcast_log(&format!("[调度器] {} 时间窗口结束", schedule_name));
            break;
        }

        let batch_num = batch_counter.fetch_add(1, Ordering::Relaxed) + 1;
        next_batch_ts.store(0, Ordering::Relaxed); // 0 = 执行中
        broadcast_log(&format!(
            "[调度器] {} 开始第 {} 批次（每批 RT 成功目标 {} 个）",
            schedule.name, batch_num, schedule.target_count
        ));

        // 构建 runner
        let runner = match crate::server::build_workflow_runner(
            &cfg,
            state.proxy_file.as_deref(),
            schedule.use_chatgpt_mail,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                broadcast_log(&format!(
                    "[调度器] 构建 runner 失败 ({}): {e}",
                    schedule_name
                ));
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
                broadcast_log(&format!(
                    "[调度器] {} 第 {} 批完成 | 注册: {} | RT: {} | S2A: {} | 耗时: {:.1}s",
                    schedule.name,
                    batch_num,
                    report.registered_ok,
                    report.rt_ok,
                    report.total_s2a_ok,
                    report.elapsed_secs
                ));
            }
            Err(e) => {
                broadcast_log(&format!(
                    "[调度器] {} 第 {} 批失败: {e:#}",
                    schedule_name, batch_num
                ));
                // 如果是被取消的，直接退出
                if cancel_flag.load(Ordering::Relaxed) {
                    broadcast_log(&format!("[调度器] {} 已停止（运行中取消）", schedule_name));
                    break;
                }
            }
        }

        // 批次间等待，期间持续检查取消信号和时间窗口
        let wait_secs = schedule.batch_interval_mins.saturating_mul(60);
        let total_wait = std::time::Duration::from_secs(wait_secs);
        let check_interval = tokio::time::Duration::from_secs(5);
        let mut elapsed = std::time::Duration::ZERO;
        let mut should_continue = true;

        // 记录下一批次预计时间
        let next_ts = chrono::Utc::now().timestamp() as u64 + wait_secs;
        next_batch_ts.store(next_ts, Ordering::Relaxed);

        broadcast_log(&format!(
            "[调度器] {} 等待 {} 分钟后执行下一批...",
            schedule_name, schedule.batch_interval_mins
        ));

        while elapsed < total_wait {
            if cancel_flag.load(Ordering::Relaxed) {
                broadcast_log(&format!(
                    "[调度器] {} 已停止（等待期间取消）",
                    schedule_name
                ));
                should_continue = false;
                break;
            }
            let Some(latest_schedule) = load_schedule_config(&state, &schedule_name).await else {
                broadcast_log(&format!(
                    "[调度器] {} 已停止（等待期间计划已删除）",
                    schedule_name
                ));
                should_continue = false;
                break;
            };
            if !latest_schedule.enabled {
                broadcast_log(&format!(
                    "[调度器] {} 已停止（等待期间计划已禁用）",
                    schedule_name
                ));
                should_continue = false;
                break;
            }
            if !is_in_window(&latest_schedule.start_time, &latest_schedule.end_time) {
                broadcast_log(&format!("[调度器] {} 等待期间时间窗口结束", schedule_name));
                should_continue = false;
                break;
            }
            tokio::time::sleep(check_interval).await;
            elapsed += std::time::Duration::from_secs(5);
        }

        // 再次检查退出条件
        if cancel_flag.load(Ordering::Relaxed) || !should_continue {
            break;
        }
    }

    // 清理活跃状态
    state.scheduler_state.remove(&schedule_name).await;
    broadcast_log(&format!(
        "[调度器] {} 批次循环结束（共执行 {} 批）",
        schedule_name,
        batch_counter.load(Ordering::Relaxed)
    ));
}
