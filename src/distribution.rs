use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::mpsc;

use crate::config::{AppConfig, DistributionEntry, S2aConfig, ScheduleConfig};
use crate::db::{NewDistribution, NewRun, RunCompletion, RunHistoryDb};
use crate::log_broadcast::broadcast_log;
use crate::models::{AccountWithRt, DistributionReport, TeamDistResult};
use crate::workflow::{WorkflowOptions, WorkflowRunner};

/// 按百分比分割账号列表。前 N-1 个号池取 floor(total * percent / 100)，最后一个取剩余。
#[allow(dead_code)]
fn split_by_percentage(
    accounts: &[AccountWithRt],
    distribution: &[(String, u8)],
) -> Vec<(String, Vec<AccountWithRt>)> {
    if distribution.is_empty() || accounts.is_empty() {
        return Vec::new();
    }

    let total = accounts.len();
    let mut result = Vec::with_capacity(distribution.len());
    let mut offset = 0usize;

    for (i, (team, percent)) in distribution.iter().enumerate() {
        let count = if i + 1 < distribution.len() {
            (total * (*percent as usize)) / 100
        } else {
            total - offset
        };
        let end = (offset + count).min(total);
        result.push((team.clone(), accounts[offset..end].to_vec()));
        offset = end;
    }

    result
}

/// 加权路由状态：跟踪每个号池的已分配数量，确保比例精确。
struct WeightedRouter {
    /// (team_name, weight/percent, count)
    slots: Vec<(String, f64, AtomicUsize)>,
}

impl WeightedRouter {
    fn new(distribution: &[DistributionEntry]) -> Self {
        let slots = distribution
            .iter()
            .map(|d| {
                (
                    d.team.clone(),
                    d.percent as f64 / 100.0,
                    AtomicUsize::new(0),
                )
            })
            .collect();
        Self { slots }
    }

    /// 选择下一个应该接收账号的号池（亏欠最多的号池优先）。
    fn next_team(&self) -> &str {
        let total: usize = self.slots.iter().map(|s| s.2.load(Ordering::Relaxed)).sum();
        let total_f = (total + 1) as f64;

        let mut best_idx = 0;
        let mut best_deficit = f64::NEG_INFINITY;
        for (i, (_, weight, count)) in self.slots.iter().enumerate() {
            let actual_ratio = count.load(Ordering::Relaxed) as f64 / total_f;
            let deficit = weight - actual_ratio;
            if deficit > best_deficit {
                best_deficit = deficit;
                best_idx = i;
            }
        }

        self.slots[best_idx].2.fetch_add(1, Ordering::Relaxed);
        &self.slots[best_idx].0
    }

    /// 获取每个号池的最终分配数量。
    fn counts(&self) -> Vec<(String, usize)> {
        self.slots
            .iter()
            .map(|(name, _, count)| (name.clone(), count.load(Ordering::Relaxed)))
            .collect()
    }
}

/// 执行多 S2A 分发流程
pub async fn run_distribution(
    runner: &WorkflowRunner,
    cfg: &AppConfig,
    schedule: &ScheduleConfig,
    db: &Arc<RunHistoryDb>,
    trigger_type: &str,
    cancel_flag: Arc<AtomicBool>,
) -> Result<DistributionReport> {
    let workflow_started = Instant::now();
    let run_id = uuid::Uuid::new_v4().to_string()[..8].to_string();

    // 1. 在 SQLite 创建 run 记录
    db.enqueue_insert_run(NewRun {
        id: run_id.clone(),
        schedule_name: Some(schedule.name.clone()),
        trigger_type: trigger_type.to_string(),
        target_count: schedule.target_count,
        started_at: crate::util::beijing_now().to_rfc3339(),
    })?;

    let dist_entries: Vec<NewDistribution> = schedule
        .distribution
        .iter()
        .map(|d| NewDistribution {
            team_name: d.team.clone(),
            percent: d.percent,
        })
        .collect();
    db.enqueue_insert_distributions(run_id.clone(), dist_entries)?;

    let register_runtime = cfg.register_runtime();
    let retry_guard = schedule
        .rt_retries
        .unwrap_or(cfg.defaults.rt_retries.unwrap_or(4))
        .max(1);
    let options = WorkflowOptions {
        target_count: schedule.target_count,
        register_workers: schedule
            .register_workers
            .unwrap_or(cfg.defaults.register_workers.unwrap_or(15))
            .max(1),
        rt_workers: schedule
            .rt_workers
            .unwrap_or(cfg.defaults.rt_workers.unwrap_or(10))
            .max(1),
        rt_retry_max: retry_guard,
        target_fill_max_rounds: retry_guard,
        push_s2a: schedule.push_s2a,
        mail_provider: schedule.effective_mail_provider(),
        free_mode: schedule.free_mode,
        register_log_mode: schedule
            .register_log_mode
            .unwrap_or(register_runtime.register_log_mode),
        register_perf_mode: schedule
            .register_perf_mode
            .unwrap_or(register_runtime.register_perf_mode),
    };

    // 2. 注册 + RT + 流式 S2A 入库
    let mode_label = if options.free_mode { "free" } else { "team" };
    broadcast_log(&format!(
        "[分发] 开始注册 + RT + 流式S2A，目标: {} 模式 RT 成功 {} 个（兜底轮次={}）",
        mode_label, schedule.target_count, retry_guard
    ));

    // 创建 S2A 流式 channel
    let (s2a_tx, s2a_rx) = if options.push_s2a {
        let (tx, rx) = mpsc::channel::<AccountWithRt>(256);
        (Some(tx), Some(rx))
    } else {
        (None, None)
    };

    // 启动分发流式 S2A 消费者
    let teams = cfg.effective_s2a_configs();
    let s2a_handle = if let Some(rx) = s2a_rx {
        let s2a_service = runner.s2a_service();
        let distribution = schedule.distribution.clone();
        let teams = teams.clone();
        let cancel = cancel_flag.clone();
        let free_mode = options.free_mode;
        Some(tokio::spawn(async move {
            dist_s2a_streaming_consumer(rx, s2a_service, &distribution, &teams, free_mode, cancel)
                .await
        }))
    } else {
        None
    };

    let reg_result = match runner
        .run_register_and_rt(cfg, &options, cancel_flag.clone(), None, s2a_tx)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            if let Some(h) = s2a_handle {
                let _ = h.await;
            }
            let _ = db.enqueue_fail_run(run_id.clone(), format!("{e:#}"));
            broadcast_log(&format!(
                "[SIG-END][ERR] schedule={} trigger={} mode={} target_rt={} stage=register_rt reason={e:#}",
                schedule.name, trigger_type, mode_label, schedule.target_count
            ));
            return Err(e);
        }
    };

    if cancel_flag.load(Ordering::Relaxed) {
        if let Some(h) = s2a_handle {
            let _ = h.await;
        }
        let _ = db.enqueue_fail_run(run_id.clone(), "用户取消".to_string());
        broadcast_log(&format!(
            "[SIG-END][ERR] schedule={} trigger={} mode={} target_rt={} stage=cancel reason=用户取消",
            schedule.name, trigger_type, mode_label, schedule.target_count
        ));
        anyhow::bail!("运行已取消");
    }

    let rt_ok = reg_result.rt_success.len();
    let rt_failed = reg_result.rt_failed.len();
    let output_files = reg_result.output_files;

    // 等待 S2A 流式消费者完成
    let (total_s2a_ok, total_s2a_failed, team_results) = if let Some(h) = s2a_handle {
        match h.await {
            Ok(r) => r,
            Err(_) => (0, 0, Vec::new()),
        }
    } else {
        (0, 0, Vec::new())
    };

    // 更新 SQLite 中每个号池的统计
    for tr in &team_results {
        let _ = db.enqueue_update_distribution(
            run_id.clone(),
            tr.team_name.clone(),
            tr.assigned_count,
            tr.s2a_ok,
            tr.s2a_failed,
        );
    }

    let elapsed = workflow_started.elapsed().as_secs_f32();

    let _ = db.enqueue_complete_run(
        run_id.clone(),
        RunCompletion {
            registered_ok: reg_result.total_registered,
            registered_failed: reg_result.total_reg_failed,
            rt_ok,
            rt_failed,
            total_s2a_ok,
            total_s2a_failed,
            elapsed_secs: elapsed as f64,
            finished_at: crate::util::beijing_now().to_rfc3339(),
        },
    );

    // D1 清理
    if cfg.d1_cleanup.enabled.unwrap_or(false) {
        broadcast_log("[分发] D1 邮件清理");
        if let Err(e) = crate::d1_cleanup::run_cleanup(&cfg.d1_cleanup).await {
            broadcast_log(&format!("[分发] D1 清理失败: {e}"));
        }
    }

    broadcast_log(&format!(
        "[SIG-END][OK] schedule={} trigger={} mode={} target_rt={} reg_ok={} reg_fail={} rt_ok={} rt_fail={} s2a_ok={} s2a_fail={} elapsed={:.1}s",
        schedule.name,
        trigger_type,
        mode_label,
        schedule.target_count,
        reg_result.total_registered,
        reg_result.total_reg_failed,
        rt_ok,
        rt_failed,
        total_s2a_ok,
        total_s2a_failed,
        elapsed
    ));

    Ok(DistributionReport {
        run_id,
        registered_ok: reg_result.total_registered,
        registered_failed: reg_result.total_reg_failed,
        rt_ok,
        rt_failed,
        total_s2a_ok,
        total_s2a_failed,
        team_results,
        output_files,
        elapsed_secs: elapsed,
        target_count: schedule.target_count,
    })
}

/// 分发流式 S2A 消费者：实时接收 RT 成功的账号并按加权比例路由到不同号池。
///
/// 返回 (total_ok, total_fail, team_results)
async fn dist_s2a_streaming_consumer(
    mut rx: mpsc::Receiver<AccountWithRt>,
    s2a_service: Arc<dyn crate::services::S2aService>,
    distribution: &[DistributionEntry],
    teams: &[S2aConfig],
    free_mode: bool,
    cancel_flag: Arc<AtomicBool>,
) -> (usize, usize, Vec<TeamDistResult>) {
    use tokio::sync::Semaphore;

    const MAX_RETRIES: usize = 3;
    const RETRY_DELAY_SECS: u64 = 3;

    let router = Arc::new(WeightedRouter::new(distribution));

    // 每个号池的统计
    let per_team_ok: Vec<Arc<AtomicUsize>> = distribution
        .iter()
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let per_team_fail: Vec<Arc<AtomicUsize>> = distribution
        .iter()
        .map(|_| Arc::new(AtomicUsize::new(0)))
        .collect();
    let per_team_ids: Vec<Arc<std::sync::Mutex<Vec<i64>>>> = distribution
        .iter()
        .map(|_| Arc::new(std::sync::Mutex::new(Vec::new())))
        .collect();

    let free_ok = Arc::new(AtomicUsize::new(0));
    let free_fail = Arc::new(AtomicUsize::new(0));

    let max_concurrency = teams.iter().map(|t| t.concurrency).sum::<usize>().max(1);
    let semaphore = Arc::new(Semaphore::new(max_concurrency));
    let mut join_set = tokio::task::JoinSet::new();

    broadcast_log(&format!(
        "[分发-Stream] 流式入库已启动，{} 个号池，最大并发 {}",
        distribution.len(),
        max_concurrency
    ));

    loop {
        let recv_result = tokio::select! {
            acc = rx.recv() => acc,
            _ = async {
                loop {
                    if cancel_flag.load(Ordering::Relaxed) { return; }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            } => {
                broadcast_log("[分发-Stream] 收到中断信号");
                None
            }
        };
        let Some(acc) = recv_result else {
            break;
        };

        let is_free = acc.plan_type.eq_ignore_ascii_case("free");

        if is_free && !free_mode {
            // 非 free 模式的 free 账号 → 找有 free_group_ids 的号池
            let free_eligible: Vec<_> = teams
                .iter()
                .filter(|t| !t.free_group_ids.is_empty())
                .collect();
            if free_eligible.is_empty() {
                broadcast_log(&format!(
                    "[分发-Stream] 跳过 free 账号（无号池配置 free 分组）: {}",
                    acc.account
                ));
                free_fail.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let free_total = free_ok.load(Ordering::Relaxed) + free_fail.load(Ordering::Relaxed);
            let target_team = &free_eligible[free_total % free_eligible.len()];
            let free_cfg = S2aConfig {
                group_ids: target_team.free_group_ids.clone(),
                priority: target_team.free_priority.unwrap_or(target_team.priority),
                concurrency: target_team
                    .free_concurrency
                    .unwrap_or(target_team.concurrency),
                ..(*target_team).clone()
            };

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let s2a = Arc::clone(&s2a_service);
            let fok = Arc::clone(&free_ok);
            let ffail = Arc::clone(&free_fail);
            join_set.spawn(async move {
                let _permit = permit;
                for retry in 0..MAX_RETRIES {
                    if retry > 0 {
                        tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                    }
                    if s2a.add_account(&free_cfg, &acc).await.is_ok() {
                        fok.fetch_add(1, Ordering::Relaxed);
                        broadcast_log(&format!("[分发-Stream] Free 入库成功 {}", acc.account));
                        return;
                    }
                }
                ffail.fetch_add(1, Ordering::Relaxed);
                broadcast_log(&format!("[分发-Stream] Free 入库失败 {}", acc.account));
            });
        } else {
            // Team 账号（或 free_mode 下的所有账号）→ 加权路由
            let target_team_name = router.next_team().to_string();

            let team_idx = distribution.iter().position(|d| d.team == target_team_name);
            let Some(idx) = team_idx else {
                broadcast_log(&format!(
                    "[分发-Stream] 未找到号池 {target_team_name}，跳过 {}",
                    acc.account
                ));
                continue;
            };

            let team_cfg_opt = teams.iter().find(|t| t.name == target_team_name);
            let Some(team_cfg) = team_cfg_opt else {
                broadcast_log(&format!(
                    "[分发-Stream] 号池配置不存在 {target_team_name}，跳过 {}",
                    acc.account
                ));
                per_team_fail[idx].fetch_add(1, Ordering::Relaxed);
                continue;
            };

            let push_cfg = if free_mode && !team_cfg.free_group_ids.is_empty() {
                S2aConfig {
                    group_ids: team_cfg.free_group_ids.clone(),
                    priority: team_cfg.free_priority.unwrap_or(team_cfg.priority),
                    concurrency: team_cfg.free_concurrency.unwrap_or(team_cfg.concurrency),
                    ..team_cfg.clone()
                }
            } else {
                team_cfg.clone()
            };

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let s2a = Arc::clone(&s2a_service);
            let ok_counter = Arc::clone(&per_team_ok[idx]);
            let fail_counter = Arc::clone(&per_team_fail[idx]);
            let ids = Arc::clone(&per_team_ids[idx]);
            let push_name = push_cfg.name.clone();
            join_set.spawn(async move {
                let _permit = permit;
                for retry in 0..MAX_RETRIES {
                    if retry > 0 {
                        tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                    }
                    match s2a.add_account(&push_cfg, &acc).await {
                        Ok(opt_id) => {
                            if let Some(id) = opt_id {
                                if let Ok(mut g) = ids.lock() {
                                    g.push(id);
                                }
                            }
                            ok_counter.fetch_add(1, Ordering::Relaxed);
                            broadcast_log(&format!(
                                "[分发-Stream] 入库成功 {} → {}",
                                acc.account, push_name
                            ));
                            return;
                        }
                        Err(_) => {}
                    }
                }
                fail_counter.fetch_add(1, Ordering::Relaxed);
                broadcast_log(&format!(
                    "[分发-Stream] 入库失败 {} → {} ({}轮重试)",
                    acc.account, push_name, MAX_RETRIES
                ));
            });
        }
    }

    // 等待所有入库任务完成
    while join_set.join_next().await.is_some() {}

    // 构建结果
    let mut total_ok = 0usize;
    let mut total_fail = 0usize;
    let mut team_results = Vec::new();

    for (i, d) in distribution.iter().enumerate() {
        let ok = per_team_ok[i].load(Ordering::Relaxed);
        let fail = per_team_fail[i].load(Ordering::Relaxed);
        total_ok += ok;
        total_fail += fail;
        team_results.push(TeamDistResult {
            team_name: d.team.clone(),
            percent: d.percent,
            assigned_count: ok + fail,
            s2a_ok: ok,
            s2a_failed: fail,
        });

        // Batch-refresh
        let refresh_ids: Vec<i64> = per_team_ids[i]
            .lock()
            .map(|g| g.clone())
            .unwrap_or_default();
        if !refresh_ids.is_empty() {
            if let Some(team_cfg) = teams.iter().find(|t| t.name == d.team) {
                broadcast_log(&format!(
                    "[分发-Refresh] 刷新 {} {} 个账号令牌...",
                    d.team,
                    refresh_ids.len()
                ));
                match s2a_service.batch_refresh(team_cfg, &refresh_ids).await {
                    Ok(n) => broadcast_log(&format!(
                        "[分发-Refresh] {} 刷新完成: {}/{} 成功",
                        d.team,
                        n,
                        refresh_ids.len()
                    )),
                    Err(e) => broadcast_log(&format!("[分发-Refresh] {} 刷新失败: {e}", d.team)),
                }
            }
        }
    }

    let fok = free_ok.load(Ordering::Relaxed);
    let ffail = free_fail.load(Ordering::Relaxed);
    if fok + ffail > 0 {
        total_ok += fok;
        total_fail += ffail;
        team_results.push(TeamDistResult {
            team_name: "free-auto".to_string(),
            percent: 0,
            assigned_count: fok + ffail,
            s2a_ok: fok,
            s2a_failed: ffail,
        });
    }

    let dist_counts = router.counts();
    broadcast_log(&format!(
        "[分发-Stream] 流式入库完成: ok={} fail={} 分配={:?}",
        total_ok, total_fail, dist_counts
    ));

    (total_ok, total_fail, team_results)
}

/// 校验分发配置的百分比之和是否为 100，且引用的号池都存在
pub fn validate_distribution(
    distribution: &[DistributionEntry],
    available_teams: &[crate::config::S2aConfig],
) -> Result<(), String> {
    if distribution.is_empty() {
        return Err("分发配置不能为空".to_string());
    }

    let total: u16 = distribution.iter().map(|d| d.percent as u16).sum();
    if total != 100 {
        return Err(format!("分发百分比总和必须为 100，当前为 {total}"));
    }

    for entry in distribution {
        if !available_teams.iter().any(|t| t.name == entry.team) {
            return Err(format!("号池不存在: {}", entry.team));
        }
        if entry.percent == 0 {
            return Err(format!("号池 {} 的百分比不能为 0", entry.team));
        }
    }

    Ok(())
}
