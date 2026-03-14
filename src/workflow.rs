use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{StreamExt, stream};
use tokio::sync::mpsc;

use crate::config::{AppConfig, RegisterLogMode, RegisterPerfMode, S2aConfig};
use crate::log_broadcast::broadcast_log;
use crate::models::{AccountWithRt, RegisteredAccount, WorkflowReport};
use crate::proxy_pool::ProxyPool;
use crate::services::{CodexService, RegisterInput, RegisterService, S2aService};
use crate::storage::save_json_records;
use crate::util::{
    generate_account_seed, log_worker, log_worker_green, mask_proxy, random_delay_ms,
};

/// 实时进度计数器（原子更新，前端可轮询）
#[derive(Debug)]
pub struct TaskProgress {
    pub reg_ok: AtomicUsize,
    pub reg_failed: AtomicUsize,
    pub rt_ok: AtomicUsize,
    pub rt_failed: AtomicUsize,
    pub s2a_ok: AtomicUsize,
    pub s2a_failed: AtomicUsize,
    pub stage: std::sync::Mutex<String>,
}

impl TaskProgress {
    pub fn new() -> Self {
        Self {
            reg_ok: AtomicUsize::new(0),
            reg_failed: AtomicUsize::new(0),
            rt_ok: AtomicUsize::new(0),
            rt_failed: AtomicUsize::new(0),
            s2a_ok: AtomicUsize::new(0),
            s2a_failed: AtomicUsize::new(0),
            stage: std::sync::Mutex::new("初始化".to_string()),
        }
    }

    pub fn set_stage(&self, s: &str) {
        if let Ok(mut stage) = self.stage.lock() {
            *stage = s.to_string();
        }
    }

    pub fn get_stage(&self) -> String {
        self.stage.lock().map(|s| s.clone()).unwrap_or_default()
    }
}

/// run_register_and_rt 的返回结果
pub struct RegisterRtResult {
    pub rt_success: Vec<AccountWithRt>,
    /// 供后续 S2A 分流使用的 RT 成功账号。
    /// team 模式下会补充非目标模式账号，避免 free 分组漏推送。
    pub rt_success_for_s2a: Vec<AccountWithRt>,
    pub rt_failed: Vec<RegisteredAccount>,
    pub total_registered: usize,
    pub total_reg_failed: usize,
    pub output_files: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct WorkflowOptions {
    pub target_count: usize,
    pub register_workers: usize,
    pub rt_workers: usize,
    pub rt_retry_max: usize,
    /// 兜底轮次：超过该轮次后，按当前 RT 成功数收敛
    pub target_fill_max_rounds: usize,
    pub push_s2a: bool,
    pub use_chatgpt_mail: bool,
    pub free_mode: bool,
    pub register_log_mode: RegisterLogMode,
    pub register_perf_mode: RegisterPerfMode,
}

pub struct WorkflowRunner {
    register_service: Arc<dyn RegisterService>,
    codex_service: Arc<dyn CodexService>,
    s2a_service: Arc<dyn S2aService>,
    proxy_pool: Arc<ProxyPool>,
}

impl WorkflowRunner {
    pub fn new(
        register_service: Arc<dyn RegisterService>,
        codex_service: Arc<dyn CodexService>,
        s2a_service: Arc<dyn S2aService>,
        proxy_pool: Arc<ProxyPool>,
    ) -> Self {
        Self {
            register_service,
            codex_service,
            s2a_service,
            proxy_pool,
        }
    }

    /// 注册 + RT 流水线（不包含 S2A 入库）
    pub async fn run_register_and_rt(
        &self,
        cfg: &AppConfig,
        options: &WorkflowOptions,
        cancel_flag: Arc<AtomicBool>,
        progress: Option<&Arc<TaskProgress>>,
    ) -> Result<RegisterRtResult> {
        let target = options.target_count;
        let reg_concurrency = options.register_workers.max(1);
        let rt_concurrency = options.rt_workers.max(1);
        let max_rounds = options.target_fill_max_rounds.max(1);

        let mut all_rt_success: Vec<AccountWithRt> = Vec::new();
        let mut all_rt_failed: Vec<RegisteredAccount> = Vec::new();
        let mut total_registered = 0usize;
        let mut total_reg_failed = 0usize;
        let mut total_task_index = 0usize;

        for round in 0..max_rounds {
            if cancel_flag.load(Ordering::Relaxed) {
                broadcast_log("已收到中断信号，跳过后续注册轮次...");
                break;
            }

            let current_ok = all_rt_success
                .iter()
                .filter(|acc| Self::is_target_mode_account(acc, options.free_mode))
                .count();
            if current_ok >= target {
                break;
            }
            let deficit = target - current_ok;
            let (reg_jitter_min_ms, reg_jitter_max_ms, reg_queue_cap) =
                match options.register_perf_mode {
                    RegisterPerfMode::Baseline => {
                        let jitter_max = (1200u64 / reg_concurrency as u64).clamp(80, 400);
                        let jitter_min = (jitter_max / 4).max(15);
                        let queue_cap = deficit.max(reg_concurrency * 4);
                        (jitter_min, jitter_max, queue_cap)
                    }
                    RegisterPerfMode::Adaptive => {
                        let jitter_max = (600u64 / reg_concurrency as u64).clamp(40, 180);
                        let jitter_min = (jitter_max / 6).max(8);
                        let queue_cap = deficit.max(reg_concurrency * 2).min(reg_concurrency * 8);
                        (jitter_min, jitter_max, queue_cap)
                    }
                };

            if round > 0 {
                broadcast_log(&format!(
                    "=== 补注册第 {} 轮: 还需 {} 个账号 (已成功 {}/{}) ===",
                    round, deficit, current_ok, target
                ));
            } else {
                let plan_label = if options.free_mode { "free" } else { "team" };
                broadcast_log(&format!(
                    "阶段1+2: 注册 {plan_label} 账号 → RT (流水线模式, 目标{plan_label} RT成功 {target})"
                ));
            }
            let round_no = round + 1;
            if matches!(options.register_log_mode, RegisterLogMode::Summary) {
                let plan_label = if options.free_mode { "free" } else { "team" };
                broadcast_log(&format!(
                    "[REG] round={round_no}/{max_rounds} mode={plan_label} target_rt={target} need={deficit} reg_conc={reg_concurrency} rt_conc={rt_concurrency}"
                ));
            }

            // 根据邮箱系统选择生成 seeds
            let seeds = if options.use_chatgpt_mail {
                let email_service = Arc::clone(&self.register_service);
                let proxy_pool = Arc::clone(&self.proxy_pool);
                let email_domains = Arc::new(cfg.email_domains.clone());
                let seed_mail_concurrency = cfg
                    .register
                    .mail_max_concurrency
                    .unwrap_or(50)
                    .max(1)
                    .min(deficit.max(1));
                stream::iter(0..deficit)
                    .map(move |_| {
                        let email_service = Arc::clone(&email_service);
                        let proxy_pool = Arc::clone(&proxy_pool);
                        let email_domains = Arc::clone(&email_domains);
                        async move {
                            let proxy = proxy_pool.next();
                            let mut seed = generate_account_seed(email_domains.as_ref());
                            match email_service.generate_email(proxy.as_deref()).await {
                                Ok(Some(email)) => seed.account = email,
                                Ok(None) => {}
                                Err(e) => {
                                    broadcast_log(&format!(
                                        "[chatgpt.org.uk] 生成邮箱失败: {e}，使用域名列表邮箱"
                                    ));
                                }
                            }
                            seed
                        }
                    })
                    .buffered(seed_mail_concurrency)
                    .collect::<Vec<_>>()
                    .await
            } else {
                (0..deficit)
                    .map(|_| generate_account_seed(&cfg.email_domains))
                    .collect::<Vec<_>>()
            };

            let (reg_tx, rt_rx) = mpsc::channel(reg_queue_cap);

            // 注册生产者
            let register_handle = {
                let register_service = Arc::clone(&self.register_service);
                let proxy_pool = Arc::clone(&self.proxy_pool);
                let seeds = seeds.clone();
                let base_idx = total_task_index;
                let cancel = cancel_flag.clone();
                let prog = progress.cloned();
                let free_mode = options.free_mode;
                let log_mode = options.register_log_mode;
                let round_no = round + 1;
                tokio::spawn(async move {
                    let round_total = seeds.len();
                    let done_counter = Arc::new(AtomicUsize::new(0));
                    let ok_counter = Arc::new(AtomicUsize::new(0));
                    let fail_counter = Arc::new(AtomicUsize::new(0));
                    let summary_stop = Arc::new(AtomicBool::new(false));
                    let summary_handle = if matches!(log_mode, RegisterLogMode::Summary) {
                        let done_counter = Arc::clone(&done_counter);
                        let ok_counter = Arc::clone(&ok_counter);
                        let fail_counter = Arc::clone(&fail_counter);
                        let summary_stop = Arc::clone(&summary_stop);
                        Some(tokio::spawn(async move {
                            let started = Instant::now();
                            let mut ticker = tokio::time::interval(Duration::from_secs(2));
                            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                            loop {
                                ticker.tick().await;
                                let done = done_counter.load(Ordering::Relaxed);
                                let ok = ok_counter.load(Ordering::Relaxed);
                                let fail = fail_counter.load(Ordering::Relaxed);
                                let inflight = round_total.saturating_sub(done);
                                let elapsed = started.elapsed().as_secs_f64().max(0.001);
                                let rate = done as f64 / elapsed;
                                if done > 0 {
                                    broadcast_log(&format!(
                                        "[REG-SUM] round={round_no} done={done}/{round_total} ok={ok} fail={fail} inflight={inflight} rate={rate:.1}/s"
                                    ));
                                }
                                if summary_stop.load(Ordering::Relaxed) || done >= round_total {
                                    break;
                                }
                            }
                        }))
                    } else {
                        None
                    };
                    let mut result_stream = stream::iter(seeds.into_iter().enumerate())
                        .map(|(idx, seed)| {
                            let register_service = Arc::clone(&register_service);
                            let proxy = proxy_pool.next();
                            let worker_id = idx % reg_concurrency + 1;
                            let reg_tx = reg_tx.clone();
                            let task_no = base_idx + idx + 1;
                            let cancel = cancel.clone();
                            let prog = prog.clone();
                            let done_counter = Arc::clone(&done_counter);
                            let ok_counter = Arc::clone(&ok_counter);
                            let fail_counter = Arc::clone(&fail_counter);
                            async move {
                                if cancel.load(Ordering::Relaxed) {
                                    fail_counter.fetch_add(1, Ordering::Relaxed);
                                    done_counter.fetch_add(1, Ordering::Relaxed);
                                    return (false, task_no, seed.account);
                                }
                                let jitter_ms =
                                    random_delay_ms(reg_jitter_min_ms, reg_jitter_max_ms);
                                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                                if cancel.load(Ordering::Relaxed) {
                                    fail_counter.fetch_add(1, Ordering::Relaxed);
                                    done_counter.fetch_add(1, Ordering::Relaxed);
                                    return (false, task_no, seed.account);
                                }
                                if matches!(log_mode, RegisterLogMode::Verbose) {
                                    log_worker(
                                        worker_id,
                                        "任务",
                                        &format!("#{task_no} (进度: {task_no}/{target})"),
                                    );
                                    log_worker(
                                        worker_id,
                                        "账号",
                                        &format!("{} | {}", seed.account, seed.password),
                                    );
                                    if let Some(p) = proxy.as_deref() {
                                        log_worker(worker_id, "代理", &mask_proxy(p));
                                    }
                                }
                                let input = RegisterInput {
                                    seed: seed.clone(),
                                    proxy: proxy.clone(),
                                    worker_id,
                                    task_index: task_no,
                                    task_total: target,
                                    skip_payment: free_mode,
                                };
                                match register_service.register(input).await {
                                    Ok(acc) => match reg_tx.send(acc).await {
                                        Ok(_) => {
                                            if let Some(ref p) = prog {
                                                p.reg_ok.fetch_add(1, Ordering::Relaxed);
                                            }
                                            ok_counter.fetch_add(1, Ordering::Relaxed);
                                            done_counter.fetch_add(1, Ordering::Relaxed);
                                            (true, task_no, seed.account)
                                        }
                                        Err(send_err) => {
                                            if let Some(ref p) = prog {
                                                p.reg_failed.fetch_add(1, Ordering::Relaxed);
                                            }
                                            fail_counter.fetch_add(1, Ordering::Relaxed);
                                            done_counter.fetch_add(1, Ordering::Relaxed);
                                            log_worker(
                                                worker_id,
                                                "ERR",
                                                &format!(
                                                    "注册成功但入RT队列失败 #{} {}: {send_err}",
                                                    task_no, seed.account
                                                ),
                                            );
                                            (false, task_no, seed.account)
                                        }
                                    },
                                    Err(err) => {
                                        if let Some(ref p) = prog {
                                            p.reg_failed.fetch_add(1, Ordering::Relaxed);
                                        }
                                        fail_counter.fetch_add(1, Ordering::Relaxed);
                                        done_counter.fetch_add(1, Ordering::Relaxed);
                                        log_worker(
                                            worker_id,
                                            "ERR",
                                            &format!(
                                                "注册失败 #{} {}: {err}",
                                                task_no, seed.account
                                            ),
                                        );
                                        (false, task_no, seed.account)
                                    }
                                }
                            }
                        })
                        .buffer_unordered(reg_concurrency);

                    while result_stream.next().await.is_some() {}
                    drop(result_stream);
                    summary_stop.store(true, Ordering::Relaxed);
                    if let Some(handle) = summary_handle {
                        let _ = handle.await;
                    }
                    let reg_ok_count = ok_counter.load(Ordering::Relaxed);
                    let reg_failed_count = fail_counter.load(Ordering::Relaxed);
                    drop(reg_tx);
                    (reg_ok_count, reg_failed_count)
                })
            };

            // RT 消费者
            let rt_handle = {
                let codex_service = Arc::clone(&self.codex_service);
                let proxy_pool = Arc::clone(&self.proxy_pool);
                let rt_retry_max = options.rt_retry_max;
                let cancel = cancel_flag.clone();
                let prog = progress.cloned();
                tokio::spawn(async move {
                    Self::rt_pipeline_consumer(
                        rt_rx,
                        codex_service,
                        proxy_pool,
                        rt_concurrency,
                        rt_retry_max,
                        cancel,
                        prog,
                    )
                    .await
                })
            };

            let (reg_ok_in_round, reg_failed_in_round) = register_handle.await.unwrap_or((0, 0));
            let (round_rt_ok, round_rt_failed) = rt_handle.await.unwrap_or_default();

            total_registered += reg_ok_in_round;
            total_reg_failed += reg_failed_in_round;
            total_task_index += deficit;
            let round_rt_mode_ok = round_rt_ok
                .iter()
                .filter(|acc| Self::is_target_mode_account(acc, options.free_mode))
                .count();
            all_rt_success.extend(round_rt_ok);
            all_rt_failed.extend(round_rt_failed);
            let cumulative_mode_ok = current_ok + round_rt_mode_ok;

            if matches!(options.register_log_mode, RegisterLogMode::Summary) {
                broadcast_log(&format!(
                    "[REG-END] round={} reg_ok={} reg_fail={} rt_ok={} cumulative_ok={}/{}",
                    round + 1,
                    reg_ok_in_round,
                    reg_failed_in_round,
                    round_rt_mode_ok,
                    cumulative_mode_ok,
                    target
                ));
            } else {
                let plan_label = if options.free_mode { "free" } else { "team" };
                broadcast_log(&format!(
                    "本轮结束: {plan_label} RT成功 {}, 累计成功 {}/{}",
                    round_rt_mode_ok, cumulative_mode_ok, target
                ));
            }

            if cumulative_mode_ok >= target {
                break;
            }
        }

        let plan_label = if options.free_mode { "free" } else { "team" };
        let (target_rt_success, rt_success_for_s2a, dropped_non_target) =
            Self::build_rt_success_views(all_rt_success, options.free_mode, target);
        if target_rt_success.len() < target && !cancel_flag.load(Ordering::Relaxed) {
            broadcast_log(&format!(
                "[REG-END] 达到重试上限 {}，{} 模式 RT 成功 {}/{}，按当前数量继续后续流程",
                max_rounds,
                plan_label,
                target_rt_success.len(),
                target
            ));
        }
        if dropped_non_target > 0 {
            broadcast_log(&format!(
                "[REG] 已忽略 {} 个非{}模式 RT 结果，仅按 {} 模式计入目标",
                dropped_non_target, plan_label, plan_label
            ));
        }

        // 输出文件
        let mut output_files = Vec::new();
        if let Some(path) = save_json_records(
            &format!("accounts-{plan_label}-with-rt"),
            &target_rt_success,
        )? {
            output_files.push(path.display().to_string());
        }
        if let Some(path) =
            save_json_records(&format!("accounts-{plan_label}-no-rt"), &all_rt_failed)?
        {
            output_files.push(path.display().to_string());
        }

        Ok(RegisterRtResult {
            rt_success: target_rt_success,
            rt_success_for_s2a,
            rt_failed: all_rt_failed,
            total_registered,
            total_reg_failed,
            output_files,
        })
    }

    /// 将账号推送到单个 S2A 号池（含重试），返回 (s2a_ok, s2a_failed)
    pub async fn push_to_s2a(
        &self,
        team: &S2aConfig,
        accounts: Vec<AccountWithRt>,
        progress: Option<&Arc<TaskProgress>>,
    ) -> (usize, usize) {
        const S2A_MAX_RETRIES: usize = 3;
        const S2A_RETRY_DELAY_SECS: u64 = 5;

        if accounts.is_empty() {
            return (0, 0);
        }

        let s2a_concurrency = team.concurrency.max(1).min(accounts.len().max(1));
        let mut s2a_ok = 0usize;
        let mut created_ids: Vec<i64> = Vec::new();
        let mut pending = accounts;
        let mut final_failed: Vec<AccountWithRt> = Vec::new();

        for retry_round in 0..S2A_MAX_RETRIES {
            if pending.is_empty() {
                break;
            }

            if retry_round > 0 {
                broadcast_log(&format!(
                    "[S2A重试] 第 {}/{} 轮，等待 {}s 后重试 {} 个失败账号...",
                    retry_round + 1,
                    S2A_MAX_RETRIES,
                    S2A_RETRY_DELAY_SECS,
                    pending.len()
                ));
                tokio::time::sleep(Duration::from_secs(S2A_RETRY_DELAY_SECS)).await;
            }

            let s2a_results = stream::iter(pending)
                .map(|acc| {
                    let s2a_service = Arc::clone(&self.s2a_service);
                    let team = team.clone();
                    async move {
                        let ret = s2a_service.add_account(&team, &acc).await;
                        (acc, ret)
                    }
                })
                .buffer_unordered(s2a_concurrency)
                .collect::<Vec<_>>()
                .await;

            let mut round_failed = Vec::new();
            for (acc, ret) in s2a_results {
                match ret {
                    Ok(opt_id) => {
                        s2a_ok += 1;
                        if let Some(id) = opt_id {
                            created_ids.push(id);
                        }
                        if let Some(p) = progress {
                            p.s2a_ok.fetch_add(1, Ordering::Relaxed);
                        }
                        broadcast_log(&format!("[S2A成功] {}", acc.account));
                    }
                    Err(err) => {
                        if let Some(p) = progress {
                            p.s2a_failed.fetch_add(1, Ordering::Relaxed);
                        }
                        broadcast_log(&format!("[S2A失败] {}: {err}", acc.account));
                        round_failed.push(acc);
                    }
                }
            }

            if retry_round + 1 >= S2A_MAX_RETRIES || round_failed.is_empty() {
                final_failed = round_failed;
                break;
            }

            pending = round_failed;
        }

        let s2a_failed = final_failed.len();
        if !final_failed.is_empty() {
            broadcast_log(&format!(
                "[S2A] 经过最多 {} 轮重试后仍有 {} 个账号失败",
                S2A_MAX_RETRIES,
                final_failed.len()
            ));
        }
        if let Ok(Some(path)) = save_json_records("accounts-s2a-failed", &final_failed) {
            broadcast_log(&format!("[S2A] 失败记录已保存: {}", path.display()));
        }

        // Batch-refresh tokens for all successfully created accounts
        if !created_ids.is_empty() {
            broadcast_log(&format!(
                "[S2A-Refresh] 正在刷新 {} 个新入库账号的令牌...",
                created_ids.len()
            ));
            match self.s2a_service.batch_refresh(team, &created_ids).await {
                Ok(n) => {
                    broadcast_log(&format!(
                        "[S2A-Refresh] 令牌刷新完成: {}/{} 成功",
                        n,
                        created_ids.len()
                    ));
                }
                Err(err) => {
                    broadcast_log(&format!(
                        "[S2A-Refresh] 令牌刷新失败（不影响入库结果）: {err}"
                    ));
                }
            }
        }

        (s2a_ok, s2a_failed)
    }

    /// 原有入口方法：注册 + RT + 单号池 S2A 入库
    pub async fn run_one_team(
        &self,
        cfg: &AppConfig,
        team: &S2aConfig,
        options: &WorkflowOptions,
        cancel_flag: Arc<AtomicBool>,
        progress: Option<Arc<TaskProgress>>,
    ) -> Result<WorkflowReport> {
        let workflow_started = Instant::now();
        let mode_label = if options.free_mode { "free" } else { "team" };

        // D1 清理（任务前）
        if cfg.d1_cleanup.enabled.unwrap_or(false)
            && cfg.d1_cleanup.cleanup_timing.unwrap_or_default()
                == crate::config::D1CleanupTiming::BeforeTask
        {
            broadcast_log("阶段0: D1 邮件清理（任务前）");
            if let Err(e) = crate::d1_cleanup::run_cleanup(&cfg.d1_cleanup).await {
                broadcast_log(&format!("D1 清理失败（不影响任务执行）: {e}"));
            }
        }

        if let Some(ref p) = progress {
            p.set_stage("注册 + RT");
        }
        let reg_result = match self
            .run_register_and_rt(cfg, options, cancel_flag, progress.as_ref())
            .await
        {
            Ok(r) => r,
            Err(e) => {
                broadcast_log(&format!(
                    "[SIG-END][ERR] mode={mode_label} stage=register_rt target_rt={} reason={e:#}",
                    options.target_count
                ));
                return Err(e);
            }
        };

        let rt_ok = reg_result.rt_success.len();
        let rt_failed = reg_result.rt_failed.len();
        let mut output_files = reg_result.output_files;

        let (s2a_ok, s2a_failed, free_s2a_ok, free_s2a_failed) = if options.push_s2a {
            if let Some(ref p) = progress {
                p.set_stage("S2A 入库");
            }
            broadcast_log(&format!("阶段3: 入库 S2A [{}]", team.name));

            // 分离 free 账号
            let (s2a_eligible, free_accounts): (Vec<AccountWithRt>, Vec<AccountWithRt>) =
                reg_result
                    .rt_success_for_s2a
                    .into_iter()
                    .partition(|acc| !acc.plan_type.eq_ignore_ascii_case("free"));

            // 推送 team 账号
            let (team_ok, team_fail) = if !s2a_eligible.is_empty() {
                broadcast_log(&format!(
                    "[S2A] 准备入库 {} 个 team 账号",
                    s2a_eligible.len()
                ));
                self.push_to_s2a(team, s2a_eligible, progress.as_ref())
                    .await
            } else {
                (0, 0)
            };

            // 推送 free 账号到 free 分组（如已配置）
            let (free_ok, free_fail) = if !free_accounts.is_empty() {
                if team.free_group_ids.is_empty() {
                    broadcast_log(&format!(
                        "[S2A] 跳过 {} 个 free 账号（未配置 free 分组）",
                        free_accounts.len()
                    ));
                    if let Some(path) = save_json_records("accounts-free-skipped", &free_accounts)?
                    {
                        output_files.push(path.display().to_string());
                    }
                    (0, 0)
                } else {
                    broadcast_log(&format!(
                        "[S2A] 推送 {} 个 free 账号到 free 分组 {:?}",
                        free_accounts.len(),
                        team.free_group_ids
                    ));
                    let free_team = S2aConfig {
                        group_ids: team.free_group_ids.clone(),
                        priority: team.free_priority.unwrap_or(team.priority),
                        concurrency: team.free_concurrency.unwrap_or(team.concurrency),
                        ..team.clone()
                    };
                    self.push_to_s2a(&free_team, free_accounts, progress.as_ref())
                        .await
                }
            } else {
                (0, 0)
            };

            // s2a_ok/s2a_failed 包含总数（向后兼容）
            (team_ok + free_ok, team_fail + free_fail, free_ok, free_fail)
        } else {
            (0, 0, 0, 0)
        };

        // D1 清理（任务后）
        if cfg.d1_cleanup.enabled.unwrap_or(false)
            && cfg.d1_cleanup.cleanup_timing.unwrap_or_default()
                != crate::config::D1CleanupTiming::BeforeTask
        {
            broadcast_log("阶段4: D1 邮件清理");
            if let Err(e) = crate::d1_cleanup::run_cleanup(&cfg.d1_cleanup).await {
                broadcast_log(&format!("D1 清理失败（不影响入库结果）: {e}"));
            }
        }

        let elapsed_secs = workflow_started.elapsed().as_secs_f32();
        broadcast_log(&format!(
            "[SIG-END][OK] mode={mode_label} target_rt={} reg_ok={} reg_fail={} rt_ok={} rt_fail={} s2a_ok={} s2a_fail={} free_ok={} free_fail={} elapsed={elapsed_secs:.1}s",
            options.target_count,
            reg_result.total_registered,
            reg_result.total_reg_failed,
            rt_ok,
            rt_failed,
            s2a_ok,
            s2a_failed,
            free_s2a_ok,
            free_s2a_failed,
        ));

        Ok(WorkflowReport {
            registered_ok: reg_result.total_registered,
            registered_failed: reg_result.total_reg_failed,
            rt_ok,
            rt_failed,
            s2a_ok,
            s2a_failed,
            free_s2a_ok,
            free_s2a_failed,
            output_files,
            elapsed_secs,
            target_count: options.target_count,
        })
    }

    /// RT 流水线消费者：从 channel 接收注册成功的账号，并发获取 RT
    async fn rt_pipeline_consumer(
        mut rx: mpsc::Receiver<crate::models::RegisteredAccount>,
        codex_service: Arc<dyn CodexService>,
        proxy_pool: Arc<ProxyPool>,
        rt_concurrency: usize,
        rt_retry_max: usize,
        cancel_flag: Arc<AtomicBool>,
        progress: Option<Arc<TaskProgress>>,
    ) -> (Vec<AccountWithRt>, Vec<crate::models::RegisteredAccount>) {
        use tokio::sync::Semaphore;

        let semaphore = Arc::new(Semaphore::new(rt_concurrency));
        let mut join_set = tokio::task::JoinSet::new();
        let mut worker_counter = 0usize;

        // 持续从 channel 接收注册成功的账号
        loop {
            let recv_result = tokio::select! {
                acc = rx.recv() => acc,
                _ = Self::wait_for_cancel(cancel_flag.clone()) => {
                    broadcast_log("已收到中断信号，停止接收新的 RT 任务...");
                    None
                }
            };
            let Some(acc) = recv_result else {
                break;
            };

            worker_counter += 1;
            let worker_id = (worker_counter - 1) % rt_concurrency + 1;
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let codex_service = Arc::clone(&codex_service);
            let proxy = proxy_pool.next();
            let retry_max = rt_retry_max;
            let cancel = cancel_flag.clone();
            let prog = progress.clone();

            join_set.spawn(async move {
                let _permit = permit; // 持有信号量直到任务完成
                let started = Instant::now();
                // 高并发下开始日志会非常密集，跳过逐账号起始日志以降低广播压力

                let mut last_err = None;
                let retries = retry_max.max(1);
                for round in 0..retries {
                    if cancel.load(Ordering::Relaxed) {
                        log_worker(worker_id, "RT", &format!("RT中断 {}", acc.account));
                        return Err(acc);
                    }
                    let fetch_result = tokio::select! {
                        res = codex_service.fetch_refresh_token(&acc, proxy.clone(), worker_id) => res,
                        _ = Self::wait_for_cancel(cancel.clone()) => {
                            log_worker(worker_id, "RT", &format!("RT中断 {}", acc.account));
                            return Err(acc);
                        }
                    };
                    match fetch_result {
                        Ok(rt) => {
                            if let Some(ref p) = prog { p.rt_ok.fetch_add(1, Ordering::Relaxed); }
                            let elapsed_sec = started.elapsed().as_secs_f32();
                            log_worker_green(
                                worker_id,
                                "OK",
                                &format!("RT获取成功 (耗时 {:.1}s)", elapsed_sec),
                            );
                            return Ok(AccountWithRt {
                                account: acc.account,
                                password: acc.password,
                                token: acc.token,
                                account_id: acc.account_id,
                                plan_type: acc.plan_type,
                                refresh_token: rt,
                            });
                        }
                        Err(err) => {
                            if round + 1 < retries {
                                let exp = (round as u32).min(5);
                                let base_delay_ms = 200u64.saturating_mul(1u64 << exp);
                                let max_delay_ms = base_delay_ms.saturating_mul(2);
                                let backoff_ms = random_delay_ms(base_delay_ms, max_delay_ms);
                                log_worker(
                                    worker_id,
                                    "RT",
                                    &format!(
                                        "RT重试 {} ({}/{}): {err}，退避 {}ms",
                                        acc.account,
                                        round + 1,
                                        retries,
                                        backoff_ms
                                    ),
                                );
                                tokio::select! {
                                    _ = tokio::time::sleep(Duration::from_millis(backoff_ms)) => {}
                                    _ = Self::wait_for_cancel(cancel.clone()) => {
                                        log_worker(worker_id, "RT", &format!("RT中断 {}", acc.account));
                                        return Err(acc);
                                    }
                                }
                            }
                            last_err = Some(err);
                        }
                    }
                }
                if let Some(ref p) = prog { p.rt_failed.fetch_add(1, Ordering::Relaxed); }
                let elapsed_sec = started.elapsed().as_secs_f32();
                log_worker(
                    worker_id,
                    "ERR",
                    &format!(
                        "RT失败 {} ({:.1}s): {}",
                        acc.account,
                        elapsed_sec,
                        last_err
                            .as_ref()
                            .map(|e| format!("{e}"))
                            .unwrap_or_default()
                    ),
                );
                Err(acc)
            });
        }

        // 等待所有 RT 任务完成
        let mut rt_success = Vec::new();
        let mut rt_failed = Vec::new();
        while let Some(join_result) = join_set.join_next().await {
            match join_result {
                Ok(Ok(acc_with_rt)) => rt_success.push(acc_with_rt),
                Ok(Err(acc)) => rt_failed.push(acc),
                Err(_) => {} // JoinError，任务 panicked
            }
        }
        (rt_success, rt_failed)
    }

    fn is_target_mode_account(acc: &AccountWithRt, free_mode: bool) -> bool {
        if free_mode {
            acc.plan_type.eq_ignore_ascii_case("free")
        } else {
            !acc.plan_type.eq_ignore_ascii_case("free")
        }
    }

    fn build_rt_success_views(
        all_rt_success: Vec<AccountWithRt>,
        free_mode: bool,
        target: usize,
    ) -> (Vec<AccountWithRt>, Vec<AccountWithRt>, usize) {
        let (mut target_rt_success, non_target_rt_success): (
            Vec<AccountWithRt>,
            Vec<AccountWithRt>,
        ) = all_rt_success
            .into_iter()
            .partition(|acc| Self::is_target_mode_account(acc, free_mode));
        target_rt_success.truncate(target);
        let dropped_non_target = non_target_rt_success.len();

        let mut rt_success_for_s2a = target_rt_success.clone();
        if !free_mode {
            rt_success_for_s2a.extend(non_target_rt_success);
        }

        (target_rt_success, rt_success_for_s2a, dropped_non_target)
    }

    async fn wait_for_cancel(cancel_flag: Arc<AtomicBool>) {
        while !cancel_flag.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_rt(account: &str, plan_type: &str) -> AccountWithRt {
        AccountWithRt {
            account: account.to_string(),
            password: "pw".to_string(),
            token: format!("tok-{account}"),
            account_id: format!("acc-{account}"),
            plan_type: plan_type.to_string(),
            refresh_token: format!("rt-{account}"),
        }
    }

    #[test]
    fn team_mode_keeps_non_team_results_for_free_distribution() {
        let all_rt = vec![
            make_rt("u1", "team"),
            make_rt("u2", "free"),
            make_rt("u3", "pro"),
            make_rt("u4", "free"),
            make_rt("u5", "team"),
        ];

        let (target_rt_success, rt_success_for_s2a, dropped_non_target) =
            WorkflowRunner::build_rt_success_views(all_rt, false, 2);

        assert_eq!(target_rt_success.len(), 2);
        assert_eq!(target_rt_success[0].account, "u1");
        assert_eq!(target_rt_success[1].account, "u3");
        assert_eq!(dropped_non_target, 2);
        assert_eq!(rt_success_for_s2a.len(), 4);
        assert_eq!(
            rt_success_for_s2a
                .iter()
                .filter(|acc| acc.plan_type.eq_ignore_ascii_case("free"))
                .count(),
            2
        );
    }

    #[test]
    fn free_mode_only_keeps_free_results_for_s2a() {
        let all_rt = vec![
            make_rt("u1", "team"),
            make_rt("u2", "free"),
            make_rt("u3", "pro"),
            make_rt("u4", "free"),
        ];

        let (target_rt_success, rt_success_for_s2a, dropped_non_target) =
            WorkflowRunner::build_rt_success_views(all_rt, true, 1);

        assert_eq!(target_rt_success.len(), 1);
        assert_eq!(target_rt_success[0].account, "u2");
        assert_eq!(dropped_non_target, 2);
        assert_eq!(rt_success_for_s2a.len(), 1);
        assert!(rt_success_for_s2a[0].plan_type.eq_ignore_ascii_case("free"));
    }
}
