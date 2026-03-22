use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{StreamExt, stream};
use tokio::sync::mpsc;

use crate::config::{AppConfig, RegisterLogMode, RegisterPerfMode, S2aConfig, TokensPoolConfig};
use crate::log_broadcast::broadcast_log;
use crate::models::{AccountWithRt, RegisteredAccount, WorkflowReport};
use crate::proxy_pool::ProxyPool;
use crate::services::{CodexService, RegisterInput, RegisterService, S2aService, TokensPoolService};
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
    #[allow(dead_code)]
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
    pub mail_provider: crate::config::MailProvider,
    pub free_mode: bool,
    pub register_log_mode: RegisterLogMode,
    pub register_perf_mode: RegisterPerfMode,
    /// Tokens 号池配置（与 S2A 互斥）
    pub tokens_pool: Option<TokensPoolConfig>,
}

pub struct WorkflowRunner {
    register_service: Arc<dyn RegisterService>,
    codex_service: Arc<dyn CodexService>,
    s2a_service: Arc<dyn S2aService>,
    tokens_pool_service: Arc<dyn TokensPoolService>,
    proxy_pool: Arc<ProxyPool>,
}

impl WorkflowRunner {
    pub fn new(
        register_service: Arc<dyn RegisterService>,
        codex_service: Arc<dyn CodexService>,
        s2a_service: Arc<dyn S2aService>,
        tokens_pool_service: Arc<dyn TokensPoolService>,
        proxy_pool: Arc<ProxyPool>,
    ) -> Self {
        Self {
            register_service,
            codex_service,
            s2a_service,
            tokens_pool_service,
            proxy_pool,
        }
    }

    /// 获取 S2A 服务实例（供分发流式消费者使用）
    pub fn s2a_service(&self) -> Arc<dyn S2aService> {
        Arc::clone(&self.s2a_service)
    }

    /// 获取 Tokens Pool 服务实例
    pub fn tokens_pool_service(&self) -> Arc<dyn TokensPoolService> {
        Arc::clone(&self.tokens_pool_service)
    }

    /// 注册 + RT 流水线（不包含 S2A 入库）
    pub async fn run_register_and_rt(
        &self,
        cfg: &AppConfig,
        options: &WorkflowOptions,
        cancel_flag: Arc<AtomicBool>,
        progress: Option<&Arc<TaskProgress>>,
        s2a_tx: Option<mpsc::Sender<AccountWithRt>>,
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
                    RegisterPerfMode::Turbo => {
                        let jitter_max = (300u64 / reg_concurrency as u64).clamp(20, 100);
                        let jitter_min = (jitter_max / 8).max(5);
                        let queue_cap = deficit.max(reg_concurrency * 2).min(reg_concurrency * 10);
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
            use crate::config::MailProvider;
            let seeds = match options.mail_provider {
                MailProvider::Chatgpt
                | MailProvider::Duckmail
                | MailProvider::Tempmail
                | MailProvider::TempmailLol => {
                    let email_service = Arc::clone(&self.register_service);
                    let proxy_pool = Arc::clone(&self.proxy_pool);
                    let email_domains = Arc::new(cfg.email_domains.clone());
                    let seed_mail_concurrency = cfg
                        .register
                        .mail_max_concurrency
                        .unwrap_or(50)
                        .max(1)
                        .min(deficit.max(1));
                    // TempmailLol 必须由 provider 创建邮箱才能收信，
                    // generate_email 失败时回退到域名列表邮箱是无效的。
                    let requires_provider_email = matches!(
                        options.mail_provider,
                        MailProvider::TempmailLol
                    );
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
                                    Ok(None) => {
                                        if requires_provider_email {
                                            return None;
                                        }
                                    }
                                    Err(e) => {
                                        if requires_provider_email {
                                            broadcast_log(&format!(
                                                "[mail-provider] 创建邮箱失败（跳过）: {e}"
                                            ));
                                            return None;
                                        }
                                        broadcast_log(&format!(
                                            "[mail-provider] 生成邮箱失败: {e}，使用域名列表邮箱"
                                        ));
                                    }
                                }
                                Some(seed)
                            }
                        })
                        .buffered(seed_mail_concurrency)
                        .filter_map(|x| async { x })
                        .collect::<Vec<_>>()
                        .await
                }
                MailProvider::Kyx => (0..deficit)
                    .map(|_| generate_account_seed(&cfg.email_domains))
                    .collect::<Vec<_>>(),
            };

            // 如果邮箱创建失败导致实际 seed 数少于目标，输出告警
            if seeds.len() < deficit {
                broadcast_log(&format!(
                    "[mail-provider] 邮箱创建成功 {}/{deficit}（{} 个因 API 失败被跳过）",
                    seeds.len(),
                    deficit - seeds.len()
                ));
            }

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
                let s2a_tx = s2a_tx.clone();
                tokio::spawn(async move {
                    Self::rt_pipeline_consumer(
                        rt_rx,
                        codex_service,
                        proxy_pool,
                        rt_concurrency,
                        rt_retry_max,
                        cancel,
                        prog,
                        s2a_tx,
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
    #[allow(dead_code)]
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

    /// S2A 流式消费者：实时接收 RT 成功的账号并入库，与注册+RT 并行运行。
    ///
    /// 返回 (team_ok + free_ok, team_fail + free_fail, free_ok, free_fail)
    async fn s2a_streaming_consumer(
        mut rx: mpsc::Receiver<AccountWithRt>,
        s2a_service: Arc<dyn crate::services::S2aService>,
        team: &S2aConfig,
        progress: Option<&Arc<TaskProgress>>,
        cancel_flag: Arc<AtomicBool>,
    ) -> (usize, usize, usize, usize) {
        use tokio::sync::Semaphore;

        const S2A_STREAM_MAX_RETRIES: usize = 3;
        const S2A_STREAM_RETRY_DELAY_SECS: u64 = 3;

        let s2a_concurrency = team.concurrency.max(1);
        let semaphore = Arc::new(Semaphore::new(s2a_concurrency));
        let mut join_set = tokio::task::JoinSet::new();

        let team_ok = Arc::new(AtomicUsize::new(0));
        let team_fail = Arc::new(AtomicUsize::new(0));
        let free_ok = Arc::new(AtomicUsize::new(0));
        let free_fail = Arc::new(AtomicUsize::new(0));
        let created_ids = Arc::new(std::sync::Mutex::new(Vec::<i64>::new()));

        // 构建 free 配置
        let free_team = if !team.free_group_ids.is_empty() {
            Some(S2aConfig {
                group_ids: team.free_group_ids.clone(),
                priority: team.free_priority.unwrap_or(team.priority),
                concurrency: team.free_concurrency.unwrap_or(team.concurrency),
                ..team.clone()
            })
        } else {
            None
        };

        broadcast_log(&format!("[S2A-Stream] 流式入库已启动 [{}]", team.name));

        // 从 channel 接收 RT 成功的账号
        loop {
            let recv_result = tokio::select! {
                acc = rx.recv() => acc,
                _ = Self::wait_for_cancel(cancel_flag.clone()) => {
                    broadcast_log("[S2A-Stream] 收到中断信号，停止接收新的入库任务");
                    None
                }
            };
            let Some(acc) = recv_result else {
                break;
            };

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let s2a_service = Arc::clone(&s2a_service);
            let is_free = acc.plan_type.eq_ignore_ascii_case("free");
            let target_cfg = if is_free {
                match &free_team {
                    Some(ft) => ft.clone(),
                    None => {
                        broadcast_log(&format!(
                            "[S2A-Stream] 跳过 free 账号（未配置 free 分组）: {}",
                            acc.account
                        ));
                        if let Some(p) = progress {
                            p.s2a_failed.fetch_add(1, Ordering::Relaxed);
                        }
                        free_fail.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                }
            } else {
                team.clone()
            };

            let team_ok = Arc::clone(&team_ok);
            let team_fail = Arc::clone(&team_fail);
            let free_ok = Arc::clone(&free_ok);
            let free_fail = Arc::clone(&free_fail);
            let created_ids = Arc::clone(&created_ids);
            let prog = progress.map(Arc::clone);

            join_set.spawn(async move {
                let _permit = permit;
                // 重试逻辑
                let mut last_err = None;
                for retry in 0..S2A_STREAM_MAX_RETRIES {
                    if retry > 0 {
                        tokio::time::sleep(Duration::from_secs(S2A_STREAM_RETRY_DELAY_SECS)).await;
                    }
                    match s2a_service.add_account(&target_cfg, &acc).await {
                        Ok(opt_id) => {
                            if let Some(id) = opt_id {
                                if let Ok(mut ids) = created_ids.lock() {
                                    ids.push(id);
                                }
                            }
                            if let Some(ref p) = prog {
                                p.s2a_ok.fetch_add(1, Ordering::Relaxed);
                            }
                            if is_free {
                                free_ok.fetch_add(1, Ordering::Relaxed);
                            } else {
                                team_ok.fetch_add(1, Ordering::Relaxed);
                            }
                            broadcast_log(&format!("[S2A-Stream] 入库成功 {}", acc.account));
                            return;
                        }
                        Err(e) => {
                            last_err = Some(e);
                        }
                    }
                }
                // 全部重试失败
                if let Some(ref p) = prog {
                    p.s2a_failed.fetch_add(1, Ordering::Relaxed);
                }
                if is_free {
                    free_fail.fetch_add(1, Ordering::Relaxed);
                } else {
                    team_fail.fetch_add(1, Ordering::Relaxed);
                }
                broadcast_log(&format!(
                    "[S2A-Stream] 入库失败 {} ({}轮重试): {}",
                    acc.account,
                    S2A_STREAM_MAX_RETRIES,
                    last_err.map(|e| format!("{e}")).unwrap_or_default()
                ));
            });
        }

        // 等待所有入库任务完成
        while join_set.join_next().await.is_some() {}

        let tok = team_ok.load(Ordering::Relaxed);
        let tfail = team_fail.load(Ordering::Relaxed);
        let fok = free_ok.load(Ordering::Relaxed);
        let ffail = free_fail.load(Ordering::Relaxed);
        let total_ok = tok + fok;
        let total_fail = tfail + ffail;

        // Batch-refresh
        let ids: Vec<i64> = created_ids.lock().map(|g| g.clone()).unwrap_or_default();
        if !ids.is_empty() {
            broadcast_log(&format!(
                "[S2A-Stream-Refresh] 刷新 {} 个账号令牌...",
                ids.len()
            ));
            match s2a_service.batch_refresh(team, &ids).await {
                Ok(n) => {
                    broadcast_log(&format!(
                        "[S2A-Stream-Refresh] 刷新完成: {}/{} 成功",
                        n,
                        ids.len()
                    ));
                }
                Err(err) => {
                    broadcast_log(&format!(
                        "[S2A-Stream-Refresh] 刷新失败（不影响入库）: {err}"
                    ));
                }
            }
        }

        broadcast_log(&format!(
            "[S2A-Stream] 流式入库完成: ok={} fail={} (team={}/{} free={}/{})",
            total_ok, total_fail, tok, tfail, fok, ffail
        ));

        (total_ok, total_fail, fok, ffail)
    }

    /// Tokens 流式消费者：实时接收 RT 成功的账号并推送到 Tokens Pool
    ///
    /// 返回 (ok, fail)
    /// 公开的静态入口，供 distribution.rs 调用
    pub async fn tokens_streaming_consumer_static(
        rx: mpsc::Receiver<AccountWithRt>,
        tokens_service: Arc<dyn TokensPoolService>,
        pool: &TokensPoolConfig,
        free_mode: bool,
        progress: Option<&Arc<TaskProgress>>,
        cancel_flag: Arc<AtomicBool>,
    ) -> (usize, usize) {
        Self::tokens_streaming_consumer(rx, tokens_service, pool, free_mode, progress, cancel_flag)
            .await
    }

    async fn tokens_streaming_consumer(
        mut rx: mpsc::Receiver<AccountWithRt>,
        tokens_service: Arc<dyn TokensPoolService>,
        pool: &TokensPoolConfig,
        free_mode: bool,
        progress: Option<&Arc<TaskProgress>>,
        cancel_flag: Arc<AtomicBool>,
    ) -> (usize, usize) {
        use tokio::sync::Semaphore;

        const MAX_RETRIES: usize = 3;
        const RETRY_DELAY_SECS: u64 = 3;
        /// 每批最多合并的 token 数
        const BATCH_SIZE: usize = 50;
        /// 攒批等待超时（毫秒），避免最后几个 token 卡太久
        const BATCH_TIMEOUT_MS: u64 = 2000;

        let concurrency = pool.concurrency.max(1);
        let semaphore = Arc::new(Semaphore::new(concurrency));
        let mut join_set = tokio::task::JoinSet::new();

        let ok_count = Arc::new(AtomicUsize::new(0));
        let fail_count = Arc::new(AtomicUsize::new(0));

        // 入库前清理失效 token
        match tokens_service.delete_deactivated(pool).await {
            Ok(_) => {
                broadcast_log(&format!(
                    "[Tokens-Stream] 已清理失效 token [{}]",
                    pool.name
                ));
            }
            Err(e) => {
                broadcast_log(&format!(
                    "[Tokens-Stream] 清理失效 token 失败（继续推送）: {e}"
                ));
            }
        }

        broadcast_log(&format!(
            "[Tokens-Stream] 流式推送已启动 [{name}] (batch={BATCH_SIZE}, concurrency={concurrency})",
            name = pool.name
        ));

        let default_plan_type = pool
            .plan_type
            .clone()
            .unwrap_or_else(|| if free_mode { "free".to_string() } else { String::new() });

        let mut batch_tokens: Vec<String> = Vec::with_capacity(BATCH_SIZE);
        let mut batch_accounts: Vec<String> = Vec::new();
        let mut batch_plan_type = default_plan_type.clone();
        let mut channel_closed = false;

        loop {
            if channel_closed && batch_tokens.is_empty() {
                break;
            }

            // 攒批：收集 token 直到满或超时或 channel 关闭
            if !channel_closed && batch_tokens.len() < BATCH_SIZE {
                let recv_result = if batch_tokens.is_empty() {
                    // 空批：阻塞等待第一个
                    tokio::select! {
                        acc = rx.recv() => acc,
                        _ = Self::wait_for_cancel(cancel_flag.clone()) => {
                            broadcast_log("[Tokens-Stream] 收到中断信号，停止接收");
                            None
                        }
                    }
                } else {
                    // 已有数据：带超时继续收集
                    tokio::select! {
                        acc = rx.recv() => acc,
                        _ = tokio::time::sleep(Duration::from_millis(BATCH_TIMEOUT_MS)) => {
                            None // 超时，发送当前批次
                        }
                        _ = Self::wait_for_cancel(cancel_flag.clone()) => {
                            broadcast_log("[Tokens-Stream] 收到中断信号，停止接收");
                            channel_closed = true;
                            None
                        }
                    }
                };

                match recv_result {
                    Some(acc) => {
                        let pt = if default_plan_type.is_empty() {
                            acc.plan_type.clone()
                        } else {
                            default_plan_type.clone()
                        };
                        // plan_type 不同时先把当前批次刷出去
                        if !batch_tokens.is_empty() && pt != batch_plan_type {
                            // 后面会发送 batch_tokens
                        } else {
                            batch_plan_type = pt;
                            batch_tokens.push(acc.refresh_token);
                            batch_accounts.push(acc.account);
                            continue;
                        }
                        // 走到这里说明 plan_type 变了，先发送当前批次
                        // 然后把这个 acc 放到下一批
                        // 下面 flush 逻辑处理后会 continue
                    }
                    None => {
                        if batch_tokens.is_empty() {
                            channel_closed = true;
                            continue;
                        }
                        // 超时或 channel 关闭，刷当前批次
                        if !channel_closed {
                            // 检查是否真的是 channel 关闭
                            // timeout 情况继续，channel 关闭在 recv 返回 None 时已标记
                        }
                    }
                }
            }

            // 发送批次
            if batch_tokens.is_empty() {
                if channel_closed {
                    break;
                }
                continue;
            }

            let tokens_to_send: Vec<String> = batch_tokens.drain(..).collect();
            let accounts_to_send: Vec<String> = batch_accounts.drain(..).collect();
            let plan_type = batch_plan_type.clone();
            let count = tokens_to_send.len();

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let tokens_service = Arc::clone(&tokens_service);
            let pool_cfg = pool.clone();
            let ok_count = Arc::clone(&ok_count);
            let fail_count = Arc::clone(&fail_count);
            let prog = progress.map(Arc::clone);

            join_set.spawn(async move {
                let _permit = permit;
                let mut last_err = None;
                for retry in 0..MAX_RETRIES {
                    if retry > 0 {
                        tokio::time::sleep(Duration::from_secs(RETRY_DELAY_SECS)).await;
                    }
                    match tokens_service
                        .add_tokens_batch(&pool_cfg, &tokens_to_send, &plan_type)
                        .await
                    {
                        Ok(_) => {
                            if let Some(ref p) = prog {
                                p.s2a_ok.fetch_add(count, Ordering::Relaxed);
                            }
                            ok_count.fetch_add(count, Ordering::Relaxed);
                            broadcast_log(&format!(
                                "[Tokens-Stream] 批量推送成功 {}个 ({}...)",
                                count,
                                accounts_to_send.first().unwrap_or(&String::new())
                            ));
                            return;
                        }
                        Err(e) => {
                            last_err = Some(e);
                        }
                    }
                }
                if let Some(ref p) = prog {
                    p.s2a_failed.fetch_add(count, Ordering::Relaxed);
                }
                fail_count.fetch_add(count, Ordering::Relaxed);
                broadcast_log(&format!(
                    "[Tokens-Stream] 批量推送失败 {}个 ({}轮重试): {}",
                    count,
                    MAX_RETRIES,
                    last_err.map(|e| format!("{e}")).unwrap_or_default()
                ));
            });
        }

        // 等待所有推送任务完成
        while join_set.join_next().await.is_some() {}

        let ok = ok_count.load(Ordering::Relaxed);
        let fail = fail_count.load(Ordering::Relaxed);
        broadcast_log(&format!(
            "[Tokens-Stream] 流式推送完成: ok={ok} fail={fail}"
        ));

        (ok, fail)
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

        // 注册前 D1 预清理，避免高并发时邮件服务过载
        if cfg.d1_cleanup.enabled.unwrap_or(false) {
            broadcast_log("阶01: 注册前 D1 邮件预清理");
            if let Err(e) = crate::d1_cleanup::run_cleanup(&cfg.d1_cleanup).await {
                broadcast_log(&format!("D1 预清理失败（继续执行）: {e}"));
            }
        }

        if let Some(ref p) = progress {
            if options.tokens_pool.is_some() {
                p.set_stage("注册 + RT + Tokens");
            } else if options.push_s2a {
                p.set_stage("注册 + RT + S2A");
            } else {
                p.set_stage("注册 + RT");
            }
        }

        // S2A 流式入库：创建 channel，RT 成功后立即推送到 S2A
        let (s2a_tx, s2a_rx) = if options.push_s2a && options.tokens_pool.is_none() {
            let (tx, rx) = mpsc::channel::<AccountWithRt>(256);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // Tokens 流式推送：创建 channel，RT 成功后立即推送到 Tokens Pool
        let (tokens_tx, tokens_rx) = if options.tokens_pool.is_some() {
            let (tx, rx) = mpsc::channel::<AccountWithRt>(256);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };

        // 启动 S2A 流式消费者（与注册+RT 并行）
        let s2a_handle = if let Some(rx) = s2a_rx {
            let s2a_service = Arc::clone(&self.s2a_service);
            let team = team.clone();
            let prog = progress.clone();
            let cancel = cancel_flag.clone();
            Some(tokio::spawn(async move {
                Self::s2a_streaming_consumer(rx, s2a_service, &team, prog.as_ref(), cancel).await
            }))
        } else {
            None
        };

        // 启动 Tokens 流式消费者（与注册+RT 并行）
        let tokens_handle = if let Some(rx) = tokens_rx {
            let tokens_service = Arc::clone(&self.tokens_pool_service);
            let pool_cfg = options.tokens_pool.clone().unwrap();
            let prog = progress.clone();
            let cancel = cancel_flag.clone();
            let free_mode = options.free_mode;
            Some(tokio::spawn(async move {
                Self::tokens_streaming_consumer(rx, tokens_service, &pool_cfg, free_mode, prog.as_ref(), cancel).await
            }))
        } else {
            None
        };

        // 传给 run_register_and_rt 的 tx：优先 tokens_tx，其次 s2a_tx
        let stream_tx = tokens_tx.or(s2a_tx);

        let reg_result = match self
            .run_register_and_rt(cfg, options, cancel_flag, progress.as_ref(), stream_tx)
            .await
        {
            Ok(r) => r,
            Err(e) => {
                broadcast_log(&format!(
                    "[SIG-END][ERR] mode={mode_label} stage=register_rt target_rt={} reason={e:#}",
                    options.target_count
                ));
                // 如果有 S2A 或 Tokens handle，等待它结束
                if let Some(h) = s2a_handle {
                    let _ = h.await;
                }
                if let Some(h) = tokens_handle {
                    let _ = h.await;
                }
                return Err(e);
            }
        };

        let rt_ok = reg_result.rt_success.len();
        let rt_failed = reg_result.rt_failed.len();
        let output_files = reg_result.output_files;

        // 等待 S2A 流式消费者完成
        let (s2a_ok, s2a_failed, free_s2a_ok, free_s2a_failed) = if let Some(h) = s2a_handle {
            match h.await {
                Ok(result) => result,
                Err(_) => (0, 0, 0, 0), // JoinError
            }
        } else if let Some(h) = tokens_handle {
            // Tokens 池结果复用 s2a 字段
            match h.await {
                Ok(result) => (result.0, result.1, 0, 0),
                Err(_) => (0, 0, 0, 0),
            }
        } else {
            (0, 0, 0, 0)
        };

        // D1 清理
        if cfg.d1_cleanup.enabled.unwrap_or(false) {
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
        s2a_tx: Option<mpsc::Sender<AccountWithRt>>,
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

                // 如果注册阶段已经获取到了 refresh_token，直接跳过 CodexService 获取
                if let Some(rt) = acc.refresh_token.clone() {
                    if let Some(ref p) = prog { p.rt_ok.fetch_add(1, Ordering::Relaxed); }
                    log_worker_green(
                        worker_id,
                        "OK",
                        &format!("RT已在注册阶段获取 (耗时 {:.1}s)", started.elapsed().as_secs_f32()),
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
                Ok(Ok(acc_with_rt)) => {
                    // S2A 流式入库：RT 成功后立即发送到 S2A consumer
                    if let Some(ref tx) = s2a_tx {
                        let _ = tx.send(acc_with_rt.clone()).await;
                    }
                    rt_success.push(acc_with_rt);
                }
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
