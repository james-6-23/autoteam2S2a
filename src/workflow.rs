use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use futures::{StreamExt, stream};
use tokio::sync::mpsc;

use crate::config::{AppConfig, S2aConfig};
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
    pub push_s2a: bool,
    pub use_chatgpt_mail: bool,
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
        const MAX_ROUNDS: usize = 5;

        let mut all_rt_success: Vec<AccountWithRt> = Vec::new();
        let mut all_rt_failed: Vec<RegisteredAccount> = Vec::new();
        let mut total_registered = 0usize;
        let mut total_reg_failed = 0usize;
        let mut total_task_index = 0usize;

        for round in 0..MAX_ROUNDS {
            if cancel_flag.load(Ordering::Relaxed) {
                broadcast_log("已收到中断信号，跳过后续注册轮次...");
                break;
            }

            let current_ok = all_rt_success.len();
            if current_ok >= target {
                break;
            }
            let deficit = target - current_ok;

            if round > 0 {
                broadcast_log(&format!(
                    "=== 补注册第 {} 轮: 还需 {} 个账号 (已成功 {}/{}) ===",
                    round, deficit, current_ok, target
                ));
            } else {
                let plan_label = if cfg.payment_enabled() {
                    "team"
                } else {
                    "free"
                };
                broadcast_log(&format!("阶段1+2: 注册 {plan_label} 账号 → RT (流水线模式, 目标 {target})"));
            }

            // 根据邮箱系统选择生成 seeds
            let seeds = if options.use_chatgpt_mail {
                let email_service = &self.register_service;
                let mut out = Vec::with_capacity(deficit);
                for _ in 0..deficit {
                    let proxy = self.proxy_pool.next();
                    let mut seed = generate_account_seed(&cfg.email_domains);
                    match email_service.generate_email(proxy.as_deref()).await {
                        Ok(Some(email)) => seed.account = email,
                        Ok(None) => {}
                        Err(e) => {
                            broadcast_log(&format!("[chatgpt.org.uk] 生成邮箱失败: {e}，使用域名列表邮箱"));
                        }
                    }
                    out.push(seed);
                }
                out
            } else {
                (0..deficit)
                    .map(|_| generate_account_seed(&cfg.email_domains))
                    .collect::<Vec<_>>()
            };

            let (reg_tx, rt_rx) = mpsc::channel(deficit.max(reg_concurrency * 4));

            // 注册生产者
            let register_handle = {
                let register_service = Arc::clone(&self.register_service);
                let proxy_pool = Arc::clone(&self.proxy_pool);
                let seeds = seeds.clone();
                let base_idx = total_task_index;
                let cancel = cancel_flag.clone();
                let prog = progress.cloned();
                tokio::spawn(async move {
                    let mut reg_failed_count = 0usize;
                    let results = stream::iter(seeds.into_iter().enumerate())
                        .map(|(idx, seed)| {
                            let register_service = Arc::clone(&register_service);
                            let proxy = proxy_pool.next();
                            let worker_id = idx % reg_concurrency + 1;
                            let reg_tx = reg_tx.clone();
                            let task_no = base_idx + idx + 1;
                            let cancel = cancel.clone();
                            let prog = prog.clone();
                            async move {
                                if cancel.load(Ordering::Relaxed) {
                                    return (false, task_no, seed.account);
                                }
                                let jitter_ms = random_delay_ms(300, 1200);
                                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;
                                if cancel.load(Ordering::Relaxed) {
                                    return (false, task_no, seed.account);
                                }
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
                                let input = RegisterInput {
                                    seed: seed.clone(),
                                    proxy: proxy.clone(),
                                    worker_id,
                                    task_index: task_no,
                                    task_total: target,
                                };
                                match register_service.register(input).await {
                                    Ok(acc) => match reg_tx.send(acc).await {
                                        Ok(_) => {
                                            if let Some(ref p) = prog { p.reg_ok.fetch_add(1, Ordering::Relaxed); }
                                            (true, task_no, seed.account)
                                        }
                                        Err(send_err) => {
                                            if let Some(ref p) = prog { p.reg_failed.fetch_add(1, Ordering::Relaxed); }
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
                                        if let Some(ref p) = prog { p.reg_failed.fetch_add(1, Ordering::Relaxed); }
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
                        .buffer_unordered(reg_concurrency)
                        .collect::<Vec<_>>()
                        .await;

                    for (ok, _, _) in &results {
                        if !*ok {
                            reg_failed_count += 1;
                        }
                    }
                    let reg_ok_count = results.len() - reg_failed_count;
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
            all_rt_success.extend(round_rt_ok);
            all_rt_failed.extend(round_rt_failed);

            broadcast_log(&format!(
                "本轮结束: RT成功 {}, 累计成功 {}/{}",
                all_rt_success.len() - current_ok,
                all_rt_success.len(),
                target
            ));

            if all_rt_success.len() >= target {
                break;
            }
        }

        // 防御性截断
        all_rt_success.truncate(target);

        // 输出文件
        let plan_label = all_rt_success
            .first()
            .map(|a| {
                if a.plan_type.contains("team") {
                    "team"
                } else {
                    "free"
                }
            })
            .unwrap_or("free");
        let mut output_files = Vec::new();
        if let Some(path) =
            save_json_records(&format!("accounts-{plan_label}-with-rt"), &all_rt_success)?
        {
            output_files.push(path.display().to_string());
        }
        if let Some(path) =
            save_json_records(&format!("accounts-{plan_label}-no-rt"), &all_rt_failed)?
        {
            output_files.push(path.display().to_string());
        }

        Ok(RegisterRtResult {
            rt_success: all_rt_success,
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
                    Ok(_) => {
                        s2a_ok += 1;
                        if let Some(p) = progress { p.s2a_ok.fetch_add(1, Ordering::Relaxed); }
                        broadcast_log(&format!("[S2A成功] {}", acc.account));
                    }
                    Err(err) => {
                        if let Some(p) = progress { p.s2a_failed.fetch_add(1, Ordering::Relaxed); }
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

        if let Some(ref p) = progress { p.set_stage("注册 + RT"); }
        let reg_result = self.run_register_and_rt(cfg, options, cancel_flag, progress.as_ref()).await?;

        let rt_ok = reg_result.rt_success.len();
        let rt_failed = reg_result.rt_failed.len();
        let mut output_files = reg_result.output_files;

        let (s2a_ok, s2a_failed) = if options.push_s2a {
            if let Some(ref p) = progress { p.set_stage("S2A 入库"); }
            broadcast_log(&format!("阶段3: 入库 S2A [{}]", team.name));

            // 过滤掉 free 账号
            let (s2a_eligible, free_accounts): (Vec<AccountWithRt>, Vec<AccountWithRt>) =
                reg_result
                    .rt_success
                    .into_iter()
                    .partition(|acc| !acc.plan_type.eq_ignore_ascii_case("free"));

            if !free_accounts.is_empty() {
                broadcast_log(&format!(
                    "[S2A] 跳过 {} 个 free 账号（plan_type=free 不入库）",
                    free_accounts.len()
                ));
                for acc in &free_accounts {
                    broadcast_log(&format!("[S2A跳过] {} (plan={})", acc.account, acc.plan_type));
                }
                if let Some(path) = save_json_records("accounts-free-skipped", &free_accounts)? {
                    output_files.push(path.display().to_string());
                }
            }

            if s2a_eligible.is_empty() {
                broadcast_log("[S2A] 没有可入库的账号（全部为 free）");
                (0, 0)
            } else {
                broadcast_log(&format!(
                    "[S2A] 准备入库 {} 个账号（已排除 {} 个 free）",
                    s2a_eligible.len(),
                    free_accounts.len()
                ));
                self.push_to_s2a(team, s2a_eligible, progress.as_ref()).await
            }
        } else {
            (0, 0)
        };

        // D1 清理
        if cfg.d1_cleanup.enabled.unwrap_or(false) {
            broadcast_log("阶段4: D1 邮件清理");
            if let Err(e) = crate::d1_cleanup::run_cleanup(&cfg.d1_cleanup).await {
                broadcast_log(&format!("D1 清理失败（不影响入库结果）: {e}"));
            }
        }

        Ok(WorkflowReport {
            registered_ok: reg_result.total_registered,
            registered_failed: reg_result.total_reg_failed,
            rt_ok,
            rt_failed,
            s2a_ok,
            s2a_failed,
            output_files,
            elapsed_secs: workflow_started.elapsed().as_secs_f32(),
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
                log_worker(
                    worker_id,
                    "RT",
                    &format!("开始获取 refresh_token {}", acc.account),
                );

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
                                &format!("RT: {rt} (耗时 {:.1}s)", elapsed_sec),
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

    async fn wait_for_cancel(cancel_flag: Arc<AtomicBool>) {
        while !cancel_flag.load(Ordering::Relaxed) {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}
