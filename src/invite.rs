use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};

use crate::config::{AppConfig, InviteRuntimeConfig, S2aConfig};
use crate::db::{InviteEmailUpdate, InviteOwnerInsert, InviteTaskUpdate, RunHistoryDb};
use crate::email_service::EmailServiceConfig;
use crate::log_broadcast::broadcast_log;
use crate::models::InviteProgress;
use crate::proxy_pool::ProxyPool;
use crate::services::{CodexService, RegisterService};

// ─── Team Owner 解析 ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TeamOwner {
    pub email: String,
    pub account_id: String,
    pub access_token: String,
    pub expires: Option<String>,
}

/// 解析上传的 accounts.json，提取 owner 信息
pub fn parse_owners_json(value: &serde_json::Value) -> Result<Vec<TeamOwner>> {
    let arr = value
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("JSON 必须是数组"))?;

    let mut owners = Vec::new();
    for (i, item) in arr.iter().enumerate() {
        let email = item
            .pointer("/user/email")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let account_id = item
            .pointer("/account/id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("第 {} 个账号缺少 account.id", i + 1))?
            .to_string();
        let access_token = item
            .get("accessToken")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("第 {} 个账号缺少 accessToken", i + 1))?
            .to_string();
        let expires = item
            .get("expires")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        owners.push(TeamOwner {
            email,
            account_id,
            access_token,
            expires,
        });
    }

    if owners.is_empty() {
        bail!("JSON 数组为空，没有解析到 owner 账号");
    }
    Ok(owners)
}

/// 将 TeamOwner 转为 DB 插入结构
pub fn owners_to_db(owners: &[TeamOwner]) -> Vec<InviteOwnerInsert> {
    owners
        .iter()
        .map(|o| InviteOwnerInsert {
            email: o.email.clone(),
            account_id: o.account_id.clone(),
            access_token: o.access_token.clone(),
            expires: o.expires.clone(),
        })
        .collect()
}

// ─── ChatGPT 邀请 API 调用 ───────────────────────────────────────────────────

#[derive(Debug)]
pub struct InviteResult {
    pub email: String,
    pub success: bool,
    pub error: Option<String>,
}

/// 调用 ChatGPT 邀请 API，逐个邮箱发送邀请
pub async fn invite_emails(
    owner: &TeamOwner,
    emails: &[String],
    invite_cfg: &InviteRuntimeConfig,
) -> Vec<InviteResult> {
    let client = rquest::Client::builder()
        .timeout(Duration::from_secs(invite_cfg.request_timeout_sec))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let mut results = Vec::with_capacity(emails.len());

    for email in emails {
        let result = invite_single_email(&client, owner, email, invite_cfg).await;
        results.push(result);
        // 邀请间短暂等待，避免触发频率限制
        tokio::time::sleep(Duration::from_millis(300)).await;
    }

    results
}

const MAX_INVITE_RETRIES: usize = 3;

async fn invite_single_email(
    client: &rquest::Client,
    owner: &TeamOwner,
    email: &str,
    invite_cfg: &InviteRuntimeConfig,
) -> InviteResult {
    let url = format!(
        "https://chatgpt.com/backend-api/accounts/{}/invites",
        owner.account_id
    );

    #[derive(serde::Serialize)]
    struct InvitePayload {
        email_addresses: Vec<String>,
        role: String,
        resend_emails: bool,
    }

    let payload = InvitePayload {
        email_addresses: vec![email.to_string()],
        role: "standard-user".to_string(),
        resend_emails: true,
    };

    for attempt in 0..MAX_INVITE_RETRIES {
        if attempt > 0 {
            let delay = match attempt {
                1 => 2000,
                _ => 5000,
            };
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }

        let mut req = client
            .post(&url)
            .header("Accept", "*/*")
            .header("Accept-Language", "zh-CN,zh;q=0.9")
            .header("Authorization", format!("Bearer {}", owner.access_token))
            .header("Chatgpt-Account-Id", &owner.account_id)
            .header("Oai-Client-Version", &invite_cfg.oai_client_version)
            .header("Oai-Language", "zh-CN")
            .header("User-Agent", &invite_cfg.user_agent)
            .header("Origin", "https://chatgpt.com")
            .header("Referer", "https://chatgpt.com/admin/members")
            .header("Content-Type", "application/json");

        // Oai-Device-Id 可选
        let device_id = uuid::Uuid::new_v4().to_string();
        req = req.header("Oai-Device-Id", &device_id);

        match req.json(&payload).send().await {
            Ok(resp) => {
                let status = resp.status();
                if status.is_success() {
                    return InviteResult {
                        email: email.to_string(),
                        success: true,
                        error: None,
                    };
                }

                let body = resp
                    .text()
                    .await
                    .unwrap_or_default();
                let body_preview = if body.len() > 300 {
                    format!("{}...", &body[..300])
                } else {
                    body
                };

                // 仅对 408/429/5xx 重试
                let should_retry = status.as_u16() == 408
                    || status.as_u16() == 429
                    || status.is_server_error();

                if !should_retry || attempt + 1 >= MAX_INVITE_RETRIES {
                    return InviteResult {
                        email: email.to_string(),
                        success: false,
                        error: Some(format!("HTTP {}: {}", status.as_u16(), body_preview)),
                    };
                }
                broadcast_log(&format!(
                    "[邀请重试] {} → HTTP {} (第{}/{}次)",
                    email,
                    status.as_u16(),
                    attempt + 1,
                    MAX_INVITE_RETRIES
                ));
            }
            Err(e) => {
                if attempt + 1 >= MAX_INVITE_RETRIES {
                    return InviteResult {
                        email: email.to_string(),
                        success: false,
                        error: Some(format!("请求失败: {e}")),
                    };
                }
            }
        }
    }

    InviteResult {
        email: email.to_string(),
        success: false,
        error: Some("超过最大重试次数".to_string()),
    }
}

// ─── 完整邀请工作流 ──────────────────────────────────────────────────────────

/// 单个 Owner 的完整邀请工作流
#[allow(clippy::too_many_arguments)]
pub async fn run_invite_workflow(
    task_id: String,
    owner_db_id: i64,
    owner: TeamOwner,
    seeds: Vec<crate::models::AccountSeed>,
    invite_cfg: InviteRuntimeConfig,
    cfg: AppConfig,
    team: Option<S2aConfig>,
    push_s2a: bool,
    db: Arc<RunHistoryDb>,
    progress: Arc<InviteProgress>,
) {
    let total = seeds.len();
    broadcast_log(&format!(
        "[邀请] 开始: owner={} 邀请 {} 个邮箱",
        owner.email, total
    ));

    // 标记任务运行中
    let _ = db.enqueue_update_invite_task(
        task_id.clone(),
        InviteTaskUpdate {
            status: Some("running".to_string()),
            ..Default::default()
        },
    );

    // 标记 owner 已使用
    let _ = db.enqueue_mark_owner_used(owner_db_id);

    // ─── 阶段 1: 邀请 ───────────────────────────────────────────────────────
    progress.set_stage("邀请中");
    let emails: Vec<String> = seeds.iter().map(|s| s.account.clone()).collect();
    let invite_results = invite_emails(&owner, &emails, &invite_cfg).await;

    let mut invited_ok = 0usize;
    let mut invited_failed = 0usize;
    let mut invited_emails_ok = Vec::new(); // (seed_index, email_db_id) 将在下面填充

    // 更新每封邮件的邀请状态
    // 先获取 email 记录（通过 task detail）
    let email_records = db
        .get_invite_task_detail(&task_id)
        .ok()
        .flatten()
        .map(|d| d.emails)
        .unwrap_or_default();

    for (i, result) in invite_results.iter().enumerate() {
        let email_db_id = email_records.get(i).map(|r| r.id).unwrap_or(0);
        if result.success {
            invited_ok += 1;
            progress.invited_ok.fetch_add(1, Ordering::Relaxed);
            let _ = db.enqueue_update_invite_email(
                email_db_id,
                InviteEmailUpdate {
                    invite_status: Some("ok".to_string()),
                    ..Default::default()
                },
            );
            invited_emails_ok.push((i, email_db_id));
            broadcast_log(&format!("[邀请成功] {}", result.email));
        } else {
            invited_failed += 1;
            progress.invited_failed.fetch_add(1, Ordering::Relaxed);
            let _ = db.enqueue_update_invite_email(
                email_db_id,
                InviteEmailUpdate {
                    invite_status: Some("failed".to_string()),
                    error: result.error.clone(),
                    ..Default::default()
                },
            );
            broadcast_log(&format!(
                "[邀请失败] {}: {}",
                result.email,
                result.error.as_deref().unwrap_or("未知")
            ));
        }
    }

    // 更新任务邀请阶段进度
    let _ = db.enqueue_update_invite_task(
        task_id.clone(),
        InviteTaskUpdate {
            invited_ok: Some(invited_ok),
            invited_failed: Some(invited_failed),
            ..Default::default()
        },
    );

    if invited_emails_ok.is_empty() {
        broadcast_log("[邀请] 所有邮箱邀请均失败，跳过后续流程");
        let _ = db.enqueue_update_invite_task(
            task_id.clone(),
            InviteTaskUpdate {
                status: Some("completed".to_string()),
                finished_at: Some(crate::util::beijing_now().to_rfc3339()),
                ..Default::default()
            },
        );
        return;
    }

    // ─── 阶段 2: 注册 ───────────────────────────────────────────────────────
    progress.set_stage("注册中");
    broadcast_log(&format!(
        "[注册] 开始注册 {} 个已邀请邮箱",
        invited_emails_ok.len()
    ));

    let register_runtime = cfg.register_runtime();
    let codex_runtime = cfg.codex_runtime();
    let proxy_pool = Arc::new(ProxyPool::new(cfg.proxy_pool.clone()));

    let email_cfg = EmailServiceConfig {
        mail_api_base: register_runtime.mail_api_base.clone(),
        mail_api_path: register_runtime.mail_api_path.clone(),
        mail_api_token: register_runtime.mail_api_token.clone(),
        request_timeout_sec: register_runtime.mail_request_timeout_sec,
    };
    let reg_email_service = Arc::new(crate::email_service::EmailService::new_http(
        email_cfg.clone(),
        register_runtime.mail_max_concurrency,
    ));

    let register_service = Arc::new(crate::services::LiveRegisterService::new(
        register_runtime.clone(),
        reg_email_service.clone(),
    ));

    let codex_email_service = Arc::new(crate::email_service::EmailService::new_http(
        email_cfg,
        register_runtime.mail_max_concurrency,
    ));

    let codex_service = Arc::new(crate::services::LiveCodexService::new(
        codex_runtime,
        codex_email_service,
    ));

    let s2a_service = Arc::new(crate::services::S2aHttpService::new());

    let mut reg_ok = 0usize;
    let mut reg_failed = 0usize;
    let mut rt_ok = 0usize;
    let mut rt_failed = 0usize;
    let mut s2a_ok_count = 0usize;
    let mut s2a_failed_count = 0usize;
    let mut accounts_for_s2a: Vec<crate::models::AccountWithRt> = Vec::new();

    for &(seed_idx, email_db_id) in &invited_emails_ok {
        let seed = &seeds[seed_idx];
        let proxy = proxy_pool.next();

        // 注册
        let input = crate::services::RegisterInput {
            seed: seed.clone(),
            proxy: proxy.clone(),
            worker_id: seed_idx + 1,
            task_index: seed_idx,
            task_total: total,
            skip_payment: true, // 邀请账号不需要支付
        };

        match register_service.register(input).await {
            Ok(registered) => {
                reg_ok += 1;
                progress.reg_ok.fetch_add(1, Ordering::Relaxed);
                let _ = db.enqueue_update_invite_email(
                    email_db_id,
                    InviteEmailUpdate {
                        reg_status: Some("ok".to_string()),
                        ..Default::default()
                    },
                );
                broadcast_log(&format!("[注册成功] {} plan={}", registered.account, registered.plan_type));

                // ─── 阶段 3: 获取 RT ───
                progress.set_stage("获取 RT");
                match codex_service
                    .fetch_refresh_token(&registered, proxy, seed_idx + 1)
                    .await
                {
                    Ok(rt) => {
                        rt_ok += 1;
                        progress.rt_ok.fetch_add(1, Ordering::Relaxed);
                        let _ = db.enqueue_update_invite_email(
                            email_db_id,
                            InviteEmailUpdate {
                                rt_status: Some("ok".to_string()),
                                ..Default::default()
                            },
                        );
                        broadcast_log(&format!("[RT成功] {}", registered.account));

                        let acc_with_rt = crate::models::AccountWithRt {
                            account: registered.account,
                            password: registered.password,
                            token: registered.token,
                            account_id: registered.account_id,
                            plan_type: registered.plan_type,
                            refresh_token: rt,
                        };
                        accounts_for_s2a.push(acc_with_rt);
                    }
                    Err(e) => {
                        rt_failed += 1;
                        progress.rt_failed.fetch_add(1, Ordering::Relaxed);
                        let _ = db.enqueue_update_invite_email(
                            email_db_id,
                            InviteEmailUpdate {
                                rt_status: Some("failed".to_string()),
                                error: Some(format!("{e:#}")),
                                ..Default::default()
                            },
                        );
                        broadcast_log(&format!("[RT失败] {}: {e:#}", registered.account));
                    }
                }
            }
            Err(e) => {
                reg_failed += 1;
                progress.reg_failed.fetch_add(1, Ordering::Relaxed);
                let _ = db.enqueue_update_invite_email(
                    email_db_id,
                    InviteEmailUpdate {
                        reg_status: Some("failed".to_string()),
                        error: Some(format!("{e:#}")),
                        ..Default::default()
                    },
                );
                broadcast_log(&format!("[注册失败] {}: {e:#}", seed.account));
            }
        }

        // 更新任务进度
        let _ = db.enqueue_update_invite_task(
            task_id.clone(),
            InviteTaskUpdate {
                reg_ok: Some(reg_ok),
                reg_failed: Some(reg_failed),
                rt_ok: Some(rt_ok),
                rt_failed: Some(rt_failed),
                ..Default::default()
            },
        );
    }

    // ─── 阶段 4: S2A 入库 ───────────────────────────────────────────────────
    if push_s2a && !accounts_for_s2a.is_empty() {
        if let Some(ref team_cfg) = team {
            progress.set_stage("S2A 入库");
            broadcast_log(&format!(
                "[S2A] 准备入库 {} 个账号到 {}",
                accounts_for_s2a.len(),
                team_cfg.name
            ));

            let workflow_runner = crate::workflow::WorkflowRunner::new(
                register_service.clone(),
                codex_service.clone(),
                s2a_service.clone(),
                proxy_pool.clone(),
            );

            let (ok, failed) = workflow_runner
                .push_to_s2a(team_cfg, accounts_for_s2a.clone(), None)
                .await;
            s2a_ok_count = ok;
            s2a_failed_count = failed;

            // 更新每个邮箱的 S2A 状态
            for (idx, &(_, email_db_id)) in invited_emails_ok.iter().enumerate() {
                // 只有成功获取 RT 的才有 S2A 结果
                if idx < accounts_for_s2a.len() {
                    let status = if idx < s2a_ok_count { "ok" } else { "failed" };
                    let _ = db.enqueue_update_invite_email(
                        email_db_id,
                        InviteEmailUpdate {
                            s2a_status: Some(status.to_string()),
                            ..Default::default()
                        },
                    );
                }
            }

            progress.s2a_ok.fetch_add(s2a_ok_count, Ordering::Relaxed);
            progress
                .s2a_failed
                .fetch_add(s2a_failed_count, Ordering::Relaxed);
        }
    }

    // ─── 完成 ────────────────────────────────────────────────────────────────
    let _ = db.enqueue_update_invite_task(
        task_id.clone(),
        InviteTaskUpdate {
            status: Some("completed".to_string()),
            invited_ok: Some(invited_ok),
            invited_failed: Some(invited_failed),
            reg_ok: Some(reg_ok),
            reg_failed: Some(reg_failed),
            rt_ok: Some(rt_ok),
            rt_failed: Some(rt_failed),
            s2a_ok: Some(s2a_ok_count),
            s2a_failed: Some(s2a_failed_count),
            finished_at: Some(crate::util::beijing_now().to_rfc3339()),
            ..Default::default()
        },
    );

    broadcast_log(&format!(
        "[邀请完成] owner={} | 邀请: {}/{} | 注册: {}/{} | RT: {}/{} | S2A: {}/{}",
        owner.email,
        invited_ok,
        total,
        reg_ok,
        invited_ok,
        rt_ok,
        reg_ok,
        s2a_ok_count,
        rt_ok,
    ));
}
