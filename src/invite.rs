use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use anyhow::{Result, bail};
use futures::{StreamExt, stream};

use crate::config::{AppConfig, InviteRuntimeConfig, S2aConfig};
use crate::db::{InviteEmailUpdate, InviteOwnerInsert, InviteTaskUpdate, RunHistoryDb};
use crate::email_service::EmailServiceConfig;
use crate::log_broadcast::broadcast_log;
use crate::models::InviteProgress;
use crate::proxy_pool::ProxyPool;
use crate::services::{CodexService, RegisterService};
use crate::util::mask_proxy;

// ─── Team Owner 解析 ──────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct TeamOwner {
    pub email: String,
    pub account_id: String,
    pub access_token: String,
    pub expires: Option<String>,
}

/// 解析上传的 accounts JSON，兼容多种格式：
/// - 格式 A（数组）：`[{ "user": { "email": "..." }, "account": { "id": "..." }, "accessToken": "..." }]`
/// - 格式 B（对象）：`{ "accounts": [{ "id": "...", "access_token": "..." }] }`
/// - 格式 C（codex）：`{ "type": "codex", "email": "...", "account_id": "...", "access_token": "...", "expired": "..." }`
/// - email 缺失时自动从 JWT access_token 中提取
pub fn parse_owners_json(value: &serde_json::Value) -> Result<Vec<TeamOwner>> {
    // 兼容多种包装格式：数组 / { "accounts": [...] } / 单个对象
    let arr = if let Some(arr) = value.as_array() {
        arr.clone()
    } else if let Some(arr) = value.pointer("/accounts").and_then(|v| v.as_array()) {
        arr.clone()
    } else if value.is_object()
        && (value.get("access_token").is_some() || value.get("accessToken").is_some())
    {
        // 单个账号对象，自动包装为数组
        vec![value.clone()]
    } else {
        bail!("JSON 格式不支持，需要数组或 {{ \"accounts\": [...] }}");
    };

    let mut owners = Vec::new();
    for (i, item) in arr.iter().enumerate() {
        // access_token: 兼容 accessToken / access_token
        let access_token = item
            .get("accessToken")
            .or_else(|| item.get("access_token"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("第 {} 个账号缺少 accessToken / access_token", i + 1))?
            .to_string();

        // account_id: 兼容 account.id / account_id / id
        let account_id = item
            .pointer("/account/id")
            .or_else(|| item.get("account_id"))
            .or_else(|| item.get("id"))
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("第 {} 个账号缺少 account.id / account_id / id", i + 1))?
            .to_string();

        // email: 兼容 user.email / email，缺失时从 JWT 提取
        let email = item
            .pointer("/user/email")
            .or_else(|| item.get("email"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .or_else(|| extract_email_from_jwt(&access_token))
            .unwrap_or_default();

        // expires: 兼容 expires / expired
        let expires = item
            .get("expires")
            .or_else(|| item.get("expired"))
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

/// 从 JWT access_token 的 payload 中提取 email
fn extract_email_from_jwt(token: &str) -> Option<String> {
    use base64::Engine;
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return None;
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(parts[1])
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    // ChatGPT JWT: "https://api.openai.com/profile" → { "email": "..." }
    json.pointer("/https:~1~1api.openai.com~1profile/email")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
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
    let total = emails.len();

    for (i, email) in emails.iter().enumerate() {
        broadcast_log(&format!("[邀请] 发送 ({}/{}) {}", i + 1, total, email));
        let result = invite_single_email(&client, owner, email, invite_cfg).await;
        // 实时输出单个结果
        if result.success {
            broadcast_log(&format!("[邀请成功] {}", email));
        } else {
            broadcast_log(&format!(
                "[邀请失败] {}: {}",
                email,
                result.error.as_deref().unwrap_or("未知错误")
            ));
        }
        results.push(result);
        // 邀请间短暂等待，避免触发频率限制
        if i + 1 < total {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
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

                let body = resp.text().await.unwrap_or_default();
                let body_preview = if body.len() > 300 {
                    format!("{}...", &body[..300])
                } else {
                    body
                };

                // 仅对 408/429/5xx 重试
                let should_retry =
                    status.as_u16() == 408 || status.as_u16() == 429 || status.is_server_error();

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

// ─── 查询 Team 成员 ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct TeamMember {
    pub email: String,
    #[allow(dead_code)]
    pub user_id: String,
}

/// 查询 team 当前的标准用户成员列表（不含 owner）
async fn fetch_team_members(
    owner: &TeamOwner,
    invite_cfg: &InviteRuntimeConfig,
) -> Result<Vec<TeamMember>> {
    let client = rquest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let url = format!(
        "https://chatgpt.com/backend-api/accounts/{}/users?offset=0&limit=25&query=",
        owner.account_id
    );

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", owner.access_token))
        .header("Chatgpt-Account-Id", &owner.account_id)
        .header("User-Agent", &invite_cfg.user_agent)
        .header("Origin", "https://chatgpt.com")
        .header("Referer", "https://chatgpt.com/")
        .send()
        .await?;

    if !resp.status().is_success() {
        bail!("HTTP {}", resp.status().as_u16());
    }

    let body: serde_json::Value = resp.json().await?;
    let items = body["items"]
        .as_array()
        .ok_or_else(|| anyhow::anyhow!("响应格式异常"))?;

    let mut members = Vec::new();
    for item in items {
        let role = item["role"].as_str().unwrap_or("");
        if role == "standard-user" {
            members.push(TeamMember {
                email: item["email"].as_str().unwrap_or("").to_string(),
                user_id: item["id"].as_str().unwrap_or("").to_string(),
            });
        }
    }
    Ok(members)
}

// ─── 查询 Pending Invites ────────────────────────────────────────────────────

/// pending invite 中的单条记录（仅提取 email）
#[derive(Debug, Clone)]
struct PendingInvite {
    pub email: String,
}

/// 查询 team 当前的 pending invites（已发送但未接受的邀请）
async fn fetch_pending_invites(
    owner: &TeamOwner,
    invite_cfg: &InviteRuntimeConfig,
) -> Result<Vec<PendingInvite>> {
    let client = rquest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let url = format!(
        "https://chatgpt.com/backend-api/accounts/{}/invites?offset=0&limit=100&query=",
        owner.account_id
    );

    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {}", owner.access_token))
        .header("Chatgpt-Account-Id", &owner.account_id)
        .header("User-Agent", &invite_cfg.user_agent)
        .header("Origin", "https://chatgpt.com")
        .header("Referer", "https://chatgpt.com/admin/members?tab=invites")
        .send()
        .await?;

    if !resp.status().is_success() {
        bail!("HTTP {}", resp.status().as_u16());
    }

    let body: serde_json::Value = resp.json().await?;

    // 响应格式：{ "items": [...], "total": N, "limit": N, "offset": N }
    let invites = body["items"]
        .as_array()
        .or_else(|| body["account_invites"].as_array())
        .or_else(|| body.as_array());

    let invites = match invites {
        Some(arr) => arr,
        None => {
            // 打印实际响应的顶层 key 便于调试
            let keys: Vec<&str> = body
                .as_object()
                .map(|obj| obj.keys().map(|k| k.as_str()).collect())
                .unwrap_or_default();
            broadcast_log(&format!(
                "[调试] pending invites 响应顶层 key: {:?}, 前200字符: {}",
                keys,
                &body.to_string().chars().take(200).collect::<String>()
            ));
            bail!("pending invites 响应格式异常 (keys={:?})", keys);
        }
    };

    let mut pending = Vec::new();
    for item in invites {
        let email = item["email_address"]
            .as_str()
            .or_else(|| item["email"].as_str())
            .unwrap_or("")
            .to_string();
        if !email.is_empty() {
            pending.push(PendingInvite { email });
        }
    }
    Ok(pending)
}

// ─── 清理 Pending Invites ────────────────────────────────────────────────────

/// 逐个删除 pending invites，释放席位
async fn delete_pending_invites(
    owner: &TeamOwner,
    pending: &[PendingInvite],
    invite_cfg: &InviteRuntimeConfig,
) {
    let client = rquest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let url = format!(
        "https://chatgpt.com/backend-api/accounts/{}/invites",
        owner.account_id
    );

    #[derive(serde::Serialize)]
    struct DeletePayload {
        email_address: String,
    }

    // 并发删除所有 pending invites
    let futs = pending.iter().map(|invite| {
        let client = &client;
        let url = &url;
        let owner = &owner;
        let invite_cfg = &invite_cfg;
        async move {
            let payload = DeletePayload {
                email_address: invite.email.clone(),
            };
            match client
                .delete(url.as_str())
                .header("Authorization", format!("Bearer {}", owner.access_token))
                .header("Chatgpt-Account-Id", &owner.account_id)
                .header("Content-Type", "application/json")
                .header("User-Agent", &invite_cfg.user_agent)
                .header("Origin", "https://chatgpt.com")
                .header("Referer", "https://chatgpt.com/admin/members?tab=invites")
                .json(&payload)
                .send()
                .await
            {
                Ok(resp) if resp.status().is_success() => {
                    broadcast_log(&format!("[清理] 已删除 pending invite: {}", invite.email));
                }
                Ok(resp) => {
                    let status = resp.status();
                    let body = resp.text().await.unwrap_or_default();
                    broadcast_log(&format!(
                        "[清理] 删除 pending invite {} 失败: HTTP {} - {}",
                        invite.email,
                        status.as_u16(),
                        if body.len() > 200 {
                            &body[..200]
                        } else {
                            &body
                        }
                    ));
                }
                Err(e) => {
                    broadcast_log(&format!(
                        "[清理] 删除 pending invite {} 请求失败: {e}",
                        invite.email
                    ));
                }
            }
        }
    });
    futures::future::join_all(futs).await;
}

// ─── 注销 Owner 账号 ────────────────────────────────────────────────────────

/// 调用 ChatGPT deactivate API 注销母号（暂停使用，保留备用）
#[allow(dead_code)]
async fn deactivate_owner(owner: &TeamOwner, invite_cfg: &InviteRuntimeConfig) -> Result<()> {
    let client = rquest::Client::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let resp = client
        .post("https://chatgpt.com/backend-api/accounts/deactivate")
        .header("Authorization", format!("Bearer {}", owner.access_token))
        .header("Content-Type", "application/json")
        .header("User-Agent", &invite_cfg.user_agent)
        .header("Origin", "https://chatgpt.com")
        .header("Referer", "https://chatgpt.com/")
        .body("{}")
        .send()
        .await?;

    let status = resp.status();
    if status.is_success() {
        Ok(())
    } else {
        let body = resp.text().await.unwrap_or_default();
        bail!(
            "HTTP {}: {}",
            status.as_u16(),
            if body.len() > 200 {
                &body[..200]
            } else {
                &body
            }
        )
    }
}

// ─── 完整邀请工作流 ──────────────────────────────────────────────────────────

/// 单个 Owner 的完整邀请工作流
///
/// - `max_members`: team 最大成员数（如 4），用于判断满员；传 0 则自动使用 seeds.len()
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
    max_members: usize,
) {
    let target = seeds.len();
    let seat_cap = if max_members > 0 { max_members } else { target };
    broadcast_log(&format!(
        "[邀请] 开始: owner={} 邀请 {} 个邮箱 (seat_cap={})",
        owner.email, target, seat_cap
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

    // ─── 初始化服务（所有轮次共用）─────────────────────────────────────────
    let register_runtime = cfg.register_runtime();
    let codex_runtime = cfg.codex_runtime();
    let proxy_pool = if cfg.proxy_enabled.unwrap_or(true) {
        Arc::new(ProxyPool::with_refresh_urls(
            cfg.proxy_pool.clone(),
            cfg.proxy_refresh_urls.clone(),
        ))
    } else {
        broadcast_log("[邀请] 代理池已禁用，使用直连模式");
        Arc::new(ProxyPool::new(vec![]))
    };

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

    // ─── 累计统计（跨轮次）───────────────────────────────────────────────
    let mut cum_invited_ok = 0usize;
    let mut cum_invited_failed = 0usize;
    let mut cum_reg_ok = 0usize;
    let mut cum_reg_failed = 0usize;
    let mut cum_rt_ok = 0usize;
    let mut cum_rt_failed = 0usize;
    let mut cum_s2a_ok = 0usize;
    let mut cum_s2a_failed = 0usize;
    // 跟踪已成功入库的邮箱，恢复路径中排除重复入库
    let mut s2a_pushed_emails: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    const MAX_ROUNDS: usize = 3;

    enum WorkerOutcome {
        RegFailed,
        RtFailed,
        Success {
            email_db_id: i64,
            account_with_rt: crate::models::AccountWithRt,
        },
    }

    // ─── 重试循环（最多 3 轮）───────────────────────────────────────────
    for round in 0..MAX_ROUNDS {
        let deficit = target - cum_s2a_ok;
        if deficit == 0 {
            break;
        }

        // ─── 检查 team 成员状态 ──────────────────────────────────────────
        let existing_members = match fetch_team_members(&owner, &invite_cfg).await {
            Ok(m) => {
                broadcast_log(&format!(
                    "[检查] team 当前 {} 个标准用户 (满员={})",
                    m.len(),
                    seat_cap
                ));
                m
            }
            Err(e) => {
                broadcast_log(&format!("[检查] 查询 team 成员失败: {e:#}, 继续正常流程"));
                Vec::new()
            }
        };

        // ─── 查询并清理 pending invites，释放被占用的席位 ──────────────────
        match fetch_pending_invites(&owner, &invite_cfg).await {
            Ok(pending) if !pending.is_empty() => {
                broadcast_log(&format!(
                    "[清理] 发现 {} 个 pending invites，开始清理释放席位",
                    pending.len()
                ));
                delete_pending_invites(&owner, &pending, &invite_cfg).await;
                broadcast_log(&format!("[清理] pending invites 清理完成"));
            }
            Ok(_) => {
                broadcast_log("[检查] 无 pending invites");
            }
            Err(e) => {
                broadcast_log(&format!(
                    "[检查] 查询 pending invites 失败: {e:#}, 继续正常流程"
                ));
            }
        }

        // 重新计算可用席位（仅计 existing members，pending 已清理）
        let max_seats = seat_cap;
        let available_seats = max_seats.saturating_sub(existing_members.len());

        // ─── 满员恢复路径：跳过邀请+注册，直接 RT → S2A ─────────────────
        if available_seats == 0 && !existing_members.is_empty() {
            broadcast_log(&format!(
                "[恢复] team 已满员 ({}/{}), 尝试对已有成员直接获取 RT + S2A",
                existing_members.len(),
                max_seats
            ));

            let member_emails: Vec<String> =
                existing_members.iter().map(|m| m.email.clone()).collect();
            let known_passwords = db
                .find_passwords_by_emails(&member_emails)
                .unwrap_or_default();

            if known_passwords.is_empty() {
                broadcast_log("[恢复] DB 中未找到已有成员的密码记录，无法恢复");
                break;
            }

            let pw_map: std::collections::HashMap<String, String> =
                known_passwords.into_iter().collect();

            // 为已有成员构造 RegisteredAccount，批量获取 RT
            let mut recovery_accounts: Vec<crate::models::AccountWithRt> = Vec::new();

            for member in &existing_members {
                // 跳过已在本轮成功入库的账号，避免重复推送
                if s2a_pushed_emails.contains(&member.email.to_lowercase()) {
                    broadcast_log(&format!(
                        "[恢复] 跳过 {} (已入库)",
                        member.email
                    ));
                    continue;
                }

                if let Some(password) = pw_map.get(&member.email) {
                    // 住宅代理：每次 RT 请求前刷新 IP
                    if proxy_pool.has_refresh_urls() {
                        proxy_pool.refresh_all_and_wait().await;
                    }
                    let proxy = proxy_pool.next();
                    let proxy_label = proxy
                        .as_deref()
                        .map(|p| mask_proxy(p))
                        .unwrap_or_else(|| "直连".to_string());

                    let fake_registered = crate::models::RegisteredAccount {
                        account: member.email.clone(),
                        password: password.clone(),
                        token: String::new(),
                        account_id: owner.account_id.clone(),
                        plan_type: "team".to_string(),
                        proxy: None,
                    };

                    broadcast_log(&format!(
                        "[恢复-RT] 获取 RT: {} (代理: {})",
                        member.email, proxy_label
                    ));
                    let rt_result = codex_service
                        .fetch_refresh_token(&fake_registered, proxy, 1)
                        .await;
                    match rt_result {
                        Ok(rt) => {
                            cum_rt_ok += 1;
                            progress.rt_ok.fetch_add(1, Ordering::Relaxed);
                            broadcast_log(&format!(
                                "[恢复-RT成功] {} (代理: {})",
                                member.email, proxy_label
                            ));
                            recovery_accounts.push(crate::models::AccountWithRt {
                                account: member.email.clone(),
                                password: password.clone(),
                                token: fake_registered.token,
                                account_id: owner.account_id.clone(),
                                plan_type: "team".to_string(),
                                refresh_token: rt,
                            });
                        }
                        Err(e) => {
                            cum_rt_failed += 1;
                            progress.rt_failed.fetch_add(1, Ordering::Relaxed);
                            broadcast_log(&format!(
                                "[恢复-RT失败] {} (代理: {}): {e:#}",
                                member.email, proxy_label
                            ));
                        }
                    }
                }
            }

            // S2A 入库
            if push_s2a && !recovery_accounts.is_empty() {
                if let Some(ref team_cfg) = team {
                    progress.set_stage("S2A 入库");
                    broadcast_log(&format!(
                        "[恢复-S2A] 准备入库 {} 个恢复账号到 {}",
                        recovery_accounts.len(),
                        team_cfg.name
                    ));
                    let workflow_runner = crate::workflow::WorkflowRunner::new(
                        register_service.clone(),
                        codex_service.clone(),
                        s2a_service.clone(),
                        proxy_pool.clone(),
                    );
                    let (ok, failed) = workflow_runner
                        .push_to_s2a(team_cfg, recovery_accounts, None)
                        .await;
                    cum_s2a_ok += ok;
                    cum_s2a_failed += failed;
                    progress.s2a_ok.fetch_add(ok, Ordering::Relaxed);
                    progress.s2a_failed.fetch_add(failed, Ordering::Relaxed);
                }
            }

            // 更新任务进度
            let _ = db.enqueue_update_invite_task(
                task_id.clone(),
                InviteTaskUpdate {
                    rt_ok: Some(cum_rt_ok),
                    rt_failed: Some(cum_rt_failed),
                    s2a_ok: Some(cum_s2a_ok),
                    s2a_failed: Some(cum_s2a_failed),
                    ..Default::default()
                },
            );

            if cum_s2a_ok >= target {
                broadcast_log(&format!("[目标达成] 恢复入库 {}/{}", cum_s2a_ok, target));
            }
            break; // 恢复路径完成，退出循环
        }

        // ─── 正常路径：邀请 → 注册 → RT → S2A ──────────────────────────
        let invite_count = deficit.min(available_seats.max(deficit)); // 如果查询失败，available_seats=0 但 deficit>0

        // 生成本轮种子
        let round_seeds: Vec<crate::models::AccountSeed> = if round == 0 {
            // 首轮：如果可用席位 < 初始种子数，只取前 N 个
            if available_seats > 0 && available_seats < seeds.len() {
                seeds[..available_seats].to_vec()
            } else {
                seeds.clone()
            }
        } else {
            broadcast_log(&format!(
                "[补邀] ── 第{}轮补充 (已入库 {}/{}, 还差 {}) ──",
                round + 1,
                cum_s2a_ok,
                target,
                invite_count
            ));
            (0..invite_count)
                .map(|_| crate::util::generate_account_seed(&cfg.email_domains))
                .collect()
        };

        if round_seeds.is_empty() {
            broadcast_log("[跳过] 无可邀请席位");
            break;
        }

        // 补轮时写入新邮箱记录到 DB
        if round > 0 {
            let email_inserts: Vec<crate::db::InviteEmailInsert> = round_seeds
                .iter()
                .map(|s| crate::db::InviteEmailInsert {
                    email: s.account.clone(),
                    password: s.password.clone(),
                })
                .collect();
            let _ = db.enqueue_insert_invite_emails(task_id.clone(), email_inserts);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let round_total = round_seeds.len();

        // ─── 阶段 1: 邀请 ───────────────────────────────────────────────
        progress.set_stage("邀请中");
        let emails: Vec<String> = round_seeds.iter().map(|s| s.account.clone()).collect();
        let invite_results = invite_emails(&owner, &emails, &invite_cfg).await;

        let mut invited_ok = 0usize;
        let mut invited_failed = 0usize;
        let mut invited_emails_ok: Vec<(usize, i64)> = Vec::new();

        // 获取最新的 email 记录（含本轮新增的）
        let email_records = db
            .get_invite_task_detail(&task_id)
            .ok()
            .flatten()
            .map(|d| d.emails)
            .unwrap_or_default();

        // 本轮的 email 记录在列表末尾
        let round_email_offset = email_records.len().saturating_sub(round_total);

        for (i, result) in invite_results.iter().enumerate() {
            let email_db_id = email_records
                .get(round_email_offset + i)
                .map(|r| r.id)
                .unwrap_or(0);
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
            }
        }

        cum_invited_ok += invited_ok;
        cum_invited_failed += invited_failed;

        if invited_emails_ok.is_empty() {
            // 检测致命错误：如果任一邀请失败原因是席位上限，则不再重试
            let has_seat_limit_error = invite_results.iter().any(|r| {
                r.error
                    .as_deref()
                    .map(|e| {
                        e.contains("maximum number of seats")
                            || e.contains("seats allowed")
                            || e.contains("seat limit")
                    })
                    .unwrap_or(false)
            });
            if has_seat_limit_error {
                broadcast_log("[邀请] 检测到席位上限错误(free trial?)，停止重试");
                // 标记该 owner 为 seat_limited，后续批量补位将自动跳过
                let now = chrono::Utc::now().to_rfc3339();
                match db.batch_update_owner_registry_state(
                    &[owner.account_id.clone()],
                    "seat_limited",
                    Some("free_trial_seat_limit"),
                    &now,
                ) {
                    Ok(n) => broadcast_log(&format!(
                        "[邀请] 已标记 owner {} 为 seat_limited (affected={})",
                        owner.account_id, n
                    )),
                    Err(e) => broadcast_log(&format!(
                        "[邀请] 标记 owner {} 为 seat_limited 失败: {e}",
                        owner.account_id
                    )),
                }
                break;
            }
            broadcast_log("[邀请] 本轮所有邮箱邀请均失败");
            continue; // 其他类型失败：进入下一轮重试
        }

        // ─── 等待邀请生效 ────────────────────────────────────────────────
        broadcast_log("[等待] 等待 5s 让邀请在服务端生效...");
        tokio::time::sleep(Duration::from_secs(5)).await;

        // ─── 校验邀请是否真正生效 ──────────────────────────────────────────
        match fetch_pending_invites(&owner, &invite_cfg).await {
            Ok(pending) => {
                let pending_set: std::collections::HashSet<String> = pending
                    .iter()
                    .map(|p| p.email.to_lowercase())
                    .collect();
                broadcast_log(&format!(
                    "[检查] 当前 pending invites: {} 条",
                    pending_set.len()
                ));
                let before = invited_emails_ok.len();
                invited_emails_ok.retain(|&(seed_idx, email_db_id)| {
                    let email_lower = round_seeds[seed_idx].account.to_lowercase();
                    if pending_set.contains(&email_lower) {
                        true
                    } else {
                        broadcast_log(&format!(
                            "[邀请校验] {} 不在 pending invites 中，邀请可能未生效",
                            round_seeds[seed_idx].account
                        ));
                        let _ = db.enqueue_update_invite_email(
                            email_db_id,
                            InviteEmailUpdate {
                                invite_status: Some("unverified".to_string()),
                                error: Some(
                                    "邀请API返回成功但未出现在pending invites中".to_string(),
                                ),
                                ..Default::default()
                            },
                        );
                        invited_failed += 1;
                        invited_ok = invited_ok.saturating_sub(1);
                        false
                    }
                });
                let removed = before - invited_emails_ok.len();
                if removed > 0 {
                    broadcast_log(&format!(
                        "[邀请校验] {}/{} 个邀请已确认，{} 个未通过校验",
                        invited_emails_ok.len(),
                        before,
                        removed
                    ));
                } else {
                    broadcast_log(&format!(
                        "[邀请校验] 全部 {} 个邀请已确认",
                        before
                    ));
                }
            }
            Err(e) => {
                broadcast_log(&format!(
                    "[邀请校验] 查询 pending invites 失败: {e:#}，跳过校验继续执行"
                ));
            }
        }

        if invited_emails_ok.is_empty() {
            broadcast_log("[邀请校验] 所有邮箱均未通过校验，跳过本轮注册");
            continue;
        }

        // ─── 阶段 2: 注册 + RT ──────────────────────────────────────────
        progress.set_stage("注册 + RT");

        // 住宅代理：批次开始前同步刷新 IP，确保本 owner 使用新 IP
        if proxy_pool.has_refresh_urls() {
            broadcast_log("[代理] 刷新住宅代理 IP（等待完成）...");
            proxy_pool.refresh_all_and_wait().await;
        }

        broadcast_log(&format!(
            "[注册+RT] 开始并发处理 {} 个已邀请邮箱",
            invited_emails_ok.len()
        ));

        let concurrency = invited_emails_ok.len();

        let worker_results: Vec<WorkerOutcome> = stream::iter(invited_emails_ok.iter().copied())
            .map(|(seed_idx, email_db_id)| {
                let register_service = register_service.clone();
                let codex_service = codex_service.clone();
                let proxy_pool = proxy_pool.clone();
                let progress = progress.clone();
                let db = db.clone();
                let seed = round_seeds[seed_idx].clone();
                let owner_account_id = owner.account_id.clone();

                async move {
                    let proxy = proxy_pool.next();
                    if let Some(p) = proxy.as_deref() {
                        broadcast_log(&format!(
                            "[邀请注册] {} 使用代理: {}",
                            seed.account,
                            mask_proxy(p)
                        ));
                    }

                    // ── 注册（最多 3 次尝试，退避 2s → 5s）──
                    const MAX_REG_RETRIES: usize = 3;
                    const REG_DELAYS: [u64; 3] = [0, 2000, 5000];
                    let mut registered = None;
                    let mut last_reg_err = String::new();

                    for attempt in 0..MAX_REG_RETRIES {
                        if attempt > 0 {
                            tokio::time::sleep(Duration::from_millis(REG_DELAYS[attempt])).await;
                            broadcast_log(&format!(
                                "[注册重试] {} (第{}/{}次)",
                                seed.account,
                                attempt + 1,
                                MAX_REG_RETRIES
                            ));
                        }

                        let input = crate::services::RegisterInput {
                            seed: seed.clone(),
                            proxy: proxy.clone(),
                            worker_id: seed_idx + 1,
                            task_index: seed_idx,
                            task_total: round_total,
                            skip_payment: true,
                        };

                        match register_service.register(input).await {
                            Ok(acc) => {
                                registered = Some(acc);
                                break;
                            }
                            Err(e) => {
                                last_reg_err = format!("{e:#}");
                            }
                        }
                    }

                    let proxy_label = proxy
                        .as_deref()
                        .map(|p| mask_proxy(p))
                        .unwrap_or_else(|| "直连".to_string());
                    let acc = match registered {
                        Some(acc) => acc,
                        None => {
                            progress.reg_failed.fetch_add(1, Ordering::Relaxed);
                            let _ = db.enqueue_update_invite_email(
                                email_db_id,
                                InviteEmailUpdate {
                                    reg_status: Some("failed".to_string()),
                                    error: Some(last_reg_err.clone()),
                                    ..Default::default()
                                },
                            );
                            broadcast_log(&format!(
                                "[注册失败] {} (代理: {}): {}",
                                seed.account, proxy_label, last_reg_err
                            ));
                            return WorkerOutcome::RegFailed;
                        }
                    };

                    progress.reg_ok.fetch_add(1, Ordering::Relaxed);
                    let _ = db.enqueue_update_invite_email(
                        email_db_id,
                        InviteEmailUpdate {
                            reg_status: Some("ok".to_string()),
                            ..Default::default()
                        },
                    );
                    broadcast_log(&format!(
                        "[注册成功] {} plan={} (代理: {})",
                        acc.account, acc.plan_type, proxy_label
                    ));

                    // 强制使用 owner 的 team account_id 进行 RT workspace 选择
                    // 如果邀请未生效，workspace/select 会直接失败，提前暴露问题
                    let acc = crate::models::RegisteredAccount {
                        account_id: owner_account_id.clone(),
                        ..acc
                    };

                    // ── RT（最多 3 次尝试，退避 1s → 3s）──
                    const MAX_RT_RETRIES: usize = 3;
                    const RT_DELAYS: [u64; 3] = [0, 1000, 3000];
                    let mut rt_result = None;
                    let mut last_rt_err = String::new();

                    for attempt in 0..MAX_RT_RETRIES {
                        if attempt > 0 {
                            tokio::time::sleep(Duration::from_millis(RT_DELAYS[attempt])).await;
                            broadcast_log(&format!(
                                "[RT重试] {} (第{}/{}次)",
                                acc.account,
                                attempt + 1,
                                MAX_RT_RETRIES
                            ));
                        }

                        match codex_service
                            .fetch_refresh_token(&acc, proxy.clone(), seed_idx + 1)
                            .await
                        {
                            Ok(rt) => {
                                rt_result = Some(rt);
                                break;
                            }
                            Err(e) => {
                                last_rt_err = format!("{e:#}");
                            }
                        }
                    }

                    match rt_result {
                        Some(rt) => {
                            progress.rt_ok.fetch_add(1, Ordering::Relaxed);
                            let _ = db.enqueue_update_invite_email(
                                email_db_id,
                                InviteEmailUpdate {
                                    rt_status: Some("ok".to_string()),
                                    refresh_token: Some(rt.clone()),
                                    ..Default::default()
                                },
                            );
                            broadcast_log(&format!("[RT成功] {}", acc.account));

                            WorkerOutcome::Success {
                                email_db_id,
                                account_with_rt: crate::models::AccountWithRt {
                                    account: acc.account,
                                    password: acc.password,
                                    token: acc.token,
                                    account_id: owner_account_id.clone(), // 强制使用 owner 的 team account_id
                                    plan_type: acc.plan_type, // 保留真实值，用于判断是否入 S2A
                                    refresh_token: rt,
                                },
                            }
                        }
                        None => {
                            progress.rt_failed.fetch_add(1, Ordering::Relaxed);
                            let _ = db.enqueue_update_invite_email(
                                email_db_id,
                                InviteEmailUpdate {
                                    rt_status: Some("failed".to_string()),
                                    error: Some(last_rt_err.clone()),
                                    ..Default::default()
                                },
                            );
                            broadcast_log(&format!("[RT失败] {}: {}", acc.account, last_rt_err));
                            WorkerOutcome::RtFailed
                        }
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect()
            .await;

        // 统计本轮结果
        let mut round_reg_ok = 0usize;
        let mut round_reg_failed = 0usize;
        let mut round_rt_ok = 0usize;
        let mut round_rt_failed = 0usize;
        let mut accounts_for_s2a: Vec<crate::models::AccountWithRt> = Vec::new();
        let mut s2a_email_db_ids: Vec<i64> = Vec::new();

        for outcome in worker_results {
            match outcome {
                WorkerOutcome::RegFailed => round_reg_failed += 1,
                WorkerOutcome::RtFailed => {
                    round_reg_ok += 1;
                    round_rt_failed += 1;
                }
                WorkerOutcome::Success {
                    email_db_id,
                    account_with_rt,
                } => {
                    round_reg_ok += 1;
                    round_rt_ok += 1;
                    // free 账户说明邀请未生效，token 无 team 权限，跳过 S2A
                    if account_with_rt.plan_type.eq_ignore_ascii_case("free") {
                        broadcast_log(&format!(
                            "[跳过S2A] {} plan=free，邀请未生效",
                            account_with_rt.account
                        ));
                        let _ = db.enqueue_update_invite_email(
                            email_db_id,
                            InviteEmailUpdate {
                                s2a_status: Some("skipped_free".to_string()),
                                ..Default::default()
                            },
                        );
                    } else {
                        s2a_email_db_ids.push(email_db_id);
                        accounts_for_s2a.push(account_with_rt);
                    }
                }
            }
        }

        cum_reg_ok += round_reg_ok;
        cum_reg_failed += round_reg_failed;
        cum_rt_ok += round_rt_ok;
        cum_rt_failed += round_rt_failed;

        // ─── S2A 入库 ───────────────────────────────────────────────────
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

                // 记录已成功入库的邮箱，防止恢复路径重复入库
                for (idx, acc) in accounts_for_s2a.iter().enumerate() {
                    if idx < ok {
                        s2a_pushed_emails.insert(acc.account.to_lowercase());
                    }
                }

                for (idx, &email_db_id) in s2a_email_db_ids.iter().enumerate() {
                    let status = if idx < ok { "ok" } else { "failed" };
                    let _ = db.enqueue_update_invite_email(
                        email_db_id,
                        InviteEmailUpdate {
                            s2a_status: Some(status.to_string()),
                            ..Default::default()
                        },
                    );
                }

                cum_s2a_ok += ok;
                cum_s2a_failed += failed;
                progress.s2a_ok.fetch_add(ok, Ordering::Relaxed);
                progress.s2a_failed.fetch_add(failed, Ordering::Relaxed);
            }
        }

        // 更新任务进度（每轮结束后刷新）
        let _ = db.enqueue_update_invite_task(
            task_id.clone(),
            InviteTaskUpdate {
                invited_ok: Some(cum_invited_ok),
                invited_failed: Some(cum_invited_failed),
                reg_ok: Some(cum_reg_ok),
                reg_failed: Some(cum_reg_failed),
                rt_ok: Some(cum_rt_ok),
                rt_failed: Some(cum_rt_failed),
                s2a_ok: Some(cum_s2a_ok),
                s2a_failed: Some(cum_s2a_failed),
                ..Default::default()
            },
        );

        // 检查是否达成目标
        if cum_s2a_ok >= target {
            broadcast_log(&format!("[目标达成] 已成功入库 {}/{}", cum_s2a_ok, target));
            break;
        }

        // 最后一轮不再 continue
        if round + 1 < MAX_ROUNDS {
            broadcast_log(&format!(
                "[未达标] 本轮入库 {}, 累计 {}/{}, 将进入下一轮",
                cum_s2a_ok - (cum_s2a_ok.saturating_sub(cum_s2a_ok)), // 本轮新增
                cum_s2a_ok,
                target
            ));
        }
    } // end retry loop

    // ─── 最终判断 ──────────────────────────────────────────────────────────
    let has_useful_output = if push_s2a {
        cum_s2a_ok > 0
    } else {
        cum_rt_ok > 0
    };

    if !has_useful_output {
        let _ = db.enqueue_reset_owner_used(owner_db_id);
        broadcast_log(&format!("[回退] {} 无有效产出，已恢复为可用", owner.email));
    }

    // ─── 完成 ────────────────────────────────────────────────────────────────
    let task_status = if has_useful_output {
        "completed"
    } else {
        "failed"
    };
    let _ = db.enqueue_update_invite_task(
        task_id.clone(),
        InviteTaskUpdate {
            status: Some(task_status.to_string()),
            invited_ok: Some(cum_invited_ok),
            invited_failed: Some(cum_invited_failed),
            reg_ok: Some(cum_reg_ok),
            reg_failed: Some(cum_reg_failed),
            rt_ok: Some(cum_rt_ok),
            rt_failed: Some(cum_rt_failed),
            s2a_ok: Some(cum_s2a_ok),
            s2a_failed: Some(cum_s2a_failed),
            finished_at: Some(crate::util::beijing_now().to_rfc3339()),
            ..Default::default()
        },
    );

    let label = if cum_s2a_ok >= target {
        "邀请完成"
    } else if has_useful_output {
        "部分完成"
    } else {
        "邀请失败"
    };
    broadcast_log(&format!(
        "[{}] owner={} | 邀请: {}/{} | 注册: {}/{} | RT: {}/{} | S2A: {}/{}",
        label,
        owner.email,
        cum_invited_ok,
        cum_invited_ok + cum_invited_failed,
        cum_reg_ok,
        cum_invited_ok,
        cum_rt_ok,
        cum_reg_ok,
        cum_s2a_ok,
        cum_rt_ok,
    ));
}
