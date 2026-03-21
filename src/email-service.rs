use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::sync::Semaphore;

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use regex::Regex;
use rquest::StatusCode;
use serde::Serialize;
use tokio::sync::oneshot;
use tokio::time::sleep;

static CODE_REGEX: OnceLock<Regex> = OnceLock::new();

fn code_regex() -> &'static Regex {
    CODE_REGEX.get_or_init(|| Regex::new(r"\b(\d{6})\b").unwrap())
}

// ==================== D1 过载自动清理 ====================

static D1_CLEANUP_CONFIG: OnceLock<crate::config::D1CleanupConfig> = OnceLock::new();
static D1_CLEANUP_RUNNING: AtomicBool = AtomicBool::new(false);
/// 上次清理完成的 UNIX 时间戳（秒）
static D1_CLEANUP_LAST: AtomicU64 = AtomicU64::new(0);
/// 防抖冷却时间（秒）
const D1_CLEANUP_COOLDOWN_SECS: u64 = 60;

/// 在程序启动时调用，注入 D1 清理配置
pub fn set_d1_cleanup_config(config: crate::config::D1CleanupConfig) {
    let _ = D1_CLEANUP_CONFIG.set(config);
}

/// 检测到 D1 过载时触发清理（带防抖）
async fn trigger_d1_cleanup_if_needed() {
    // 检查是否已配置且启用
    let Some(config) = D1_CLEANUP_CONFIG.get() else {
        return;
    };
    if !config.enabled.unwrap_or(false) {
        return;
    }

    // 检查冷却时间
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last = D1_CLEANUP_LAST.load(Ordering::Relaxed);
    if now.saturating_sub(last) < D1_CLEANUP_COOLDOWN_SECS {
        return;
    }

    // CAS：确保只有一个任务在执行清理
    if D1_CLEANUP_RUNNING
        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
        .is_err()
    {
        return;
    }

    let config = config.clone();
    tokio::spawn(async move {
        crate::log_broadcast::broadcast_log("[D1-AUTO] 检测到 D1 过载，自动触发邮箱清理...");
        match crate::d1_cleanup::run_cleanup(&config).await {
            Ok(()) => {
                crate::log_broadcast::broadcast_log("[D1-AUTO] D1 邮箱清理完成");
            }
            Err(e) => {
                crate::log_broadcast::broadcast_log(&format!("[D1-AUTO] D1 清理失败: {e}"));
            }
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        D1_CLEANUP_LAST.store(now, Ordering::Relaxed);
        D1_CLEANUP_RUNNING.store(false, Ordering::SeqCst);
    });
}

/// 后台邮箱轮询句柄
pub struct EmailCodeHandle {
    result_rx: oneshot::Receiver<Result<String>>,
    cancel_tx: Option<oneshot::Sender<()>>,
}

impl EmailCodeHandle {
    pub async fn wait(&mut self) -> std::result::Result<Result<String>, oneshot::error::RecvError> {
        (&mut self.result_rx).await
    }

    pub fn cancel(&mut self) {
        if let Some(tx) = self.cancel_tx.take() {
            let _ = tx.send(());
        }
    }
}

#[derive(Debug, Clone)]
pub struct EmailServiceConfig {
    pub mail_api_base: String,
    pub mail_api_path: String,
    pub mail_api_token: String,
    pub request_timeout_sec: u64,
}

#[derive(Debug, Clone)]
pub struct WaitCodeOptions {
    pub after_email_id: i64,
    /// 启动后台轮询前先拉取一次最新邮件 ID 作为基线，减少一次无效首轮抓取
    pub prefetch_latest_email_id: bool,
    pub max_retries: usize,
    pub interval_ms: u64,
    pub max_interval_ms: u64,
    pub interval_backoff_step_ms: u64,
    pub size: usize,
    pub subject_must_contain_code_word: bool,
    pub strict_fetch: bool,
}

#[derive(Debug, Clone)]
pub struct EmailMessage {
    pub email_id: i64,
    pub subject: String,
    pub content: String,
    pub text: String,
}

#[async_trait]
pub trait EmailProvider: Send + Sync {
    async fn fetch_emails(
        &self,
        target_email: &str,
        size: usize,
        proxy: Option<&str>,
    ) -> Result<Vec<EmailMessage>>;

    /// 自动生成邮箱地址（仅部分 provider 支持）
    async fn generate_email(&self, _proxy: Option<&str>) -> Result<Option<String>> {
        Ok(None)
    }
}

#[derive(Debug)]
pub struct HttpEmailProvider {
    cfg: EmailServiceConfig,
    clients: Mutex<HashMap<String, rquest::Client>>,
}

impl HttpEmailProvider {
    pub fn new(cfg: EmailServiceConfig) -> Self {
        Self {
            cfg,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn build_client(&self, proxy: Option<&str>) -> Result<rquest::Client> {
        let timeout_sec = self.cfg.request_timeout_sec.max(5);
        let connect_timeout_sec = (timeout_sec / 2).clamp(3, 12);
        let mut builder = rquest::Client::builder()
            .timeout(Duration::from_secs(timeout_sec))
            .connect_timeout(Duration::from_secs(connect_timeout_sec))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .pool_max_idle_per_host(8)
            .no_proxy();
        if let Some(p) = proxy {
            builder = builder.proxy(rquest::Proxy::all(p)?);
        }
        Ok(builder.build()?)
    }

    fn get_client(&self, proxy: Option<&str>) -> Result<rquest::Client> {
        let key = proxy.unwrap_or_default().to_string();
        {
            let cache = self
                .clients
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some(client) = cache.get(&key) {
                return Ok(client.clone());
            }
        }

        let client = self.build_client(proxy)?;
        let mut cache = self
            .clients
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        Ok(cache.entry(key).or_insert_with(|| client.clone()).clone())
    }
}

#[async_trait]
impl EmailProvider for HttpEmailProvider {
    async fn fetch_emails(
        &self,
        target_email: &str,
        size: usize,
        proxy: Option<&str>,
    ) -> Result<Vec<EmailMessage>> {
        let client = self.get_client(proxy)?;
        let url = format!(
            "{}{}",
            self.cfg.mail_api_base.trim_end_matches('/'),
            self.cfg.mail_api_path
        );
        let payload = EmailListRequest {
            to_email: target_email.to_string(),
            time_sort: "desc".to_string(),
            size,
        };

        let resp = client
            .post(url)
            .header("Authorization", self.cfg.mail_api_token.clone())
            .json(&payload)
            .send()
            .await?;
        let status = resp.status();
        if status != StatusCode::OK {
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "验证码接口状态异常: HTTP {} {}",
                status,
                truncate_text(&body, 160)
            );
        }

        let raw = resp
            .text()
            .await
            .context("邮箱接口响应读取失败（网络或编码异常）")?;
        let json: serde_json::Value = serde_json::from_str(&raw)
            .with_context(|| format!("邮箱接口响应不是 JSON: {}", truncate_text(&raw, 160)))?;

        let code = json.get("code").and_then(|v| v.as_i64()).unwrap_or(-1);
        if code != 200 {
            // 检测 D1 过载错误，自动触发清理
            if code == 500 {
                if let Some(msg) = json.get("message").and_then(|v| v.as_str()) {
                    if msg.contains("D1_ERROR") || msg.contains("D1 DB is overloaded") {
                        trigger_d1_cleanup_if_needed().await;
                    }
                }
            }
            bail!(
                "验证码接口返回错误码: {} {}",
                code,
                truncate_text(&raw, 160)
            );
        }

        let mut out = Vec::new();
        if let Some(items) = json.get("data").and_then(|v| v.as_array()) {
            out.reserve(items.len());
            for item in items {
                out.push(EmailMessage {
                    email_id: item
                        .get("emailId")
                        .or_else(|| item.get("email_id"))
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0),
                    subject: item
                        .get("subject")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    content: item
                        .get("content")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    text: item
                        .get("text")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                });
            }
        }

        Ok(out)
    }
}

// ==================== ChatgptOrgUk Provider ====================

#[derive(Debug)]
pub struct ChatgptOrgUkProvider {
    api_base: String,
    api_key: String,
    domains: Vec<String>,
    clients: Mutex<HashMap<String, rquest::Client>>,
}

impl ChatgptOrgUkProvider {
    pub fn new(api_key: String, domains: Vec<String>) -> Self {
        Self {
            api_base: "https://mail.chatgpt.org.uk".to_string(),
            api_key,
            domains,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn build_client(&self, proxy: Option<&str>) -> Result<rquest::Client> {
        let mut builder = rquest::Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(8))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .pool_max_idle_per_host(8)
            .no_proxy();
        if let Some(p) = proxy {
            builder = builder.proxy(rquest::Proxy::all(p)?);
        }
        Ok(builder.build()?)
    }

    fn get_client(&self, proxy: Option<&str>) -> Result<rquest::Client> {
        let key = proxy.unwrap_or_default().to_string();
        {
            let cache = self
                .clients
                .lock()
                .unwrap_or_else(|poison| poison.into_inner());
            if let Some(client) = cache.get(&key) {
                return Ok(client.clone());
            }
        }
        let client = self.build_client(proxy)?;
        let mut cache = self
            .clients
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        Ok(cache.entry(key).or_insert_with(|| client.clone()).clone())
    }
}

#[async_trait]
impl EmailProvider for ChatgptOrgUkProvider {
    async fn fetch_emails(
        &self,
        target_email: &str,
        _size: usize,
        proxy: Option<&str>,
    ) -> Result<Vec<EmailMessage>> {
        let client = self.get_client(proxy)?;
        let url = format!(
            "{}/api/emails?email={}",
            self.api_base,
            urlencoding::encode(target_email)
        );
        let resp = client
            .get(&url)
            .header("X-API-Key", &self.api_key)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await?;
        if resp.status() != StatusCode::OK {
            let body = resp.text().await.unwrap_or_default();
            bail!("chatgpt.org.uk 邮件接口异常: {}", truncate_text(&body, 160));
        }
        let json: serde_json::Value = resp.json().await.context("chatgpt.org.uk 响应解析失败")?;
        let mut out = Vec::new();
        if let Some(emails) = json
            .get("data")
            .and_then(|d| d.get("emails"))
            .and_then(|e| e.as_array())
        {
            for (idx, item) in emails.iter().enumerate() {
                let from = item
                    .get("from_address")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                // 仅保留 openai 发送的邮件
                if !from.to_lowercase().contains("openai") {
                    continue;
                }
                out.push(EmailMessage {
                    email_id: (idx + 1) as i64,
                    subject: item
                        .get("subject")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    content: item
                        .get("html_content")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                    text: item
                        .get("text")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default()
                        .to_string(),
                });
            }
        }
        Ok(out)
    }

    async fn generate_email(&self, proxy: Option<&str>) -> Result<Option<String>> {
        let client = self.get_client(proxy)?;
        let url = format!("{}/api/generate-email", self.api_base);

        let resp = if self.domains.is_empty() {
            // 无域名配置：GET 完全随机
            client
                .get(&url)
                .header("X-API-Key", &self.api_key)
                .header("User-Agent", "Mozilla/5.0")
                .send()
                .await?
        } else {
            // 从配置的域名列表中随机选一个
            use rand::Rng;
            let idx = rand::rng().random_range(0..self.domains.len());
            let domain = self.domains[idx].trim_start_matches('@').to_string();
            client
                .post(&url)
                .header("X-API-Key", &self.api_key)
                .header("User-Agent", "Mozilla/5.0")
                .header("Content-Type", "application/json")
                .body(serde_json::json!({"domain": domain}).to_string())
                .send()
                .await?
        };
        if resp.status() != StatusCode::OK {
            bail!("chatgpt.org.uk 生成邮箱失败: HTTP {}", resp.status());
        }
        let json: serde_json::Value = resp
            .json()
            .await
            .context("chatgpt.org.uk 生成邮箱响应解析失败")?;
        let email = json
            .get("data")
            .and_then(|d| d.get("email"))
            .and_then(|e| e.as_str())
            .ok_or_else(|| anyhow::anyhow!("chatgpt.org.uk 返回中无 email 字段"))?;
        Ok(Some(email.to_string()))
    }
}

// ==================== TempMail Provider ====================

/// TempMail 临时邮箱提供商（https://mail.123nhh.de）
///
/// 通过 Bearer token 认证，支持 `generate_email` 创建随机邮箱。
/// 邮件按 mailbox UUID 查询，内部维护 email → mailbox_id 映射缓存。
pub struct TempMailProvider {
    api_base: String,
    api_key: String,
    domains: Vec<String>,
    /// email_address → mailbox_id 缓存
    mailbox_cache: Mutex<HashMap<String, String>>,
    clients: Mutex<HashMap<String, rquest::Client>>,
}

impl std::fmt::Debug for TempMailProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TempMailProvider")
            .field("api_base", &self.api_base)
            .finish()
    }
}

impl TempMailProvider {
    pub fn new(api_key: String, domains: Vec<String>) -> Self {
        Self {
            api_base: "https://mail.123nhh.de".to_string(),
            api_key,
            domains,
            mailbox_cache: Mutex::new(HashMap::new()),
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn build_client(&self, proxy: Option<&str>) -> Result<rquest::Client> {
        let mut builder = rquest::Client::builder()
            .timeout(Duration::from_secs(15))
            .connect_timeout(Duration::from_secs(8))
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .pool_max_idle_per_host(8)
            .no_proxy();
        if let Some(p) = proxy {
            builder = builder.proxy(rquest::Proxy::all(p)?);
        }
        Ok(builder.build()?)
    }

    fn get_client(&self, proxy: Option<&str>) -> Result<rquest::Client> {
        let key = proxy.unwrap_or_default().to_string();
        {
            let cache = self.clients.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(c) = cache.get(&key) {
                return Ok(c.clone());
            }
        }
        let client = self.build_client(proxy)?;
        let mut cache = self.clients.lock().unwrap_or_else(|p| p.into_inner());
        Ok(cache.entry(key).or_insert_with(|| client.clone()).clone())
    }

    /// 缓存 email → mailbox_id 映射
    fn cache_mailbox(&self, email: &str, mailbox_id: &str) {
        let mut cache = self.mailbox_cache.lock().unwrap_or_else(|p| p.into_inner());
        cache.insert(email.to_string(), mailbox_id.to_string());
    }

    /// 获取已缓存的 mailbox_id
    fn cached_mailbox_id(&self, email: &str) -> Option<String> {
        self.mailbox_cache
            .lock()
            .ok()
            .and_then(|c| c.get(email).cloned())
    }

    /// 创建临时邮箱
    async fn create_mailbox(
        &self,
        proxy: Option<&str>,
        address: Option<&str>,
        domain: Option<&str>,
    ) -> Result<(String, String)> {
        let client = self.get_client(proxy)?;
        let mut body = serde_json::Map::new();
        if let Some(addr) = address {
            body.insert("address".to_string(), serde_json::Value::String(addr.to_string()));
        }
        if let Some(dom) = domain {
            body.insert("domain".to_string(), serde_json::Value::String(dom.to_string()));
        }

        let resp = client
            .post(format!("{}/api/mailboxes", self.api_base))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .header("Content-Type", "application/json")
            .body(serde_json::Value::Object(body).to_string())
            .send()
            .await
            .context("TempMail POST /api/mailboxes 失败")?;

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            bail!("TempMail 创建邮箱失败: HTTP {} {}", status, truncate_text(&text, 200));
        }

        let json: serde_json::Value = resp.json().await.context("TempMail 创建邮箱响应解析失败")?;
        let mailbox = json.get("mailbox").unwrap_or(&json);
        let id = mailbox
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("TempMail 响应缺少 mailbox.id"))?;
        let full_address = mailbox
            .get("full_address")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("TempMail 响应缺少 mailbox.full_address"))?;

        // 缓存映射
        self.cache_mailbox(full_address, id);

        Ok((full_address.to_string(), id.to_string()))
    }

    /// 通过 mailbox_id 拉取邮件列表
    async fn fetch_mailbox_emails(
        &self,
        mailbox_id: &str,
        proxy: Option<&str>,
    ) -> Result<Vec<EmailMessage>> {
        let client = self.get_client(proxy)?;
        let resp = client
            .get(format!("{}/api/mailboxes/{}/emails?page=1&size=20", self.api_base, mailbox_id))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await
            .context("TempMail GET /api/mailboxes/{id}/emails 失败")?;

        if !resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!("TempMail 邮件列表异常: HTTP {}", truncate_text(&body, 200));
        }

        let json: serde_json::Value = resp.json().await.context("TempMail 邮件列表解析失败")?;
        let items = json.get("data").and_then(|v| v.as_array());
        let mut out = Vec::new();

        if let Some(items) = items {
            for item in items {
                let email_id_str = item
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                // 将 UUID 的前 8 字节 hash 作为 i64 email_id
                let email_id = Self::uuid_to_i64(email_id_str);

                let subject = item
                    .get("subject")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string();

                // 如果 subject 中已包含 6 位验证码，跳过全文获取
                if code_regex().is_match(&subject) {
                    out.push(EmailMessage {
                        email_id,
                        subject,
                        content: String::new(),
                        text: String::new(),
                    });
                    continue;
                }

                // 获取邮件详情
                if !email_id_str.is_empty() {
                    if let Ok(detail) = self
                        .fetch_email_detail(mailbox_id, email_id_str, proxy)
                        .await
                    {
                        out.push(EmailMessage {
                            email_id,
                            subject,
                            content: detail.0,
                            text: detail.1,
                        });
                        continue;
                    }
                }

                out.push(EmailMessage {
                    email_id,
                    subject,
                    content: String::new(),
                    text: String::new(),
                });
            }
        }

        Ok(out)
    }

    /// 获取单封邮件详情
    async fn fetch_email_detail(
        &self,
        mailbox_id: &str,
        email_id: &str,
        proxy: Option<&str>,
    ) -> Result<(String, String)> {
        let client = self.get_client(proxy)?;
        let resp = client
            .get(format!(
                "{}/api/mailboxes/{}/emails/{}",
                self.api_base, mailbox_id, email_id
            ))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await?;

        if !resp.status().is_success() {
            bail!("TempMail 邮件详情 HTTP {}", resp.status());
        }

        let json: serde_json::Value = resp.json().await?;
        let html = json
            .get("html")
            .or_else(|| json.get("content"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let text = json
            .get("text")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        Ok((html, text))
    }

    /// 将 UUID 字符串转为 i64（取前 8 字节 hash），保证唯一性足够用于轮询比较
    fn uuid_to_i64(uuid_str: &str) -> i64 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        uuid_str.hash(&mut hasher);
        // 取绝对值确保正数
        (hasher.finish() as i64).abs()
    }

    /// 查找已创建的邮箱 ID（先查缓存，再列表查询）
    async fn resolve_mailbox_id(
        &self,
        target_email: &str,
        proxy: Option<&str>,
    ) -> Result<String> {
        // 先查缓存
        if let Some(id) = self.cached_mailbox_id(target_email) {
            return Ok(id);
        }

        // 缓存未命中，列出所有邮箱查找
        let client = self.get_client(proxy)?;
        let resp = client
            .get(format!("{}/api/mailboxes?page=1&size=100", self.api_base))
            .header("Authorization", format!("Bearer {}", self.api_key))
            .send()
            .await
            .context("TempMail GET /api/mailboxes 失败")?;

        if !resp.status().is_success() {
            bail!("TempMail 邮箱列表 HTTP {}", resp.status());
        }

        let json: serde_json::Value = resp.json().await?;
        if let Some(items) = json.get("data").and_then(|v| v.as_array()) {
            for item in items {
                let addr = item
                    .get("full_address")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                let id = item
                    .get("id")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default();
                if !addr.is_empty() && !id.is_empty() {
                    self.cache_mailbox(addr, id);
                    if addr == target_email {
                        return Ok(id.to_string());
                    }
                }
            }
        }

        bail!("TempMail 未找到邮箱: {target_email}")
    }
}

#[async_trait]
impl EmailProvider for TempMailProvider {
    async fn fetch_emails(
        &self,
        target_email: &str,
        _size: usize,
        proxy: Option<&str>,
    ) -> Result<Vec<EmailMessage>> {
        let mailbox_id = self.resolve_mailbox_id(target_email, proxy).await?;
        self.fetch_mailbox_emails(&mailbox_id, proxy).await
    }

    async fn generate_email(&self, proxy: Option<&str>) -> Result<Option<String>> {
        // 从配置的域名列表中随机选一个
        let domain = if self.domains.is_empty() {
            None
        } else {
            use rand::Rng;
            let idx = rand::rng().random_range(0..self.domains.len());
            Some(self.domains[idx].trim_start_matches('@').to_string())
        };

        let (email, _mailbox_id) = self
            .create_mailbox(proxy, None, domain.as_deref())
            .await?;
        Ok(Some(email))
    }
}

pub struct EmailService {
    provider: Arc<dyn EmailProvider>,
    /// 限制同时访问邮件 API 的并发数，避免高并发时打垮邮件服务
    mail_semaphore: Arc<Semaphore>,
}

impl EmailService {
    pub fn with_concurrency(provider: Arc<dyn EmailProvider>, max_concurrency: usize) -> Self {
        Self {
            provider,
            mail_semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
        }
    }

    pub fn new_http(cfg: EmailServiceConfig, max_concurrency: usize) -> Self {
        Self::with_concurrency(Arc::new(HttpEmailProvider::new(cfg)), max_concurrency)
    }

    pub fn new_chatgpt_org_uk(
        api_key: String,
        domains: Vec<String>,
        max_concurrency: usize,
    ) -> Self {
        Self::with_concurrency(
            Arc::new(ChatgptOrgUkProvider::new(api_key, domains)),
            max_concurrency,
        )
    }

    pub fn new_tempmail(
        api_key: String,
        domains: Vec<String>,
        max_concurrency: usize,
    ) -> Self {
        Self::with_concurrency(
            Arc::new(TempMailProvider::new(api_key, domains)),
            max_concurrency,
        )
    }

    /// 通过 provider 自动生成邮箱（仅 chatgpt.org.uk 等支持）
    pub async fn generate_email(&self, proxy: Option<&str>) -> Result<Option<String>> {
        self.provider.generate_email(proxy).await
    }

    pub async fn get_latest_email_id(
        &self,
        target_email: &str,
        proxy: Option<&str>,
    ) -> Result<i64> {
        let _permit = self
            .mail_semaphore
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("邮件 API 信号量已关闭"))?;
        let data = self.provider.fetch_emails(target_email, 1, proxy).await?;
        Ok(data.first().map(|v| v.email_id).unwrap_or(0))
    }

    /// 启动后台邮箱轮询，立即返回一个 handle。
    /// 调用方在需要验证码时对 handle `.wait()` 即可获取结果。
    pub async fn start_background_poll(
        &self,
        target_email: &str,
        proxy: Option<&str>,
        options: WaitCodeOptions,
    ) -> Result<EmailCodeHandle> {
        let mut poll_options = options;
        let wait_before_first_poll = poll_options.prefetch_latest_email_id;
        if wait_before_first_poll {
            let _permit = self
                .mail_semaphore
                .acquire()
                .await
                .map_err(|_| anyhow::anyhow!("邮件 API 信号量已关闭"))?;
            let latest = self.provider.fetch_emails(target_email, 1, proxy).await?;
            if let Some(mail) = latest.first() {
                poll_options.after_email_id = poll_options.after_email_id.max(mail.email_id);
            }
            poll_options.prefetch_latest_email_id = false;
        }

        let provider = Arc::clone(&self.provider);
        let semaphore = Arc::clone(&self.mail_semaphore);
        let email = target_email.to_string();
        let proxy_owned = proxy.map(|s| s.to_string());

        let (tx, rx) = oneshot::channel();
        let (cancel_tx, mut cancel_rx) = oneshot::channel();

        tokio::spawn(async move {
            let result = Self::background_poll_loop_with_semaphore(
                provider,
                semaphore,
                &email,
                proxy_owned.as_deref(),
                &poll_options,
                wait_before_first_poll,
                &mut cancel_rx,
            )
            .await;
            let _ = tx.send(result);
        });

        Ok(EmailCodeHandle {
            result_rx: rx,
            cancel_tx: Some(cancel_tx),
        })
    }

    /// 后台轮询循环（带 Semaphore 限流）
    async fn background_poll_loop_with_semaphore(
        provider: Arc<dyn EmailProvider>,
        semaphore: Arc<Semaphore>,
        target_email: &str,
        proxy: Option<&str>,
        options: &WaitCodeOptions,
        wait_before_first_poll: bool,
        cancel_rx: &mut oneshot::Receiver<()>,
    ) -> Result<String> {
        let regex = code_regex();
        let mut current_interval = options.interval_ms.max(200);
        let max_interval = options.max_interval_ms.max(current_interval);
        let interval_backoff_step = options.interval_backoff_step_ms;
        if wait_before_first_poll {
            tokio::select! {
                _ = &mut *cancel_rx => return Err(anyhow!("邮箱轮询已取消")),
                _ = sleep(Duration::from_millis(current_interval)) => {}
            }
            if interval_backoff_step > 0 {
                current_interval = current_interval
                    .saturating_add(interval_backoff_step)
                    .min(max_interval);
            }
        }
        let rounds = options.max_retries.max(1);
        for _ in 0..rounds {
            let permit = tokio::select! {
                _ = &mut *cancel_rx => return Err(anyhow!("邮箱轮询已取消")),
                acquire_result = semaphore.acquire() => {
                    acquire_result.map_err(|_| anyhow!("邮件 API 信号量已关闭"))?
                }
            };
            let result = {
                tokio::select! {
                    _ = &mut *cancel_rx => return Err(anyhow!("邮箱轮询已取消")),
                    fetch_result = Self::try_extract_code_static(&provider, target_email, proxy, options, regex) => fetch_result
                }
            };
            drop(permit);
            match result {
                Ok(Some(code)) => return Ok(code),
                Ok(None) => {}
                Err(err) => {
                    if options.strict_fetch {
                        return Err(err);
                    }
                }
            }
            tokio::select! {
                _ = &mut *cancel_rx => return Err(anyhow!("邮箱轮询已取消")),
                _ = sleep(Duration::from_millis(current_interval)) => {}
            }
            if interval_backoff_step > 0 {
                current_interval = current_interval
                    .saturating_add(interval_backoff_step)
                    .min(max_interval);
            }
        }
        bail!("验证码获取超时")
    }

    /// 静态版本的 try_extract_code，供后台 task 使用（不需要 &self）
    async fn try_extract_code_static(
        provider: &Arc<dyn EmailProvider>,
        target_email: &str,
        proxy: Option<&str>,
        options: &WaitCodeOptions,
        regex: &Regex,
    ) -> Result<Option<String>> {
        let data = provider
            .fetch_emails(target_email, options.size.max(1), proxy)
            .await?;

        for mail in data {
            if mail.email_id <= options.after_email_id {
                continue;
            }
            if let Some(code) =
                extract_code_from_message(&mail, options.subject_must_contain_code_word, regex)
            {
                return Ok(Some(code));
            }
        }

        Ok(None)
    }

    pub async fn wait_for_code(
        &self,
        target_email: &str,
        proxy: Option<&str>,
        options: WaitCodeOptions,
    ) -> Result<String> {
        let regex = code_regex();
        let mut current_interval = options.interval_ms.max(200);
        let max_interval = options.max_interval_ms.max(current_interval);
        let interval_backoff_step = options.interval_backoff_step_ms;
        let rounds = options.max_retries.max(1);
        for _ in 0..rounds {
            let result = {
                let _permit = self
                    .mail_semaphore
                    .acquire()
                    .await
                    .map_err(|_| anyhow::anyhow!("邮件 API 信号量已关闭"))?;
                self.try_extract_code(target_email, proxy, &options, regex)
                    .await
            };
            match result {
                Ok(Some(code)) => return Ok(code),
                Ok(None) => {}
                Err(err) => {
                    if options.strict_fetch {
                        return Err(err);
                    }
                }
            }
            sleep(Duration::from_millis(current_interval)).await;
            if interval_backoff_step > 0 {
                current_interval = current_interval
                    .saturating_add(interval_backoff_step)
                    .min(max_interval);
            }
        }
        bail!("验证码获取超时");
    }

    async fn try_extract_code(
        &self,
        target_email: &str,
        proxy: Option<&str>,
        options: &WaitCodeOptions,
        regex: &Regex,
    ) -> Result<Option<String>> {
        let data = self
            .provider
            .fetch_emails(target_email, options.size.max(1), proxy)
            .await?;

        for mail in data {
            if mail.email_id <= options.after_email_id {
                continue;
            }
            if let Some(code) =
                extract_code_from_message(&mail, options.subject_must_contain_code_word, regex)
            {
                return Ok(Some(code));
            }
        }

        Ok(None)
    }
}

fn extract_code_from_message(
    mail: &EmailMessage,
    subject_must_contain_code_word: bool,
    regex: &Regex,
) -> Option<String> {
    if subject_must_contain_code_word && !mail.subject.to_lowercase().contains("code") {
        return None;
    }
    for candidate in [&mail.subject, &mail.text, &mail.content] {
        if let Some(caps) = regex.captures(candidate)
            && let Some(code) = caps.get(1)
        {
            return Some(code.as_str().to_string());
        }
    }
    None
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EmailListRequest {
    to_email: String,
    time_sort: String,
    size: usize,
}

fn truncate_text(text: &str, max_len: usize) -> &str {
    if text.len() <= max_len {
        return text;
    }
    let mut end = max_len;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}
