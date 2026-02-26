use std::{
    collections::HashMap,
    sync::{Arc, Mutex, OnceLock},
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
    pub max_retries: usize,
    pub interval_ms: u64,
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
    clients: Mutex<HashMap<String, rquest::Client>>,
}

impl ChatgptOrgUkProvider {
    pub fn new(api_key: String) -> Self {
        Self {
            api_base: "https://mail.chatgpt.org.uk".to_string(),
            api_key,
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
        let resp = client
            .get(&url)
            .header("X-API-Key", &self.api_key)
            .header("User-Agent", "Mozilla/5.0")
            .send()
            .await?;
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

/// 默认邮件 API 最大并发数
const DEFAULT_MAIL_CONCURRENCY: usize = 20;

pub struct EmailService {
    provider: Arc<dyn EmailProvider>,
    /// 限制同时访问邮件 API 的并发数，避免高并发时打垮邮件服务
    mail_semaphore: Arc<Semaphore>,
}

impl EmailService {
    pub fn new(provider: Arc<dyn EmailProvider>) -> Self {
        Self::with_concurrency(provider, DEFAULT_MAIL_CONCURRENCY)
    }

    pub fn with_concurrency(provider: Arc<dyn EmailProvider>, max_concurrency: usize) -> Self {
        Self {
            provider,
            mail_semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
        }
    }

    pub fn new_http(cfg: EmailServiceConfig) -> Self {
        Self::new(Arc::new(HttpEmailProvider::new(cfg)))
    }

    pub fn new_chatgpt_org_uk(api_key: String) -> Self {
        Self::new(Arc::new(ChatgptOrgUkProvider::new(api_key)))
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
    /// `options.after_email_id` 由调用方传入，避免内部重复基线化。
    pub async fn start_background_poll(
        &self,
        target_email: &str,
        proxy: Option<&str>,
        options: WaitCodeOptions,
    ) -> Result<EmailCodeHandle> {
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
                &options,
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
        cancel_rx: &mut oneshot::Receiver<()>,
    ) -> Result<String> {
        let regex = code_regex();
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
                _ = sleep(Duration::from_millis(options.interval_ms.max(200))) => {}
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
            sleep(Duration::from_millis(options.interval_ms.max(200))).await;
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
