use std::{fs, path::Path};

use anyhow::Result;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
pub struct AppConfig {
    #[serde(default)]
    pub proxy_pool: Vec<String>,
    #[serde(default)]
    pub s2a: Vec<S2aConfig>,
    #[serde(default)]
    pub defaults: RuntimeDefaults,
    #[serde(default)]
    pub register: RegisterConfig,
    #[serde(default)]
    pub codex: CodexConfig,
    #[serde(default)]
    pub payment: PaymentConfig,
    #[serde(default)]
    pub email_domains: Vec<String>,
    #[serde(default)]
    pub d1_cleanup: D1CleanupConfig,
    pub api_base: Option<String>,
    pub admin_key: Option<String>,
    pub concurrency: Option<usize>,
    pub priority: Option<usize>,
    pub group_ids: Option<Vec<i64>>,
    /// 代理健康检测超时秒数（默认 5）
    pub proxy_check_timeout_sec: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RuntimeDefaults {
    pub target_count: Option<usize>,
    pub register_workers: Option<usize>,
    pub rt_workers: Option<usize>,
    pub rt_retries: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RegisterConfig {
    pub mail_api_base: Option<String>,
    pub mail_api_path: Option<String>,
    pub mail_api_token: Option<String>,
    pub mail_request_timeout_sec: Option<u64>,
    pub use_proxy_for_mail: Option<bool>,
    pub init_retries: Option<usize>,
    pub otp_max_retries: Option<usize>,
    pub otp_interval_ms: Option<u64>,
    pub request_timeout_sec: Option<u64>,
    pub user_agent: Option<String>,
    pub tls_emulation: Option<String>,
    pub chatgpt_mail_api_key: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct PaymentConfig {
    pub stripe_public_key: Option<String>,
    pub stripe_version: Option<String>,
    pub stripe_js_version: Option<String>,
    pub seat_quantity: Option<usize>,
    pub payment_retries: Option<usize>,
    pub poll_max_attempts: Option<usize>,
    pub poll_interval_ms: Option<u64>,
    /// 是否启用支付（如果设为 false 则跳过支付步骤，注册 free 账号）
    #[serde(default)]
    #[allow(dead_code)]
    pub enabled: Option<bool>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct CodexConfig {
    // 保留这些字段以兼容现有 config.toml，实际邮箱配置已统一由 RegisterRuntimeConfig 提供
    #[allow(dead_code)]
    pub mail_api_base: Option<String>,
    #[allow(dead_code)]
    pub mail_api_path: Option<String>,
    #[allow(dead_code)]
    pub mail_api_token: Option<String>,
    pub use_proxy_for_mail: Option<bool>,
    pub request_timeout_sec: Option<u64>,
    pub user_agent: Option<String>,
    pub tls_emulation: Option<String>,
    pub client_id: Option<String>,
    pub redirect_uri: Option<String>,
    pub scope: Option<String>,
    pub otp_max_retries: Option<usize>,
    pub otp_interval_ms: Option<u64>,
    pub max_redirects: Option<usize>,
    pub sentinel_enabled: Option<bool>,
    pub strict_sentinel: Option<bool>,
    pub pow_max_iterations: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct RegisterRuntimeConfig {
    pub mail_api_base: String,
    pub mail_api_path: String,
    pub mail_api_token: String,
    pub mail_request_timeout_sec: u64,
    pub use_proxy_for_mail: bool,
    pub init_retries: usize,
    pub otp_max_retries: usize,
    pub otp_interval_ms: u64,
    pub request_timeout_sec: u64,
    pub user_agent: String,
    pub tls_emulation: String,
    pub chatgpt_mail_api_key: String,
    pub payment: crate::stripe::PaymentRuntimeConfig,
}

#[derive(Debug, Clone)]
pub struct CodexRuntimeConfig {
    pub use_proxy_for_mail: bool,
    pub request_timeout_sec: u64,
    pub user_agent: String,
    pub tls_emulation: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub scope: String,
    pub otp_max_retries: usize,
    pub otp_interval_ms: u64,
    pub max_redirects: usize,
    pub sentinel_enabled: bool,
    pub strict_sentinel: bool,
    pub pow_max_iterations: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct S2aConfig {
    pub name: String,
    pub api_base: String,
    pub admin_key: String,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_priority")]
    pub priority: usize,
    #[serde(default)]
    pub group_ids: Vec<i64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct D1CleanupConfig {
    /// 是否启用 D1 清理，默认 false
    pub enabled: Option<bool>,
    /// Cloudflare Account ID
    pub account_id: Option<String>,
    /// Cloudflare API Token
    pub api_key: Option<String>,
    /// 目标数据库列表
    pub databases: Option<Vec<D1Database>>,
    /// 保留比例，默认 0.1 (10%)
    pub keep_percent: Option<f64>,
    /// 每批删除数量，默认 5000
    pub batch_size: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct D1Database {
    pub name: String,
    pub id: String,
}

fn default_concurrency() -> usize {
    50
}

fn default_priority() -> usize {
    30
}

fn default_mail_api_base() -> String {
    "https://kyx-cloud-email.kkyyxx.top".to_string()
}

fn default_mail_api_path() -> String {
    "/api/public/emailList".to_string()
}

fn default_register_user_agent() -> String {
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".to_string()
}

fn default_codex_user_agent() -> String {
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36".to_string()
}

fn default_register_tls_emulation() -> String {
    "auto".to_string()
}

fn default_codex_tls_emulation() -> String {
    "auto".to_string()
}

fn default_codex_client_id() -> String {
    "app_EMoamEEZ73f0CkXaXp7hrann".to_string()
}

fn default_codex_redirect_uri() -> String {
    "http://localhost:1455/auth/callback".to_string()
}

fn default_codex_scope() -> String {
    "openid profile email offline_access".to_string()
}

impl AppConfig {
    pub fn load(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)?;
        let cfg: AppConfig = toml::from_str(&content)?;
        Ok(cfg)
    }

    pub fn effective_s2a_configs(&self) -> Vec<S2aConfig> {
        if !self.s2a.is_empty() {
            return self.s2a.clone();
        }

        if let (Some(api_base), Some(admin_key)) = (self.api_base.clone(), self.admin_key.clone()) {
            return vec![S2aConfig {
                name: "default".to_string(),
                api_base,
                admin_key,
                concurrency: self.concurrency.unwrap_or(default_concurrency()),
                priority: self.priority.unwrap_or(default_priority()),
                group_ids: self.group_ids.clone().unwrap_or_default(),
            }];
        }
        Vec::new()
    }

    pub fn register_runtime(&self) -> RegisterRuntimeConfig {
        RegisterRuntimeConfig {
            mail_api_base: self
                .register
                .mail_api_base
                .clone()
                .unwrap_or_else(default_mail_api_base),
            mail_api_path: self
                .register
                .mail_api_path
                .clone()
                .unwrap_or_else(default_mail_api_path),
            mail_api_token: self.register.mail_api_token.clone().unwrap_or_default(),
            mail_request_timeout_sec: self.register.mail_request_timeout_sec.unwrap_or(12).max(5),
            use_proxy_for_mail: self.register.use_proxy_for_mail.unwrap_or(false),
            init_retries: self.register.init_retries.unwrap_or(3).max(1),
            otp_max_retries: self.register.otp_max_retries.unwrap_or(18).max(1),
            otp_interval_ms: self.register.otp_interval_ms.unwrap_or(1000).max(200),
            request_timeout_sec: self.register.request_timeout_sec.unwrap_or(20).max(5),
            user_agent: self
                .register
                .user_agent
                .clone()
                .unwrap_or_else(default_register_user_agent),
            tls_emulation: self
                .register
                .tls_emulation
                .clone()
                .unwrap_or_else(default_register_tls_emulation),
            chatgpt_mail_api_key: self
                .register
                .chatgpt_mail_api_key
                .clone()
                .unwrap_or_else(|| "sk-HQ5kHZao".to_string()),
            payment: self.payment_runtime(),
        }
    }

    pub fn payment_runtime(&self) -> crate::stripe::PaymentRuntimeConfig {
        crate::stripe::PaymentRuntimeConfig {
            stripe_public_key: self
                .payment
                .stripe_public_key
                .clone()
                .unwrap_or_else(|| crate::stripe::DEFAULT_STRIPE_PUBLIC_KEY.to_string()),
            stripe_version: self
                .payment
                .stripe_version
                .clone()
                .unwrap_or_else(|| crate::stripe::DEFAULT_STRIPE_VERSION.to_string()),
            stripe_js_version: self
                .payment
                .stripe_js_version
                .clone()
                .unwrap_or_else(|| crate::stripe::DEFAULT_STRIPE_JS_VERSION.to_string()),
            seat_quantity: self.payment.seat_quantity.unwrap_or(5),
            payment_retries: self.payment.payment_retries.unwrap_or(3).max(1),
            poll_max_attempts: self.payment.poll_max_attempts.unwrap_or(15).max(3),
            poll_interval_ms: self.payment.poll_interval_ms.unwrap_or(500).max(200),
        }
    }

    pub fn payment_enabled(&self) -> bool {
        self.payment.enabled.unwrap_or(true)
    }

    pub fn codex_runtime(&self) -> CodexRuntimeConfig {
        CodexRuntimeConfig {
            use_proxy_for_mail: self.codex.use_proxy_for_mail.unwrap_or(false),
            request_timeout_sec: self.codex.request_timeout_sec.unwrap_or(30).max(8),
            user_agent: self
                .codex
                .user_agent
                .clone()
                .unwrap_or_else(default_codex_user_agent),
            tls_emulation: self
                .codex
                .tls_emulation
                .clone()
                .unwrap_or_else(default_codex_tls_emulation),
            client_id: self
                .codex
                .client_id
                .clone()
                .unwrap_or_else(default_codex_client_id),
            redirect_uri: self
                .codex
                .redirect_uri
                .clone()
                .unwrap_or_else(default_codex_redirect_uri),
            scope: self.codex.scope.clone().unwrap_or_else(default_codex_scope),
            otp_max_retries: self.codex.otp_max_retries.unwrap_or(30).max(1),
            otp_interval_ms: self.codex.otp_interval_ms.unwrap_or(1000).max(200),
            max_redirects: self.codex.max_redirects.unwrap_or(10).max(3),
            sentinel_enabled: self.codex.sentinel_enabled.unwrap_or(true),
            strict_sentinel: self.codex.strict_sentinel.unwrap_or(false),
            pow_max_iterations: self.codex.pow_max_iterations.unwrap_or(5_000_000).max(1000),
        }
    }
}
