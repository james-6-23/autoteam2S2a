use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose};
use rand::Rng;
use rquest::{
    StatusCode,
    header::{self, HeaderMap},
};
use serde::{Deserialize, Serialize};
use tokio::time::sleep;
use uuid::Uuid;

use crate::config::{
    CodexProxyPoolConfig, CodexRuntimeConfig, CpaPoolConfig, RegisterPerfMode,
    RegisterRuntimeConfig, S2aConfig, S2aExtraConfig, TokensPoolConfig,
};
use crate::email_service::{EmailService, WaitCodeOptions};
use crate::fingerprint::{
    build_fingerprint_material, is_retryable_challenge_error, looks_like_challenge_page,
};
use crate::models::{AccountSeed, AccountWithRt, RegisteredAccount};
use crate::sentinel;
use crate::util::{log_worker, random_delay_ms, random_hex, summarize_user_agent, token_preview};

#[derive(Debug, Clone)]
pub struct RegisterInput {
    pub seed: AccountSeed,
    pub proxy: Option<String>,
    pub worker_id: usize,
    pub task_index: usize,
    pub task_total: usize,
    pub skip_payment: bool,
}

#[async_trait]
pub trait RegisterService: Send + Sync {
    async fn register(&self, input: RegisterInput) -> Result<RegisteredAccount>;
    async fn generate_email(&self, _proxy: Option<&str>) -> Result<Option<String>> {
        Ok(None)
    }
}

#[async_trait]
pub trait CodexService: Send + Sync {
    /// 返回 (refresh_token, id_token)
    async fn fetch_refresh_token(
        &self,
        account: &RegisteredAccount,
        proxy: Option<String>,
        worker_id: usize,
    ) -> Result<(String, String)>;
}

#[async_trait]
pub trait S2aService: Send + Sync {
    async fn test_connection(&self, team: &S2aConfig) -> Result<()>;
    async fn add_account(&self, team: &S2aConfig, account: &AccountWithRt) -> Result<Option<i64>>;
    async fn batch_refresh(&self, team: &S2aConfig, account_ids: &[i64]) -> Result<usize>;
}

#[derive(Default)]
pub struct DryRunRegisterService;

#[async_trait]
impl RegisterService for DryRunRegisterService {
    async fn register(&self, input: RegisterInput) -> Result<RegisteredAccount> {
        let wait_ms = random_delay_ms(60, 180);
        sleep(Duration::from_millis(wait_ms)).await;
        Ok(RegisteredAccount {
            account: input.seed.account,
            password: input.seed.password,
            token: format!("tok_{}", random_hex(32)),
            account_id: format!("acc_{}", random_hex(12)),
            plan_type: "free".to_string(),
            proxy: input.proxy,
            refresh_token: None,
        })
    }
}

pub struct LiveRegisterService {
    cfg: RegisterRuntimeConfig,
    email_service: Arc<EmailService>,
    sentinel_cache: sentinel::SentinelCache,
}

impl LiveRegisterService {
    pub fn new(cfg: RegisterRuntimeConfig, email_service: Arc<EmailService>) -> Self {
        Self {
            cfg,
            email_service,
            sentinel_cache: sentinel::SentinelCache::new(120),
        }
    }

    fn build_client(
        &self,
        proxy: Option<&str>,
        attempt: usize,
    ) -> Result<(rquest::Client, HeaderMap, String)> {
        let fingerprint =
            build_fingerprint_material(&self.cfg.tls_emulation, attempt, &self.cfg.user_agent)?;
        let fp_label = format!(
            "rquest/{} {} [salt={attempt}]",
            fingerprint.emulation_name,
            summarize_user_agent(&fingerprint.user_agent)
        );
        let user_agent = fingerprint.user_agent;
        let default_headers = fingerprint.default_headers;
        let navigation_headers = fingerprint.navigation_headers;
        let request_timeout = self.cfg.request_timeout_sec.max(5);
        let connect_timeout_sec = (request_timeout / 2).clamp(3, 12);
        let mut builder = rquest::Client::builder()
            .emulation(fingerprint.emulation)
            .default_headers(default_headers)
            .cookie_store(true)
            .redirect(rquest::redirect::Policy::limited(10))
            .timeout(Duration::from_secs(request_timeout))
            .connect_timeout(Duration::from_secs(connect_timeout_sec))
            .user_agent(&user_agent)
            .no_proxy()
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .pool_max_idle_per_host(8);

        if let Some(p) = proxy {
            builder = builder.proxy(rquest::Proxy::all(p)?);
        }

        Ok((builder.build()?, navigation_headers, fp_label))
    }

    async fn do_register(
        &self,
        client: &rquest::Client,
        navigation_headers: &HeaderMap,
        input: &RegisterInput,
    ) -> Result<RegisteredAccount> {
        let auth_session_logging_id = Uuid::new_v4().to_string();
        let (oai_did, csrf_token) = self.init_session(client, navigation_headers).await?;
        log_worker(input.worker_id, "OK", "初始化会话成功");

        log_worker(input.worker_id, "注册", "获取授权链接...");
        let authorize_url = self
            .get_authorize_url(
                client,
                &oai_did,
                &csrf_token,
                &auth_session_logging_id,
                &input.seed.account,
            )
            .await?;

        log_worker(input.worker_id, "注册", "启动授权...");
        self.start_authorize(client, navigation_headers, &authorize_url)
            .await?;
        log_worker(input.worker_id, "OK", "授权成功");

        log_worker(input.worker_id, "注册", "注册账户...");
        self.register_account(client, &input.seed.account, &input.seed.password)
            .await?;
        log_worker(input.worker_id, "OK", "注册接口成功");

        let mail_proxy = if self.cfg.use_proxy_for_mail {
            input.proxy.as_deref()
        } else {
            None
        };

        let optimized_poll = matches!(self.cfg.register_perf_mode, RegisterPerfMode::Adaptive);
        let turbo_mode = matches!(self.cfg.register_perf_mode, RegisterPerfMode::Turbo);
        let primary_timeout_secs = if turbo_mode {
            25
        } else if optimized_poll {
            32
        } else {
            35
        };
        let retry_timeout_secs = if optimized_poll { 24 } else { 20 };
        let primary_options = if turbo_mode {
            WaitCodeOptions {
                after_email_id: 0,
                prefetch_latest_email_id: true,
                max_retries: (self.cfg.otp_max_retries + 8).max(12),
                interval_ms: (self.cfg.otp_interval_ms / 5).clamp(150, 300),
                max_interval_ms: self.cfg.otp_interval_ms.max(800).min(1200),
                interval_backoff_step_ms: 80,
                size: 3,
                subject_must_contain_code_word: false,
                strict_fetch: false,
            }
        } else if optimized_poll {
            WaitCodeOptions {
                after_email_id: 0,
                prefetch_latest_email_id: true,
                max_retries: (self.cfg.otp_max_retries + 6).max(8),
                interval_ms: (self.cfg.otp_interval_ms / 3).clamp(250, 500),
                max_interval_ms: self.cfg.otp_interval_ms.max(1100).min(1800),
                interval_backoff_step_ms: 120,
                size: 3,
                subject_must_contain_code_word: false,
                strict_fetch: false,
            }
        } else {
            WaitCodeOptions {
                after_email_id: 0,
                prefetch_latest_email_id: true,
                max_retries: self.cfg.otp_max_retries,
                interval_ms: self.cfg.otp_interval_ms,
                max_interval_ms: self.cfg.otp_interval_ms,
                interval_backoff_step_ms: 0,
                size: 5,
                subject_must_contain_code_word: false,
                strict_fetch: true,
            }
        };
        let retry_options = if optimized_poll {
            WaitCodeOptions {
                after_email_id: 0,
                prefetch_latest_email_id: false,
                max_retries: 18,
                interval_ms: 900,
                max_interval_ms: 1800,
                interval_backoff_step_ms: 180,
                size: 6,
                subject_must_contain_code_word: false,
                strict_fetch: false,
            }
        } else {
            WaitCodeOptions {
                after_email_id: 0,
                prefetch_latest_email_id: false,
                max_retries: 15,
                interval_ms: 800,
                max_interval_ms: 800,
                interval_backoff_step_ms: 0,
                size: 5,
                subject_must_contain_code_word: false,
                strict_fetch: false, // 网络异常不中断
            }
        };

        // 先启动后台轮询再发送验证码，利用发送耗时提前开始轮询。
        let mut otp_handle = self
            .email_service
            .start_background_poll(&input.seed.account, mail_proxy, primary_options)
            .await?;

        log_worker(input.worker_id, "注册", "发送验证码...");
        self.send_verification_email(client).await?;

        let wait_hint = if turbo_mode {
            format!("等待验证码 (turbo, {}s超时)...", primary_timeout_secs)
        } else if optimized_poll {
            format!("等待验证码 (自适应轮询, {}s超时)...", primary_timeout_secs)
        } else {
            format!("等待验证码 ({}s超时)...", primary_timeout_secs)
        };
        log_worker(input.worker_id, "注册", &wait_hint);

        // === 分级超时策略 ===
        // 第1轮：主超时等待后台轮询结果
        let otp_code = match tokio::time::timeout(
            Duration::from_secs(primary_timeout_secs),
            otp_handle.wait(),
        )
        .await
        {
            Ok(Ok(Ok(code))) => code,
            Ok(Ok(Err(e))) => bail!("验证码轮询失败: {e}"),
            Ok(Err(_)) => bail!("后台验证码轮询任务意外终止"),
            Err(_) => {
                otp_handle.cancel();
                // Turbo 模式：不做补偿轮询，直接跳过
                if turbo_mode {
                    bail!("otp_timeout_skip");
                }
                // 第1轮超时，重新轮询邮箱
                let retry_hint = if optimized_poll {
                    format!(
                        "验证码主轮询超时，进入补偿轮询 ({}s)...",
                        retry_timeout_secs
                    )
                } else {
                    format!(
                        "验证码 {}s 超时，重新轮询邮箱 ({}s重试)...",
                        primary_timeout_secs, retry_timeout_secs
                    )
                };
                log_worker(input.worker_id, "注册", &retry_hint);

                let mail_proxy_retry = if self.cfg.use_proxy_for_mail {
                    input.proxy.as_deref()
                } else {
                    None
                };
                let mut retry_handle = self
                    .email_service
                    .start_background_poll(&input.seed.account, mail_proxy_retry, retry_options)
                    .await?;

                match tokio::time::timeout(
                    Duration::from_secs(retry_timeout_secs),
                    retry_handle.wait(),
                )
                .await
                {
                    Ok(Ok(Ok(code))) => code,
                    _ => {
                        retry_handle.cancel();
                        log_worker(input.worker_id, "ERR", "验证码补偿轮询仍超时，跳过此账号");
                        bail!("otp_timeout_skip");
                    }
                }
            }
        };
        log_worker(input.worker_id, "OK", &format!("验证码: {otp_code}"));
        log_worker(input.worker_id, "注册", &format!("验证码: {otp_code}"));

        self.validate_otp(client, &otp_code).await?;
        log_worker(input.worker_id, "OK", "OTP 校验成功");

        log_worker(input.worker_id, "注册", "获取 Sentinel token...");
        let sentinel_cfg = sentinel::SentinelConfig {
            enabled: self.cfg.sentinel_enabled,
            strict: self.cfg.strict_sentinel,
            pow_max_iterations: self.cfg.pow_max_iterations,
        };
        // 使用 oai_did 作为 device_id，user_agent 从配置获取
        let mut sentinel_state = sentinel::SentinelState::new(&oai_did, &self.cfg.user_agent);
        let sentinel_header = sentinel::build_sentinel_header_cached(
            client,
            &sentinel_cfg,
            &mut sentinel_state,
            "create_account",
            input.worker_id,
            "REG-POW",
            &self.sentinel_cache,
        )
        .await
        .ok();

        log_worker(input.worker_id, "注册", "创建账户...");
        self.create_account(
            client,
            &input.seed.real_name,
            &input.seed.birthdate,
            sentinel_header.as_deref(),
        )
        .await?;
        log_worker(input.worker_id, "OK", "账户创建成功");

        let access_token = self.get_session_token(client).await?;
        log_worker(
            input.worker_id,
            "OK",
            &format!("Token: {}", token_preview(&access_token)),
        );

        // 注册阶段不再尝试获取 RT，全部交由独立 RT 阶段处理（成功率更高）
        let refresh_token: Option<String> = None;

        // ========== 支付步骤 ==========
        if !input.skip_payment && self.cfg.payment.payment_retries > 0 {
            log_worker(input.worker_id, "支付", "获取 checkout URL...");
            let checkout_url = self
                .get_checkout_url(client, &access_token, &oai_did)
                .await?;
            log_worker(input.worker_id, "OK", "获取 checkout URL 成功");

            log_worker(input.worker_id, "支付", "SEPA 支付处理中...");
            self.run_payment(client, &checkout_url, &input.seed.account, input.worker_id)
                .await?;
            log_worker(input.worker_id, "OK", "支付成功");
        } else {
            log_worker(input.worker_id, "跳过", "Free 模式，跳过支付");
        }

        // 支付后轮询等待 plan_type 更新（Stripe → OpenAI 同步需要时间）
        let paid = !input.skip_payment && self.cfg.payment.payment_retries > 0;
        let (account_id, plan_type) = if paid {
            self.wait_for_plan_activation(client, &access_token, input.worker_id)
                .await?
        } else {
            self.check_account_status_with_timeout(client, &access_token)
                .await?
        };
        log_worker(input.worker_id, "OK", "注册成功");
        log_worker(
            input.worker_id,
            "OK",
            &format!("AccountID: {} ({})", account_id, plan_type),
        );

        Ok(RegisteredAccount {
            account: input.seed.account.clone(),
            password: input.seed.password.clone(),
            token: access_token,
            account_id,
            plan_type,
            proxy: input.proxy.clone(),
            refresh_token,
        })
    }

    fn next_plan_poll_delay_ms(&self, retry_index: usize) -> u64 {
        let exp = (retry_index as u32).min(6);
        let base_delay_ms = self
            .cfg
            .plan_poll_initial_delay_ms
            .saturating_mul(1u64 << exp)
            .min(self.cfg.plan_poll_max_delay_ms);
        let min_ms = (base_delay_ms.saturating_mul(8) / 10).max(100);
        let max_ms = (base_delay_ms.saturating_mul(12) / 10).max(min_ms);
        random_delay_ms(min_ms, max_ms)
    }

    fn is_account_check_fatal_error(err_text: &str) -> bool {
        err_text.contains("account_check: token_expired")
            || err_text.contains("account_check: banned")
            || (err_text.contains("account_check: HTTP 4")
                && !err_text.contains("account_check: HTTP 429"))
    }

    async fn check_account_status_with_timeout(
        &self,
        client: &rquest::Client,
        access_token: &str,
    ) -> Result<(String, String)> {
        let timeout = Duration::from_secs(self.cfg.plan_status_timeout_sec.max(3));
        match tokio::time::timeout(timeout, self.check_account_status(client, access_token)).await {
            Ok(result) => result,
            Err(_) => bail!("account_check: timeout({}s)", timeout.as_secs()),
        }
    }

    async fn wait_for_plan_activation(
        &self,
        client: &rquest::Client,
        access_token: &str,
        worker_id: usize,
    ) -> Result<(String, String)> {
        log_worker(worker_id, "验证", "等待 plan 激活...");
        let max_attempts = self.cfg.plan_poll_max_attempts.max(1);
        let mut final_id = String::new();
        let mut final_plan = "free".to_string();
        let mut has_status = false;
        let mut last_err: Option<anyhow::Error> = None;

        for poll in 0..max_attempts {
            if poll > 0 {
                let wait_ms = self.next_plan_poll_delay_ms(poll - 1);
                sleep(Duration::from_millis(wait_ms)).await;
            }
            match self
                .check_account_status_with_timeout(client, access_token)
                .await
            {
                Ok((id, plan)) => {
                    has_status = true;
                    final_id = id;
                    final_plan = plan.clone();
                    if plan != "free" {
                        break;
                    }
                    if poll + 1 < max_attempts {
                        log_worker(
                            worker_id,
                            "验证",
                            &format!("plan 仍为 free，等待中 ({}/{})...", poll + 1, max_attempts),
                        );
                    }
                }
                Err(e) => {
                    let err_text = format!("{e:#}");
                    if Self::is_account_check_fatal_error(&err_text) {
                        return Err(e);
                    }
                    if poll + 1 < max_attempts {
                        let preview = if err_text.len() > 120 {
                            format!("{}...", &err_text[..120])
                        } else {
                            err_text
                        };
                        log_worker(
                            worker_id,
                            "验证",
                            &format!(
                                "plan 状态检查异常，准备重试 ({}/{}): {}",
                                poll + 1,
                                max_attempts,
                                preview
                            ),
                        );
                    }
                    last_err = Some(e);
                }
            }
        }

        if has_status {
            return Ok((final_id, final_plan));
        }
        if let Some(err) = last_err {
            return Err(err);
        }
        bail!("account_check: no_result");
    }

    async fn init_session(
        &self,
        client: &rquest::Client,
        navigation_headers: &HeaderMap,
    ) -> Result<(String, String)> {
        let resp = client
            .get("https://chatgpt.com")
            .headers(navigation_headers.clone())
            .send()
            .await?;
        let status = resp.status();
        if status == StatusCode::FORBIDDEN {
            bail!("init_session_403");
        }
        if status != StatusCode::OK {
            let body = resp.text().await.unwrap_or_default();
            if looks_like_challenge_page(&body) {
                bail!("init_session_challenge");
            }
            bail!("初始化失败，状态码: {}", status);
        }

        let headers = resp.headers().clone();
        let oai_did = extract_cookie_value(&headers, "oai-did");
        let csrf_cookie = extract_cookie_value(&headers, "__Host-next-auth.csrf-token");
        let (oai_did, csrf_cookie) = match (oai_did, csrf_cookie) {
            (Some(oai_did), Some(csrf_cookie)) => (oai_did, csrf_cookie),
            _ => {
                let body = resp.text().await.unwrap_or_default();
                if looks_like_challenge_page(&body) {
                    bail!("init_session_challenge");
                }
                bail!("无法获取初始化会话 cookie");
            }
        };
        let decoded = urlencoding::decode(&csrf_cookie).context("csrf 解码失败")?;
        let csrf_token = decoded
            .split('|')
            .next()
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("无法解析 csrf token"))?
            .to_string();

        let login_url = format!("https://chatgpt.com/auth/login?openaicom-did={oai_did}");
        client
            .get(login_url)
            .headers(navigation_headers.clone())
            .send()
            .await?;
        Ok((oai_did, csrf_token))
    }

    async fn get_authorize_url(
        &self,
        client: &rquest::Client,
        oai_did: &str,
        csrf_token: &str,
        auth_session_logging_id: &str,
        email: &str,
    ) -> Result<String> {
        let login_hint = urlencoding::encode(email);
        let url = format!(
            "https://chatgpt.com/api/auth/signin/openai?prompt=login&ext-oai-did={oai_did}&auth_session_logging_id={auth_session_logging_id}&screen_hint=login_or_signup&login_hint={login_hint}"
        );

        let resp = client
            .post(url)
            .header("Origin", "https://chatgpt.com")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&[
                ("callbackUrl", "https://chatgpt.com/"),
                ("csrfToken", csrf_token),
                ("json", "true"),
            ])
            .send()
            .await?;

        let status = resp.status();
        let content_type = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_ascii_lowercase();
        let body_text = resp.text().await.unwrap_or_default();

        if status != StatusCode::OK {
            if looks_like_challenge_page(&body_text) {
                bail!("auth_challenge");
            }
            bail!(
                "获取授权 URL 失败: HTTP {} {}",
                status,
                truncate_text(&body_text, 180)
            );
        }
        if !content_type.contains("json") || looks_like_challenge_page(&body_text) {
            bail!("auth_challenge");
        }
        let body: AuthUrlResponse = serde_json::from_str(&body_text).with_context(|| {
            format!("授权 URL 响应解析失败: {}", truncate_text(&body_text, 180))
        })?;
        if body.url.contains("auth.openai.com") {
            return Ok(body.url);
        }
        bail!("授权 URL 非预期");
    }

    async fn start_authorize(
        &self,
        client: &rquest::Client,
        navigation_headers: &HeaderMap,
        authorize_url: &str,
    ) -> Result<()> {
        let resp = client
            .get(authorize_url)
            .headers(navigation_headers.clone())
            .send()
            .await?;
        if !resp.status().is_success() {
            bail!("启动授权失败: HTTP {}", resp.status());
        }
        let final_url = resp.url().to_string();
        if final_url.contains("create-account") || final_url.contains("log-in") {
            return Ok(());
        }
        bail!("授权流程启动失败，final_url={final_url}");
    }

    async fn register_account(
        &self,
        client: &rquest::Client,
        email: &str,
        password: &str,
    ) -> Result<()> {
        let payload = RegisterPayload {
            username: email.to_string(),
            password: password.to_string(),
        };
        let resp = client
            .post("https://auth.openai.com/api/accounts/user/register")
            .header("Origin", "https://auth.openai.com")
            .json(&payload)
            .send()
            .await?;
        if resp.status() == StatusCode::OK {
            return Ok(());
        }
        bail!("注册失败: HTTP {}", resp.status());
    }

    async fn send_verification_email(&self, client: &rquest::Client) -> Result<()> {
        let resp = client
            .get("https://auth.openai.com/api/accounts/email-otp/send")
            .send()
            .await?;
        if resp.status() == StatusCode::OK {
            return Ok(());
        }
        bail!("发送验证码失败: HTTP {}", resp.status());
    }

    async fn validate_otp(&self, client: &rquest::Client, code: &str) -> Result<()> {
        let payload = OtpPayload {
            code: code.to_string(),
        };
        let resp = client
            .post("https://auth.openai.com/api/accounts/email-otp/validate")
            .header("Origin", "https://auth.openai.com")
            .json(&payload)
            .send()
            .await?;
        if resp.status() == StatusCode::OK {
            return Ok(());
        }
        bail!("OTP 校验失败: HTTP {}", resp.status());
    }

    async fn create_account(
        &self,
        client: &rquest::Client,
        real_name: &str,
        birthdate: &str,
        sentinel_header: Option<&str>,
    ) -> Result<()> {
        let payload = CreateAccountPayload {
            name: real_name.to_string(),
            birthdate: birthdate.to_string(),
        };
        let mut req = client
            .post("https://auth.openai.com/api/accounts/create_account")
            .header("Origin", "https://auth.openai.com");
        if let Some(header) = sentinel_header {
            req = req.header("OpenAI-Sentinel-Token", header);
        }
        let resp = req.json(&payload).send().await?;

        if resp.status() != StatusCode::OK {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "创建账号失败: HTTP {} {}",
                status,
                truncate_text(&body, 200)
            );
        }

        let json: serde_json::Value = resp.json().await.unwrap_or_default();
        if let Some(url) = json.get("continue_url").and_then(|v| v.as_str()) {
            let _ = client.get(url).send().await;
        }
        Ok(())
    }

    async fn get_session_token(&self, client: &rquest::Client) -> Result<String> {
        let resp = client
            .get("https://chatgpt.com/api/auth/session")
            .send()
            .await?;
        if resp.status() != StatusCode::OK {
            bail!("获取 access token 失败: HTTP {}", resp.status());
        }
        let body: SessionResponse = resp
            .json()
            .await
            .context("解析 session 响应失败（可能被 challenge）")?;
        if let Some(token) = body.access_token
            && !token.is_empty()
        {
            return Ok(token);
        }
        bail!("响应中没有 accessToken")
    }

    async fn check_account_status(
        &self,
        client: &rquest::Client,
        access_token: &str,
    ) -> Result<(String, String)> {
        let resp = client
            .get("https://chatgpt.com/backend-api/accounts/check/v4-2023-04-27")
            .header("Authorization", format!("Bearer {access_token}"))
            .header("Accept", "application/json")
            .send()
            .await?;

        match resp.status() {
            StatusCode::UNAUTHORIZED => bail!("account_check: token_expired"),
            StatusCode::FORBIDDEN => bail!("account_check: banned"),
            StatusCode::OK => {}
            other => bail!("account_check: HTTP {}", other),
        }

        let body: AccountCheckResponse = resp
            .json()
            .await
            .context("解析账号状态响应失败（可能被 challenge）")?;
        let accounts = body.accounts.unwrap_or_default();
        if accounts.is_empty() {
            bail!("account_check: banned(empty)");
        }

        for (account_id, entry) in accounts {
            if account_id != "default" {
                let plan = if entry.account.plan_type.is_empty() {
                    "free".to_string()
                } else {
                    entry.account.plan_type
                };
                return Ok((account_id, plan));
            }
        }
        bail!("account_check: banned(default_only)");
    }

    /// 获取 Team Plan checkout URL
    async fn get_checkout_url(
        &self,
        client: &rquest::Client,
        access_token: &str,
        oai_did: &str,
    ) -> Result<String> {
        let workspace_name = format!("Team-{}", crate::util::random_hex(4).to_uppercase());
        let seat_quantity = self.cfg.payment.seat_quantity;

        let payload = serde_json::json!({
            "plan_name": "chatgptteamplan",
            "team_plan_data": {
                "workspace_name": workspace_name,
                "price_interval": "month",
                "seat_quantity": seat_quantity,
            },
            "billing_details": {
                "country": "DE",
                "currency": "EUR",
            },
            "promo_campaign": {
                "promo_campaign_id": "team-1-month-free",
                "is_coupon_from_query_param": false,
            },
            "checkout_ui_mode": "redirect",
        });

        let device_id = if oai_did.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            oai_did.to_string()
        };

        let resp = client
            .post("https://chatgpt.com/backend-api/payments/checkout")
            .header("Authorization", format!("Bearer {access_token}"))
            .header("Content-Type", "application/json")
            .header("Origin", "https://chatgpt.com")
            .header("OAI-Device-Id", &device_id)
            .json(&payload)
            .send()
            .await?;

        let status = resp.status();
        if status != StatusCode::OK {
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "获取 checkout URL 失败: HTTP {} {}",
                status,
                &body[..body.len().min(200)]
            );
        }

        let body: serde_json::Value = resp.json().await.context("解析 checkout 响应失败")?;

        if let Some(url) = body.get("url").and_then(|v| v.as_str()) {
            return Ok(url.to_string());
        }

        bail!("checkout 响应中没有 url 字段");
    }

    /// 执行支付流程（含重试）
    async fn run_payment(
        &self,
        client: &rquest::Client,
        checkout_url: &str,
        email: &str,
        worker_id: usize,
    ) -> Result<()> {
        let iban_gen = crate::iban::GermanIbanGenerator::new();
        let max_retries = self.cfg.payment.payment_retries.max(1);

        // Go 复用注册时同一个 TLS Client，并且只创建一个 StripePaymentAPI 实例
        let mut stripe_svc = crate::stripe::StripePaymentService::new(
            checkout_url,
            client.clone(),
            self.cfg.payment.clone(),
        )?;

        let mut last_err: Option<anyhow::Error> = None;

        for retry in 0..max_retries {
            let iban_number = iban_gen.generate();
            let used_blz = iban_gen.last_blz();
            let (street, postal_code, city) = crate::iban::random_german_address();
            // Go 使用 utils.GenerateRealName() 生成英文真实名
            let billing_name = crate::util::generate_real_name_pub();

            if retry > 0 {
                log_worker(
                    worker_id,
                    "支付",
                    &format!("重试 {}/{}", retry, max_retries - 1),
                );
            }
            log_worker(
                worker_id,
                "支付",
                &format!(
                    "IBAN: {} | {}, {} {} | {}",
                    &iban_number[..8],
                    billing_name,
                    postal_code,
                    city,
                    street
                ),
            );

            match stripe_svc
                .complete_payment(
                    &iban_number,
                    &billing_name,
                    email,
                    &street,
                    &city,
                    &postal_code,
                    "DE",
                    worker_id,
                )
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let err_str = format!("{e:#}");

                    // BLZ 问题 → 移除并重试
                    if err_str.contains("bank_account_unusable") || err_str.contains("BIC") {
                        iban_gen.remove_blz(&used_blz);
                        log_worker(worker_id, "支付", &format!("BLZ {used_blz} 不可用，已移除"));
                        if retry + 1 < max_retries {
                            last_err = Some(e);
                            continue;
                        }
                    }

                    // 可重试错误
                    let is_retryable = err_str.contains("400")
                        || err_str.contains("500")
                        || err_str.contains("确认失败")
                        || err_str.contains("timeout")
                        || err_str.contains("EOF")
                        || err_str.contains("connection");

                    if is_retryable && retry + 1 < max_retries {
                        sleep(Duration::from_secs(1)).await;
                        last_err = Some(e);
                        continue;
                    }

                    return Err(e);
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("支付失败，未知错误")))
    }

    // ========== PKCE OAuth: 注册阶段直接获取 refresh_token ==========

    /// 尝试在已认证的注册会话中获取 (refresh_token, id_token)。
    /// 失败不影响注册结果，返回 None 由 RT 阶段补获。
    async fn try_get_refresh_token(
        &self,
        client: &rquest::Client,
        worker_id: usize,
    ) -> Option<(String, String)> {
        // 15s 超时保护，避免 PKCE 流程卡住影响注册速度
        match tokio::time::timeout(
            Duration::from_secs(15),
            self.do_get_refresh_token(client, worker_id),
        )
        .await
        {
            Ok(Ok(pair)) => Some(pair),
            Ok(Err(e)) => {
                log_worker(
                    worker_id,
                    "RT",
                    &format!("注册阶段 RT 流程失败（不影响注册）: {e:#}"),
                );
                None
            }
            Err(_) => {
                log_worker(worker_id, "RT", "注册阶段 RT 15s 超时（不影响注册）");
                None
            }
        }
    }

    /// PKCE OAuth 流程：利用注册会话 Cookie 获取 (refresh_token, id_token)
    ///
    /// 注意：注册客户端使用 `Policy::limited(10)` 自动跟随重定向。
    /// 当重定向链命中 localhost:1455 回调时，会触发连接错误，
    /// 此时从错误的 URL 中提取 authorization_code。
    async fn do_get_refresh_token(
        &self,
        client: &rquest::Client,
        worker_id: usize,
    ) -> Result<(String, String)> {
        // 1. 生成 PKCE 参数
        let code_verifier = generate_code_verifier();
        let code_challenge = generate_code_challenge(&code_verifier);
        let state = Uuid::new_v4().to_string();

        // 2. 构建并访问 OAuth authorize URL
        //    客户端会自动跟随重定向，可能的结果：
        //    a) 直接到 consent 页面 → 需要 workspace select 流程
        //    b) 重定向到 localhost:1455 → 连接失败，从 error URL 中提取 code
        //    c) 成功返回某个页面 → 检查最终 URL 是否包含 code
        let auth_url = self.build_pkce_authorize_url(&code_challenge, &state)?;
        match client
            .get(&auth_url)
            .header("Referer", "https://auth.openai.com/")
            .send()
            .await
        {
            Ok(resp) => {
                // 检查最终 URL 是否包含 code（自动重定向可能已到达回调）
                if let Ok(code) = self.extract_code_from_url(resp.url().as_str()) {
                    log_worker(worker_id, "RT", "PKCE: 授权自动重定向获得 code");
                    return self
                        .exchange_code_for_refresh_token(client, &code, &code_verifier)
                        .await;
                }
                // 没有直接获取到 code，需要走 workspace select 流程
            }
            Err(e) => {
                // 连接错误可能是因为重定向到了 localhost:1455
                if let Some(code) = self.extract_code_from_redirect_error(&e) {
                    log_worker(worker_id, "RT", "PKCE: 从重定向错误中提取到 code");
                    return self
                        .exchange_code_for_refresh_token(client, &code, &code_verifier)
                        .await;
                }
                bail!("PKCE: 访问 authorize URL 失败: {e:#}");
            }
        }

        // 3. 没有直接获取到 code，尝试从 Cookie 解析 workspace 并选择
        let workspace_id = self.extract_workspace_from_auth_cookie(client)?;
        log_worker(
            worker_id,
            "RT",
            &format!(
                "PKCE: 选择 workspace {}",
                &workspace_id[..workspace_id.len().min(12)]
            ),
        );

        let workspace_result = client
            .post("https://auth.openai.com/api/accounts/workspace/select")
            .header("Origin", "https://auth.openai.com")
            .header(
                "Referer",
                "https://auth.openai.com/sign-in-with-chatgpt/codex/consent",
            )
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({ "workspace_id": workspace_id }))
            .send()
            .await;

        match workspace_result {
            Ok(resp) => {
                // 检查最终 URL 是否包含 code
                if let Ok(code) = self.extract_code_from_url(resp.url().as_str()) {
                    log_worker(
                        worker_id,
                        "RT",
                        "PKCE: workspace select 后自动重定向获得 code",
                    );
                    return self
                        .exchange_code_for_refresh_token(client, &code, &code_verifier)
                        .await;
                }
                if resp.status() != StatusCode::OK {
                    bail!("PKCE: workspace select 失败: HTTP {}", resp.status());
                }
                let ws_data: serde_json::Value =
                    resp.json().await.context("PKCE: 解析 workspace 响应失败")?;
                let continue_url = ws_data
                    .get("continue_url")
                    .and_then(|v| v.as_str())
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| anyhow!("PKCE: workspace 响应缺少 continue_url"))?;

                // 4. 跟随 continue_url（客户端自动重定向，可能触发连接错误）
                match client
                    .get(continue_url)
                    .header("Referer", "https://auth.openai.com/")
                    .send()
                    .await
                {
                    Ok(final_resp) => {
                        if let Ok(code) = self.extract_code_from_url(final_resp.url().as_str()) {
                            log_worker(worker_id, "RT", "PKCE: continue_url 重定向获得 code");
                            return self
                                .exchange_code_for_refresh_token(client, &code, &code_verifier)
                                .await;
                        }
                        bail!(
                            "PKCE: continue_url 未重定向到回调 (最终 URL: {})",
                            final_resp.url()
                        );
                    }
                    Err(e) => {
                        if let Some(code) = self.extract_code_from_redirect_error(&e) {
                            log_worker(worker_id, "RT", "PKCE: continue_url 错误中提取到 code");
                            return self
                                .exchange_code_for_refresh_token(client, &code, &code_verifier)
                                .await;
                        }
                        bail!("PKCE: continue_url 请求失败: {e:#}");
                    }
                }
            }
            Err(e) => {
                // workspace select 本身不应重定向到 localhost，但以防万一
                if let Some(code) = self.extract_code_from_redirect_error(&e) {
                    log_worker(worker_id, "RT", "PKCE: workspace select 错误中提取到 code");
                    return self
                        .exchange_code_for_refresh_token(client, &code, &code_verifier)
                        .await;
                }
                bail!("PKCE: workspace select 请求失败: {e:#}");
            }
        }
    }

    fn build_pkce_authorize_url(&self, code_challenge: &str, state: &str) -> Result<String> {
        let mut url = rquest::Url::parse("https://auth.openai.com/oauth/authorize")?;
        url.query_pairs_mut()
            .append_pair("client_id", &self.cfg.codex_client_id)
            .append_pair("code_challenge", code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("codex_cli_simplified_flow", "true")
            .append_pair("id_token_add_organizations", "true")
            .append_pair("redirect_uri", &self.cfg.codex_redirect_uri)
            .append_pair("response_type", "code")
            .append_pair("scope", &self.cfg.codex_scope)
            .append_pair("state", state);
        Ok(url.to_string())
    }

    /// 从 oai-client-auth-session Cookie 中解析第一个 workspace ID
    fn extract_workspace_from_auth_cookie(&self, client: &rquest::Client) -> Result<String> {
        // rquest 的 cookie_store 将 Cookie 存在内部，
        // 我们需要通过构造一个 URL 来获取对应的 Cookie
        let url = rquest::Url::parse("https://auth.openai.com/")?;
        let cookies = client
            .get_cookies(&url)
            .ok_or_else(|| anyhow!("PKCE: 无法获取 auth.openai.com 的 Cookie"))?;

        let cookie_str = cookies
            .to_str()
            .map_err(|_| anyhow!("PKCE: Cookie 解码失败"))?;

        // 查找 oai-client-auth-session=xxx
        let auth_value = cookie_str
            .split("; ")
            .find_map(|pair| {
                if let Some(rest) = pair.strip_prefix("oai-client-auth-session=") {
                    Some(rest.to_string())
                } else {
                    None
                }
            })
            .ok_or_else(|| anyhow!("PKCE: 未找到 oai-client-auth-session Cookie"))?;

        // 解码第一段（Base64 JSON，非 JWT）
        let first_segment = auth_value
            .split('.')
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("PKCE: auth cookie 格式异常"))?;

        let padded = match first_segment.len() % 4 {
            2 => format!("{first_segment}=="),
            3 => format!("{first_segment}="),
            _ => first_segment.to_string(),
        };
        let decoded = general_purpose::URL_SAFE_NO_PAD
            .decode(&padded)
            .or_else(|_| general_purpose::STANDARD.decode(&padded))
            .context("PKCE: auth cookie Base64 解码失败")?;
        let json: serde_json::Value =
            serde_json::from_slice(&decoded).context("PKCE: auth cookie JSON 解析失败")?;

        let workspaces = json
            .get("workspaces")
            .and_then(|v| v.as_array())
            .filter(|arr| !arr.is_empty())
            .ok_or_else(|| anyhow!("PKCE: auth cookie 中无 workspace 信息"))?;

        let ws_id = workspaces[0]
            .get("id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("PKCE: workspace id 为空"))?;

        Ok(ws_id.to_string())
    }

    /// 从 rquest 的连接错误中提取 authorization_code。
    /// 当自动重定向到 localhost:1455 时连接失败，
    /// 此时可以从错误的 URL 中提取 code 参数。
    fn extract_code_from_redirect_error(&self, err: &rquest::Error) -> Option<String> {
        // rquest::Error 有一个 url() 方法，返回触发错误的 URL
        if let Some(url) = err.url() {
            if let Some((_, code)) = url.query_pairs().find(|(k, _)| k == "code") {
                let code = code.to_string();
                if !code.is_empty() {
                    return Some(code);
                }
            }
        }
        // 如果 url() 返回 None，尝试从错误消息中解析
        let err_text = format!("{err:#}");
        if let Some(start) = err_text.find("code=") {
            let rest = &err_text[start + 5..];
            let code: String = rest
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '-' || *c == '_')
                .collect();
            if !code.is_empty() {
                return Some(code);
            }
        }
        None
    }

    /// 从 URL 中提取 code 参数
    fn extract_code_from_url(&self, url: &str) -> Result<String> {
        let parsed = rquest::Url::parse(url).context("URL 解析失败")?;
        parsed
            .query_pairs()
            .find(|(k, _)| k == "code")
            .map(|(_, v)| v.to_string())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("URL 中未找到 code 参数"))
    }

    /// 用 authorization_code + code_verifier 换取 (refresh_token, id_token)
    async fn exchange_code_for_refresh_token(
        &self,
        client: &rquest::Client,
        code: &str,
        code_verifier: &str,
    ) -> Result<(String, String)> {
        // 第一次尝试
        match self.do_exchange_code(client, code, code_verifier).await {
            Ok(pair) => return Ok(pair),
            Err(first_err) => {
                // 重试一次
                tokio::time::sleep(Duration::from_millis(500)).await;
                self.do_exchange_code(client, code, code_verifier)
                    .await
                    .map_err(|_| first_err)
            }
        }
    }

    /// 返回 (refresh_token, id_token)
    async fn do_exchange_code(
        &self,
        client: &rquest::Client,
        code: &str,
        code_verifier: &str,
    ) -> Result<(String, String)> {
        let resp = client
            .post("https://auth.openai.com/oauth/token")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", self.cfg.codex_redirect_uri.as_str()),
                ("client_id", self.cfg.codex_client_id.as_str()),
                ("code_verifier", code_verifier),
            ])
            .send()
            .await
            .context("PKCE: token exchange 请求失败")?;

        if resp.status() != StatusCode::OK {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "PKCE: token exchange 失败: HTTP {} {}",
                status,
                truncate_text(&body, 180)
            );
        }

        let body: serde_json::Value = resp
            .json()
            .await
            .context("PKCE: token exchange 响应解析失败")?;
        let rt = body
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("PKCE: token 响应中缺少 refresh_token"))?;
        let id_token = body
            .get("id_token")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        Ok((rt.to_string(), id_token.to_string()))
    }
}

#[async_trait]
impl RegisterService for LiveRegisterService {
    async fn register(&self, input: RegisterInput) -> Result<RegisteredAccount> {
        let mut last_error: Option<anyhow::Error> = None;
        for retry in 0..self.cfg.init_retries {
            let entropy = input
                .task_index
                .wrapping_mul(131)
                .wrapping_add(input.worker_id.wrapping_mul(17));
            let attempt = fingerprint_salt(input.proxy.as_deref(), retry, entropy);
            let (client, navigation_headers, fp_label) =
                self.build_client(input.proxy.as_deref(), attempt)?;
            log_worker(
                input.worker_id,
                "注册",
                &format!(
                    "初始化会话... [{fp_label}] (任务 {}/{})",
                    input.task_index, input.task_total
                ),
            );
            match self.do_register(&client, &navigation_headers, &input).await {
                Ok(acc) => return Ok(acc),
                Err(err) => {
                    let text = format!("{err:#}");
                    if is_retryable_challenge_error(&text) && retry + 1 < self.cfg.init_retries {
                        log_worker(
                            input.worker_id,
                            "注册",
                            &format!(
                                "触发挑战，准备重试 ({}/{}) 任务 {}/{}",
                                retry + 1,
                                self.cfg.init_retries,
                                input.task_index,
                                input.task_total
                            ),
                        );
                        sleep(Duration::from_millis(350)).await;
                        last_error = Some(err);
                        continue;
                    }
                    return Err(err);
                }
            }
        }
        Err(last_error.unwrap_or_else(|| anyhow!("注册失败，未知错误")))
    }

    async fn generate_email(&self, proxy: Option<&str>) -> Result<Option<String>> {
        self.email_service.generate_email(proxy).await
    }
}

#[derive(Default)]
pub struct DryRunCodexService;

#[async_trait]
impl CodexService for DryRunCodexService {
    async fn fetch_refresh_token(
        &self,
        _account: &RegisteredAccount,
        _proxy: Option<String>,
        _worker_id: usize,
    ) -> Result<(String, String)> {
        let wait_ms = random_delay_ms(80, 220);
        sleep(Duration::from_millis(wait_ms)).await;
        Ok((format!("rt_{}", random_hex(48)), String::new()))
    }
}

pub struct LiveCodexService {
    cfg: CodexRuntimeConfig,
    email_service: Arc<EmailService>,
}

type CodexAuthState = sentinel::SentinelState;

impl LiveCodexService {
    pub fn new(cfg: CodexRuntimeConfig, email_service: Arc<EmailService>) -> Self {
        Self { cfg, email_service }
    }

    fn build_client(
        &self,
        proxy: Option<&str>,
        attempt: usize,
    ) -> Result<(rquest::Client, String, HeaderMap)> {
        let fingerprint =
            build_fingerprint_material(&self.cfg.tls_emulation, attempt, &self.cfg.user_agent)?;
        let user_agent = fingerprint.user_agent;
        let default_headers = fingerprint.default_headers;
        let navigation_headers = fingerprint.navigation_headers;
        let request_timeout = self.cfg.request_timeout_sec.max(8);
        let connect_timeout_sec = (request_timeout / 2).clamp(3, 12);
        let mut builder = rquest::Client::builder()
            .emulation(fingerprint.emulation)
            .default_headers(default_headers)
            .cookie_store(true)
            .redirect(rquest::redirect::Policy::none())
            .timeout(Duration::from_secs(request_timeout))
            .connect_timeout(Duration::from_secs(connect_timeout_sec))
            .user_agent(&user_agent)
            .no_proxy()
            .tcp_keepalive(Some(Duration::from_secs(30)))
            .pool_idle_timeout(Some(Duration::from_secs(90)))
            .pool_max_idle_per_host(8);
        if let Some(p) = proxy {
            builder = builder.proxy(rquest::Proxy::all(p)?);
        }
        Ok((builder.build()?, user_agent, navigation_headers))
    }

    fn build_authorize_url(&self, code_challenge: &str, state: &str) -> Result<String> {
        let mut url = rquest::Url::parse("https://auth.openai.com/oauth/authorize")?;
        url.query_pairs_mut()
            .append_pair("client_id", &self.cfg.client_id)
            .append_pair("code_challenge", code_challenge)
            .append_pair("code_challenge_method", "S256")
            .append_pair("codex_cli_simplified_flow", "true")
            .append_pair("id_token_add_organizations", "true")
            .append_pair("redirect_uri", &self.cfg.redirect_uri)
            .append_pair("response_type", "code")
            .append_pair("scope", &self.cfg.scope)
            .append_pair("state", state);
        Ok(url.to_string())
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_authorization_code(
        &self,
        client: &rquest::Client,
        navigation_headers: &HeaderMap,
        account: &RegisteredAccount,
        proxy: Option<&str>,
        auth_url: &str,
        state: &mut CodexAuthState,
        worker_id: usize,
    ) -> Result<String> {
        let mut referer = "https://auth.openai.com/log-in".to_string();
        let mut current_url = auth_url.to_string();

        let mut resp = client
            .get(&current_url)
            .headers(navigation_headers.clone())
            .header("Origin", "https://auth.openai.com")
            .header("Referer", &referer)
            .header("Content-Type", "application/json")
            .send()
            .await?;

        for _ in 0..self.cfg.max_redirects {
            if !resp.status().is_redirection() {
                break;
            }
            let location = resp
                .headers()
                .get(header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .ok_or_else(|| anyhow!("授权重定向缺少 location"))?;
            current_url = resolve_location(resp.url(), location)?;
            referer = current_url.clone();
            resp = client
                .get(&current_url)
                .headers(navigation_headers.clone())
                .header("Origin", "https://auth.openai.com")
                .header("Referer", &referer)
                .header("Content-Type", "application/json")
                .send()
                .await?;
        }

        self.call_sentinel_req(client, state, "login_web_init", worker_id)
            .await?;
        let submit_resp = client
            .post("https://auth.openai.com/api/accounts/authorize/continue")
            .header("Origin", "https://auth.openai.com")
            .header("Referer", &referer)
            .header("Content-Type", "application/json")
            .header(
                "OpenAI-Sentinel-Token",
                self.get_sentinel_header(state, "authorize_continue")?,
            )
            .json(&serde_json::json!({
                "username": {
                    "kind": "email",
                    "value": account.account
                }
            }))
            .send()
            .await?;
        if submit_resp.status() != StatusCode::OK {
            let status = submit_resp.status();
            let body = submit_resp.text().await.unwrap_or_default();
            bail!(
                "提交邮箱失败: HTTP {} {}",
                status,
                truncate_text(&body, 180)
            );
        }
        let mut data: serde_json::Value = submit_resp
            .json()
            .await
            .context("解析提交邮箱响应失败（可能被 challenge）")?;

        let mut page_type = data
            .get("page")
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        if page_type == "password" || data.to_string().contains("password") {
            let mail_proxy = if self.cfg.use_proxy_for_mail {
                proxy
            } else {
                None
            };
            let latest_email_id = self
                .email_service
                .get_latest_email_id(account.account.as_str(), mail_proxy)
                .await
                .unwrap_or(0);

            log_worker(worker_id, "RT", "进入密码验证阶段...");
            self.call_sentinel_req(client, state, "authorize_continue__auto", worker_id)
                .await?;
            let password_resp = client
                .post("https://auth.openai.com/api/accounts/password/verify")
                .header("Origin", "https://auth.openai.com")
                .header("Referer", &referer)
                .header("Content-Type", "application/json")
                .header(
                    "OpenAI-Sentinel-Token",
                    self.get_sentinel_header(state, "password_verify")?,
                )
                .json(&serde_json::json!({
                    "password": account.password
                }))
                .send()
                .await?;
            if password_resp.status().is_redirection() {
                let redirect_referer = password_resp.url().to_string();
                let next = resolve_redirect_location(&password_resp, "password_verify")?;
                return self
                    .follow_redirects_for_code(
                        client,
                        navigation_headers,
                        &next,
                        &redirect_referer,
                        worker_id,
                    )
                    .await;
            }
            if password_resp.status() != StatusCode::OK {
                let status = password_resp.status();
                let body = password_resp.text().await.unwrap_or_default();
                bail!(
                    "密码验证失败: HTTP {} {}",
                    status,
                    truncate_text(&body, 180)
                );
            }
            data = password_resp
                .json()
                .await
                .context("解析密码验证响应失败（可能被 challenge）")?;

            page_type = data
                .get("page")
                .and_then(|v| v.get("type"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            if page_type == "email_otp_verification" {
                let otp_code = self
                    .email_service
                    .wait_for_code(
                        account.account.as_str(),
                        mail_proxy,
                        WaitCodeOptions {
                            after_email_id: latest_email_id,
                            prefetch_latest_email_id: false,
                            max_retries: self.cfg.otp_max_retries,
                            interval_ms: self.cfg.otp_interval_ms,
                            max_interval_ms: self.cfg.otp_interval_ms,
                            interval_backoff_step_ms: 0,
                            size: 3,
                            subject_must_contain_code_word: true,
                            strict_fetch: false,
                        },
                    )
                    .await?;
                log_worker(worker_id, "RT", "已收到 OTP，提交校验...");
                self.call_sentinel_req(client, state, "email_otp_verification__auto", worker_id)
                    .await?;
                let otp_resp = client
                    .post("https://auth.openai.com/api/accounts/email-otp/validate")
                    .header("Origin", "https://auth.openai.com")
                    .header("Referer", &referer)
                    .header("Content-Type", "application/json")
                    .header(
                        "OpenAI-Sentinel-Token",
                        self.get_sentinel_header(state, "email_otp_validate")?,
                    )
                    .json(&serde_json::json!({ "code": otp_code }))
                    .send()
                    .await?;
                if otp_resp.status().is_redirection() {
                    let redirect_referer = otp_resp.url().to_string();
                    let next = resolve_redirect_location(&otp_resp, "email_otp_validate")?;
                    return self
                        .follow_redirects_for_code(
                            client,
                            navigation_headers,
                            &next,
                            &redirect_referer,
                            worker_id,
                        )
                        .await;
                }
                if otp_resp.status() != StatusCode::OK {
                    let status = otp_resp.status();
                    let body = otp_resp.text().await.unwrap_or_default();
                    bail!(
                        "OTP 验证失败: HTTP {} {}",
                        status,
                        truncate_text(&body, 180)
                    );
                }
                data = otp_resp
                    .json()
                    .await
                    .context("解析 OTP 验证响应失败（可能被 challenge）")?;
            }

            if let Some(continue_url) = data.get("continue_url").and_then(|v| v.as_str())
                && !continue_url.is_empty()
                && !continue_url.contains("email-verification")
            {
                if continue_url.contains("consent") {
                    let _ = client
                        .get(continue_url)
                        .headers(navigation_headers.clone())
                        .header("Origin", "https://auth.openai.com")
                        .header("Referer", &referer)
                        .send()
                        .await;
                } else {
                    return self
                        .follow_redirects_for_code(
                            client,
                            navigation_headers,
                            continue_url,
                            &referer,
                            worker_id,
                        )
                        .await;
                }
            }
        }

        log_worker(worker_id, "RT", "选择工作区...");
        self.call_sentinel_req(client, state, "password_verify__auto", worker_id)
            .await?;
        let workspace_resp = client
            .post("https://auth.openai.com/api/accounts/workspace/select")
            .header("Origin", "https://auth.openai.com")
            .header("Referer", &referer)
            .header("Content-Type", "application/json")
            .header(
                "OpenAI-Sentinel-Token",
                self.get_sentinel_header(state, "workspace_select")?,
            )
            .json(&serde_json::json!({
                "workspace_id": account.account_id
            }))
            .send()
            .await?;
        if workspace_resp.status().is_redirection() {
            let redirect_referer = workspace_resp.url().to_string();
            let next = resolve_redirect_location(&workspace_resp, "workspace_select")?;
            return self
                .follow_redirects_for_code(
                    client,
                    navigation_headers,
                    &next,
                    &redirect_referer,
                    worker_id,
                )
                .await;
        }
        if workspace_resp.status() != StatusCode::OK {
            let status = workspace_resp.status();
            let body = workspace_resp.text().await.unwrap_or_default();
            bail!(
                "选择工作区失败: HTTP {} {}",
                status,
                truncate_text(&body, 180)
            );
        }
        let workspace_data: serde_json::Value = workspace_resp
            .json()
            .await
            .context("解析 workspace 选择响应失败（可能被 challenge）")?;
        let continue_url = workspace_data
            .get("continue_url")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("未获取到 continue_url"))?;

        self.follow_redirects_for_code(
            client,
            navigation_headers,
            continue_url,
            &referer,
            worker_id,
        )
        .await
    }

    async fn follow_redirects_for_code(
        &self,
        client: &rquest::Client,
        navigation_headers: &HeaderMap,
        continue_url: &str,
        referer: &str,
        worker_id: usize,
    ) -> Result<String> {
        let started = Instant::now();
        let mut current = continue_url.to_string();
        let mut current_referer = referer.to_string();
        let mut visited = HashSet::new();
        let mut hops = 0usize;

        for _ in 0..self.cfg.max_redirects {
            if !visited.insert(current.clone()) {
                bail!("授权重定向出现循环: {current}");
            }

            let resp = client
                .get(&current)
                .headers(navigation_headers.clone())
                .header("Origin", "https://auth.openai.com")
                .header("Referer", &current_referer)
                .header("Content-Type", "application/json")
                .send()
                .await?;
            if resp.status().is_redirection() {
                hops += 1;
                let location = resp
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| anyhow!("授权回调重定向缺少 location"))?;
                let next = resolve_location(resp.url(), location)?;
                if next.contains("localhost:1455") {
                    let parsed = rquest::Url::parse(&next)?;
                    if let Some((_, code)) = parsed.query_pairs().find(|(k, _)| k == "code")
                        && !code.is_empty()
                    {
                        log_worker(
                            worker_id,
                            "RT",
                            &format!(
                                "授权回调命中 code (跳转 {} 次, 耗时 {:.2}s)",
                                hops,
                                started.elapsed().as_secs_f32()
                            ),
                        );
                        return Ok(code.to_string());
                    }
                    bail!("回调 URL 中缺少 code");
                }
                current_referer = current;
                current = next;
                continue;
            }

            if let Some((_, code)) = resp.url().query_pairs().find(|(k, _)| k == "code")
                && !code.is_empty()
            {
                log_worker(
                    worker_id,
                    "RT",
                    &format!(
                        "授权回调获取 code (跳转 {} 次, 耗时 {:.2}s)",
                        hops,
                        started.elapsed().as_secs_f32()
                    ),
                );
                return Ok(code.to_string());
            }
            let final_url = resp.url().to_string();
            if resp.status().is_success() {
                let body = resp.text().await.unwrap_or_default();
                if looks_like_challenge_page(&body) {
                    bail!("授权流程命中挑战页: {}", truncate_text(&body, 120));
                }
                bail!("授权流程未跳转到回调: {final_url}");
            }
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "获取授权码失败: HTTP {} {}",
                status,
                truncate_text(&body, 180)
            );
        }
        bail!("授权重定向次数超过限制")
    }

    /// 返回 (refresh_token, id_token)
    async fn exchange_code_for_token(
        &self,
        client: &rquest::Client,
        code: &str,
        code_verifier: &str,
    ) -> Result<(String, String)> {
        let resp = client
            .post("https://auth.openai.com/oauth/token")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", self.cfg.redirect_uri.as_str()),
                ("client_id", self.cfg.client_id.as_str()),
                ("code_verifier", code_verifier),
            ])
            .send()
            .await?;
        if resp.status() != StatusCode::OK {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "exchange token 失败: HTTP {} {}",
                status,
                truncate_text(&body, 180)
            );
        }
        let body: serde_json::Value = resp.json().await.unwrap_or_default();
        let rt = body
            .get("refresh_token")
            .and_then(|v| v.as_str())
            .filter(|v| !v.is_empty())
            .ok_or_else(|| anyhow!("token 响应缺少 refresh_token"))?;
        let id_token = body
            .get("id_token")
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        Ok((rt.to_string(), id_token.to_string()))
    }

    async fn call_sentinel_req(
        &self,
        client: &rquest::Client,
        state: &mut CodexAuthState,
        flow: &str,
        worker_id: usize,
    ) -> Result<()> {
        let cfg = sentinel::SentinelConfig {
            enabled: self.cfg.sentinel_enabled,
            strict: self.cfg.strict_sentinel,
            pow_max_iterations: self.cfg.pow_max_iterations,
        };
        sentinel::call_sentinel_req(client, &cfg, state, flow, worker_id, "RT-POW").await
    }

    fn get_sentinel_header(&self, state: &CodexAuthState, flow: &str) -> Result<String> {
        sentinel::get_sentinel_header_value(state, flow)
    }
}

#[async_trait]
impl CodexService for LiveCodexService {
    async fn fetch_refresh_token(
        &self,
        account: &RegisteredAccount,
        proxy: Option<String>,
        worker_id: usize,
    ) -> Result<(String, String)> {
        let rt_started = Instant::now();
        let retry_salt = rand::rng().random_range(0..1024usize);
        let attempt = fingerprint_salt(proxy.as_deref(), retry_salt, worker_id);
        let (client, runtime_user_agent, navigation_headers) =
            self.build_client(proxy.as_deref(), attempt)?;
        log_worker(worker_id, "RT", "初始化 OAuth 参数...");

        let code_verifier = generate_code_verifier();
        let code_challenge = generate_code_challenge(&code_verifier);
        let auth_url = self.build_authorize_url(&code_challenge, &Uuid::new_v4().to_string())?;

        let mut state =
            sentinel::SentinelState::new(&Uuid::new_v4().to_string(), &runtime_user_agent);

        let auth_started = Instant::now();
        let code = self
            .get_authorization_code(
                &client,
                &navigation_headers,
                account,
                proxy.as_deref(),
                &auth_url,
                &mut state,
                worker_id,
            )
            .await?;
        log_worker(
            worker_id,
            "RT",
            &format!(
                "授权码获取成功 (耗时 {:.2}s)",
                auth_started.elapsed().as_secs_f32()
            ),
        );

        let token_started = Instant::now();
        let (rt, id_token) = self
            .exchange_code_for_token(&client, &code, &code_verifier)
            .await?;
        log_worker(
            worker_id,
            "RT",
            &format!(
                "Token 交换成功 (耗时 {:.2}s)",
                token_started.elapsed().as_secs_f32()
            ),
        );
        log_worker(
            worker_id,
            "RT",
            &format!(
                "RT 流程完成 (总耗时 {:.2}s)",
                rt_started.elapsed().as_secs_f32()
            ),
        );
        Ok((rt, id_token))
    }
}

#[derive(Default)]
pub struct DryRunS2aService;

#[async_trait]
impl S2aService for DryRunS2aService {
    async fn test_connection(&self, _team: &S2aConfig) -> Result<()> {
        Ok(())
    }

    async fn add_account(
        &self,
        _team: &S2aConfig,
        _account: &AccountWithRt,
    ) -> Result<Option<i64>> {
        let wait_ms = random_delay_ms(40, 120);
        sleep(Duration::from_millis(wait_ms)).await;
        Ok(None)
    }

    async fn batch_refresh(&self, _team: &S2aConfig, _account_ids: &[i64]) -> Result<usize> {
        Ok(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum S2aAuthMode {
    BearerAndKeys,
    RawAndKeys,
    KeysOnly,
}

impl S2aAuthMode {
    fn name(&self) -> &'static str {
        match self {
            Self::BearerAndKeys => "bearer+x-keys",
            Self::RawAndKeys => "raw-auth+x-keys",
            Self::KeysOnly => "x-keys-only",
        }
    }

    fn all() -> [Self; 3] {
        [Self::BearerAndKeys, Self::RawAndKeys, Self::KeysOnly]
    }
}

pub struct S2aHttpService {
    client: rquest::Client,
    auth_mode_cache: Mutex<HashMap<String, S2aAuthMode>>,
}

impl S2aHttpService {
    pub fn new() -> Self {
        let client = rquest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|_| rquest::Client::new());
        Self {
            client,
            auth_mode_cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn normalized_api_base(api_base: &str) -> String {
        let trimmed = api_base.trim().trim_end_matches('/');
        if trimmed.ends_with("/api/v1") {
            return trimmed.to_string();
        }
        format!("{trimmed}/api/v1")
    }

    fn get_cached_mode(&self, base: &str) -> Option<S2aAuthMode> {
        self.auth_mode_cache
            .lock()
            .ok()
            .and_then(|m| m.get(base).copied())
    }

    fn remember_mode(&self, base: &str, mode: S2aAuthMode) {
        if let Ok(mut m) = self.auth_mode_cache.lock() {
            m.insert(base.to_string(), mode);
        }
    }

    fn mode_candidates(&self, base: &str) -> Vec<S2aAuthMode> {
        if let Some(mode) = self.get_cached_mode(base) {
            let mut out = vec![mode];
            for each in S2aAuthMode::all() {
                if each != mode {
                    out.push(each);
                }
            }
            return out;
        }
        S2aAuthMode::all().to_vec()
    }

    fn with_auth_headers(
        &self,
        req: rquest::RequestBuilder,
        admin_key: &str,
        mode: S2aAuthMode,
    ) -> rquest::RequestBuilder {
        let key = admin_key.trim();
        let mut req = req
            .header("Accept", "application/json")
            .header("X-API-Key", key)
            .header("X-Admin-Key", key);

        match mode {
            S2aAuthMode::BearerAndKeys => {
                let bearer = if key.to_ascii_lowercase().starts_with("bearer ") {
                    key.to_string()
                } else {
                    format!("Bearer {key}")
                };
                req = req.header("Authorization", bearer);
            }
            S2aAuthMode::RawAndKeys => {
                req = req.header("Authorization", key);
            }
            S2aAuthMode::KeysOnly => {}
        }
        req
    }

    fn ensure_s2a_body_ok(body: &str) -> Result<()> {
        if body.trim().is_empty() {
            return Ok(());
        }
        let json: serde_json::Value = match serde_json::from_str(body) {
            Ok(v) => v,
            Err(_) => return Ok(()),
        };

        let code_bad = json.get("code").and_then(|v| {
            if let Some(n) = v.as_i64() {
                return Some(n != 0);
            }
            if let Some(s) = v.as_str() {
                return Some(s != "0");
            }
            None
        });
        if code_bad == Some(true) {
            let msg = json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            bail!("S2A 返回失败: {}", truncate_text(msg, 200));
        }
        Ok(())
    }

    /// Best-effort extraction of account ID from S2A create response.
    /// Expected format: `{"code":0,"data":{"id":123,...}}`
    fn extract_account_id(body: &str) -> Option<i64> {
        let json: serde_json::Value = serde_json::from_str(body).ok()?;
        // Try data.id first (standard S2A response wrapper)
        json.get("data")
            .and_then(|d| d.get("id"))
            .and_then(|v| v.as_i64())
            .or_else(|| {
                // Fallback: top-level id
                json.get("id").and_then(|v| v.as_i64())
            })
    }
}

#[derive(Debug, Serialize)]
struct S2aAddPayload<'a> {
    name: String,
    platform: &'static str,
    r#type: &'static str,
    credentials: S2aCredentials<'a>,
    concurrency: usize,
    priority: usize,
    group_ids: &'a [i64],
    extra: S2aExtraConfig,
}

#[derive(Debug, Serialize)]
struct S2aCredentials<'a> {
    access_token: &'a str,
    refresh_token: &'a str,
}

#[async_trait]
impl S2aService for S2aHttpService {
    async fn test_connection(&self, team: &S2aConfig) -> Result<()> {
        let base = Self::normalized_api_base(&team.api_base);
        let url = format!("{base}/admin/accounts?page=1&size=1");
        let modes = self.mode_candidates(&base);
        let mut failures = Vec::new();

        for mode in modes {
            let resp = self
                .with_auth_headers(self.client.get(&url), &team.admin_key, mode)
                .send()
                .await;
            let resp = match resp {
                Ok(v) => v,
                Err(err) => {
                    failures.push(format!("{} 请求异常: {err}", mode.name()));
                    continue;
                }
            };
            let status = resp.status();
            if status == StatusCode::OK {
                self.remember_mode(&base, mode);
                return Ok(());
            }
            let body = resp.text().await.unwrap_or_default();
            failures.push(format!(
                "{} HTTP {} {}",
                mode.name(),
                status,
                truncate_text(&body, 120)
            ));
        }

        bail!("S2A 连接测试失败: {}", failures.join(" | "))
    }

    async fn add_account(&self, team: &S2aConfig, account: &AccountWithRt) -> Result<Option<i64>> {
        let prefix = if account.plan_type.contains("team") {
            "team"
        } else {
            "free"
        };
        let payload = S2aAddPayload {
            name: format!("{prefix}-{}", account.account),
            platform: "openai",
            r#type: "oauth",
            credentials: S2aCredentials {
                access_token: &account.token,
                refresh_token: &account.refresh_token,
            },
            concurrency: team.concurrency,
            priority: team.priority,
            group_ids: &team.group_ids,
            extra: team.extra.clone(),
        };

        let base = Self::normalized_api_base(&team.api_base);
        let url = format!("{base}/admin/accounts");

        const MAX_RETRIES: usize = 3;
        const RETRY_DELAYS_MS: [u64; 3] = [800, 2000, 4000];

        for (attempt, _) in RETRY_DELAYS_MS.iter().enumerate().take(MAX_RETRIES) {
            let modes = self.mode_candidates(&base);
            let mut failures = Vec::new();
            let mut got_server_error = false;

            for mode in modes {
                let resp = self
                    .with_auth_headers(self.client.post(&url), &team.admin_key, mode)
                    .json(&payload)
                    .send()
                    .await;
                let resp = match resp {
                    Ok(v) => v,
                    Err(err) => {
                        failures.push(format!("{} 请求异常: {err}", mode.name()));
                        continue;
                    }
                };
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();

                if status == StatusCode::OK || status == StatusCode::CREATED {
                    Self::ensure_s2a_body_ok(&body)?;
                    self.remember_mode(&base, mode);
                    // Best-effort: try to extract the account ID from response
                    let account_id = Self::extract_account_id(&body);
                    return Ok(account_id);
                }

                failures.push(format!(
                    "{} HTTP {} {}",
                    mode.name(),
                    status,
                    truncate_text(&body, 160)
                ));

                if status.is_server_error() {
                    got_server_error = true;
                    break;
                }
                if status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN {
                    break;
                }
            }

            // 5xx 服务端错误：重试
            if got_server_error && attempt + 1 < MAX_RETRIES {
                let delay = RETRY_DELAYS_MS[attempt];
                sleep(Duration::from_millis(delay)).await;
                continue;
            }

            bail!("S2A 入库失败: {}", failures.join(" | "));
        }

        bail!("S2A 入库失败: 重试耗尽");
    }

    async fn batch_refresh(&self, team: &S2aConfig, account_ids: &[i64]) -> Result<usize> {
        if account_ids.is_empty() {
            return Ok(0);
        }

        let base = Self::normalized_api_base(&team.api_base);
        let url = format!("{base}/admin/accounts/batch-refresh");

        let body = serde_json::json!({ "account_ids": account_ids });

        let modes = self.mode_candidates(&base);
        let mut failures = Vec::new();

        for mode in modes {
            let resp = self
                .with_auth_headers(self.client.post(&url), &team.admin_key, mode)
                .json(&body)
                .send()
                .await;
            let resp = match resp {
                Ok(v) => v,
                Err(err) => {
                    failures.push(format!("{} 请求异常: {err}", mode.name()));
                    continue;
                }
            };
            let status = resp.status();
            let resp_body = resp.text().await.unwrap_or_default();

            if status == StatusCode::OK {
                Self::ensure_s2a_body_ok(&resp_body)?;
                self.remember_mode(&base, mode);
                // Parse success count from response: {"code":0,"data":{"success":N,...}}
                let success_count = serde_json::from_str::<serde_json::Value>(&resp_body)
                    .ok()
                    .and_then(|v| v.get("data")?.get("success")?.as_u64())
                    .unwrap_or(0) as usize;
                return Ok(success_count);
            }

            failures.push(format!(
                "{} HTTP {} {}",
                mode.name(),
                status,
                truncate_text(&resp_body, 160)
            ));

            if status != StatusCode::UNAUTHORIZED && status != StatusCode::FORBIDDEN {
                break;
            }
        }

        bail!("S2A batch-refresh 失败: {}", failures.join(" | "));
    }
}

#[derive(Debug, Deserialize)]
struct AuthUrlResponse {
    url: String,
}

#[derive(Debug, Serialize)]
struct RegisterPayload {
    username: String,
    password: String,
}

#[derive(Debug, Serialize)]
struct OtpPayload {
    code: String,
}

#[derive(Debug, Serialize)]
struct CreateAccountPayload {
    name: String,
    birthdate: String,
}

#[derive(Debug, Deserialize)]
struct SessionResponse {
    #[serde(rename = "accessToken")]
    access_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AccountCheckResponse {
    accounts: Option<HashMap<String, AccountCheckEntry>>,
}

#[derive(Debug, Deserialize)]
struct AccountCheckEntry {
    account: AccountMeta,
}

#[derive(Debug, Deserialize)]
struct AccountMeta {
    #[serde(default)]
    plan_type: String,
}

fn extract_cookie_value(headers: &HeaderMap, key: &str) -> Option<String> {
    let prefix = format!("{key}=");
    for value in headers.get_all(header::SET_COOKIE) {
        let text = value.to_str().ok()?;
        if let Some(rest) = text.strip_prefix(&prefix) {
            return Some(rest.split(';').next().unwrap_or("").to_string());
        }
    }
    None
}

fn resolve_location(base: &rquest::Url, location: &str) -> Result<String> {
    if location.starts_with("http://") || location.starts_with("https://") {
        return Ok(location.to_string());
    }
    Ok(base.join(location)?.to_string())
}

fn resolve_redirect_location(resp: &rquest::Response, step: &str) -> Result<String> {
    let location = resp
        .headers()
        .get(header::LOCATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| anyhow!("{step}: 重定向缺少 location"))?;
    resolve_location(resp.url(), location)
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

fn fingerprint_salt(proxy: Option<&str>, retry: usize, entropy: usize) -> usize {
    let key = proxy.unwrap_or("direct");
    let base = fnv1a32(key.as_bytes()) as usize;
    base.wrapping_add(retry.wrapping_mul(131))
        .wrapping_add(entropy.wrapping_mul(17))
}

fn fnv1a32(data: &[u8]) -> u32 {
    let mut h: u32 = 2_166_136_261;
    for b in data {
        h ^= *b as u32;
        h = h.wrapping_mul(16_777_619);
    }
    h ^= h >> 16;
    h = h.wrapping_mul(2_246_822_507);
    h ^= h >> 13;
    h = h.wrapping_mul(3_266_489_909);
    h ^= h >> 16;
    h
}

fn generate_code_verifier() -> String {
    let mut bytes = [0_u8; 32];
    let mut rng = rand::rng();
    rng.fill(&mut bytes);
    general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn generate_code_challenge(code_verifier: &str) -> String {
    let digest = ring::digest::digest(&ring::digest::SHA256, code_verifier.as_bytes());
    general_purpose::URL_SAFE_NO_PAD.encode(digest.as_ref())
}

// ─── Tokens Pool Service ────────────────────────────────────────────────────

#[async_trait]
pub trait TokensPoolService: Send + Sync {
    async fn test_connection(&self, pool: &TokensPoolConfig) -> Result<()>;
    /// 入库前清理失效 token
    async fn delete_deactivated(&self, pool: &TokensPoolConfig) -> Result<usize>;
    async fn add_token(
        &self,
        pool: &TokensPoolConfig,
        refresh_token: &str,
        plan_type: &str,
    ) -> Result<()>;
    /// 批量推送多个 token（用 \n 拼接），返回本批 token 数量
    async fn add_tokens_batch(
        &self,
        pool: &TokensPoolConfig,
        refresh_tokens: &[String],
        plan_type: &str,
    ) -> Result<usize>;
}

pub struct TokensPoolHttpService {
    client: rquest::Client,
}

impl TokensPoolHttpService {
    pub fn new() -> Self {
        let client = rquest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|_| rquest::Client::new());
        Self { client }
    }

    fn normalized_base(api_base: &str) -> String {
        api_base.trim().trim_end_matches('/').to_string()
    }

    fn bearer(token: &str) -> String {
        let t = token.trim();
        if t.to_ascii_lowercase().starts_with("bearer ") {
            t.to_string()
        } else {
            format!("Bearer {t}")
        }
    }
}

#[async_trait]
impl TokensPoolService for TokensPoolHttpService {
    async fn test_connection(&self, pool: &TokensPoolConfig) -> Result<()> {
        let base = Self::normalized_base(&pool.api_base);
        let url = format!("{base}/admin-api/tokens");
        let bearer = Self::bearer(&pool.auth_token);

        // 发送一个空 token 来测试连接（或 GET 请求）
        // 由于该 API 可能不支持 GET，我们用 POST 发送一个测试请求
        let resp = self
            .client
            .get(&url)
            .header("Authorization", &bearer)
            .header("Accept", "application/json")
            .send()
            .await
            .context("Tokens Pool 连接失败")?;

        // 任何非 5xx 的响应都视为连接成功（包括 405 Method Not Allowed）
        if resp.status().is_server_error() {
            bail!("Tokens Pool 服务端错误: HTTP {}", resp.status());
        }
        Ok(())
    }

    async fn delete_deactivated(&self, pool: &TokensPoolConfig) -> Result<usize> {
        let base = Self::normalized_base(&pool.api_base);
        let url = format!("{base}/admin-api/tokens/delete_deactivate");
        let bearer = Self::bearer(&pool.auth_token);

        let resp = self
            .client
            .delete(&url)
            .header("Authorization", &bearer)
            .header("Content-Type", "application/json")
            .send()
            .await
            .context("Tokens Pool 清理失效 token 请求失败")?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "Tokens Pool 清理失效 token 失败: HTTP {} {}",
                status,
                truncate_text(&body, 200)
            );
        }
        let json: serde_json::Value = resp.json().await.unwrap_or_default();
        let count = json
            .get("count")
            .or_else(|| json.get("deleted"))
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as usize;
        Ok(count)
    }

    async fn add_token(
        &self,
        pool: &TokensPoolConfig,
        refresh_token: &str,
        plan_type: &str,
    ) -> Result<()> {
        let base = Self::normalized_base(&pool.api_base);
        let url = format!("{base}/admin-api/tokens");
        let bearer = Self::bearer(&pool.auth_token);

        let body = serde_json::json!({
            "token": refresh_token,
            "plan_type": plan_type,
            "platform": pool.platform,
            "remark": "",
            "proxy": "",
            "index": 0,
            "spliter": ""
        });

        let resp = self
            .client
            .post(&url)
            .header("Authorization", &bearer)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Tokens Pool 推送失败")?;

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let resp_body = resp.text().await.unwrap_or_default();
        bail!(
            "Tokens Pool 推送失败: HTTP {} {}",
            status,
            truncate_text(&resp_body, 200)
        );
    }

    async fn add_tokens_batch(
        &self,
        pool: &TokensPoolConfig,
        refresh_tokens: &[String],
        plan_type: &str,
    ) -> Result<usize> {
        if refresh_tokens.is_empty() {
            return Ok(0);
        }
        let count = refresh_tokens.len();
        let joined = refresh_tokens.join("\n");
        let base = Self::normalized_base(&pool.api_base);
        let url = format!("{base}/admin-api/tokens");
        let bearer = Self::bearer(&pool.auth_token);

        let body = serde_json::json!({
            "token": joined,
            "plan_type": plan_type,
            "platform": pool.platform,
            "remark": "",
            "proxy": "",
            "index": 0,
            "spliter": ""
        });

        let resp = self
            .client
            .post(&url)
            .header("Authorization", &bearer)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .context("Tokens Pool 批量推送失败")?;

        let status = resp.status();
        if status.is_success() {
            return Ok(count);
        }
        let resp_body = resp.text().await.unwrap_or_default();
        bail!(
            "Tokens Pool 批量推送失败: HTTP {} {}",
            status,
            truncate_text(&resp_body, 200)
        );
    }
}

// =================== CodexProxy 号池 Service ===================

#[async_trait]
pub trait CodexProxyPoolService: Send + Sync {
    async fn test_connection(&self, pool: &CodexProxyPoolConfig) -> Result<()>;
    async fn add_accounts_batch(
        &self,
        pool: &CodexProxyPoolConfig,
        refresh_tokens: &[String],
    ) -> Result<usize>;
}

pub struct CodexProxyPoolHttpService {
    client: rquest::Client,
}

impl CodexProxyPoolHttpService {
    pub fn new() -> Self {
        let client = rquest::Client::builder()
            .timeout(Duration::from_secs(15))
            .build()
            .unwrap_or_else(|_| rquest::Client::new());
        Self { client }
    }

    fn normalized_base(api_base: &str) -> String {
        api_base.trim().trim_end_matches('/').to_string()
    }

    fn endpoint(pool: &CodexProxyPoolConfig) -> String {
        format!(
            "{}/api/admin/accounts",
            Self::normalized_base(&pool.api_base)
        )
    }
}

#[async_trait]
impl CodexProxyPoolService for CodexProxyPoolHttpService {
    async fn test_connection(&self, pool: &CodexProxyPoolConfig) -> Result<()> {
        let url = Self::endpoint(pool);
        let payload = serde_json::json!({ "refresh_token": "" });
        let resp = self
            .client
            .post(&url)
            .header("X-Admin-Key", pool.admin_key.trim())
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("CodexProxy 连接失败")?;

        let status = resp.status();
        if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
            bail!("CodexProxy 管理密钥认证失败: HTTP {}", status);
        }
        if status == StatusCode::NOT_FOUND {
            bail!("CodexProxy 接口不存在: HTTP 404");
        }
        if status.is_server_error() {
            bail!("CodexProxy 服务端错误: HTTP {}", status);
        }
        Ok(())
    }

    async fn add_accounts_batch(
        &self,
        pool: &CodexProxyPoolConfig,
        refresh_tokens: &[String],
    ) -> Result<usize> {
        let tokens: Vec<String> = refresh_tokens
            .iter()
            .map(|rt| rt.trim())
            .filter(|rt| !rt.is_empty())
            .map(|rt| rt.to_string())
            .collect();
        if tokens.is_empty() {
            return Ok(0);
        }

        let url = Self::endpoint(pool);
        let payload = serde_json::json!({
            "refresh_token": tokens.join("\n")
        });

        let resp = self
            .client
            .post(&url)
            .header("X-Admin-Key", pool.admin_key.trim())
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("CodexProxy 批量推送失败")?;

        let status = resp.status();
        if status.is_success() {
            return Ok(tokens.len());
        }

        let body = resp.text().await.unwrap_or_default();
        bail!(
            "CodexProxy 批量推送失败: HTTP {} {}",
            status,
            truncate_text(&body, 200)
        );
    }
}

// =================== CPA 号池 Service ===================

/// CPA Token JSON 格式（与 protocol_keygen.py 兼容）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpaTokenJson {
    pub r#type: String,
    pub email: String,
    pub expired: String,
    pub id_token: String,
    pub account_id: String,
    pub access_token: String,
    pub last_refresh: String,
    pub refresh_token: String,
}

impl CpaTokenJson {
    /// 从 AccountWithRt 构造 CPA Token JSON
    pub fn from_account(acc: &crate::models::AccountWithRt) -> Self {
        use base64::Engine;
        // 解析 JWT 的 exp 字段计算过期时间
        let expired = Self::extract_jwt_expiry(&acc.token).unwrap_or_default();
        let now = chrono::Utc::now()
            .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600).unwrap());
        let last_refresh = now.format("%Y-%m-%dT%H:%M:%S+08:00").to_string();

        Self {
            r#type: "codex".to_string(),
            email: acc.account.clone(),
            expired,
            id_token: acc.id_token.clone(),
            account_id: acc.account_id.clone(),
            access_token: acc.token.clone(),
            last_refresh,
            refresh_token: acc.refresh_token.clone(),
        }
    }

    /// 从 JWT access_token 中解析 exp 字段
    fn extract_jwt_expiry(token: &str) -> Option<String> {
        let parts: Vec<&str> = token.split('.').collect();
        if parts.len() != 3 {
            return None;
        }
        let payload = parts[1];
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(payload)
            .ok()?;
        let json: serde_json::Value = serde_json::from_slice(&decoded).ok()?;
        let exp = json.get("exp")?.as_i64()?;
        let dt = chrono::DateTime::from_timestamp(exp, 0)?
            .with_timezone(&chrono::FixedOffset::east_opt(8 * 3600)?);
        Some(dt.format("%Y-%m-%dT%H:%M:%S+08:00").to_string())
    }
}

#[async_trait]
pub trait CpaPoolService: Send + Sync {
    /// 测试 CPA 平台连接
    async fn test_connection(&self, pool: &CpaPoolConfig) -> Result<()>;
    /// 上传 Token JSON 到 CPA 平台（根据 upload_method 选择方式）
    async fn upload_token(&self, pool: &CpaPoolConfig, token_json: &CpaTokenJson) -> Result<()>;
    /// 扫描号池健康状况：拉取库存 + 并发探测使用量
    async fn scan_health(&self, pool: &CpaPoolConfig) -> Result<CpaHealthReport>;
    /// 删除指定账号
    async fn delete_account(&self, pool: &CpaPoolConfig, name: &str) -> Result<()>;
    /// 设置账号启用/禁用状态
    async fn set_account_disabled(&self, pool: &CpaPoolConfig, name: &str, disabled: bool) -> Result<()>;
}

/// CPA 号池健康扫描报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpaHealthReport {
    pub total: usize,
    pub valid: usize,
    pub invalid_401: usize,
    pub quota_exhausted: usize,
    pub disabled: usize,
    pub probe_errors: usize,
    /// 401 账号名称列表（用于清理）
    #[serde(skip_serializing)]
    pub invalid_401_names: Vec<String>,
    /// 配额耗尽账号名称列表（用于清理）
    #[serde(skip_serializing)]
    pub quota_exhausted_names: Vec<String>,
}

pub struct CpaPoolHttpService {
    client: rquest::Client,
}

impl CpaPoolHttpService {
    pub fn new() -> Self {
        let client = rquest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| rquest::Client::new());
        Self { client }
    }

    fn bearer(token: &str) -> String {
        let t = token.trim();
        if t.to_ascii_lowercase().starts_with("bearer ") {
            t.to_string()
        } else {
            format!("Bearer {t}")
        }
    }

    /// 拉取 auth-files 列表
    async fn fetch_auth_files(&self, pool: &CpaPoolConfig) -> Result<Vec<serde_json::Value>> {
        let url = pool.auth_files_url();
        let bearer = Self::bearer(&pool.auth_token);
        let resp = self.client
            .get(&url)
            .header("Authorization", &bearer)
            .send()
            .await
            .context("CPA 拉取 auth-files 失败")?;
        if !resp.status().is_success() {
            bail!("CPA 拉取 auth-files: HTTP {}", resp.status());
        }
        let body: serde_json::Value = resp.json().await.context("解析 auth-files 响应失败")?;
        let files = body.get("files")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        Ok(files)
    }

    /// 通过 api-call 探测单个账号的 wham/usage
    async fn probe_account_usage(&self, pool: &CpaPoolConfig, auth_index: &str, chatgpt_account_id: &str) -> Result<serde_json::Value> {
        let url = pool.api_call_url();
        let bearer = Self::bearer(&pool.auth_token);

        let mut header_map = serde_json::Map::new();
        header_map.insert("Authorization".into(), serde_json::Value::String("Bearer $TOKEN$".into()));
        header_map.insert("Content-Type".into(), serde_json::Value::String("application/json".into()));
        header_map.insert("User-Agent".into(), serde_json::Value::String("codex_cli_rs/0.76.0".into()));
        if !chatgpt_account_id.is_empty() {
            header_map.insert("Chatgpt-Account-Id".into(), serde_json::Value::String(chatgpt_account_id.into()));
        }

        let payload = serde_json::json!({
            "authIndex": auth_index,
            "method": "GET",
            "url": "https://chatgpt.com/backend-api/wham/usage",
            "header": header_map,
        });

        let resp = self.client
            .post(&url)
            .header("Authorization", &bearer)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("CPA api-call 请求失败")?;

        if !resp.status().is_success() {
            bail!("CPA api-call: HTTP {}", resp.status());
        }
        let body: serde_json::Value = resp.json().await.context("解析 api-call 响应失败")?;
        Ok(body)
    }
}

#[async_trait]
impl CpaPoolService for CpaPoolHttpService {
    async fn test_connection(&self, pool: &CpaPoolConfig) -> Result<()> {
        let url = pool.auth_files_url();
        let bearer = Self::bearer(&pool.auth_token);

        // 用 GET auth-files 验证连接和认证
        let resp = self
            .client
            .get(&url)
            .header("Authorization", &bearer)
            .send()
            .await
            .context("CPA 平台连接失败")?;

        if resp.status().is_server_error() {
            bail!("CPA 平台服务端错误: HTTP {}", resp.status());
        }
        if resp.status() == rquest::StatusCode::UNAUTHORIZED {
            bail!("CPA 平台认证失败: HTTP 401");
        }
        Ok(())
    }

    async fn upload_token(&self, pool: &CpaPoolConfig, token_json: &CpaTokenJson) -> Result<()> {
        let base_url = pool.auth_files_url();
        let bearer = Self::bearer(&pool.auth_token);

        let json_bytes = serde_json::to_vec(token_json)
            .context("序列化 CPA Token JSON 失败")?;
        let filename = format!("{}.json", token_json.email);

        let resp = if pool.upload_method == "json" {
            // JSON 上传方式: POST {base}/v0/management/auth-files?name={filename}
            let encoded_name = urlencoding::encode(&filename);
            let url = format!("{}?name={}", base_url, encoded_name);
            self.client
                .post(&url)
                .header("Authorization", &bearer)
                .header("Content-Type", "application/json")
                .body(json_bytes)
                .send()
                .await
                .context("CPA JSON 上传失败")?
        } else {
            // multipart 上传方式（默认，与 protocol_keygen.py 一致）
            let part = rquest::multipart::Part::bytes(json_bytes)
                .file_name(filename)
                .mime_str("application/json")?;
            let form = rquest::multipart::Form::new().part("file", part);
            self.client
                .post(&base_url)
                .header("Authorization", &bearer)
                .multipart(form)
                .send()
                .await
                .context("CPA multipart 上传失败")?
        };

        let status = resp.status();
        if status.is_success() {
            return Ok(());
        }
        let resp_body = resp.text().await.unwrap_or_default();
        bail!(
            "CPA 上传失败: HTTP {} {}",
            status,
            truncate_text(&resp_body, 200)
        );
    }

    async fn scan_health(&self, pool: &CpaPoolConfig) -> Result<CpaHealthReport> {
        // 1. 拉取 auth-files 列表
        let files = self.fetch_auth_files(pool).await?;

        let total;
        let mut valid = 0usize;
        let mut invalid_401 = 0usize;
        let mut quota_exhausted = 0usize;
        let mut disabled_count = 0usize;
        let mut probe_errors = 0usize;
        let mut invalid_401_names = Vec::new();
        let mut quota_exhausted_names = Vec::new();

        // 过滤 codex 类型的账号
        let accounts: Vec<_> = files.iter().filter(|f| {
            f.get("type").and_then(|v| v.as_str()).unwrap_or_default() == "codex"
        }).collect();
        total = accounts.len();

        // 2. 按状态分类：先处理 unavailable 和 disabled
        let mut need_probe: Vec<(&serde_json::Value, String, String)> = Vec::new();

        for acc in &accounts {
            let name = acc.get("name").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            let is_unavailable = acc.get("unavailable").and_then(|v| v.as_bool()).unwrap_or(false);
            let is_disabled = acc.get("disabled").and_then(|v| v.as_bool()).unwrap_or(false);

            if is_unavailable {
                invalid_401 += 1;
                invalid_401_names.push(name);
                continue;
            }
            if is_disabled {
                disabled_count += 1;
                continue;
            }

            let auth_index = acc.get("auth_index")
                .or_else(|| acc.get("authIndex"))
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();
            let chatgpt_account_id = acc.get("chatgpt_account_id")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_string();

            need_probe.push((acc, auth_index, chatgpt_account_id));
        }

        // 3. 并发探测使用量
        let semaphore = Arc::new(tokio::sync::Semaphore::new(pool.concurrency.max(1).min(50)));
        let mut handles = Vec::new();

        for (_acc, auth_index, chatgpt_account_id) in &need_probe {
            let sem = Arc::clone(&semaphore);
            let pool_clone = pool.clone();
            let ai = auth_index.clone();
            let cai = chatgpt_account_id.clone();
            // 为每个探测创建独立的 service（复用 client 配置）
            let client = self.client.clone();

            handles.push(tokio::spawn(async move {
                let _permit = sem.acquire().await;
                let svc = CpaPoolHttpService { client };
                let result = svc.probe_account_usage(&pool_clone, &ai, &cai).await;
                result
            }));
        }

        let probe_results = futures::future::join_all(handles).await;

        for (i, result) in probe_results.into_iter().enumerate() {
            let name = need_probe[i].0.get("name").and_then(|v| v.as_str()).unwrap_or_default().to_string();

            match result {
                Ok(Ok(body)) => {
                    let status_code = body.get("status_code").and_then(|v| v.as_i64()).unwrap_or(0);
                    if status_code == 401 {
                        invalid_401 += 1;
                        invalid_401_names.push(name);
                        continue;
                    }

                    // 解析 body 中的 wham/usage 响应
                    let usage_body = if let Some(b) = body.get("body") {
                        if b.is_string() {
                            serde_json::from_str(b.as_str().unwrap_or("{}")).unwrap_or_default()
                        } else {
                            b.clone()
                        }
                    } else {
                        serde_json::Value::Null
                    };

                    if status_code == 200 {
                        // 检查是否配额耗尽
                        let limit_reached = usage_body
                            .get("rate_limit")
                            .and_then(|rl| rl.get("limit_reached"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false);

                        if limit_reached {
                            quota_exhausted += 1;
                            quota_exhausted_names.push(name);
                        } else {
                            valid += 1;
                        }
                    } else {
                        probe_errors += 1;
                    }
                }
                Ok(Err(_)) | Err(_) => {
                    probe_errors += 1;
                }
            }
        }

        Ok(CpaHealthReport {
            total,
            valid,
            invalid_401,
            quota_exhausted,
            disabled: disabled_count,
            probe_errors,
            invalid_401_names,
            quota_exhausted_names,
        })
    }

    async fn delete_account(&self, pool: &CpaPoolConfig, name: &str) -> Result<()> {
        let encoded = urlencoding::encode(name);
        let url = format!("{}?name={}", pool.auth_files_url(), encoded);
        let bearer = Self::bearer(&pool.auth_token);

        let resp = self.client
            .delete(&url)
            .header("Authorization", &bearer)
            .send()
            .await
            .context("CPA 删除账号失败")?;

        if resp.status().is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        bail!("CPA 删除账号失败: HTTP {}", truncate_text(&body, 200));
    }

    async fn set_account_disabled(&self, pool: &CpaPoolConfig, name: &str, disabled: bool) -> Result<()> {
        let url = pool.auth_status_url();
        let bearer = Self::bearer(&pool.auth_token);

        let payload = serde_json::json!({
            "name": name,
            "disabled": disabled,
        });

        let resp = self.client
            .patch(&url)
            .header("Authorization", &bearer)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("CPA 设置账号状态失败")?;

        if resp.status().is_success() {
            return Ok(());
        }
        let body = resp.text().await.unwrap_or_default();
        bail!("CPA 设置账号状态失败: HTTP {}", truncate_text(&body, 200));
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{S2aAddPayload, S2aCredentials};
    use crate::config::S2aExtraConfig;

    #[test]
    fn s2a_add_payload_serializes_supported_extra_variants() {
        let cases = [
            (
                S2aExtraConfig {
                    openai_passthrough: None,
                    openai_oauth_responses_websockets_v2_mode: None,
                    openai_oauth_responses_websockets_v2_enabled: None,
                },
                json!({}),
            ),
            (
                S2aExtraConfig {
                    openai_passthrough: Some(true),
                    openai_oauth_responses_websockets_v2_mode: None,
                    openai_oauth_responses_websockets_v2_enabled: None,
                },
                json!({"openai_passthrough": true}),
            ),
            (
                S2aExtraConfig {
                    openai_passthrough: None,
                    openai_oauth_responses_websockets_v2_mode: Some("passthrough".to_string()),
                    openai_oauth_responses_websockets_v2_enabled: Some(true),
                },
                json!({
                    "openai_oauth_responses_websockets_v2_mode": "passthrough",
                    "openai_oauth_responses_websockets_v2_enabled": true
                }),
            ),
            (
                S2aExtraConfig::default(),
                json!({
                    "openai_passthrough": true,
                    "openai_oauth_responses_websockets_v2_mode": "passthrough",
                    "openai_oauth_responses_websockets_v2_enabled": true
                }),
            ),
        ];

        for (extra, expected_extra) in cases {
            let payload = S2aAddPayload {
                name: "team-demo@example.com".to_string(),
                platform: "openai",
                r#type: "oauth",
                credentials: S2aCredentials {
                    access_token: "access-token",
                    refresh_token: "refresh-token",
                },
                concurrency: 200,
                priority: 50,
                group_ids: &[2],
                extra,
            };

            let json = serde_json::to_value(&payload).expect("payload 应可序列化");
            assert_eq!(json["extra"], expected_extra);
        }
    }
}
