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

use crate::config::{CodexRuntimeConfig, RegisterRuntimeConfig, S2aConfig};
use crate::email_service::{EmailService, WaitCodeOptions};
use crate::fingerprint::{
    build_fingerprint_material, is_retryable_challenge_error, looks_like_challenge_page,
};
use crate::models::{AccountSeed, AccountWithRt, RegisteredAccount};
use crate::util::{log_worker, random_delay_ms, random_hex, summarize_user_agent, token_preview};

#[derive(Debug, Clone)]
pub struct RegisterInput {
    pub seed: AccountSeed,
    pub proxy: Option<String>,
    pub worker_id: usize,
    pub task_index: usize,
    pub task_total: usize,
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
    async fn fetch_refresh_token(
        &self,
        account: &RegisteredAccount,
        proxy: Option<String>,
        worker_id: usize,
    ) -> Result<String>;
}

#[async_trait]
pub trait S2aService: Send + Sync {
    async fn test_connection(&self, team: &S2aConfig) -> Result<()>;
    async fn add_account(&self, team: &S2aConfig, account: &AccountWithRt) -> Result<()>;
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
        })
    }
}

pub struct LiveRegisterService {
    cfg: RegisterRuntimeConfig,
    email_service: Arc<EmailService>,
}

impl LiveRegisterService {
    pub fn new(cfg: RegisterRuntimeConfig, email_service: Arc<EmailService>) -> Self {
        Self { cfg, email_service }
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
        // 先记录当前邮件基线，再触发发码，避免“秒到验证码”被当成基线跳过
        let baseline_email_id = self
            .email_service
            .get_latest_email_id(&input.seed.account, mail_proxy)
            .await
            .unwrap_or(0);

        log_worker(input.worker_id, "注册", "发送验证码...");
        self.send_verification_email(client).await?;

        let mut otp_handle = self
            .email_service
            .start_background_poll(
                &input.seed.account,
                mail_proxy,
                WaitCodeOptions {
                    after_email_id: baseline_email_id,
                    max_retries: self.cfg.otp_max_retries,
                    interval_ms: self.cfg.otp_interval_ms,
                    size: 5,
                    subject_must_contain_code_word: false,
                    strict_fetch: true,
                },
            )
            .await?;
        log_worker(input.worker_id, "注册", "等待验证码 (20s超时)...");

        // === 分级超时策略 ===
        // 第1轮：20s 超时等待后台轮询结果
        let otp_code = match tokio::time::timeout(Duration::from_secs(20), otp_handle.wait()).await
        {
            Ok(Ok(Ok(code))) => code,
            Ok(Ok(Err(e))) => bail!("验证码轮询失败: {e}"),
            Ok(Err(_)) => bail!("后台验证码轮询任务意外终止"),
            Err(_) => {
                otp_handle.cancel();
                // 第1轮超时，重新轮询邮箱
                log_worker(
                    input.worker_id,
                    "注册",
                    "验证码 20s 超时，重新轮询邮箱 (10s重试)...",
                );

                let mail_proxy_retry = if self.cfg.use_proxy_for_mail {
                    input.proxy.as_deref()
                } else {
                    None
                };
                let mut retry_handle = self
                    .email_service
                    .start_background_poll(
                        &input.seed.account,
                        mail_proxy_retry,
                        WaitCodeOptions {
                            after_email_id: baseline_email_id,
                            max_retries: 8,
                            interval_ms: 800,
                            size: 5,
                            subject_must_contain_code_word: false,
                            strict_fetch: false, // 网络异常不中断
                        },
                    )
                    .await?;

                match tokio::time::timeout(Duration::from_secs(10), retry_handle.wait()).await {
                    Ok(Ok(Ok(code))) => code,
                    _ => {
                        retry_handle.cancel();
                        log_worker(input.worker_id, "ERR", "验证码 10s 重试仍超时，跳过此账号");
                        bail!("otp_timeout_skip");
                    }
                }
            }
        };
        log_worker(input.worker_id, "OK", &format!("验证码: {otp_code}"));
        log_worker(input.worker_id, "注册", &format!("验证码: {otp_code}"));

        self.validate_otp(client, &otp_code).await?;
        log_worker(input.worker_id, "OK", "OTP 校验成功");

        log_worker(input.worker_id, "注册", "创建账户...");
        self.create_account(client, &input.seed.real_name, &input.seed.birthdate)
            .await?;
        log_worker(input.worker_id, "OK", "账户创建成功");

        let access_token = self.get_session_token(client).await?;
        log_worker(
            input.worker_id,
            "OK",
            &format!("Token: {}", token_preview(&access_token)),
        );

        // ========== 支付步骤 ==========
        if self.cfg.payment.payment_retries > 0 {
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
            log_worker(input.worker_id, "跳过", "支付未启用，注册 free 账号");
        }

        // 支付后轮询等待 plan_type 更新（Stripe → OpenAI 同步需要时间）
        let paid = self.cfg.payment.payment_retries > 0;
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
    ) -> Result<()> {
        let payload = CreateAccountPayload {
            name: real_name.to_string(),
            birthdate: birthdate.to_string(),
        };
        let resp = client
            .post("https://auth.openai.com/api/accounts/create_account")
            .header("Origin", "https://auth.openai.com")
            .json(&payload)
            .send()
            .await?;

        if resp.status() != StatusCode::OK {
            bail!("创建账号失败: HTTP {}", resp.status());
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
                    street,
                    city,
                    postal_code,
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
    ) -> Result<String> {
        let wait_ms = random_delay_ms(80, 220);
        sleep(Duration::from_millis(wait_ms)).await;
        Ok(format!("rt_{}", random_hex(48)))
    }
}

pub struct LiveCodexService {
    cfg: CodexRuntimeConfig,
    email_service: Arc<EmailService>,
}

#[derive(Debug, Clone)]
struct CodexAuthState {
    sid: String,
    device_id: String,
    user_agent: String,
    sentinel_token: String,
    solved_pow: String,
}

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
                            max_retries: self.cfg.otp_max_retries,
                            interval_ms: self.cfg.otp_interval_ms,
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

    async fn exchange_code_for_token(
        &self,
        client: &rquest::Client,
        code: &str,
        code_verifier: &str,
    ) -> Result<String> {
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
        Ok(rt.to_string())
    }

    async fn call_sentinel_req(
        &self,
        client: &rquest::Client,
        state: &mut CodexAuthState,
        flow: &str,
        worker_id: usize,
    ) -> Result<()> {
        let sentinel_started = Instant::now();
        let init_token = self.get_requirements_token(&state.sid, &state.user_agent);
        state.solved_pow = init_token.clone();

        if !self.cfg.sentinel_enabled {
            log_worker(
                worker_id,
                "RT-POW",
                &format!("{flow}: Sentinel 已关闭，跳过"),
            );
            return Ok(());
        }

        let resp = client
            .post("https://sentinel.openai.com/backend-api/sentinel/req")
            .header("Content-Type", "application/json")
            .json(&serde_json::json!({
                "p": init_token,
                "id": state.device_id,
                "flow": flow
            }))
            .send()
            .await;

        let resp = match resp {
            Ok(v) => v,
            Err(err) => {
                log_worker(
                    worker_id,
                    "RT-POW",
                    &format!(
                        "{flow}: Sentinel 请求异常 ({:.2}s): {err}",
                        sentinel_started.elapsed().as_secs_f32()
                    ),
                );
                if self.cfg.strict_sentinel {
                    bail!("sentinel 请求失败: {err}");
                }
                return Ok(());
            }
        };
        if resp.status() != StatusCode::OK {
            log_worker(
                worker_id,
                "RT-POW",
                &format!(
                    "{flow}: Sentinel 状态异常 HTTP {} ({:.2}s)",
                    resp.status(),
                    sentinel_started.elapsed().as_secs_f32()
                ),
            );
            if self.cfg.strict_sentinel {
                bail!("sentinel 状态异常: HTTP {}", resp.status());
            }
            return Ok(());
        }

        let data: serde_json::Value = resp.json().await.unwrap_or_default();
        if let Some(token) = data.get("token").and_then(|v| v.as_str()) {
            state.sentinel_token = token.to_string();
        }

        let pow_required = data
            .get("proofofwork")
            .and_then(|v| v.get("required"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !pow_required {
            log_worker(
                worker_id,
                "RT-POW",
                &format!(
                    "{flow}: 不需要 PoW (sentinel {:.2}s)",
                    sentinel_started.elapsed().as_secs_f32()
                ),
            );
            return Ok(());
        }

        let seed = data
            .get("proofofwork")
            .and_then(|v| v.get("seed"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        let difficulty = data
            .get("proofofwork")
            .and_then(|v| v.get("difficulty"))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        if seed.is_empty() || difficulty.is_empty() {
            log_worker(worker_id, "RT-POW", &format!("{flow}: PoW 参数缺失"));
            if self.cfg.strict_sentinel {
                bail!("sentinel PoW 参数缺失");
            }
            return Ok(());
        }

        log_worker(
            worker_id,
            "RT-POW",
            &format!(
                "{flow}: 开始 PoW difficulty={} max_iter={}",
                truncate_text(difficulty, 10),
                self.cfg.pow_max_iterations
            ),
        );
        let pow_started = Instant::now();
        // 将 CPU 密集型 PoW 移到阻塞线程池，避免卡住 tokio 工作线程
        let seed_owned = seed.to_string();
        let difficulty_owned = difficulty.to_string();
        let sid_owned = state.sid.clone();
        let ua_owned = state.user_agent.clone();
        let max_iterations = self.cfg.pow_max_iterations;
        let pow_result = tokio::task::spawn_blocking(move || {
            Self::solve_pow(
                &seed_owned,
                &difficulty_owned,
                &sid_owned,
                &ua_owned,
                max_iterations,
            )
        })
        .await
        .ok()
        .flatten();

        if let Some((solved, iterations)) = pow_result {
            state.solved_pow = format!("gAAAAAB{solved}");
            log_worker(
                worker_id,
                "RT-POW",
                &format!(
                    "{flow}: PoW 成功 iter={} 耗时 {:.2}s",
                    iterations,
                    pow_started.elapsed().as_secs_f32()
                ),
            );
            return Ok(());
        }
        log_worker(
            worker_id,
            "RT-POW",
            &format!(
                "{flow}: PoW 失败 (max_iter={}) 耗时 {:.2}s",
                self.cfg.pow_max_iterations,
                pow_started.elapsed().as_secs_f32()
            ),
        );
        if self.cfg.strict_sentinel {
            bail!("sentinel PoW 计算失败");
        }
        Ok(())
    }

    fn get_sentinel_header(&self, state: &CodexAuthState, flow: &str) -> Result<String> {
        let mut obj = serde_json::Map::new();
        obj.insert(
            "p".to_string(),
            serde_json::Value::String(state.solved_pow.clone()),
        );
        obj.insert(
            "id".to_string(),
            serde_json::Value::String(state.device_id.clone()),
        );
        obj.insert(
            "flow".to_string(),
            serde_json::Value::String(flow.to_string()),
        );
        if !state.sentinel_token.is_empty() {
            obj.insert(
                "c".to_string(),
                serde_json::Value::String(state.sentinel_token.clone()),
            );
        }
        Ok(serde_json::to_string(&serde_json::Value::Object(obj))?)
    }

    fn solve_pow(
        seed: &str,
        difficulty: &str,
        sid: &str,
        user_agent: &str,
        max_iterations: usize,
    ) -> Option<(String, usize)> {
        let mut config = Self::build_pow_config(sid, user_agent);
        let seed_bytes = seed.as_bytes();
        let prefix_len = difficulty.len().min(8);
        if prefix_len == 0 {
            let encoded = general_purpose::STANDARD.encode(serde_json::to_vec(&config).ok()?);
            return Some((format!("{encoded}~S"), 0));
        }
        let target_prefix = difficulty.get(..prefix_len)?;

        for iteration in 0..max_iterations {
            config[3] = serde_json::Value::from(iteration as i64);
            config[9] = serde_json::Value::from(0);

            let json_str = serde_json::to_vec(&config).ok()?;
            let encoded = general_purpose::STANDARD.encode(json_str);

            let mut combined = Vec::with_capacity(seed_bytes.len() + encoded.len());
            combined.extend_from_slice(seed_bytes);
            combined.extend_from_slice(encoded.as_bytes());
            let hash = fnv1a32(&combined);
            let hex_hash = format!("{hash:08x}");
            if let Some(prefix) = hex_hash.get(..prefix_len)
                && prefix <= target_prefix
            {
                return Some((format!("{encoded}~S"), iteration + 1));
            }
        }
        None
    }

    fn get_requirements_token(&self, sid: &str, user_agent: &str) -> String {
        let mut config = Self::build_pow_config(sid, user_agent);
        config[3] = serde_json::Value::from(0);
        config[9] = serde_json::Value::from(0);
        let encoded =
            general_purpose::STANDARD.encode(serde_json::to_vec(&config).unwrap_or_default());
        format!("gAAAAAC{encoded}~S")
    }

    fn build_pow_config(sid: &str, user_agent: &str) -> Vec<serde_json::Value> {
        let mut rng = rand::rng();
        let now_ms = unix_millis();
        let parse_time = fixed_parse_time_string();
        vec![
            serde_json::Value::from(rng.random_range(2500..3500)),
            serde_json::Value::String(parse_time),
            serde_json::Value::from(4_294_967_296_u64),
            serde_json::Value::from(0),
            serde_json::Value::String(user_agent.to_string()),
            serde_json::Value::String(
                "chrome-extension://pgojnojmmhpofjgdmaebadhbocahppod/assets/aW5qZWN0X2hhc2g/aW5qZ"
                    .to_string(),
            ),
            serde_json::Value::Null,
            serde_json::Value::String("zh-CN".to_string()),
            serde_json::Value::String("zh-CN".to_string()),
            serde_json::Value::from(0),
            serde_json::Value::String("canShare-function canShare() { [native code] }".to_string()),
            serde_json::Value::String(format!(
                "_reactListening{}",
                rng.random_range(1_000_000..10_000_000)
            )),
            serde_json::Value::String("onhashchange".to_string()),
            serde_json::Value::from(now_ms as f64),
            serde_json::Value::String(sid.to_string()),
            serde_json::Value::String(String::new()),
            serde_json::Value::from(24),
            serde_json::Value::from(now_ms - rng.random_range(10_000..50_000)),
        ]
    }
}

#[async_trait]
impl CodexService for LiveCodexService {
    async fn fetch_refresh_token(
        &self,
        account: &RegisteredAccount,
        proxy: Option<String>,
        worker_id: usize,
    ) -> Result<String> {
        let rt_started = Instant::now();
        let retry_salt = rand::rng().random_range(0..1024usize);
        let attempt = fingerprint_salt(proxy.as_deref(), retry_salt, worker_id);
        let (client, runtime_user_agent, navigation_headers) =
            self.build_client(proxy.as_deref(), attempt)?;
        log_worker(worker_id, "RT", "初始化 OAuth 参数...");

        let code_verifier = generate_code_verifier();
        let code_challenge = generate_code_challenge(&code_verifier);
        let auth_url = self.build_authorize_url(&code_challenge, &Uuid::new_v4().to_string())?;

        let mut state = CodexAuthState {
            sid: Uuid::new_v4().to_string(),
            device_id: Uuid::new_v4().to_string(),
            user_agent: runtime_user_agent,
            sentinel_token: String::new(),
            solved_pow: String::new(),
        };
        state.solved_pow = self.get_requirements_token(&state.sid, &state.user_agent);

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
        let rt = self
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
        Ok(rt)
    }
}

#[derive(Default)]
pub struct DryRunS2aService;

#[async_trait]
impl S2aService for DryRunS2aService {
    async fn test_connection(&self, _team: &S2aConfig) -> Result<()> {
        Ok(())
    }

    async fn add_account(&self, _team: &S2aConfig, _account: &AccountWithRt) -> Result<()> {
        let wait_ms = random_delay_ms(40, 120);
        sleep(Duration::from_millis(wait_ms)).await;
        Ok(())
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

    fn normalized_api_base(api_base: &str) -> String {
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

    async fn add_account(&self, team: &S2aConfig, account: &AccountWithRt) -> Result<()> {
        let prefix = if account.plan_type.contains("team") { "team" } else { "free" };
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
        };

        let base = Self::normalized_api_base(&team.api_base);
        let url = format!("{base}/admin/accounts");

        const MAX_RETRIES: usize = 3;
        const RETRY_DELAYS_MS: [u64; 3] = [800, 2000, 4000];

        for attempt in 0..MAX_RETRIES {
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
                    return Ok(());
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

fn unix_millis() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    dur.as_millis() as i64
}

fn fixed_parse_time_string() -> String {
    "Tue Jan 30 2024 12:00:00 GMT+0000 (Coordinated Universal Time)".to_string()
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
