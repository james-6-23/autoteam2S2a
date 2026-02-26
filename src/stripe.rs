use std::time::Duration;

use anyhow::{Result, bail};
use regex::Regex;
use tokio::time::sleep;
use uuid::Uuid;

use crate::util::{log_worker, random_delay_ms, random_hex};

// ── Stripe 常量 (默认值) ────────────────────────────────────────

pub const DEFAULT_STRIPE_VERSION: &str = "2020-08-27;custom_checkout_beta=v1";
pub const DEFAULT_STRIPE_JS_VERSION: &str = "c8cd270e71";
pub const DEFAULT_STRIPE_PUBLIC_KEY: &str = "pk_live_51Pj377KslHRdbaPgTJYjThzH3f5dt1N1vK7LUp0qh0yNSarhfZ6nfbG7FFlh8KLxVkvdMWN5o6Mc4Vda6NHaSnaV00C2Sbl8Zs";

// ── 配置结构 ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct PaymentRuntimeConfig {
    pub stripe_public_key: String,
    pub stripe_version: String,
    pub stripe_js_version: String,
    pub seat_quantity: usize,
    pub payment_retries: usize,
    pub poll_max_attempts: usize,
    pub poll_interval_ms: u64,
}

impl Default for PaymentRuntimeConfig {
    fn default() -> Self {
        Self {
            stripe_public_key: DEFAULT_STRIPE_PUBLIC_KEY.to_string(),
            stripe_version: DEFAULT_STRIPE_VERSION.to_string(),
            stripe_js_version: DEFAULT_STRIPE_JS_VERSION.to_string(),
            seat_quantity: 5,
            payment_retries: 3,
            poll_max_attempts: 15,
            poll_interval_ms: 500,
        }
    }
}

// ── Stripe SEPA 支付处理器 ──────────────────────────────────────

/// Stripe SEPA 支付三步曲处理器
pub struct StripePaymentService {
    checkout_url: String,
    session_id: String,
    cfg: PaymentRuntimeConfig,
    init_checksum: String,
    js_checksum: String,
    guid: String,
    muid: String,
    sid: String,
    client_session_id: String,
    checkout_config_id: String,
    client: rquest::Client,
}

impl StripePaymentService {
    /// 从 checkout URL 和已有的 rquest::Client 创建支付处理器
    pub fn new(
        checkout_url: &str,
        client: rquest::Client,
        cfg: PaymentRuntimeConfig,
    ) -> Result<Self> {
        let session_id = extract_session_id(checkout_url).unwrap_or_default();
        if session_id.is_empty() {
            bail!("无法从 checkout URL 提取 session_id: {checkout_url}");
        }

        Ok(Self {
            checkout_url: checkout_url.to_string(),
            session_id,
            cfg,
            init_checksum: String::new(),
            js_checksum: String::new(),
            guid: generate_stripe_fingerprint(),
            muid: generate_stripe_fingerprint(),
            sid: generate_stripe_fingerprint(),
            client_session_id: Uuid::new_v4().to_string(),
            checkout_config_id: Uuid::new_v4().to_string(),
            client,
        })
    }

    // ── Step 0: 获取 checkout 页面参数 ────────────────────────

    pub async fn fetch_checkout_page(&mut self) -> Result<()> {
        let resp = self
            .client
            .get(&self.checkout_url)
            .header(
                "Accept",
                "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            )
            .send()
            .await?;

        if resp.status().is_success() {
            let body = resp.text().await.unwrap_or_default();

            // 提取 initChecksum
            if let Some(caps) = Regex::new(r#""initChecksum":\s*"([^"]+)""#)
                .ok()
                .and_then(|re| re.captures(&body))
            {
                self.init_checksum = caps[1].to_string();
            }

            // 提取 jsChecksum
            if let Some(caps) = Regex::new(r#""jsChecksum":\s*"([^"]+)""#)
                .ok()
                .and_then(|re| re.captures(&body))
            {
                self.js_checksum = caps[1].to_string();
            }
        }

        Ok(())
    }

    // ── Step 1: 创建支付方式 ─────────────────────────────────

    #[allow(clippy::too_many_arguments)]
    pub async fn create_payment_method(
        &self,
        iban: &str,
        name: &str,
        email: &str,
        address: &str,
        city: &str,
        postal_code: &str,
        country: &str,
    ) -> Result<String> {
        let payment_user_agent = format!(
            "stripe.js/{v}; stripe-js-v3/{v}; checkout",
            v = self.cfg.stripe_js_version
        );

        let form_params = [
            ("type", "sepa_debit"),
            ("sepa_debit[iban]", iban),
            ("billing_details[name]", name),
            ("billing_details[email]", email),
            ("billing_details[address][country]", country),
            ("billing_details[address][line1]", address),
            ("billing_details[address][city]", city),
            ("billing_details[address][postal_code]", postal_code),
            ("guid", &self.guid),
            ("muid", &self.muid),
            ("sid", &self.sid),
            ("_stripe_version", &self.cfg.stripe_version),
            ("key", &self.cfg.stripe_public_key),
            ("payment_user_agent", &payment_user_agent),
            (
                "client_attribution_metadata[client_session_id]",
                &self.client_session_id,
            ),
            (
                "client_attribution_metadata[checkout_session_id]",
                &self.session_id,
            ),
            (
                "client_attribution_metadata[merchant_integration_source]",
                "checkout",
            ),
            (
                "client_attribution_metadata[merchant_integration_version]",
                "hosted_checkout",
            ),
            (
                "client_attribution_metadata[payment_method_selection_flow]",
                "automatic",
            ),
            (
                "client_attribution_metadata[checkout_config_id]",
                &self.checkout_config_id,
            ),
        ];

        let resp = self
            .client
            .post("https://api.stripe.com/v1/payment_methods")
            .headers(stripe_headers())
            .form(&form_params)
            .send()
            .await?;

        let status = resp.status();
        if status.is_success() {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            if let Some(id) = body.get("id").and_then(|v| v.as_str()) {
                return Ok(id.to_string());
            }
            bail!("创建支付方式失败: 响应中无 id 字段");
        }

        let body_text = resp.text().await.unwrap_or_default();
        bail!(
            "创建支付方式失败: HTTP {} {}",
            status,
            truncate(&body_text, 200)
        );
    }

    // ── Step 2: 确认支付 ─────────────────────────────────────

    pub async fn confirm_payment(&self, payment_method_id: &str) -> Result<()> {
        let api_url = format!(
            "https://api.stripe.com/v1/payment_pages/{}/confirm",
            self.session_id
        );

        let mut form: Vec<(&str, &str)> = vec![
            ("eid", "NA"),
            ("payment_method", payment_method_id),
            ("expected_amount", "0"),
            ("consent[terms_of_service]", "accepted"),
            ("tax_id_collection[purchasing_as_business]", "false"),
            ("expected_payment_method_type", "sepa_debit"),
            ("_stripe_version", &self.cfg.stripe_version),
            ("guid", &self.guid),
            ("muid", &self.muid),
            ("sid", &self.sid),
            ("key", &self.cfg.stripe_public_key),
            ("version", &self.cfg.stripe_js_version),
            ("referrer", "https://chatgpt.com"),
            (
                "client_attribution_metadata[client_session_id]",
                &self.client_session_id,
            ),
            (
                "client_attribution_metadata[checkout_session_id]",
                &self.session_id,
            ),
            (
                "client_attribution_metadata[merchant_integration_source]",
                "checkout",
            ),
            (
                "client_attribution_metadata[merchant_integration_version]",
                "hosted_checkout",
            ),
            (
                "client_attribution_metadata[payment_method_selection_flow]",
                "automatic",
            ),
            (
                "client_attribution_metadata[checkout_config_id]",
                &self.checkout_config_id,
            ),
        ];

        if !self.init_checksum.is_empty() {
            form.push(("init_checksum", &self.init_checksum));
        }
        if !self.js_checksum.is_empty() {
            form.push(("js_checksum", &self.js_checksum));
        }

        let resp = self
            .client
            .post(&api_url)
            .headers(stripe_headers())
            .form(&form)
            .send()
            .await?;

        let status = resp.status();
        let body_text = resp.text().await.unwrap_or_default();

        if status.is_success() {
            let body: serde_json::Value = serde_json::from_str(&body_text).unwrap_or_default();
            let state = body
                .get("state")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            match state {
                "succeeded" | "processing" | "processing_subscription" => return Ok(()),
                "failed" => {
                    let err_msg = body
                        .get("error")
                        .map(|e| e.to_string())
                        .unwrap_or_else(|| "unknown".to_string());
                    bail!("支付失败: {err_msg}");
                }
                _ => return Ok(()), // 继续轮询
            }
        }

        bail!("确认失败: HTTP {} {}", status, truncate(&body_text, 300));
    }

    // ── Step 3: 轮询支付状态 ─────────────────────────────────

    pub async fn poll_payment_status(&self) -> Result<String> {
        let api_url = format!(
            "https://api.stripe.com/v1/payment_pages/{}/poll?key={}",
            self.session_id, self.cfg.stripe_public_key
        );

        let max_attempts = self.cfg.poll_max_attempts.max(5);
        let poll_interval_ms = self.cfg.poll_interval_ms.max(200);

        for _ in 0..max_attempts {
            let resp = self
                .client
                .get(&api_url)
                .headers(stripe_headers())
                .send()
                .await;

            match resp {
                Ok(r) if r.status().is_success() => {
                    let body: serde_json::Value = r.json().await.unwrap_or_default();
                    let state = body
                        .get("state")
                        .and_then(|v| v.as_str())
                        .unwrap_or_default();

                    match state {
                        "succeeded" => return Ok("succeeded".to_string()),
                        "failed" | "canceled" => bail!("支付 {state}"),
                        _ => {} // processing, 继续轮询
                    }
                }
                _ => {} // 网络错误，继续轮询
            }

            // 轮询间隔
            let jitter = random_delay_ms(0, 200);
            sleep(Duration::from_millis(poll_interval_ms + jitter)).await;
        }

        bail!("轮询超时");
    }

    // ── 完整支付编排 ─────────────────────────────────────────

    /// 执行完整的 Stripe SEPA 支付流程
    #[allow(clippy::too_many_arguments)]
    pub async fn complete_payment(
        &mut self,
        iban: &str,
        name: &str,
        email: &str,
        address: &str,
        city: &str,
        postal_code: &str,
        country: &str,
        worker_id: usize,
    ) -> Result<()> {
        // Step 0: 获取页面参数
        self.fetch_checkout_page().await?;

        // 人类行为延迟: 30-100ms
        sleep(Duration::from_millis(random_delay_ms(30, 100))).await;

        // Step 1: 创建支付方式
        log_worker(worker_id, "支付", "创建 SEPA 支付方式...");
        let payment_method_id = self
            .create_payment_method(iban, name, email, address, city, postal_code, country)
            .await?;
        log_worker(
            worker_id,
            "OK",
            &format!("支付方式: {}", truncate(&payment_method_id, 20)),
        );

        // 人类行为延迟: 30-80ms
        sleep(Duration::from_millis(random_delay_ms(30, 80))).await;

        // Step 2: 确认支付
        log_worker(worker_id, "支付", "确认支付...");
        self.confirm_payment(&payment_method_id).await?;

        // Step 3: 轮询状态
        log_worker(worker_id, "支付", "轮询支付状态...");
        let state = self.poll_payment_status().await?;
        if state == "succeeded" {
            return Ok(());
        }

        bail!("支付最终状态非 succeeded: {state}");
    }
}

// ── 辅助函数 ────────────────────────────────────────────────────

/// 从 checkout URL 提取 session_id (cs_live_xxx 或 cs_test_xxx)
fn extract_session_id(url: &str) -> Option<String> {
    let re = Regex::new(r"(cs_(?:live|test)_[a-zA-Z0-9]+)").ok()?;
    re.captures(url)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str().to_string())
}

/// 生成 Stripe 指纹 (32位十六进制)
fn generate_stripe_fingerprint() -> String {
    random_hex(32)
}

/// Stripe API 通用请求头
fn stripe_headers() -> rquest::header::HeaderMap {
    let mut headers = rquest::header::HeaderMap::new();
    headers.insert(
        "Content-Type",
        "application/x-www-form-urlencoded".parse().unwrap(),
    );
    headers.insert("Accept", "application/json".parse().unwrap());
    headers.insert("Origin", "https://pay.openai.com".parse().unwrap());
    headers.insert("Referer", "https://pay.openai.com/".parse().unwrap());
    headers.insert("Sec-Fetch-Site", "cross-site".parse().unwrap());
    headers.insert("Sec-Fetch-Mode", "cors".parse().unwrap());
    headers.insert("Sec-Fetch-Dest", "empty".parse().unwrap());
    headers.insert("Sec-Ch-Ua-Platform", "\"Windows\"".parse().unwrap());
    headers.insert("Accept-Language", "en-US,en;q=0.9".parse().unwrap());
    headers
}

/// 截断文本
fn truncate(s: &str, max_len: usize) -> &str {
    if s.len() <= max_len { s } else { &s[..max_len] }
}
