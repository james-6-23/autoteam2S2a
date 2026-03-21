//! Sentinel PoW（Proof of Work）共享模块。
//!
//! 提供 OpenAI Sentinel 验证的核心逻辑，供 `LiveRegisterService`
//! 和 `LiveCodexService` 共同使用。

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, bail};
use base64::{Engine as _, engine::general_purpose};
use rand::Rng;
use rquest::StatusCode;

use crate::util::log_worker;

// ─── 配置 ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SentinelConfig {
    pub enabled: bool,
    pub strict: bool,
    pub pow_max_iterations: usize,
}

// ─── 状态 ───────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SentinelState {
    pub sid: String,
    pub device_id: String,
    pub user_agent: String,
    pub sentinel_token: String,
    pub solved_pow: String,
}

impl SentinelState {
    pub fn new(device_id: &str, user_agent: &str) -> Self {
        let sid = uuid::Uuid::new_v4().to_string();
        let init_pow = get_requirements_token(&sid, user_agent);
        Self {
            sid,
            device_id: device_id.to_string(),
            user_agent: user_agent.to_string(),
            sentinel_token: String::new(),
            solved_pow: init_pow,
        }
    }
}

// ─── 公开 API ───────────────────────────────────────────

/// 一站式获取 Sentinel header 的值（JSON 字符串）。
///
/// 内部依次：call_sentinel_req → solve PoW → build header。
/// 调用方只需把返回值设为 `OpenAI-Sentinel-Token` header。
///
/// 如果 Sentinel 已禁用或出错（非 strict 模式），返回一个基础 token。
pub async fn build_sentinel_header(
    client: &rquest::Client,
    cfg: &SentinelConfig,
    state: &mut SentinelState,
    flow: &str,
    worker_id: usize,
    log_tag: &str,
) -> Result<String> {
    call_sentinel_req(client, cfg, state, flow, worker_id, log_tag).await?;
    get_sentinel_header_value(state, flow)
}

/// 向 sentinel.openai.com 发送请求，获取 token / PoW 挑战。
pub async fn call_sentinel_req(
    client: &rquest::Client,
    cfg: &SentinelConfig,
    state: &mut SentinelState,
    flow: &str,
    worker_id: usize,
    log_tag: &str,
) -> Result<()> {
    let sentinel_started = std::time::Instant::now();
    let init_token = get_requirements_token(&state.sid, &state.user_agent);
    state.solved_pow = init_token.clone();

    if !cfg.enabled {
        log_worker(
            worker_id,
            log_tag,
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
                log_tag,
                &format!(
                    "{flow}: Sentinel 请求异常 ({:.2}s): {err}",
                    sentinel_started.elapsed().as_secs_f32()
                ),
            );
            if cfg.strict {
                bail!("sentinel 请求失败: {err}");
            }
            return Ok(());
        }
    };
    if resp.status() != StatusCode::OK {
        log_worker(
            worker_id,
            log_tag,
            &format!(
                "{flow}: Sentinel 状态异常 HTTP {} ({:.2}s)",
                resp.status(),
                sentinel_started.elapsed().as_secs_f32()
            ),
        );
        if cfg.strict {
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
            log_tag,
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
        log_worker(worker_id, log_tag, &format!("{flow}: PoW 参数缺失"));
        if cfg.strict {
            bail!("sentinel PoW 参数缺失");
        }
        return Ok(());
    }

    log_worker(
        worker_id,
        log_tag,
        &format!(
            "{flow}: 开始 PoW difficulty={} max_iter={}",
            truncate_text(difficulty, 10),
            cfg.pow_max_iterations
        ),
    );
    let pow_started = std::time::Instant::now();
    let seed_owned = seed.to_string();
    let difficulty_owned = difficulty.to_string();
    let sid_owned = state.sid.clone();
    let ua_owned = state.user_agent.clone();
    let max_iterations = cfg.pow_max_iterations;
    let pow_result = tokio::task::spawn_blocking(move || {
        solve_pow(
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
            log_tag,
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
        log_tag,
        &format!(
            "{flow}: PoW 失败 (max_iter={}) 耗时 {:.2}s",
            cfg.pow_max_iterations,
            pow_started.elapsed().as_secs_f32()
        ),
    );
    if cfg.strict {
        bail!("sentinel PoW 计算失败");
    }
    Ok(())
}

/// 构建 OpenAI-Sentinel-Token header 的 JSON 值。
pub fn get_sentinel_header_value(state: &SentinelState, flow: &str) -> Result<String> {
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

/// 生成初始 requirements token（无 PoW）。
pub fn get_requirements_token(sid: &str, user_agent: &str) -> String {
    let mut config = build_pow_config(sid, user_agent);
    config[3] = serde_json::Value::from(0);
    config[9] = serde_json::Value::from(0);
    let encoded =
        general_purpose::STANDARD.encode(serde_json::to_vec(&config).unwrap_or_default());
    format!("gAAAAAC{encoded}~S")
}

// ─── 内部实现 ────────────────────────────────────────────

pub fn solve_pow(
    seed: &str,
    difficulty: &str,
    sid: &str,
    user_agent: &str,
    max_iterations: usize,
) -> Option<(String, usize)> {
    let mut config = build_pow_config(sid, user_agent);
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

fn unix_millis() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    dur.as_millis() as i64
}

fn fixed_parse_time_string() -> String {
    "Tue Jan 30 2024 12:00:00 GMT+0000 (Coordinated Universal Time)".to_string()
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
