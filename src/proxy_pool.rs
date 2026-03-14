use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Result, bail};

pub struct ProxyPool {
    proxies: Vec<String>,
    index: AtomicUsize,
}

impl ProxyPool {
    pub fn new(proxies: Vec<String>) -> Self {
        Self {
            proxies: proxies
                .into_iter()
                .map(|p| p.trim().to_string())
                .filter(|p| !p.is_empty())
                .collect(),
            index: AtomicUsize::new(0),
        }
    }

    pub fn next(&self) -> Option<String> {
        if self.proxies.is_empty() {
            return None;
        }
        let idx = self.index.fetch_add(1, Ordering::Relaxed);
        Some(self.proxies[idx % self.proxies.len()].clone())
    }

    pub fn len(&self) -> usize {
        self.proxies.len()
    }

    pub fn mode_name(&self) -> &'static str {
        if self.proxies.is_empty() {
            "direct"
        } else if self.proxies.len() == 1 {
            "single-proxy"
        } else {
            "proxy-pool"
        }
    }
}

// ============================================================
// 代理文件加载
// ============================================================

/// 从文件加载代理列表（每行一个代理地址）
/// 支持 `#` 注释行和空行，自动补 `http://` 前缀
pub fn load_from_file(path: &Path) -> Result<Vec<String>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("读取代理文件失败 {}: {e}", path.display()))?;
    let proxies: Vec<String> = content
        .lines()
        .map(|l| l.trim())
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(normalize_proxy)
        .collect();
    if proxies.is_empty() {
        bail!("代理文件 {} 中没有有效的代理地址", path.display());
    }
    println!("从 {} 加载了 {} 个代理地址", path.display(), proxies.len());
    Ok(proxies)
}

/// 自动补协议前缀（参考 Go 版本）
pub fn normalize_proxy(proxy: &str) -> String {
    if proxy.contains("://") {
        proxy.to_string()
    } else {
        format!("http://{proxy}")
    }
}

// ============================================================
// 平台感知代理解析
// ============================================================

/// 平台感知的代理来源解析
///
/// 优先级:
/// 1. `--proxy-file` CLI 指定的文件
/// 2. Linux: 当前目录 `proxy.txt` 自动检测
/// 3. `config.toml` 中的 `proxy_pool`
/// 4. Windows: 回退到 `127.0.0.1:7890`
pub fn resolve_proxies(
    cli_proxy_file: Option<&Path>,
    config_proxies: &[String],
) -> Result<Vec<String>> {
    // 1. CLI 指定的文件最优先
    if let Some(file) = cli_proxy_file {
        println!("代理来源: 命令行参数 --proxy-file {}", file.display());
        return load_from_file(file);
    }

    // 2. Linux: 自动检测当前目录的 proxy.txt
    #[cfg(target_os = "linux")]
    {
        let proxy_txt = Path::new("proxy.txt");
        if proxy_txt.is_file() {
            println!("代理来源: 自动检测到 proxy.txt");
            return load_from_file(proxy_txt);
        }
    }

    // 3. config.toml 中的 proxy_pool
    if !config_proxies.is_empty() {
        println!(
            "代理来源: config.toml proxy_pool ({} 个)",
            config_proxies.len()
        );
        return Ok(config_proxies.iter().map(|p| normalize_proxy(p)).collect());
    }

    // 4. Windows: 回退到本地代理
    #[cfg(target_os = "windows")]
    {
        println!("代理来源: Windows 默认 (127.0.0.1:7890)");
        return Ok(vec!["http://127.0.0.1:7890".to_string()]);
    }

    // 5. Linux/其他: 允许直连
    #[cfg(not(target_os = "windows"))]
    {
        println!("代理来源: 直连 (未找到代理配置)");
        Ok(vec![])
    }
}

// ============================================================
// 代理健康检测（参考 Go 版本: testProxy + 全并发）
// ============================================================

/// 检测结果
#[derive(Debug, Clone, serde::Serialize)]
pub struct ProxyCheckResult {
    pub proxy: String,
    pub ok: bool,
    pub reason: String,
}

/// 异步全并发健康检测所有代理（参考 Go 的 WaitGroup 全并发模式）
///
/// 多层检测:
/// 1. httpbin.org/ip — 基础连通性（GET, 期望 200）
/// 2. auth.openai.com — OpenAI 认证服务可达性
/// 3. chatgpt.com — ChatGPT 前端可达性
///    默认并发上限 64，避免代理规模较大时一次性打满运行时和网络资源
pub async fn health_check(
    proxies: &[String],
    timeout_sec: u64,
    proxy_file: Option<&Path>,
) -> Result<Vec<String>> {
    // 直连模式：无代理，跳过检测
    if proxies.is_empty() {
        println!("\n直连模式: 跳过代理检测");
        return Ok(vec![]);
    }

    println!(
        "\n检测代理可用性 ({} 个代理, 超时: {}s)...",
        proxies.len(),
        timeout_sec
    );
    println!("  检测项: httpbin.org/ip | auth.openai.com | chatgpt.com");

    const MAX_HEALTH_CHECK_CONCURRENCY: usize = 64;
    let timeout = Duration::from_secs(timeout_sec);
    let check_concurrency = proxies.len().clamp(1, MAX_HEALTH_CHECK_CONCURRENCY);
    println!("  检测并发上限: {check_concurrency}");

    let semaphore = Arc::new(tokio::sync::Semaphore::new(check_concurrency));
    let mut join_set = tokio::task::JoinSet::new();

    for proxy in proxies {
        let proxy = proxy.clone();
        let permit = semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| anyhow::anyhow!("代理检测并发控制异常: {e}"))?;
        join_set.spawn(async move {
            let _permit = permit;
            check_proxy_multilayer(&proxy, timeout).await
        });
    }

    // 按完成顺序收集结果，避免被慢代理阻塞整批汇总
    let mut healthy = Vec::new();
    let mut failed: Vec<String> = Vec::new();

    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(result) => {
                let masked = crate::util::mask_proxy(&result.proxy);
                if result.ok {
                    println!("  ✓ {masked}  {}", result.reason);
                    healthy.push(result.proxy);
                } else {
                    println!("  ✗ {masked}  {}", result.reason);
                    failed.push(result.proxy);
                }
            }
            Err(err) => println!("  ✗ 检测任务异常: {err}"),
        }
    }

    println!("检测完成: {} 通过, {} 失败\n", healthy.len(), failed.len());

    // 更新失败计数 + 自动移除（异步文件 I/O）
    if !failed.is_empty() || !healthy.is_empty() {
        if let Err(e) = update_failure_tracking(&healthy, &failed, proxy_file).await {
            println!("  ⚠ 更新代理失败跟踪异常: {e}");
        }
    }

    if healthy.is_empty() {
        bail!("所有代理均不可用，请检查代理配置或网络连接");
    }

    Ok(healthy)
}

/// 全并发健康检测所有代理并返回详细结果（供前端使用）
pub async fn health_check_detailed(proxies: &[String], timeout_sec: u64) -> Vec<ProxyCheckResult> {
    if proxies.is_empty() {
        return vec![];
    }

    const MAX_HEALTH_CHECK_CONCURRENCY: usize = 64;
    let timeout = Duration::from_secs(timeout_sec);
    let check_concurrency = proxies.len().clamp(1, MAX_HEALTH_CHECK_CONCURRENCY);

    let semaphore = Arc::new(tokio::sync::Semaphore::new(check_concurrency));
    let mut join_set = tokio::task::JoinSet::new();

    for proxy in proxies {
        let proxy = proxy.clone();
        let permit = match semaphore.clone().acquire_owned().await {
            Ok(p) => p,
            Err(_) => continue,
        };
        join_set.spawn(async move {
            let _permit = permit;
            check_proxy_multilayer(&proxy, timeout).await
        });
    }

    let mut results = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        match join_result {
            Ok(result) => results.push(result),
            Err(_) => {}
        }
    }
    results
}

/// 多层检测单个代理（参考 Go 的 test_proxies.go）
///
/// 1. httpbin.org/ip — 基础连通性（必须通过）
/// 2. auth.openai.com — 认证服务（通过=可用）
/// 3. chatgpt.com — 前端服务（通过=可用，403 也算代理可达）
async fn check_proxy_multilayer(proxy: &str, timeout: Duration) -> ProxyCheckResult {
    let client = match rquest::Client::builder()
        .proxy(match rquest::Proxy::all(proxy) {
            Ok(p) => p,
            Err(e) => {
                return ProxyCheckResult {
                    proxy: proxy.to_string(),
                    ok: false,
                    reason: format!("代理地址无效: {e}"),
                };
            }
        })
        .timeout(timeout)
        .connect_timeout(timeout)
        .no_proxy()
        .redirect(rquest::redirect::Policy::limited(5))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return ProxyCheckResult {
                proxy: proxy.to_string(),
                ok: false,
                reason: format!("创建客户端失败: {e}"),
            };
        }
    };

    // 层1: httpbin.org/ip（基础连通性，必须通过）
    let httpbin_result = match client.get("https://httpbin.org/ip").send().await {
        Ok(resp) if resp.status().as_u16() == 200 => Ok(()),
        Ok(resp) => Err(format!("HTTP {}", resp.status())),
        Err(e) => {
            let err_str = format!("{e}");
            // 检测代理认证失败
            if err_str.contains("401")
                || err_str.contains("407")
                || err_str.contains("Unauthorized")
                || err_str.contains("Proxy Authentication")
            {
                Err("代理认证失败(401/407)".to_string())
            } else if err_str.contains("timeout") || err_str.contains("Timeout") {
                Err("超时".to_string())
            } else if err_str.contains("connection refused")
                || err_str.contains("Connection refused")
            {
                Err("连接被拒绝".to_string())
            } else {
                Err(err_str[..err_str.len().min(80)].to_string())
            }
        }
    };

    if let Err(err_detail) = httpbin_result {
        return ProxyCheckResult {
            proxy: proxy.to_string(),
            ok: false,
            reason: format!("httpbin ✗ ({err_detail})"),
        };
    }

    // 层2: auth.openai.com（认证服务可达性）
    let auth_ok = match client.get("https://auth.openai.com").send().await {
        Ok(resp) => {
            let s = resp.status().as_u16();
            // 2xx, 3xx, 401, 403 都表示代理到达了目标
            s < 500
        }
        Err(_) => false,
    };

    // 层3: chatgpt.com（前端可达性）
    let chatgpt_ok = match client.get("https://chatgpt.com").send().await {
        Ok(resp) => {
            let s = resp.status().as_u16();
            s < 500
        }
        Err(_) => false,
    };

    // 综合判定：httpbin 必须通过 + 至少一个 OpenAI 目标可达
    let detail = format!(
        "httpbin ✓ | auth {}  | chatgpt {}",
        if auth_ok { "✓" } else { "✗" },
        if chatgpt_ok { "✓" } else { "✗" },
    );

    if auth_ok || chatgpt_ok {
        ProxyCheckResult {
            proxy: proxy.to_string(),
            ok: true,
            reason: detail,
        }
    } else {
        ProxyCheckResult {
            proxy: proxy.to_string(),
            ok: false,
            reason: format!("{detail}  (OpenAI 不可达)"),
        }
    }
}

// ============================================================
// 代理失败计数跟踪 + 自动移除
// ============================================================

const FAILURE_FILE: &str = "proxy_failures.json";
const MAX_FAILURES: u32 = 3;

/// 加载失败计数记录
async fn load_failure_counts() -> HashMap<String, u32> {
    tokio::task::spawn_blocking(|| {
        let path = Path::new(FAILURE_FILE);
        if !path.is_file() {
            return HashMap::new();
        }
        match std::fs::read_to_string(path) {
            Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
            Err(_) => HashMap::new(),
        }
    })
    .await
    .unwrap_or_default()
}

/// 保存失败计数记录
async fn save_failure_counts(counts: &HashMap<String, u32>) -> Result<()> {
    let payload = counts.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let json = serde_json::to_string_pretty(&payload)?;
        let tmp_path = format!("{FAILURE_FILE}.tmp");
        std::fs::write(&tmp_path, json)?;

        if std::fs::metadata(FAILURE_FILE).is_ok() {
            let _ = std::fs::remove_file(FAILURE_FILE);
        }
        std::fs::rename(&tmp_path, FAILURE_FILE)?;
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("保存失败计数任务异常: {e}"))??;
    Ok(())
}

/// 更新失败跟踪：成功清零、失败累加、超阈值自动移除
async fn update_failure_tracking(
    healthy: &[String],
    failed: &[String],
    proxy_file: Option<&Path>,
) -> Result<()> {
    let mut counts = load_failure_counts().await;

    // 成功的代理清零失败计数
    for proxy in healthy {
        counts.remove(proxy);
    }

    // 失败的代理累加
    for proxy in failed {
        let count = counts.entry(proxy.clone()).or_insert(0);
        *count += 1;
    }

    // 找出需要移除的代理（失败次数 >= MAX_FAILURES）
    let to_remove: HashSet<String> = counts
        .iter()
        .filter(|(_, count)| **count >= MAX_FAILURES)
        .map(|(proxy, count)| {
            println!(
                "  ⚠ 代理 {} 连续失败 {} 次 (≥{}), 将被移除",
                crate::util::mask_proxy(proxy),
                count,
                MAX_FAILURES
            );
            proxy.clone()
        })
        .collect();

    // 从 proxy.txt 移除失效代理
    if !to_remove.is_empty() {
        let file_path = resolve_proxy_file_path(proxy_file).await;
        if let Some(path) = file_path
            && remove_proxies_from_file(&path, &to_remove).await.is_ok()
        {
            println!(
                "  已从 {} 移除 {} 个失效代理",
                path.display(),
                to_remove.len()
            );
        }
        // 移除后从计数中也删除
        for proxy in &to_remove {
            counts.remove(proxy);
        }
    }

    // 保存更新后的失败计数
    save_failure_counts(&counts).await?;
    Ok(())
}

/// 解析实际的 proxy.txt 文件路径
async fn resolve_proxy_file_path(cli_path: Option<&Path>) -> Option<PathBuf> {
    let cli = cli_path.map(Path::to_path_buf);
    tokio::task::spawn_blocking(move || {
        if let Some(p) = cli {
            return Some(p);
        }
        let default_path = Path::new("proxy.txt");
        if default_path.is_file() {
            return Some(default_path.to_path_buf());
        }
        None
    })
    .await
    .unwrap_or(None)
}

/// 从 proxy.txt 文件中移除指定的代理
async fn remove_proxies_from_file(path: &Path, to_remove: &HashSet<String>) -> Result<()> {
    let path_buf = path.to_path_buf();
    let remove_set = to_remove.clone();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let content = std::fs::read_to_string(&path_buf)?;
        let new_lines: Vec<&str> = content
            .lines()
            .filter(|line| {
                let trimmed = line.trim();
                // 保留空行和注释
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    return true;
                }
                // 检查是否在移除列表中（标准化后比较）
                let normalized = normalize_proxy(trimmed);
                !remove_set.contains(&normalized)
            })
            .collect();
        std::fs::write(&path_buf, new_lines.join("\n") + "\n")?;
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("移除失效代理任务异常: {e}"))??;
    Ok(())
}

// ============================================================
// 代理测试 + 深度质量检测
// ============================================================

/// 构建带代理的 HTTP 客户端（公共辅助函数）
fn build_proxy_client(proxy_url: &str, timeout: Duration) -> Result<rquest::Client, String> {
    rquest::Client::builder()
        .proxy(rquest::Proxy::all(proxy_url).map_err(|e| format!("代理地址无效: {e}"))?)
        .timeout(timeout)
        .connect_timeout(timeout)
        .no_proxy()
        .redirect(rquest::redirect::Policy::limited(5))
        .build()
        .map_err(|e| format!("创建客户端失败: {e}"))
}

#[derive(serde::Deserialize)]
struct IpApiResponse {
    status: String,
    query: Option<String>,
    city: Option<String>,
    region: Option<String>,
    #[serde(rename = "regionName")]
    region_name: Option<String>,
    country: Option<String>,
    #[serde(rename = "countryCode")]
    country_code: Option<String>,
}

#[derive(serde::Deserialize)]
struct HttpBinIpResponse {
    origin: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct ProxyExitInfo {
    pub ip: String,
    pub city: String,
    pub region: String,
    pub country: String,
    pub country_code: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProxyTestResult {
    pub success: bool,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub city: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country_code: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProxyQualityCheckItem {
    pub target: String,
    pub status: String,       // pass / warn / fail / challenge
    #[serde(skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ProxyQualityCheckResult {
    pub score: i32,
    pub grade: String,
    pub summary: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exit_ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_latency_ms: Option<i64>,
    pub passed_count: i32,
    pub warn_count: i32,
    pub failed_count: i32,
    pub challenge_count: i32,
    pub checked_at: i64,
    pub items: Vec<ProxyQualityCheckItem>,
}

struct QualityTarget {
    target: &'static str,
    url: &'static str,
    method: &'static str,
    allowed_statuses: &'static [u16],
}

const QUALITY_TARGETS: &[QualityTarget] = &[
    QualityTarget {
        target: "openai",
        url: "https://api.openai.com/v1/models",
        method: "GET",
        allowed_statuses: &[401],
    },
    QualityTarget {
        target: "anthropic",
        url: "https://api.anthropic.com/v1/messages",
        method: "GET",
        allowed_statuses: &[401, 405, 404, 400],
    },
    QualityTarget {
        target: "gemini",
        url: "https://generativelanguage.googleapis.com/$discovery/rest?version=v1beta",
        method: "GET",
        allowed_statuses: &[200],
    },
];

/// 探测代理出口信息（ip-api.com 优先，httpbin.org 降级）
async fn probe_proxy_exit_info(client: &rquest::Client) -> Result<(ProxyExitInfo, i64), String> {
    // 尝试 ip-api.com
    let start = std::time::Instant::now();
    match client.get("http://ip-api.com/json/?lang=zh-CN").send().await {
        Ok(resp) if resp.status().as_u16() == 200 => {
            let latency_ms = start.elapsed().as_millis() as i64;
            if let Ok(info) = resp.json::<IpApiResponse>().await {
                if info.status.to_lowercase() == "success" {
                    let region = info.region_name.or(info.region).unwrap_or_default();
                    return Ok((
                        ProxyExitInfo {
                            ip: info.query.unwrap_or_default(),
                            city: info.city.unwrap_or_default(),
                            region,
                            country: info.country.unwrap_or_default(),
                            country_code: info.country_code.unwrap_or_default(),
                        },
                        latency_ms,
                    ));
                }
            }
        }
        _ => {}
    }

    // 降级到 httpbin.org
    let start = std::time::Instant::now();
    match client.get("https://httpbin.org/ip").send().await {
        Ok(resp) if resp.status().as_u16() == 200 => {
            let latency_ms = start.elapsed().as_millis() as i64;
            if let Ok(info) = resp.json::<HttpBinIpResponse>().await {
                if let Some(origin) = info.origin {
                    if !origin.is_empty() {
                        return Ok((
                            ProxyExitInfo {
                                ip: origin,
                                ..Default::default()
                            },
                            latency_ms,
                        ));
                    }
                }
            }
            Err("httpbin 响应中未找到 IP".to_string())
        }
        Ok(resp) => Err(format!("httpbin 返回 HTTP {}", resp.status())),
        Err(e) => Err(format!("所有探测 URL 均失败: {e}")),
    }
}

/// 测试单个代理的连通性，返回出口 IP、地理信息和延迟
pub async fn test_single_proxy(proxy_url: &str, timeout_sec: u64) -> ProxyTestResult {
    let timeout = Duration::from_secs(timeout_sec.max(5));
    let client = match build_proxy_client(proxy_url, timeout) {
        Ok(c) => c,
        Err(e) => {
            return ProxyTestResult {
                success: false,
                message: e,
                latency_ms: None,
                ip_address: None,
                city: None,
                region: None,
                country: None,
                country_code: None,
            };
        }
    };

    match probe_proxy_exit_info(&client).await {
        Ok((info, latency_ms)) => ProxyTestResult {
            success: true,
            message: "代理可用".to_string(),
            latency_ms: Some(latency_ms),
            ip_address: Some(info.ip),
            city: if info.city.is_empty() { None } else { Some(info.city) },
            region: if info.region.is_empty() { None } else { Some(info.region) },
            country: if info.country.is_empty() { None } else { Some(info.country) },
            country_code: if info.country_code.is_empty() { None } else { Some(info.country_code) },
        },
        Err(e) => ProxyTestResult {
            success: false,
            message: e,
            latency_ms: None,
            ip_address: None,
            city: None,
            region: None,
            country: None,
            country_code: None,
        },
    }
}

/// 对单个质量检测目标执行检测
async fn run_quality_target(
    client: &rquest::Client,
    target: &QualityTarget,
) -> ProxyQualityCheckItem {
    let req = match target.method {
        "GET" => client.get(target.url),
        "POST" => client.post(target.url),
        _ => client.get(target.url),
    };

    let req = req
        .header("Accept", "application/json,text/html,*/*")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36");

    let start = std::time::Instant::now();
    match req.send().await {
        Ok(resp) => {
            let latency_ms = start.elapsed().as_millis() as i64;
            let status_code = resp.status().as_u16();

            if target.allowed_statuses.contains(&status_code) {
                if status_code == 200 {
                    ProxyQualityCheckItem {
                        target: target.target.to_string(),
                        status: "pass".to_string(),
                        http_status: Some(status_code),
                        latency_ms: Some(latency_ms),
                        message: None,
                    }
                } else {
                    ProxyQualityCheckItem {
                        target: target.target.to_string(),
                        status: "warn".to_string(),
                        http_status: Some(status_code),
                        latency_ms: Some(latency_ms),
                        message: Some("目标可达，需要认证".to_string()),
                    }
                }
            } else {
                ProxyQualityCheckItem {
                    target: target.target.to_string(),
                    status: "fail".to_string(),
                    http_status: Some(status_code),
                    latency_ms: Some(latency_ms),
                    message: Some(format!("非预期状态码: {status_code}")),
                }
            }
        }
        Err(e) => {
            let latency_ms = start.elapsed().as_millis() as i64;
            ProxyQualityCheckItem {
                target: target.target.to_string(),
                status: "fail".to_string(),
                http_status: None,
                latency_ms: Some(latency_ms),
                message: Some(format!("请求失败: {e}")),
            }
        }
    }
}

/// 计算质量评分和等级
fn finalize_quality_result(result: &mut ProxyQualityCheckResult) {
    let score = 100 - result.warn_count * 10 - result.failed_count * 22 - result.challenge_count * 30;
    result.score = score.max(0);
    result.grade = match result.score {
        s if s >= 90 => "A",
        s if s >= 75 => "B",
        s if s >= 60 => "C",
        s if s >= 40 => "D",
        _ => "F",
    }
    .to_string();
    result.summary = format!(
        "通过 {} 项，告警 {} 项，失败 {} 项，挑战 {} 项",
        result.passed_count, result.warn_count, result.failed_count, result.challenge_count
    );
}

/// 深度质量检测：基础连通性 + AI API 目标可达性检测 + 评分
pub async fn check_proxy_quality(proxy_url: &str, timeout_sec: u64) -> ProxyQualityCheckResult {
    let timeout = Duration::from_secs(timeout_sec.max(10));
    let mut result = ProxyQualityCheckResult {
        score: 100,
        grade: "A".to_string(),
        summary: String::new(),
        exit_ip: None,
        country: None,
        country_code: None,
        base_latency_ms: None,
        passed_count: 0,
        warn_count: 0,
        failed_count: 0,
        challenge_count: 0,
        checked_at: chrono::Utc::now().timestamp(),
        items: Vec::with_capacity(QUALITY_TARGETS.len() + 1),
    };

    let client = match build_proxy_client(proxy_url, timeout) {
        Ok(c) => c,
        Err(e) => {
            result.items.push(ProxyQualityCheckItem {
                target: "base_connectivity".to_string(),
                status: "fail".to_string(),
                http_status: None,
                latency_ms: None,
                message: Some(e),
            });
            result.failed_count += 1;
            finalize_quality_result(&mut result);
            return result;
        }
    };

    // 阶段 1: 基础连通性探测
    match probe_proxy_exit_info(&client).await {
        Ok((info, latency_ms)) => {
            result.items.push(ProxyQualityCheckItem {
                target: "base_connectivity".to_string(),
                status: "pass".to_string(),
                http_status: Some(200),
                latency_ms: Some(latency_ms),
                message: None,
            });
            result.passed_count += 1;
            result.exit_ip = Some(info.ip);
            result.country = if info.country.is_empty() { None } else { Some(info.country) };
            result.country_code = if info.country_code.is_empty() { None } else { Some(info.country_code) };
            result.base_latency_ms = Some(latency_ms);
        }
        Err(e) => {
            result.items.push(ProxyQualityCheckItem {
                target: "base_connectivity".to_string(),
                status: "fail".to_string(),
                http_status: None,
                latency_ms: None,
                message: Some(e),
            });
            result.failed_count += 1;
            finalize_quality_result(&mut result);
            return result;
        }
    }

    // 阶段 2: AI API 目标逐个检测
    for target in QUALITY_TARGETS {
        let item = run_quality_target(&client, target).await;
        match item.status.as_str() {
            "pass" => result.passed_count += 1,
            "warn" => result.warn_count += 1,
            "fail" => result.failed_count += 1,
            "challenge" => result.challenge_count += 1,
            _ => {}
        }
        result.items.push(item);
    }

    // 阶段 3: 评分计算
    finalize_quality_result(&mut result);
    result
}
