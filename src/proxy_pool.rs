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
        .map(|l| normalize_proxy(l))
        .collect();
    if proxies.is_empty() {
        bail!("代理文件 {} 中没有有效的代理地址", path.display());
    }
    println!("从 {} 加载了 {} 个代理地址", path.display(), proxies.len());
    Ok(proxies)
}

/// 自动补协议前缀（参考 Go 版本）
fn normalize_proxy(proxy: &str) -> String {
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
        return Ok(vec![]);
    }
}

// ============================================================
// 代理健康检测（参考 Go 版本: testProxy + 全并发）
// ============================================================

/// 检测结果
struct ProxyCheckResult {
    proxy: String,
    ok: bool,
    reason: String,
}

/// 异步全并发健康检测所有代理（参考 Go 的 WaitGroup 全并发模式）
///
/// 多层检测:
/// 1. httpbin.org/ip — 基础连通性（GET, 期望 200）
/// 2. auth.openai.com — OpenAI 认证服务可达性
/// 3. chatgpt.com — ChatGPT 前端可达性
/// 默认并发上限 64，避免代理规模较大时一次性打满运行时和网络资源
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
        let timeout = timeout;
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

    // 更新失败计数 + 自动移除（放到阻塞线程，避免阻塞 Tokio worker）
    if !failed.is_empty() || !healthy.is_empty() {
        let healthy_for_tracking = healthy.clone();
        let failed_for_tracking = failed.clone();
        let proxy_path = proxy_file.map(Path::to_path_buf);
        match tokio::task::spawn_blocking(move || {
            update_failure_tracking(
                &healthy_for_tracking,
                &failed_for_tracking,
                proxy_path.as_deref(),
            )
        })
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(e)) => println!("  ⚠ 更新代理失败跟踪异常: {e}"),
            Err(e) => println!("  ⚠ 失败跟踪任务异常: {e}"),
        }
    }

    if healthy.is_empty() {
        bail!("所有代理均不可用，请检查代理配置或网络连接");
    }

    Ok(healthy)
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
            if err_str.contains("401") || err_str.contains("407") || err_str.contains("Unauthorized") || err_str.contains("Proxy Authentication") {
                Err("代理认证失败(401/407)".to_string())
            } else if err_str.contains("timeout") || err_str.contains("Timeout") {
                Err("超时".to_string())
            } else if err_str.contains("connection refused") || err_str.contains("Connection refused") {
                Err("连接被拒绝".to_string())
            } else {
                Err(format!("{}", &err_str[..err_str.len().min(80)]))
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
fn load_failure_counts() -> HashMap<String, u32> {
    let path = Path::new(FAILURE_FILE);
    if !path.is_file() {
        return HashMap::new();
    }
    match std::fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content).unwrap_or_default(),
        Err(_) => HashMap::new(),
    }
}

/// 保存失败计数记录
fn save_failure_counts(counts: &HashMap<String, u32>) -> Result<()> {
    let json = serde_json::to_string_pretty(counts)?;
    let tmp_path = format!("{FAILURE_FILE}.tmp");
    std::fs::write(&tmp_path, json)?;

    if std::fs::metadata(FAILURE_FILE).is_ok() {
        let _ = std::fs::remove_file(FAILURE_FILE);
    }
    std::fs::rename(&tmp_path, FAILURE_FILE)?;
    Ok(())
}

/// 更新失败跟踪：成功清零、失败累加、超阈值自动移除
fn update_failure_tracking(
    healthy: &[String],
    failed: &[String],
    proxy_file: Option<&Path>,
) -> Result<()> {
    let mut counts = load_failure_counts();

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
        let file_path = resolve_proxy_file_path(proxy_file);
        if let Some(path) = file_path {
            if remove_proxies_from_file(&path, &to_remove).is_ok() {
                println!(
                    "  已从 {} 移除 {} 个失效代理",
                    path.display(),
                    to_remove.len()
                );
            }
        }
        // 移除后从计数中也删除
        for proxy in &to_remove {
            counts.remove(proxy);
        }
    }

    // 保存更新后的失败计数
    save_failure_counts(&counts)?;
    Ok(())
}

/// 解析实际的 proxy.txt 文件路径
fn resolve_proxy_file_path(cli_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(p) = cli_path {
        return Some(p.to_path_buf());
    }
    let default_path = Path::new("proxy.txt");
    if default_path.is_file() {
        return Some(default_path.to_path_buf());
    }
    None
}

/// 从 proxy.txt 文件中移除指定的代理
fn remove_proxies_from_file(path: &Path, to_remove: &HashSet<String>) -> Result<()> {
    let content = std::fs::read_to_string(path)?;
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
            !to_remove.contains(&normalized)
        })
        .collect();
    std::fs::write(path, new_lines.join("\n") + "\n")?;
    Ok(())
}
