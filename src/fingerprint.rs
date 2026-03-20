use anyhow::Result;
use rquest::header::{self, HeaderMap, HeaderName, HeaderValue};
use rquest_util::Emulation;

#[derive(Clone)]
pub struct FingerprintMaterial {
    pub emulation_name: String,
    pub emulation: Emulation,
    pub user_agent: String,
    pub default_headers: HeaderMap,
    pub navigation_headers: HeaderMap,
}

// ── 浏览器自动轮换池（16 种，无重复）──────────────────────────────
// 每个条目映射到 rquest_util::Emulation 枚举，由 rquest 内部完成
// TLS 指纹 (JA3/JA4) + HTTP/2 指纹 + 请求头顺序的完整模拟
const AUTO_ROTATING_PROFILES: [&str; 16] = [
    "chrome136",
    "chrome135",
    "chrome134",
    "chrome133",
    "chrome131",
    "edge134",
    "edge131",
    "chrome136_mac",
    "chrome135_mac",
    "chrome136_linux",
    "edge134_mac",
    "firefox136",
    "firefox136_mac",
    "firefox136_linux",
    "safari17_5",
    "safari18",
];

// ── Accept-Language 池（12 种）─────────────────────────────────
const ACCEPT_LANGUAGE_POOL: [&str; 12] = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,de;q=0.8",
    "en-US,en;q=0.9,fr;q=0.8",
    "en-US,en;q=0.9,es;q=0.8",
    "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
    "en-US,en;q=0.9,ja;q=0.8",
    "en-US,en;q=0.9,pt;q=0.8",
    "en-US,en;q=0.9,it;q=0.8",
    "en-US,en;q=0.9,nl;q=0.8",
    "en-US,en;q=0.9,ko;q=0.8",
    "en-US,en;q=0.9,zh-CN;q=0.8",
];

// ── UA 小版本号池 ──────────────────────────────────────────────
const CHROME_136_PATCHES: [&str; 4] = [
    "136.0.7103.48",
    "136.0.7103.92",
    "136.0.7103.113",
    "136.0.7103.130",
];
const CHROME_135_PATCHES: [&str; 4] = [
    "135.0.7049.52",
    "135.0.7049.84",
    "135.0.7049.95",
    "135.0.7049.115",
];
const CHROME_134_PATCHES: [&str; 3] = ["134.0.6998.45", "134.0.6998.89", "134.0.6998.118"];
const CHROME_133_PATCHES: [&str; 3] = ["133.0.6943.53", "133.0.6943.98", "133.0.6943.127"];
const CHROME_131_PATCHES: [&str; 3] = ["131.0.6778.86", "131.0.6778.109", "131.0.6778.140"];
const EDGE_134_PATCHES: [&str; 3] = ["134.0.3124.51", "134.0.3124.68", "134.0.3124.93"];
const EDGE_131_PATCHES: [&str; 3] = ["131.0.2903.51", "131.0.2903.70", "131.0.2903.99"];

/// 构建指纹材料，包含 Emulation 枚举用于 rquest 的 TLS/HTTP2 模拟
pub fn build_fingerprint_material(
    raw: &str,
    salt: usize,
    _fallback_user_agent: &str,
) -> Result<FingerprintMaterial> {
    let profile_name = resolve_emulation_name(raw, salt);
    let accept_language_idx = salt % ACCEPT_LANGUAGE_POOL.len();
    let accept_language = ACCEPT_LANGUAGE_POOL[accept_language_idx];

    let emulation = resolve_emulation(&profile_name);
    let user_agent = build_user_agent(&profile_name, salt);
    let default_headers = build_default_headers(&profile_name, accept_language, salt)?;
    let navigation_headers = build_navigation_headers(default_headers.clone());

    Ok(FingerprintMaterial {
        emulation_name: profile_name,
        emulation,
        user_agent,
        default_headers,
        navigation_headers,
    })
}

fn resolve_emulation_name(raw: &str, salt: usize) -> String {
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "auto" {
        return AUTO_ROTATING_PROFILES[salt % AUTO_ROTATING_PROFILES.len()].to_string();
    }
    normalized
}

/// 将 profile 名称映射到 rquest Emulation 枚举
/// macOS/Linux 变体共用同一 Emulation（TLS 指纹不区分 OS）
fn resolve_emulation(name: &str) -> Emulation {
    match name {
        "chrome136" | "chrome136_mac" | "chrome136_linux" => Emulation::Chrome136,
        "chrome135" | "chrome135_mac" => Emulation::Chrome135,
        "chrome134" => Emulation::Chrome134,
        "chrome133" => Emulation::Chrome133,
        "chrome131" | "chrome" => Emulation::Chrome131,
        "edge134" | "edge134_mac" => Emulation::Edge134,
        "edge131" | "edge" => Emulation::Edge131,
        "firefox136" | "firefox" | "firefox136_mac" | "firefox136_linux" => Emulation::Firefox136,
        "safari17_5" | "safari" => Emulation::Safari17_5,
        "safari18" => Emulation::Safari18,
        _ => Emulation::Chrome136, // 安全的默认值
    }
}

// ── UA 构建 ────────────────────────────────────────────────────

fn pick_chrome_patch(major: u32, salt: usize) -> &'static str {
    match major {
        136 => CHROME_136_PATCHES[salt / 3 % CHROME_136_PATCHES.len()],
        135 => CHROME_135_PATCHES[salt / 3 % CHROME_135_PATCHES.len()],
        134 => CHROME_134_PATCHES[salt / 3 % CHROME_134_PATCHES.len()],
        133 => CHROME_133_PATCHES[salt / 3 % CHROME_133_PATCHES.len()],
        _ => CHROME_131_PATCHES[salt / 3 % CHROME_131_PATCHES.len()],
    }
}

fn pick_edge_patch(major: u32, salt: usize) -> &'static str {
    match major {
        134 => EDGE_134_PATCHES[salt / 3 % EDGE_134_PATCHES.len()],
        _ => EDGE_131_PATCHES[salt / 3 % EDGE_131_PATCHES.len()],
    }
}

fn build_user_agent(name: &str, salt: usize) -> String {
    match name {
        // Chrome Windows
        "chrome136" => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(136, salt)),
        "chrome135" => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(135, salt)),
        "chrome134" => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(134, salt)),
        "chrome133" => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(133, salt)),
        "chrome131" | "chrome" => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(131, salt)),
        // Chrome macOS
        "chrome136_mac" => format!("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(136, salt)),
        "chrome135_mac" => format!("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(135, salt)),
        // Chrome Linux
        "chrome136_linux" => format!("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(136, salt)),
        // Edge Windows
        "edge134" => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36 Edg/{}", pick_chrome_patch(134, salt), pick_edge_patch(134, salt)),
        "edge131" | "edge" => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36 Edg/{}", pick_chrome_patch(131, salt), pick_edge_patch(131, salt)),
        // Edge macOS
        "edge134_mac" => format!("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36 Edg/{}", pick_chrome_patch(134, salt), pick_edge_patch(134, salt)),
        // Firefox
        "firefox136" | "firefox" => "Mozilla/5.0 (Windows NT 10.0; rv:136.0) Gecko/20100101 Firefox/136.0".to_string(),
        "firefox136_mac" => "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:136.0) Gecko/20100101 Firefox/136.0".to_string(),
        "firefox136_linux" => "Mozilla/5.0 (X11; Linux x86_64; rv:136.0) Gecko/20100101 Firefox/136.0".to_string(),
        // Safari
        "safari17_5" | "safari" => "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15".to_string(),
        "safari18" => "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15".to_string(),
        // Fallback
        _ => format!("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{} Safari/537.36", pick_chrome_patch(136, salt)),
    }
}

// ── 请求头构建 ────────────────────────────────────────────────

/// 判断浏览器是否为 Chromium 系
fn is_chromium(name: &str) -> bool {
    name.starts_with("chrome") || name.starts_with("edge")
}

/// 判断 Accept-Encoding 是否应包含 zstd
fn accept_encoding_for(name: &str) -> &'static str {
    match name {
        n if n.starts_with("safari") => "gzip, deflate, br",
        "chrome131" | "chrome" | "edge131" | "edge" => "gzip, deflate, br",
        _ if is_chromium(name) => "gzip, deflate, br, zstd",
        n if n.starts_with("firefox") => "gzip, deflate, br, zstd",
        _ => "gzip, deflate, br",
    }
}

/// 获取 Accept 头（按浏览器版本精确匹配差异）
fn accept_for(name: &str) -> &'static str {
    match name {
        n if n.starts_with("safari") => {
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        n if n.starts_with("firefox") => {
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
        }
        n if n.starts_with("edge") => {
            // Edge 使用 signed-exchange
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        }
        // Chrome 131+ 包含 signed-exchange
        _ => {
            "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7"
        }
    }
}

// ── sec-ch-ua 品牌顺序随机化 ──────────────────────────────────

fn not_a_brand_tag(chrome_major: u32) -> (&'static str, &'static str) {
    match chrome_major {
        131..=u32::MAX => ("Not:A-Brand", "24"),
        _ => ("Not_A Brand", "8"),
    }
}

fn build_chrome_sec_ch_ua(major: u32, salt: usize) -> String {
    let (brand_name, brand_ver) = not_a_brand_tag(major);
    let major_str = major.to_string();
    let items = [
        format!("\"Chromium\";v=\"{major_str}\""),
        format!("\"{brand_name}\";v=\"{brand_ver}\""),
        format!("\"Google Chrome\";v=\"{major_str}\""),
    ];
    let permutations: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let perm = &permutations[salt % 6];
    format!("{}, {}, {}", items[perm[0]], items[perm[1]], items[perm[2]])
}

/// 构建带完整补丁版本号的 sec-ch-ua-full-version-list（Chrome）
fn build_chrome_sec_ch_ua_full_version_list(major: u32, salt: usize) -> String {
    let (brand_name, brand_ver) = not_a_brand_tag(major);
    let patch = pick_chrome_patch(major, salt);
    let items = [
        format!("\"Chromium\";v=\"{patch}\""),
        format!("\"{brand_name}\";v=\"{brand_ver}.0.0.0\""),
        format!("\"Google Chrome\";v=\"{patch}\""),
    ];
    let permutations: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let perm = &permutations[salt % 6];
    format!("{}, {}, {}", items[perm[0]], items[perm[1]], items[perm[2]])
}

fn build_edge_sec_ch_ua(chromium_major: u32, edge_major: u32, salt: usize) -> String {
    let (brand_name, brand_ver) = not_a_brand_tag(chromium_major);
    let chromium_str = chromium_major.to_string();
    let edge_str = edge_major.to_string();
    let items = [
        format!("\"Chromium\";v=\"{chromium_str}\""),
        format!("\"{brand_name}\";v=\"{brand_ver}\""),
        format!("\"Microsoft Edge\";v=\"{edge_str}\""),
    ];
    let permutations: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let perm = &permutations[salt % 6];
    format!("{}, {}, {}", items[perm[0]], items[perm[1]], items[perm[2]])
}

/// 构建带完整补丁版本号的 sec-ch-ua-full-version-list（Edge）
fn build_edge_sec_ch_ua_full_version_list(chromium_major: u32, edge_major: u32, salt: usize) -> String {
    let (brand_name, brand_ver) = not_a_brand_tag(chromium_major);
    let chrome_patch = pick_chrome_patch(chromium_major, salt);
    let edge_patch = pick_edge_patch(edge_major, salt);
    let items = [
        format!("\"Chromium\";v=\"{chrome_patch}\""),
        format!("\"{brand_name}\";v=\"{brand_ver}.0.0.0\""),
        format!("\"Microsoft Edge\";v=\"{edge_patch}\""),
    ];
    let permutations: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    let perm = &permutations[salt % 6];
    format!("{}, {}, {}", items[perm[0]], items[perm[1]], items[perm[2]])
}

// ── 平台版本随机池 ──────────────────────────────────────────────
const WIN_PLATFORM_VERSIONS: [&str; 4] = ["\"10.0.0\"", "\"15.0.0\"", "\"10.0.1\"", "\"15.0.1\""];
const MAC_PLATFORM_VERSIONS: [&str; 4] = ["\"14.5.0\"", "\"14.7.0\"", "\"15.0.0\"", "\"15.1.0\""];
const LINUX_PLATFORM_VERSIONS: [&str; 3] = ["\"6.5.0\"", "\"6.8.0\"", "\"6.10.0\""];

fn pick_platform_version(name: &str, salt: usize) -> Option<String> {
    if name.contains("_mac") || name.starts_with("safari") {
        Some(MAC_PLATFORM_VERSIONS[salt / 7 % MAC_PLATFORM_VERSIONS.len()].to_string())
    } else if name.contains("_linux") {
        Some(LINUX_PLATFORM_VERSIONS[salt / 7 % LINUX_PLATFORM_VERSIONS.len()].to_string())
    } else if name.starts_with("firefox") || name.starts_with("safari") {
        None // Firefox / Safari 不发送 sec-ch-ua
    } else {
        // Windows 默认
        Some(WIN_PLATFORM_VERSIONS[salt / 7 % WIN_PLATFORM_VERSIONS.len()].to_string())
    }
}

fn pick_platform(name: &str) -> Option<&'static str> {
    if !is_chromium(name) {
        return None;
    }
    if name.contains("_mac") {
        Some("\"macOS\"")
    } else if name.contains("_linux") {
        Some("\"Linux\"")
    } else {
        Some("\"Windows\"")
    }
}

fn pick_arch(name: &str, salt: usize) -> Option<&'static str> {
    if !is_chromium(name) {
        return None;
    }
    if name.contains("_mac") {
        Some(if salt.is_multiple_of(3) {
            "\"arm\""
        } else {
            "\"x86\""
        })
    } else if name.contains("_linux") {
        Some("\"x86\"")
    } else {
        // Windows
        Some(if salt.is_multiple_of(11) {
            "\"arm\""
        } else {
            "\"x86\""
        })
    }
}

fn build_sec_ch_ua(name: &str, salt: usize) -> Option<String> {
    match name {
        n if n.starts_with("chrome") => {
            let major = extract_chrome_major(n);
            Some(build_chrome_sec_ch_ua(major, salt))
        }
        n if n.starts_with("edge") => {
            let major = extract_edge_major(n);
            Some(build_edge_sec_ch_ua(major, major, salt))
        }
        _ => None, // Firefox / Safari 不发送
    }
}

/// 构建 sec-ch-ua-full-version-list（包含完整补丁版本号）
fn build_sec_ch_ua_full_version_list(name: &str, salt: usize) -> Option<String> {
    match name {
        n if n.starts_with("chrome") => {
            let major = extract_chrome_major(n);
            Some(build_chrome_sec_ch_ua_full_version_list(major, salt))
        }
        n if n.starts_with("edge") => {
            let major = extract_edge_major(n);
            Some(build_edge_sec_ch_ua_full_version_list(major, major, salt))
        }
        _ => None,
    }
}

/// 构建 sec-ch-ua-full-version（单个浏览器完整版本号）
fn build_sec_ch_ua_full_version(name: &str, salt: usize) -> Option<String> {
    match name {
        n if n.starts_with("chrome") => {
            let major = extract_chrome_major(n);
            Some(format!("\"{}\"", pick_chrome_patch(major, salt)))
        }
        n if n.starts_with("edge") => {
            let major = extract_edge_major(n);
            Some(format!("\"{}\"", pick_edge_patch(major, salt)))
        }
        _ => None,
    }
}

fn extract_chrome_major(name: &str) -> u32 {
    // "chrome136_mac" -> 136, "chrome131" -> 131
    let stripped = name.strip_prefix("chrome").unwrap_or("136");
    stripped
        .split('_')
        .next()
        .unwrap_or("136")
        .parse()
        .unwrap_or(136)
}

fn extract_edge_major(name: &str) -> u32 {
    let stripped = name.strip_prefix("edge").unwrap_or("134");
    stripped
        .split('_')
        .next()
        .unwrap_or("134")
        .parse()
        .unwrap_or(134)
}

fn build_default_headers(name: &str, accept_language: &str, salt: usize) -> Result<HeaderMap> {
    let mut headers = HeaderMap::new();
    headers.insert(header::ACCEPT, HeaderValue::from_static(accept_for(name)));
    headers.insert(
        header::ACCEPT_LANGUAGE,
        HeaderValue::from_str(accept_language)?,
    );
    headers.insert(
        header::ACCEPT_ENCODING,
        HeaderValue::from_static(accept_encoding_for(name)),
    );

    // ── DNT (Do Not Track) 随机发送 ──
    // 真实浏览器约 15-25% 启用此设置
    if salt % 5 == 0 {
        headers.insert(
            HeaderName::from_static("dnt"),
            HeaderValue::from_static("1"),
        );
    }

    // ── Client Hints（仅 Chromium 系）──
    if let Some(ch_ua) = build_sec_ch_ua(name, salt) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua"),
            HeaderValue::from_str(&ch_ua)?,
        );
    }
    // sec-ch-ua-full-version-list: 带完整补丁版本号
    if let Some(full_ver_list) = build_sec_ch_ua_full_version_list(name, salt) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua-full-version-list"),
            HeaderValue::from_str(&full_ver_list)?,
        );
    }
    // sec-ch-ua-full-version: 单个浏览器完整版本
    if let Some(full_ver) = build_sec_ch_ua_full_version(name, salt) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua-full-version"),
            HeaderValue::from_str(&full_ver)?,
        );
    }
    if is_chromium(name) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua-mobile"),
            HeaderValue::from_static("?0"),
        );
        // sec-ch-ua-model: 桌面端为空字符串
        headers.insert(
            HeaderName::from_static("sec-ch-ua-model"),
            HeaderValue::from_static("\"\""),
        );
    }
    if let Some(platform) = pick_platform(name) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua-platform"),
            HeaderValue::from_static(platform),
        );
    }
    if let Some(pv) = pick_platform_version(name, salt) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua-platform-version"),
            HeaderValue::from_str(&pv)?,
        );
    }
    if let Some(arch) = pick_arch(name, salt) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua-arch"),
            HeaderValue::from_static(arch),
        );
    }
    if is_chromium(name) {
        headers.insert(
            HeaderName::from_static("sec-ch-ua-bitness"),
            HeaderValue::from_static("\"64\""),
        );
        // sec-ch-prefers-color-scheme: 随机 light/dark
        headers.insert(
            HeaderName::from_static("sec-ch-prefers-color-scheme"),
            HeaderValue::from_static(if salt % 3 == 0 { "dark" } else { "light" }),
        );
    }

    // ── Firefox 专有头 ──
    if name.starts_with("firefox") {
        // Firefox 发送 TE: trailers
        headers.insert(
            HeaderName::from_static("te"),
            HeaderValue::from_static("trailers"),
        );
    }

    Ok(headers)
}

fn build_navigation_headers(mut headers: HeaderMap) -> HeaderMap {
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("max-age=0"));
    headers.insert(
        HeaderName::from_static("pragma"),
        HeaderValue::from_static("no-cache"),
    );
    headers.insert(
        HeaderName::from_static("upgrade-insecure-requests"),
        HeaderValue::from_static("1"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-dest"),
        HeaderValue::from_static("document"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-mode"),
        HeaderValue::from_static("navigate"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-site"),
        HeaderValue::from_static("none"),
    );
    headers.insert(
        HeaderName::from_static("sec-fetch-user"),
        HeaderValue::from_static("?1"),
    );
    headers
}

pub fn is_retryable_challenge_error(text: &str) -> bool {
    let lower = text.to_ascii_lowercase();
    lower.contains("init_session_403")
        || lower.contains("init_session_challenge")
        || lower.contains("auth_challenge")
        || lower.contains("cf-mitigated")
        || lower.contains("cloudflare")
        || lower.contains("captcha")
        || lower.contains("verify you are human")
}

pub fn looks_like_challenge_page(body: &str) -> bool {
    let lower = body.to_ascii_lowercase();
    lower.contains("cf-mitigated")
        || lower.contains("cloudflare")
        || lower.contains("cf-ray")
        || lower.contains("__cf_bm")
        || lower.contains("just a moment")
        || lower.contains("attention required")
        || lower.contains("challenge-form")
        || lower.contains("captcha")
        || lower.contains("verify you are human")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_profile_should_rotate() {
        let a = build_fingerprint_material("auto", 0, "ua").unwrap();
        let b = build_fingerprint_material("auto", 1, "ua").unwrap();
        assert_ne!(a.user_agent, b.user_agent);
    }

    #[test]
    fn chromium_profile_should_have_client_hints() {
        let fp = build_fingerprint_material("chrome131", 0, "ua").unwrap();
        assert!(
            fp.default_headers
                .contains_key(HeaderName::from_static("sec-ch-ua"))
        );
        assert!(
            fp.default_headers
                .contains_key(HeaderName::from_static("sec-ch-ua-platform"))
        );
    }

    #[test]
    fn firefox_profile_should_not_have_chromium_client_hints() {
        let fp = build_fingerprint_material("firefox136", 0, "ua").unwrap();
        assert!(
            !fp.default_headers
                .contains_key(HeaderName::from_static("sec-ch-ua"))
        );
    }

    #[test]
    fn mac_profile_should_have_macos_platform() {
        let fp = build_fingerprint_material("chrome136_mac", 42, "ua").unwrap();
        let platform = fp
            .default_headers
            .get(HeaderName::from_static("sec-ch-ua-platform"))
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(platform, "\"macOS\"");
        assert!(fp.user_agent.contains("Macintosh"));
    }

    #[test]
    fn linux_profile_should_have_linux_platform() {
        let fp = build_fingerprint_material("chrome136_linux", 99, "ua").unwrap();
        let platform = fp
            .default_headers
            .get(HeaderName::from_static("sec-ch-ua-platform"))
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(platform, "\"Linux\"");
        assert!(fp.user_agent.contains("Linux"));
    }

    #[test]
    fn emulation_should_map_correctly() {
        let fp = build_fingerprint_material("chrome136", 0, "ua").unwrap();
        assert_eq!(fp.emulation, Emulation::Chrome136);

        let fp = build_fingerprint_material("edge134_mac", 0, "ua").unwrap();
        assert_eq!(fp.emulation, Emulation::Edge134);

        let fp = build_fingerprint_material("firefox136", 0, "ua").unwrap();
        assert_eq!(fp.emulation, Emulation::Firefox136);

        let fp = build_fingerprint_material("safari18", 0, "ua").unwrap();
        assert_eq!(fp.emulation, Emulation::Safari18);
    }

    #[test]
    fn ua_should_contain_patch_version() {
        let fp = build_fingerprint_material("chrome136", 0, "ua").unwrap();
        assert!(
            !fp.user_agent.contains("136.0.0.0"),
            "should have real patch, got: {}",
            fp.user_agent
        );
        assert!(fp.user_agent.contains("136.0.710"));
    }

    #[test]
    fn sec_ch_ua_brand_order_should_vary() {
        let fp_a = build_fingerprint_material("chrome136", 0, "ua").unwrap();
        let fp_b = build_fingerprint_material("chrome136", 1, "ua").unwrap();
        let a = fp_a
            .default_headers
            .get(HeaderName::from_static("sec-ch-ua"))
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        let b = fp_b
            .default_headers
            .get(HeaderName::from_static("sec-ch-ua"))
            .unwrap()
            .to_str()
            .unwrap()
            .to_string();
        assert_ne!(a, b, "brand order should differ with salt");
    }

    #[test]
    fn auto_pool_covers_all_platforms() {
        let mut win = false;
        let mut mac = false;
        let mut linux = false;
        for salt in 0..AUTO_ROTATING_PROFILES.len() {
            let fp = build_fingerprint_material("auto", salt, "ua").unwrap();
            if fp.user_agent.contains("Windows") {
                win = true;
            }
            if fp.user_agent.contains("Macintosh") || fp.user_agent.contains("Mac OS") {
                mac = true;
            }
            if fp.user_agent.contains("Linux") {
                linux = true;
            }
        }
        assert!(win && mac && linux, "should cover all platforms");
    }

    #[test]
    fn auto_pool_has_no_duplicates() {
        let mut seen = std::collections::HashSet::new();
        for p in &AUTO_ROTATING_PROFILES {
            assert!(seen.insert(*p), "duplicate: {p}");
        }
    }

    #[test]
    fn accept_encoding_should_match_browser() {
        let fp = build_fingerprint_material("chrome136", 0, "ua").unwrap();
        let ae = fp
            .default_headers
            .get(header::ACCEPT_ENCODING)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(ae.contains("zstd"), "Chrome 136 should support zstd");

        let fp = build_fingerprint_material("safari18", 0, "ua").unwrap();
        let ae = fp
            .default_headers
            .get(header::ACCEPT_ENCODING)
            .unwrap()
            .to_str()
            .unwrap();
        assert!(!ae.contains("zstd"), "Safari should NOT support zstd");
    }
}
