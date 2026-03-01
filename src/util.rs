use chrono::{DateTime, FixedOffset, Utc};
use fake::Fake;
use fake::faker::name::en::{FirstName, LastName};
use rand::Rng;

use crate::models::AccountSeed;

/// 北京时间 (UTC+8)
pub fn beijing_now() -> DateTime<FixedOffset> {
    let tz = FixedOffset::east_opt(8 * 3600).unwrap();
    Utc::now().with_timezone(&tz)
}

const DEFAULT_DOMAINS: [&str; 8] = [
    "@ic.loopkit.de5.net",
    "@kr.loopkit.de5.net",
    "@pm.kyyx.us.ci",
    "@pn.kyyx.us.ci",
    "@apexly.kyx03.de",
    "@axissys.kyx03.de",
    "@veloux.114514222.de",
    "@atomix.114514222.de",
];

pub fn generate_account_seed(email_domains: &[String]) -> AccountSeed {
    AccountSeed {
        account: generate_email(email_domains),
        password: generate_password(),
        real_name: generate_real_name(),
        birthdate: generate_birthdate(),
    }
}

pub fn mask_proxy(proxy: &str) -> String {
    if let Some((scheme, rest)) = proxy.split_once("://")
        && let Some((auth, host)) = rest.split_once('@')
        && let Some((user, _pass)) = auth.split_once(':')
    {
        return format!("{scheme}://{user}:***@{host}");
    }
    if proxy.len() <= 18 {
        return "***".to_string();
    }
    format!("{}***{}", &proxy[..9], &proxy[proxy.len() - 6..])
}

pub fn random_hex(len: usize) -> String {
    let charset = b"0123456789abcdef";
    let mut rng = rand::rng();
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..charset.len());
            charset[idx] as char
        })
        .collect()
}

pub fn random_delay_ms(min: u64, max: u64) -> u64 {
    let mut rng = rand::rng();
    rng.random_range(min..=max)
}

pub fn now_hms() -> String {
    beijing_now().format("%H:%M:%S").to_string()
}

pub fn log_worker(worker_id: usize, stage: &str, message: &str) {
    let ts = now_hms();
    let clean = format!("[{ts}] [W{worker_id}] [{stage}] {message}");
    match stage {
        "OK" => println!("\x1b[32m{clean}\x1b[0m"),
        "ERR" => println!("\x1b[31m{clean}\x1b[0m"),
        _ => println!("{clean}"),
    }
    crate::log_broadcast::send_log(&clean);
}

/// 绿色高亮日志（兼容旧调用）
pub fn log_worker_green(worker_id: usize, stage: &str, message: &str) {
    let clean = format!("[{}] [W{worker_id}] [{stage}] {message}", now_hms());
    println!("\x1b[32m{clean}\x1b[0m");
    crate::log_broadcast::send_log(&clean);
}

pub fn token_preview(token: &str) -> String {
    if token.len() <= 20 {
        return token.to_string();
    }
    format!("{}...", &token[..20])
}

pub fn summarize_user_agent(ua: &str) -> String {
    let browser = if let Some(v) = extract_version(ua, "Edg/") {
        format!("edge {v}")
    } else if let Some(v) = extract_version(ua, "Chrome/") {
        format!("chrome {v}")
    } else if let Some(v) = extract_version(ua, "Firefox/") {
        format!("firefox {v}")
    } else if let Some(v) = extract_version(ua, "Version/") {
        format!("safari {v}")
    } else {
        "unknown".to_string()
    };

    let os = if ua.contains("Windows") {
        "Windows"
    } else if ua.contains("Linux") {
        "Linux"
    } else if ua.contains("Mac OS X") || ua.contains("Macintosh") {
        "macOS"
    } else {
        "UnknownOS"
    };

    format!("{browser} ({os})")
}

fn extract_version(ua: &str, marker: &str) -> Option<String> {
    let start = ua.find(marker)?;
    let rest = &ua[start + marker.len()..];
    let major = rest.split('.').next()?.trim();
    if major.is_empty() {
        return None;
    }
    Some(major.to_string())
}

fn random_string(len: usize, charset: &[u8]) -> String {
    let mut rng = rand::rng();
    (0..len)
        .map(|_| {
            let idx = rng.random_range(0..charset.len());
            charset[idx] as char
        })
        .collect()
}

fn generate_email(domains: &[String]) -> String {
    let domain = if domains.is_empty() {
        DEFAULT_DOMAINS[rand::rng().random_range(0..DEFAULT_DOMAINS.len())].to_string()
    } else {
        domains[rand::rng().random_range(0..domains.len())].clone()
    };
    let local = random_string(15, b"abcdefghijklmnopqrstuvwxyz0123456789");
    format!("{local}{domain}")
}

fn generate_password() -> String {
    let upper = random_string(2, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ");
    let lower = random_string(8, b"abcdefghijklmnopqrstuvwxyz");
    let digits = random_string(2, b"0123456789");
    let symbols = ["!", "@", "#", "$", "%"];
    let special = symbols[rand::rng().random_range(0..symbols.len())];
    format!("{upper}{lower}{digits}{special}")
}

pub fn generate_real_name_pub() -> String {
    generate_real_name()
}

fn generate_real_name() -> String {
    let first: String = FirstName().fake();
    let last: String = LastName().fake();
    format!("{first} {last}")
}

fn generate_birthdate() -> String {
    let mut rng = rand::rng();
    let year = rng.random_range(2000..=2004);
    let month = rng.random_range(1..=12);
    let max_day = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            if year % 4 == 0 {
                29
            } else {
                28
            }
        }
    };
    let day = rng.random_range(1..=max_day);
    format!("{year:04}-{month:02}-{day:02}")
}
