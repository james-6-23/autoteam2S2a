use chrono::Local;
use rand::Rng;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::models::AccountSeed;

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

const FIRST_NAMES: [&str; 48] = [
    "James",
    "Robert",
    "John",
    "Michael",
    "David",
    "William",
    "Richard",
    "Joseph",
    "Thomas",
    "Charles",
    "Christopher",
    "Daniel",
    "Matthew",
    "Anthony",
    "Mark",
    "Donald",
    "Steven",
    "Paul",
    "Andrew",
    "Joshua",
    "Kenneth",
    "Kevin",
    "Brian",
    "George",
    "Timothy",
    "Ronald",
    "Edward",
    "Jason",
    "Jeffrey",
    "Ryan",
    "Jacob",
    "Gary",
    "Nicholas",
    "Eric",
    "Jonathan",
    "Stephen",
    "Larry",
    "Justin",
    "Scott",
    "Brandon",
    "Benjamin",
    "Samuel",
    "Gregory",
    "Frank",
    "Alexander",
    "Patrick",
    "Raymond",
    "Dennis",
];
const LAST_NAMES: [&str; 48] = [
    "Smith",
    "Johnson",
    "Williams",
    "Brown",
    "Jones",
    "Garcia",
    "Miller",
    "Davis",
    "Rodriguez",
    "Martinez",
    "Hernandez",
    "Lopez",
    "Gonzalez",
    "Wilson",
    "Anderson",
    "Taylor",
    "Moore",
    "Jackson",
    "Martin",
    "Lee",
    "Perez",
    "Thompson",
    "White",
    "Harris",
    "Sanchez",
    "Clark",
    "Ramirez",
    "Lewis",
    "Robinson",
    "Walker",
    "Young",
    "Allen",
    "King",
    "Wright",
    "Scott",
    "Torres",
    "Nguyen",
    "Hill",
    "Flores",
    "Green",
    "Adams",
    "Nelson",
    "Baker",
    "Hall",
    "Rivera",
    "Campbell",
    "Mitchell",
    "Carter",
];
static FIRST_NAME_INDEX: AtomicUsize = AtomicUsize::new(0);
static LAST_NAME_INDEX: AtomicUsize = AtomicUsize::new(0);

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
    Local::now().format("%H:%M:%S").to_string()
}

pub fn log_worker(worker_id: usize, stage: &str, message: &str) {
    println!("[{}] [W{}] [{}] {}", now_hms(), worker_id, stage, message);
}

/// 绿色高亮日志，用于关键成功信息（如 RT 获取成功）
pub fn log_worker_green(worker_id: usize, stage: &str, message: &str) {
    println!(
        "\x1b[32m[{}] [W{}] [{}] {}\x1b[0m",
        now_hms(),
        worker_id,
        stage,
        message
    );
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
    let first_idx = FIRST_NAME_INDEX.fetch_add(1, Ordering::Relaxed) % FIRST_NAMES.len();
    let last_idx = LAST_NAME_INDEX.fetch_add(1, Ordering::Relaxed) % LAST_NAMES.len();
    let first = FIRST_NAMES[first_idx];
    let last = LAST_NAMES[last_idx];
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
