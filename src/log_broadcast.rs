use std::sync::OnceLock;
use tokio::sync::broadcast;

static LOG_TX: OnceLock<broadcast::Sender<String>> = OnceLock::new();

#[derive(Copy, Clone, Eq, PartialEq)]
enum LogLevel {
    Normal,
    Success,
    Error,
}

/// 初始化全局日志广播通道（在 start_server 中调用一次）
pub fn init_log(tx: broadcast::Sender<String>) {
    LOG_TX.set(tx).ok();
}

/// 仅广播到 SSE 通道（不打印到 stdout）
pub fn send_log(msg: &str) {
    if let Some(tx) = LOG_TX.get() {
        let _ = tx.send(msg.to_string());
    }
}

/// 同时打印到 stdout 并广播到 SSE 通道
pub fn broadcast_log(msg: &str) {
    let clean = with_timestamp(msg);
    match detect_level(&clean) {
        LogLevel::Success => println!("\x1b[32m{clean}\x1b[0m"),
        LogLevel::Error => println!("\x1b[91m{clean}\x1b[0m"),
        LogLevel::Normal => println!("{clean}"),
    }
    send_log(&clean);
}

fn with_timestamp(msg: &str) -> String {
    if has_hms_prefix(msg) {
        return msg.to_string();
    }
    format!("[{}] {msg}", crate::util::now_hms())
}

fn has_hms_prefix(msg: &str) -> bool {
    let bytes = msg.as_bytes();
    if bytes.len() < 10 {
        return false;
    }
    if bytes[0] != b'[' || bytes[3] != b':' || bytes[6] != b':' || bytes[9] != b']' {
        return false;
    }
    bytes[1].is_ascii_digit()
        && bytes[2].is_ascii_digit()
        && bytes[4].is_ascii_digit()
        && bytes[5].is_ascii_digit()
        && bytes[7].is_ascii_digit()
        && bytes[8].is_ascii_digit()
}

fn detect_level(msg: &str) -> LogLevel {
    if msg.contains("[SIG-END][ERR]") || msg.contains("[ERR]") {
        return LogLevel::Error;
    }
    if msg.contains("[SIG-END][OK]") || msg.contains("[OK]") {
        return LogLevel::Success;
    }
    LogLevel::Normal
}
