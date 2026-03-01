use std::sync::OnceLock;
use tokio::sync::broadcast;

static LOG_TX: OnceLock<broadcast::Sender<String>> = OnceLock::new();

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
    println!("{msg}");
    send_log(msg);
}
