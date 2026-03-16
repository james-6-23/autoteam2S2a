use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountSeed {
    pub account: String,
    pub password: String,
    pub real_name: String,
    pub birthdate: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegisteredAccount {
    pub account: String,
    pub password: String,
    pub token: String,
    pub account_id: String,
    pub plan_type: String,
    pub proxy: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountWithRt {
    pub account: String,
    pub password: String,
    pub token: String,
    pub account_id: String,
    pub plan_type: String,
    pub refresh_token: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowReport {
    pub registered_ok: usize,
    pub registered_failed: usize,
    pub rt_ok: usize,
    pub rt_failed: usize,
    pub s2a_ok: usize,
    pub s2a_failed: usize,
    /// free 账号独立推送成功数（已包含在 s2a_ok 总数中）
    #[serde(default)]
    pub free_s2a_ok: usize,
    /// free 账号独立推送失败数（已包含在 s2a_failed 总数中）
    #[serde(default)]
    pub free_s2a_failed: usize,
    pub output_files: Vec<String>,
    pub elapsed_secs: f32,
    pub target_count: usize,
}

/// 每个 S2A 号池的分发结果
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamDistResult {
    pub team_name: String,
    pub percent: u8,
    pub assigned_count: usize,
    pub s2a_ok: usize,
    pub s2a_failed: usize,
}

/// 多S2A分发总报告
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributionReport {
    pub run_id: String,
    pub registered_ok: usize,
    pub registered_failed: usize,
    pub rt_ok: usize,
    pub rt_failed: usize,
    pub total_s2a_ok: usize,
    pub total_s2a_failed: usize,
    pub team_results: Vec<TeamDistResult>,
    pub output_files: Vec<String>,
    pub elapsed_secs: f32,
    pub target_count: usize,
}

/// 邀请任务进度（原子计数器，线程安全）
pub struct InviteProgress {
    pub invited_ok: AtomicUsize,
    pub invited_failed: AtomicUsize,
    pub reg_ok: AtomicUsize,
    pub reg_failed: AtomicUsize,
    pub rt_ok: AtomicUsize,
    pub rt_failed: AtomicUsize,
    pub s2a_ok: AtomicUsize,
    pub s2a_failed: AtomicUsize,
    stage: Mutex<String>,
}

impl InviteProgress {
    pub fn new() -> Self {
        Self {
            invited_ok: AtomicUsize::new(0),
            invited_failed: AtomicUsize::new(0),
            reg_ok: AtomicUsize::new(0),
            reg_failed: AtomicUsize::new(0),
            rt_ok: AtomicUsize::new(0),
            rt_failed: AtomicUsize::new(0),
            s2a_ok: AtomicUsize::new(0),
            s2a_failed: AtomicUsize::new(0),
            stage: Mutex::new("准备中".to_string()),
        }
    }

    pub fn set_stage(&self, stage: &str) {
        if let Ok(mut s) = self.stage.lock() {
            *s = stage.to_string();
        }
    }
}
