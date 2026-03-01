use serde::{Deserialize, Serialize};

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
