use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, broadcast};
use tower_http::cors::CorsLayer;

use crate::config::{AppConfig, S2aConfig};
use crate::db::RunHistoryDb;
use crate::email_service;
use crate::models::WorkflowReport;
use crate::proxy_pool::{ProxyPool, health_check, resolve_proxies};
use crate::services::{LiveCodexService, LiveRegisterService, S2aHttpService, S2aService};
use crate::workflow::{WorkflowOptions, WorkflowRunner};

// ─── Embedded frontend ──────────────────────────────────────────────────────

const INDEX_HTML: &str = include_str!("../static/index.html");

// ─── Data types ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Running,
    Completed,
    Failed,
    Cancelled,
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskStatus::Pending => write!(f, "pending"),
            TaskStatus::Running => write!(f, "running"),
            TaskStatus::Completed => write!(f, "completed"),
            TaskStatus::Failed => write!(f, "failed"),
            TaskStatus::Cancelled => write!(f, "cancelled"),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TaskEntry {
    pub task_id: String,
    pub status: TaskStatus,
    pub team: String,
    pub target: usize,
    pub created_at: String,
    pub report: Option<WorkflowReport>,
    pub error: Option<String>,
    #[serde(skip)]
    pub cancel_flag: Arc<AtomicBool>,
}

// ─── Task Manager ────────────────────────────────────────────────────────────

pub struct TaskManager {
    tasks: Mutex<HashMap<String, TaskEntry>>,
    max_concurrent: usize,
}

impl TaskManager {
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            tasks: Mutex::new(HashMap::new()),
            max_concurrent,
        }
    }

    pub async fn submit(&self, team: String, target: usize) -> Result<String, String> {
        let mut tasks = self.tasks.lock().await;
        let running = tasks
            .values()
            .filter(|t| matches!(t.status, TaskStatus::Pending | TaskStatus::Running))
            .count();
        if running >= self.max_concurrent {
            return Err(format!(
                "已达最大并发任务数 ({}/{}), 请等待现有任务完成",
                running, self.max_concurrent
            ));
        }
        let task_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let entry = TaskEntry {
            task_id: task_id.clone(),
            status: TaskStatus::Pending,
            team,
            target,
            created_at: crate::util::beijing_now().to_rfc3339(),
            report: None,
            error: None,
            cancel_flag: Arc::new(AtomicBool::new(false)),
        };
        tasks.insert(task_id.clone(), entry);
        Ok(task_id)
    }

    pub async fn set_running(&self, task_id: &str) {
        if let Some(t) = self.tasks.lock().await.get_mut(task_id) {
            t.status = TaskStatus::Running;
        }
    }

    pub async fn set_completed(&self, task_id: &str, report: WorkflowReport) {
        if let Some(t) = self.tasks.lock().await.get_mut(task_id) {
            t.status = TaskStatus::Completed;
            t.report = Some(report);
        }
    }

    pub async fn set_failed(&self, task_id: &str, error: String) {
        if let Some(t) = self.tasks.lock().await.get_mut(task_id) {
            t.status = TaskStatus::Failed;
            t.error = Some(error);
        }
    }

    pub async fn set_cancelled(&self, task_id: &str) {
        if let Some(t) = self.tasks.lock().await.get_mut(task_id) {
            if matches!(t.status, TaskStatus::Pending | TaskStatus::Running) {
                t.cancel_flag.store(true, Ordering::SeqCst);
                t.status = TaskStatus::Cancelled;
            }
        }
    }

    pub async fn get(&self, task_id: &str) -> Option<TaskEntry> {
        self.tasks.lock().await.get(task_id).cloned()
    }

    pub async fn list(&self) -> Vec<TaskEntry> {
        let tasks = self.tasks.lock().await;
        let mut list: Vec<TaskEntry> = tasks.values().cloned().collect();
        list.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        list
    }

    pub async fn get_cancel_flag(&self, task_id: &str) -> Option<Arc<AtomicBool>> {
        self.tasks
            .lock()
            .await
            .get(task_id)
            .map(|t| t.cancel_flag.clone())
    }
}

// ─── App State ───────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<RwLock<AppConfig>>,
    pub config_path: PathBuf,
    pub task_manager: Arc<TaskManager>,
    pub proxy_file: Option<PathBuf>,
    pub started_at: Instant,
    pub run_history_db: Arc<RunHistoryDb>,
    pub scheduler_state: Arc<crate::scheduler::SchedulerState>,
    pub log_tx: broadcast::Sender<String>,
}

// ─── Request / Response types ────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct CreateTaskRequest {
    pub team: Option<String>,
    pub target: Option<usize>,
    pub register_workers: Option<usize>,
    pub rt_workers: Option<usize>,
    pub rt_retries: Option<usize>,
    pub push_s2a: Option<bool>,
    pub use_chatgpt_mail: Option<bool>,
}

#[derive(Serialize)]
struct CreateTaskResponse {
    task_id: String,
    status: String,
    message: String,
}

#[derive(Serialize)]
struct TaskListResponse {
    tasks: Vec<TaskSummaryResp>,
}

#[derive(Serialize)]
struct TaskSummaryResp {
    task_id: String,
    status: String,
    team: String,
    target: usize,
    created_at: String,
}

#[derive(Serialize)]
struct TaskDetailResponse {
    task_id: String,
    status: String,
    team: String,
    target: usize,
    created_at: String,
    report: Option<WorkflowReport>,
    error: Option<String>,
}

#[derive(Serialize)]
struct CancelResponse {
    task_id: String,
    status: String,
    message: String,
}

#[derive(Serialize)]
struct FullConfigResponse {
    teams: Vec<S2aConfig>,
    defaults: DefaultsResp,
    register: RegisterResp,
    proxy_pool: Vec<String>,
    email_domains: Vec<String>,
    d1_cleanup: D1CleanupResp,
}

#[derive(Serialize)]
struct D1CleanupResp {
    enabled: bool,
    account_id: String,
    api_key: String,
    databases: Vec<crate::config::D1Database>,
    keep_percent: f64,
    batch_size: usize,
}

#[derive(Serialize)]
struct DefaultsResp {
    target_count: usize,
    register_workers: usize,
    rt_workers: usize,
    rt_retries: usize,
}

#[derive(Serialize)]
struct RegisterResp {
    mail_api_base: String,
    mail_api_path: String,
    mail_api_token: String,
    mail_request_timeout_sec: u64,
    otp_max_retries: usize,
    otp_interval_ms: u64,
    request_timeout_sec: u64,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    version: String,
    uptime_secs: u64,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Serialize)]
struct MsgResponse {
    message: String,
}

// Config mutation request types
#[derive(Deserialize)]
struct AddS2aRequest {
    name: String,
    api_base: String,
    admin_key: String,
    concurrency: Option<usize>,
    priority: Option<usize>,
    group_ids: Option<Vec<i64>>,
}

#[derive(Deserialize)]
struct UpdateDefaultsRequest {
    target_count: Option<usize>,
    register_workers: Option<usize>,
    rt_workers: Option<usize>,
    rt_retries: Option<usize>,
}

#[derive(Deserialize)]
struct UpdateRegisterRequest {
    mail_api_base: Option<String>,
    mail_api_path: Option<String>,
    mail_api_token: Option<String>,
    mail_request_timeout_sec: Option<u64>,
    otp_max_retries: Option<usize>,
    otp_interval_ms: Option<u64>,
    request_timeout_sec: Option<u64>,
}

#[derive(Deserialize)]
struct EmailDomainRequest {
    domain: String,
}

#[derive(Deserialize)]
struct UpdateD1CleanupRequest {
    enabled: Option<bool>,
    account_id: Option<String>,
    api_key: Option<String>,
    databases: Option<Vec<crate::config::D1Database>>,
    keep_percent: Option<f64>,
    batch_size: Option<usize>,
}

fn error_json(status: StatusCode, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

// ─── Handlers ────────────────────────────────────────────────────────────────

async fn index_handler() -> impl IntoResponse {
    Html(INDEX_HTML)
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: state.started_at.elapsed().as_secs(),
    })
}

async fn config_handler(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.config.read().await;
    let teams = cfg.effective_s2a_configs();
    let reg = &cfg.register;
    Json(FullConfigResponse {
        teams,
        defaults: DefaultsResp {
            target_count: cfg.defaults.target_count.unwrap_or(1),
            register_workers: cfg.defaults.register_workers.unwrap_or(15),
            rt_workers: cfg.defaults.rt_workers.unwrap_or(10),
            rt_retries: cfg.defaults.rt_retries.unwrap_or(4),
        },
        register: RegisterResp {
            mail_api_base: reg
                .mail_api_base
                .clone()
                .unwrap_or_else(|| "https://kyx-cloud-email.kkyyxx.top".into()),
            mail_api_path: reg
                .mail_api_path
                .clone()
                .unwrap_or_else(|| "/api/public/emailList".into()),
            mail_api_token: reg.mail_api_token.clone().unwrap_or_default(),
            mail_request_timeout_sec: reg.mail_request_timeout_sec.unwrap_or(12),
            otp_max_retries: reg.otp_max_retries.unwrap_or(18),
            otp_interval_ms: reg.otp_interval_ms.unwrap_or(1000),
            request_timeout_sec: reg.request_timeout_sec.unwrap_or(20),
        },
        proxy_pool: cfg.proxy_pool.clone(),
        email_domains: cfg.email_domains.clone(),
        d1_cleanup: D1CleanupResp {
            enabled: cfg.d1_cleanup.enabled.unwrap_or(false),
            account_id: cfg.d1_cleanup.account_id.clone().unwrap_or_default(),
            api_key: cfg.d1_cleanup.api_key.clone().unwrap_or_default(),
            databases: cfg.d1_cleanup.databases.clone().unwrap_or_default(),
            keep_percent: cfg.d1_cleanup.keep_percent.unwrap_or(0.1),
            batch_size: cfg.d1_cleanup.batch_size.unwrap_or(5000),
        },
    })
}

// S2A team management
async fn add_s2a_handler(
    State(state): State<AppState>,
    Json(req): Json<AddS2aRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    // Check for duplicate
    if cfg.s2a.iter().any(|t| t.name == req.name) {
        return Err(error_json(
            StatusCode::CONFLICT,
            &format!("号池已存在: {}", req.name),
        ));
    }
    cfg.s2a.push(S2aConfig {
        name: req.name.clone(),
        api_base: req.api_base,
        admin_key: req.admin_key,
        concurrency: req.concurrency.unwrap_or(50),
        priority: req.priority.unwrap_or(30),
        group_ids: req.group_ids.unwrap_or_default(),
    });
    auto_save(&cfg, &state.config_path);
    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("号池 {} 已添加", req.name),
        }),
    ))
}

async fn delete_s2a_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    let before = cfg.s2a.len();
    cfg.s2a.retain(|t| t.name != name);
    if cfg.s2a.len() == before {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("未找到号池: {name}"),
        ));
    }
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("号池 {name} 已删除"),
    }))
}

async fn test_s2a_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let team = cfg
        .effective_s2a_configs()
        .into_iter()
        .find(|t| t.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("未找到号池: {name}")))?;
    drop(cfg);

    let svc = S2aHttpService::new();
    svc.test_connection(&team)
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("连接失败: {e:#}")))?;

    Ok(Json(MsgResponse {
        message: "连接成功".to_string(),
    }))
}

#[derive(Serialize)]
struct S2aStatsResponse {
    active: usize,
    rate_limited: usize,
    available: usize,
}

async fn s2a_stats_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let team = cfg
        .effective_s2a_configs()
        .into_iter()
        .find(|t| t.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("未找到号池: {name}")))?;
    drop(cfg);

    let svc = S2aHttpService::new();
    let base = S2aHttpService::normalized_api_base(&team.api_base);

    // 构建 group 参数
    let group_param = team
        .group_ids
        .first()
        .map(|g| format!("&group={g}"))
        .unwrap_or_default();

    // 并发获取 active 和 rate_limited 数量
    let active_url = format!(
        "{base}/admin/accounts?page=1&page_size=1&status=active{group_param}&timezone=Asia%2FShanghai"
    );
    let rl_url = format!(
        "{base}/admin/accounts?page=1&page_size=1&status=rate_limited{group_param}&timezone=Asia%2FShanghai"
    );

    let (active_res, rl_res) = tokio::join!(
        fetch_s2a_total(&svc, &active_url, &team.admin_key),
        fetch_s2a_total(&svc, &rl_url, &team.admin_key),
    );

    let active = active_res.map_err(|e| {
        error_json(
            StatusCode::BAD_GATEWAY,
            &format!("获取 active 数据失败: {e:#}"),
        )
    })?;
    let rate_limited = rl_res.map_err(|e| {
        error_json(
            StatusCode::BAD_GATEWAY,
            &format!("获取 rate_limited 数据失败: {e:#}"),
        )
    })?;

    Ok(Json(S2aStatsResponse {
        active,
        rate_limited,
        available: active.saturating_sub(rate_limited),
    }))
}

async fn fetch_s2a_total(
    _svc: &S2aHttpService,
    url: &str,
    admin_key: &str,
) -> anyhow::Result<usize> {
    let client = rquest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let key = admin_key.trim();
    let bearer = if key.to_ascii_lowercase().starts_with("bearer ") {
        key.to_string()
    } else {
        format!("Bearer {key}")
    };

    let resp = client
        .get(url)
        .header("Accept", "application/json")
        .header("Authorization", &bearer)
        .header("X-API-Key", key)
        .header("X-Admin-Key", key)
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }

    let json: serde_json::Value = resp.json().await?;
    let total = json
        .get("data")
        .and_then(|d| d.get("total"))
        .and_then(|t| t.as_u64())
        .unwrap_or(0) as usize;

    Ok(total)
}

// ─── Fetch S2A groups (proxy) ───────────────────────────────────────────────

#[derive(Deserialize)]
struct FetchGroupsRequest {
    api_base: String,
    admin_key: String,
}

#[derive(Serialize)]
struct GroupItem {
    id: i64,
    name: String,
    status: String,
    account_count: u64,
}

async fn fetch_s2a_groups_handler(
    Json(req): Json<FetchGroupsRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let base = S2aHttpService::normalized_api_base(&req.api_base);
    let url = format!("{base}/admin/groups?page=1&page_size=100&status=&timezone=Asia%2FShanghai");

    let client = rquest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let key = req.admin_key.trim();
    let bearer = if key.to_ascii_lowercase().starts_with("bearer ") {
        key.to_string()
    } else {
        format!("Bearer {key}")
    };

    let resp = client
        .get(&url)
        .header("Accept", "application/json")
        .header("Authorization", &bearer)
        .header("X-API-Key", key)
        .header("X-Admin-Key", key)
        .send()
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("请求失败: {e}")))?;

    if !resp.status().is_success() {
        return Err(error_json(
            StatusCode::BAD_GATEWAY,
            &format!("HTTP {}", resp.status()),
        ));
    }

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("解析失败: {e}")))?;

    let items = json
        .get("data")
        .and_then(|d| d.get("items"))
        .and_then(|i| i.as_array())
        .cloned()
        .unwrap_or_default();

    let groups: Vec<GroupItem> = items
        .iter()
        .filter_map(|item| {
            Some(GroupItem {
                id: item.get("id")?.as_i64()?,
                name: item.get("name")?.as_str()?.to_string(),
                status: item
                    .get("status")
                    .and_then(|s| s.as_str())
                    .unwrap_or("unknown")
                    .to_string(),
                account_count: item
                    .get("account_count")
                    .and_then(|c| c.as_u64())
                    .unwrap_or(0),
            })
        })
        .collect();

    Ok(Json(groups))
}

async fn update_defaults_handler(
    State(state): State<AppState>,
    Json(req): Json<UpdateDefaultsRequest>,
) -> impl IntoResponse {
    let mut cfg = state.config.write().await;
    if let Some(v) = req.target_count {
        cfg.defaults.target_count = Some(v);
    }
    if let Some(v) = req.register_workers {
        cfg.defaults.register_workers = Some(v);
    }
    if let Some(v) = req.rt_workers {
        cfg.defaults.rt_workers = Some(v);
    }
    if let Some(v) = req.rt_retries {
        cfg.defaults.rt_retries = Some(v);
    }
    auto_save(&cfg, &state.config_path);
    Json(MsgResponse {
        message: "运行参数已更新".to_string(),
    })
}

async fn update_register_handler(
    State(state): State<AppState>,
    Json(req): Json<UpdateRegisterRequest>,
) -> impl IntoResponse {
    let mut cfg = state.config.write().await;
    if let Some(v) = req.mail_api_base {
        cfg.register.mail_api_base = Some(v);
    }
    if let Some(v) = req.mail_api_path {
        cfg.register.mail_api_path = Some(v);
    }
    if let Some(v) = req.mail_api_token {
        cfg.register.mail_api_token = Some(v);
    }
    if let Some(v) = req.mail_request_timeout_sec {
        cfg.register.mail_request_timeout_sec = Some(v);
    }
    if let Some(v) = req.otp_max_retries {
        cfg.register.otp_max_retries = Some(v);
    }
    if let Some(v) = req.otp_interval_ms {
        cfg.register.otp_interval_ms = Some(v);
    }
    if let Some(v) = req.request_timeout_sec {
        cfg.register.request_timeout_sec = Some(v);
    }
    auto_save(&cfg, &state.config_path);
    Json(MsgResponse {
        message: "注册配置已更新".to_string(),
    })
}

async fn add_email_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    if domain.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "域名不能为空"));
    }
    let mut cfg = state.config.write().await;
    if cfg.email_domains.contains(&domain) {
        return Err(error_json(
            StatusCode::CONFLICT,
            &format!("域名已存在: {domain}"),
        ));
    }
    cfg.email_domains.push(domain.clone());
    auto_save(&cfg, &state.config_path);
    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("域名 {domain} 已添加"),
        }),
    ))
}

async fn delete_email_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    let mut cfg = state.config.write().await;
    let before = cfg.email_domains.len();
    cfg.email_domains.retain(|d| d != &domain);
    if cfg.email_domains.len() == before {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("未找到域名: {domain}"),
        ));
    }
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("域名 {domain} 已删除"),
    }))
}

async fn update_d1_cleanup_handler(
    State(state): State<AppState>,
    Json(req): Json<UpdateD1CleanupRequest>,
) -> impl IntoResponse {
    let mut cfg = state.config.write().await;
    if let Some(v) = req.enabled {
        cfg.d1_cleanup.enabled = Some(v);
    }
    if let Some(v) = req.account_id {
        cfg.d1_cleanup.account_id = Some(v);
    }
    if let Some(v) = req.api_key {
        cfg.d1_cleanup.api_key = Some(v);
    }
    if let Some(v) = req.databases {
        cfg.d1_cleanup.databases = Some(v);
    }
    if let Some(v) = req.keep_percent {
        cfg.d1_cleanup.keep_percent = Some(v);
    }
    if let Some(v) = req.batch_size {
        cfg.d1_cleanup.batch_size = Some(v);
    }
    auto_save(&cfg, &state.config_path);
    Json(MsgResponse {
        message: "D1 清理配置已更新".to_string(),
    })
}

/// 自动持久化配置到文件（静默，不阻塞请求）
fn auto_save(config: &AppConfig, path: &std::path::Path) {
    match toml::to_string_pretty(config) {
        Ok(toml_str) => {
            if let Err(e) = std::fs::write(path, &toml_str) {
                println!("[自动保存] 写入失败: {e}");
            }
        }
        Err(e) => println!("[自动保存] 序列化失败: {e}"),
    }
}

async fn save_config_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let toml_str = toml::to_string_pretty(&*cfg).map_err(|e| {
        error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("序列化失败: {e}"),
        )
    })?;
    std::fs::write(&state.config_path, &toml_str).map_err(|e| {
        error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("写入文件失败: {e}"),
        )
    })?;
    Ok(Json(MsgResponse {
        message: format!("配置已保存到 {}", state.config_path.display()),
    }))
}

// Task handlers
async fn create_task_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateTaskRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let teams = cfg.effective_s2a_configs();
    if teams.is_empty() {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            "配置中没有可用的 S2A 号池",
        ));
    }

    let team = if let Some(ref name) = req.team {
        teams
            .iter()
            .find(|t| t.name == *name)
            .ok_or_else(|| error_json(StatusCode::BAD_REQUEST, &format!("未找到号池: {name}")))?
            .clone()
    } else {
        teams[0].clone()
    };

    let target = req.target.or(cfg.defaults.target_count).unwrap_or(1).max(1);
    let register_workers = req
        .register_workers
        .or(cfg.defaults.register_workers)
        .unwrap_or(15)
        .max(1);
    let rt_workers = req
        .rt_workers
        .or(cfg.defaults.rt_workers)
        .unwrap_or(10)
        .max(1);
    let rt_retries = req
        .rt_retries
        .or(cfg.defaults.rt_retries)
        .unwrap_or(4)
        .max(1);
    let push_s2a = req.push_s2a.unwrap_or(true);
    let use_chatgpt_mail = req.use_chatgpt_mail.unwrap_or(false);
    let config_snapshot = cfg.clone();
    drop(cfg); // release read lock

    let task_id = state
        .task_manager
        .submit(team.name.clone(), target)
        .await
        .map_err(|e| error_json(StatusCode::TOO_MANY_REQUESTS, &e))?;

    let task_id_clone = task_id.clone();
    let task_manager = state.task_manager.clone();
    let proxy_file = state.proxy_file.clone();
    let run_history_db = state.run_history_db.clone();

    tokio::spawn(async move {
        execute_task(
            task_id_clone,
            task_manager,
            run_history_db,
            config_snapshot,
            team,
            target,
            register_workers,
            rt_workers,
            rt_retries,
            push_s2a,
            use_chatgpt_mail,
            proxy_file,
        )
        .await;
    });

    Ok((
        StatusCode::CREATED,
        Json(CreateTaskResponse {
            task_id,
            status: "pending".to_string(),
            message: "任务已创建".to_string(),
        }),
    ))
}

async fn list_tasks_handler(State(state): State<AppState>) -> impl IntoResponse {
    let tasks = state.task_manager.list().await;
    Json(TaskListResponse {
        tasks: tasks
            .into_iter()
            .map(|t| TaskSummaryResp {
                task_id: t.task_id,
                status: t.status.to_string(),
                team: t.team,
                target: t.target,
                created_at: t.created_at,
            })
            .collect(),
    })
}

async fn get_task_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_manager
        .get(&task_id)
        .await
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("任务不存在: {task_id}")))?;

    Ok(Json(TaskDetailResponse {
        task_id: task.task_id,
        status: task.status.to_string(),
        team: task.team,
        target: task.target,
        created_at: task.created_at,
        report: task.report,
        error: task.error,
    }))
}

async fn cancel_task_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_manager
        .get(&task_id)
        .await
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("任务不存在: {task_id}")))?;

    match task.status {
        TaskStatus::Pending | TaskStatus::Running => {
            state.task_manager.set_cancelled(&task_id).await;
            Ok(Json(CancelResponse {
                task_id,
                status: "cancelled".to_string(),
                message: "取消请求已发送".to_string(),
            }))
        }
        _ => Err(error_json(
            StatusCode::BAD_REQUEST,
            &format!("任务状态为 {}, 无法取消", task.status),
        )),
    }
}

// ─── Task execution ──────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
async fn execute_task(
    task_id: String,
    task_manager: Arc<TaskManager>,
    run_history_db: Arc<RunHistoryDb>,
    cfg: AppConfig,
    team: S2aConfig,
    target: usize,
    register_workers: usize,
    rt_workers: usize,
    rt_retries: usize,
    push_s2a: bool,
    use_chatgpt_mail: bool,
    proxy_file: Option<PathBuf>,
) {
    task_manager.set_running(&task_id).await;

    // 写入运行记录
    let run_id = task_id.clone();
    if let Err(e) = run_history_db.insert_run(&crate::db::NewRun {
        id: run_id.clone(),
        schedule_name: None,
        trigger_type: "manual_task".to_string(),
        target_count: target,
        started_at: crate::util::beijing_now().to_rfc3339(),
    }) {
        println!("[任务] 写入运行记录失败: {e}");
    }

    let cancel_flag = match task_manager.get_cancel_flag(&task_id).await {
        Some(flag) => flag,
        None => {
            task_manager
                .set_failed(&task_id, "内部错误: 找不到任务".to_string())
                .await;
            let _ = run_history_db.fail_run(&run_id, "内部错误: 找不到任务");
            return;
        }
    };

    if cancel_flag.load(Ordering::Relaxed) {
        let _ = run_history_db.fail_run(&run_id, "任务在启动前被取消");
        return;
    }

    let proxy_pool = match build_proxy_pool(&cfg, proxy_file.as_deref()).await {
        Ok(pool) => pool,
        Err(e) => {
            let msg = format!("代理初始化失败: {e}");
            task_manager.set_failed(&task_id, msg.clone()).await;
            let _ = run_history_db.fail_run(&run_id, &msg);
            return;
        }
    };

    let register_runtime = cfg.register_runtime();
    let codex_runtime = cfg.codex_runtime();

    let (register_service, codex_service): (
        Arc<dyn crate::services::RegisterService>,
        Arc<dyn crate::services::CodexService>,
    ) = if use_chatgpt_mail {
        let api_key = register_runtime.chatgpt_mail_api_key.clone();
        let reg_email = Arc::new(email_service::EmailService::new_chatgpt_org_uk(
            api_key.clone(),
        ));
        let rt_email = Arc::new(email_service::EmailService::new_chatgpt_org_uk(api_key));
        (
            Arc::new(LiveRegisterService::new(
                register_runtime.clone(),
                reg_email,
            )) as Arc<dyn crate::services::RegisterService>,
            Arc::new(LiveCodexService::new(codex_runtime.clone(), rt_email))
                as Arc<dyn crate::services::CodexService>,
        )
    } else {
        let email_cfg = email_service::EmailServiceConfig {
            mail_api_base: register_runtime.mail_api_base.clone(),
            mail_api_path: register_runtime.mail_api_path.clone(),
            mail_api_token: register_runtime.mail_api_token.clone(),
            request_timeout_sec: register_runtime.mail_request_timeout_sec,
        };
        let reg_email = Arc::new(email_service::EmailService::new_http(email_cfg.clone()));
        let rt_email = Arc::new(email_service::EmailService::new_http(email_cfg));
        (
            Arc::new(LiveRegisterService::new(
                register_runtime.clone(),
                reg_email,
            )) as Arc<dyn crate::services::RegisterService>,
            Arc::new(LiveCodexService::new(codex_runtime.clone(), rt_email))
                as Arc<dyn crate::services::CodexService>,
        )
    };

    let s2a_service: Arc<dyn crate::services::S2aService> = Arc::new(S2aHttpService::new());

    let options = WorkflowOptions {
        target_count: target,
        register_workers,
        rt_workers,
        rt_retry_max: rt_retries,
        push_s2a,
        use_chatgpt_mail,
    };

    let started = std::time::Instant::now();
    let runner = WorkflowRunner::new(register_service, codex_service, s2a_service, proxy_pool);
    match runner
        .run_one_team(&cfg, &team, &options, cancel_flag.clone())
        .await
    {
        Ok(report) => {
            let _ = run_history_db.complete_run(
                &run_id,
                &crate::db::RunCompletion {
                    registered_ok: report.registered_ok,
                    registered_failed: report.registered_failed,
                    rt_ok: report.rt_ok,
                    rt_failed: report.rt_failed,
                    total_s2a_ok: report.s2a_ok,
                    total_s2a_failed: report.s2a_failed,
                    elapsed_secs: started.elapsed().as_secs_f64(),
                    finished_at: crate::util::beijing_now().to_rfc3339(),
                },
            );
            task_manager.set_completed(&task_id, report).await;
        }
        Err(e) => {
            let msg = format!("{e:#}");
            let _ = run_history_db.fail_run(&run_id, &msg);
            task_manager.set_failed(&task_id, msg).await;
        }
    }
}

async fn build_proxy_pool(
    cfg: &AppConfig,
    proxy_file: Option<&std::path::Path>,
) -> anyhow::Result<Arc<ProxyPool>> {
    let proxy_list = resolve_proxies(proxy_file, &cfg.proxy_pool)?;
    let check_timeout = cfg.proxy_check_timeout_sec.unwrap_or(5);
    let healthy_proxies = health_check(&proxy_list, check_timeout, proxy_file).await?;
    println!(
        "[server] 代理池初始化完成: {} 个可用代理",
        healthy_proxies.len()
    );
    Ok(Arc::new(ProxyPool::new(healthy_proxies)))
}

// ─── Schedule / Runs request types ───────────────────────────────────────────

#[derive(Deserialize)]
struct CreateScheduleRequest {
    name: String,
    start_time: String,
    end_time: String,
    target_count: usize,
    #[serde(default = "default_batch_interval_serde")]
    batch_interval_mins: u64,
    #[serde(default = "default_true_serde")]
    enabled: bool,
    register_workers: Option<usize>,
    rt_workers: Option<usize>,
    rt_retries: Option<usize>,
    #[serde(default = "default_true_serde")]
    push_s2a: bool,
    #[serde(default)]
    use_chatgpt_mail: bool,
    distribution: Vec<crate::config::DistributionEntry>,
}

fn default_true_serde() -> bool {
    true
}

fn default_batch_interval_serde() -> u64 {
    30
}

#[derive(Deserialize)]
struct UpdateScheduleRequest {
    start_time: Option<String>,
    end_time: Option<String>,
    enabled: Option<bool>,
    target_count: Option<usize>,
    batch_interval_mins: Option<u64>,
    register_workers: Option<usize>,
    rt_workers: Option<usize>,
    rt_retries: Option<usize>,
    push_s2a: Option<bool>,
    use_chatgpt_mail: Option<bool>,
    distribution: Option<Vec<crate::config::DistributionEntry>>,
}

#[derive(Deserialize)]
struct RunsQuery {
    page: Option<usize>,
    per_page: Option<usize>,
    schedule: Option<String>,
}

#[derive(Serialize)]
struct ScheduleWithStatus {
    #[serde(flatten)]
    config: crate::config::ScheduleConfig,
    running: bool,
}

#[derive(Serialize)]
struct ScheduleListResponse {
    schedules: Vec<ScheduleWithStatus>,
}

#[derive(Serialize)]
struct RunListResponse {
    runs: Vec<crate::db::RunRecord>,
    total: usize,
    page: usize,
    per_page: usize,
}

// ─── Schedule handlers ──────────────────────────────────────────────────────

async fn list_schedules_handler(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.config.read().await;
    let active_names = state.scheduler_state.active_names().await;
    let schedules = cfg
        .schedule
        .iter()
        .map(|s| ScheduleWithStatus {
            running: active_names.contains(&s.name),
            config: s.clone(),
        })
        .collect();
    Json(ScheduleListResponse { schedules })
}

async fn create_schedule_handler(
    State(state): State<AppState>,
    Json(req): Json<CreateScheduleRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if req.name.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "name 不能为空"));
    }

    // 校验时间格式
    crate::scheduler::validate_time(&req.start_time)
        .map_err(|e| error_json(StatusCode::BAD_REQUEST, &e))?;
    crate::scheduler::validate_time(&req.end_time)
        .map_err(|e| error_json(StatusCode::BAD_REQUEST, &e))?;

    if req.start_time == req.end_time {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            "开始时间和结束时间不能相同",
        ));
    }

    let mut cfg = state.config.write().await;

    // 检查重名
    if cfg.schedule.iter().any(|s| s.name == req.name) {
        return Err(error_json(
            StatusCode::CONFLICT,
            &format!("定时计划已存在: {}", req.name),
        ));
    }

    // 校验分发配置
    let teams = cfg.effective_s2a_configs();
    if let Err(e) = crate::distribution::validate_distribution(&req.distribution, &teams) {
        return Err(error_json(StatusCode::BAD_REQUEST, &e));
    }

    cfg.schedule.push(crate::config::ScheduleConfig {
        name: req.name.clone(),
        start_time: req.start_time,
        end_time: req.end_time,
        target_count: req.target_count,
        batch_interval_mins: req.batch_interval_mins,
        enabled: req.enabled,
        register_workers: req.register_workers,
        rt_workers: req.rt_workers,
        rt_retries: req.rt_retries,
        push_s2a: req.push_s2a,
        use_chatgpt_mail: req.use_chatgpt_mail,
        distribution: req.distribution,
    });
    auto_save(&cfg, &state.config_path);

    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("定时计划 {} 已创建", req.name),
        }),
    ))
}

async fn update_schedule_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateScheduleRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    let sched = cfg
        .schedule
        .iter_mut()
        .find(|s| s.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("定时计划不存在: {name}")))?;

    if let Some(ref st) = req.start_time {
        crate::scheduler::validate_time(st).map_err(|e| error_json(StatusCode::BAD_REQUEST, &e))?;
        sched.start_time = st.clone();
    }
    if let Some(ref et) = req.end_time {
        crate::scheduler::validate_time(et).map_err(|e| error_json(StatusCode::BAD_REQUEST, &e))?;
        sched.end_time = et.clone();
    }
    if sched.start_time == sched.end_time {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            "开始时间和结束时间不能相同",
        ));
    }
    if let Some(bi) = req.batch_interval_mins {
        sched.batch_interval_mins = bi.max(1);
    }
    if let Some(enabled) = req.enabled {
        sched.enabled = enabled;
    }
    if let Some(target_count) = req.target_count {
        sched.target_count = target_count;
    }
    if let Some(rw) = req.register_workers {
        sched.register_workers = Some(rw);
    }
    if let Some(rw) = req.rt_workers {
        sched.rt_workers = Some(rw);
    }
    if let Some(rr) = req.rt_retries {
        sched.rt_retries = Some(rr);
    }
    if let Some(ps) = req.push_s2a {
        sched.push_s2a = ps;
    }
    if let Some(cm) = req.use_chatgpt_mail {
        sched.use_chatgpt_mail = cm;
    }
    if let Some(dist) = req.distribution {
        let teams = cfg.effective_s2a_configs();
        if let Err(e) = crate::distribution::validate_distribution(&dist, &teams) {
            return Err(error_json(StatusCode::BAD_REQUEST, &e));
        }
        // re-borrow since we used cfg above
        cfg.schedule
            .iter_mut()
            .find(|s| s.name == name)
            .unwrap()
            .distribution = dist;
    }
    auto_save(&cfg, &state.config_path);

    Ok(Json(MsgResponse {
        message: format!("定时计划 {name} 已更新"),
    }))
}

async fn delete_schedule_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    let before = cfg.schedule.len();
    cfg.schedule.retain(|s| s.name != name);
    if cfg.schedule.len() == before {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("定时计划不存在: {name}"),
        ));
    }
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("定时计划 {name} 已删除"),
    }))
}

async fn toggle_schedule_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    let sched = cfg
        .schedule
        .iter_mut()
        .find(|s| s.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("定时计划不存在: {name}")))?;

    sched.enabled = !sched.enabled;
    let status = if sched.enabled {
        "已启用"
    } else {
        "已禁用"
    };

    // 禁用时，如果正在运行则停止
    if !sched.enabled {
        state.scheduler_state.stop(&name).await;
    }
    auto_save(&cfg, &state.config_path);

    Ok(Json(MsgResponse {
        message: format!("定时计划 {name} {status}"),
    }))
}

/// 手动启动一个计划的批次循环（不受时间窗口限制，直到手动停止）
async fn trigger_schedule_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let schedule = cfg
        .schedule
        .iter()
        .find(|s| s.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("定时计划不存在: {name}")))?
        .clone();
    drop(cfg);

    let cancel_flag = state
        .scheduler_state
        .start(&name)
        .await
        .ok_or_else(|| error_json(StatusCode::CONFLICT, &format!("计划 {name} 已在运行中")))?;

    let state_clone = state.clone();
    let db = state.run_history_db.clone();

    tokio::spawn(async move {
        // 手动触发的批次循环：只检查 cancel_flag，不检查时间窗口
        let mut batch_num = 0u64;
        println!(
            "[手动触发] {} 批次循环开始 (每批 {} 个, 间隔 {} 分钟)",
            schedule.name, schedule.target_count, schedule.batch_interval_mins
        );

        loop {
            if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                println!("[手动触发] {} 已停止", schedule.name);
                break;
            }

            batch_num += 1;
            println!("[手动触发] {} 开始第 {} 批次", schedule.name, batch_num);

            let config_snapshot = state_clone.config.read().await.clone();
            let runner =
                match build_workflow_runner(&config_snapshot, state_clone.proxy_file.as_deref())
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        println!("[手动触发] 构建 runner 失败 ({}): {e}", schedule.name);
                        tokio::time::sleep(tokio::time::Duration::from_secs(30)).await;
                        continue;
                    }
                };

            match crate::distribution::run_distribution(
                &runner,
                &config_snapshot,
                &schedule,
                &db,
                "manual",
                cancel_flag.clone(),
            )
            .await
            {
                Ok(report) => {
                    println!(
                        "[手动触发] {} 第 {} 批完成 | S2A: {} | 耗时: {:.1}s",
                        schedule.name, batch_num, report.total_s2a_ok, report.elapsed_secs
                    );
                }
                Err(e) => {
                    println!(
                        "[手动触发] {} 第 {} 批失败: {e:#}",
                        schedule.name, batch_num
                    );
                    if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            }

            // 批次间等待
            let total_wait = std::time::Duration::from_secs(schedule.batch_interval_mins * 60);
            let check_interval = tokio::time::Duration::from_secs(5);
            let mut elapsed = std::time::Duration::ZERO;

            while elapsed < total_wait {
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(check_interval).await;
                elapsed += std::time::Duration::from_secs(5);
            }

            if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
        }

        state_clone.scheduler_state.remove(&schedule.name).await;
        println!(
            "[手动触发] {} 批次循环结束（共 {} 批）",
            schedule.name, batch_num
        );
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(MsgResponse {
            message: format!("定时计划 {name} 已手动启动"),
        }),
    ))
}

/// 停止一个正在运行的计划
async fn stop_schedule_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    if state.scheduler_state.stop(&name).await {
        Ok(Json(MsgResponse {
            message: format!("定时计划 {name} 停止信号已发送"),
        }))
    } else {
        Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("计划 {name} 未在运行中"),
        ))
    }
}

// ─── Run history handlers ───────────────────────────────────────────────────

async fn list_runs_handler(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<RunsQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(20).clamp(1, 100);
    let schedule = query.schedule.as_deref();

    let (runs, total) = state
        .run_history_db
        .list_runs(page, per_page, schedule)
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询运行记录失败: {e}"),
            )
        })?;

    Ok(Json(RunListResponse {
        runs,
        total,
        page,
        per_page,
    }))
}

async fn get_run_handler(
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let detail = state
        .run_history_db
        .get_run(&run_id)
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询运行记录失败: {e}"),
            )
        })?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("运行记录不存在: {run_id}")))?;

    Ok(Json(detail))
}

// ─── Public helper: build workflow runner ────────────────────────────────────

pub async fn build_workflow_runner(
    cfg: &AppConfig,
    proxy_file: Option<&std::path::Path>,
) -> anyhow::Result<WorkflowRunner> {
    let proxy_pool = build_proxy_pool(cfg, proxy_file).await?;
    let register_runtime = cfg.register_runtime();
    let codex_runtime = cfg.codex_runtime();

    let chatgpt_mail_api_key = register_runtime.chatgpt_mail_api_key.clone();
    let (register_service, codex_service): (
        Arc<dyn crate::services::RegisterService>,
        Arc<dyn crate::services::CodexService>,
    ) = {
        let email_cfg = crate::email_service::EmailServiceConfig {
            mail_api_base: register_runtime.mail_api_base.clone(),
            mail_api_path: register_runtime.mail_api_path.clone(),
            mail_api_token: register_runtime.mail_api_token.clone(),
            request_timeout_sec: register_runtime.mail_request_timeout_sec,
        };
        // 默认使用 kyx-cloud 邮箱系统（调度器场景下通过 schedule 配置决定）
        let _ = chatgpt_mail_api_key; // suppress unused warning
        let reg_email = Arc::new(crate::email_service::EmailService::new_http(
            email_cfg.clone(),
        ));
        let rt_email = Arc::new(crate::email_service::EmailService::new_http(email_cfg));
        (
            Arc::new(LiveRegisterService::new(
                register_runtime.clone(),
                reg_email,
            )) as Arc<dyn crate::services::RegisterService>,
            Arc::new(LiveCodexService::new(codex_runtime.clone(), rt_email))
                as Arc<dyn crate::services::CodexService>,
        )
    };

    let s2a_service: Arc<dyn crate::services::S2aService> = Arc::new(S2aHttpService::new());

    Ok(WorkflowRunner::new(
        register_service,
        codex_service,
        s2a_service,
        proxy_pool,
    ))
}

// ─── SSE log stream ─────────────────────────────────────────────────────────

async fn log_stream_handler(
    State(state): State<AppState>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = state.log_tx.subscribe();

    let stream = futures::stream::unfold(rx, |mut rx| async move {
        loop {
            match rx.recv().await {
                Ok(msg) => {
                    return Some((Ok(Event::default().data(msg)), rx));
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    let msg = format!("[... 跳过 {} 条日志]", n);
                    return Some((Ok(Event::default().data(msg)), rx));
                }
                Err(broadcast::error::RecvError::Closed) => {
                    return None;
                }
            }
        }
    });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

// ─── Server entry point ─────────────────────────────────────────────────────

pub async fn start_server(
    cfg: AppConfig,
    config_path: PathBuf,
    host: String,
    port: u16,
    proxy_file: Option<PathBuf>,
) -> anyhow::Result<()> {
    let max_concurrent = cfg.server.max_concurrent_tasks.unwrap_or(3).max(1);
    let task_manager = Arc::new(TaskManager::new(max_concurrent));

    // 初始化 SQLite 运行记录数据库
    let db = Arc::new(
        RunHistoryDb::open(&std::path::PathBuf::from("data/history.db"))
            .expect("无法初始化运行记录数据库"),
    );

    // 初始化日志广播通道
    let (log_tx, _) = broadcast::channel::<String>(1000);
    crate::log_broadcast::init_log(log_tx.clone());

    let state = AppState {
        config: Arc::new(RwLock::new(cfg)),
        config_path,
        task_manager,
        proxy_file,
        started_at: Instant::now(),
        run_history_db: db.clone(),
        scheduler_state: Arc::new(crate::scheduler::SchedulerState::new()),
        log_tx,
    };

    // 启动后台调度器
    crate::scheduler::start_scheduler(state.clone(), db);

    let app = Router::new()
        // Frontend
        .route("/", get(index_handler))
        // Health
        .route("/health", get(health_handler))
        // Config management
        .route("/api/config", get(config_handler))
        .route("/api/config/s2a", post(add_s2a_handler))
        .route("/api/config/s2a/{name}", delete(delete_s2a_handler))
        .route("/api/config/s2a/{name}/test", post(test_s2a_handler))
        .route("/api/config/s2a/{name}/stats", get(s2a_stats_handler))
        .route("/api/s2a/fetch-groups", post(fetch_s2a_groups_handler))
        .route("/api/config/defaults", put(update_defaults_handler))
        .route("/api/config/register", put(update_register_handler))
        .route("/api/config/d1_cleanup", put(update_d1_cleanup_handler))
        .route("/api/config/email_domains", post(add_email_domain_handler))
        .route(
            "/api/config/email_domains",
            delete(delete_email_domain_handler),
        )
        .route("/api/config/save", post(save_config_handler))
        // Task management
        .route("/api/tasks", post(create_task_handler))
        .route("/api/tasks", get(list_tasks_handler))
        .route("/api/tasks/{task_id}", get(get_task_handler))
        .route("/api/tasks/{task_id}/cancel", post(cancel_task_handler))
        // Schedule management
        .route("/api/schedules", get(list_schedules_handler))
        .route("/api/schedules", post(create_schedule_handler))
        .route("/api/schedules/{name}", put(update_schedule_handler))
        .route("/api/schedules/{name}", delete(delete_schedule_handler))
        .route(
            "/api/schedules/{name}/toggle",
            post(toggle_schedule_handler),
        )
        .route(
            "/api/schedules/{name}/trigger",
            post(trigger_schedule_handler),
        )
        .route("/api/schedules/{name}/stop", post(stop_schedule_handler))
        // Run history
        .route("/api/runs", get(list_runs_handler))
        .route("/api/runs/{run_id}", get(get_run_handler))
        // Log stream (SSE)
        .route("/api/logs/stream", get(log_stream_handler))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    crate::log_broadcast::broadcast_log("═══════════════════════════════════════════");
    crate::log_broadcast::broadcast_log(&format!("   autoteam2s2a 服务模式 v{}", env!("CARGO_PKG_VERSION")));
    crate::log_broadcast::broadcast_log(&format!("   监听地址: http://{addr}"));
    crate::log_broadcast::broadcast_log(&format!("   管理面板: http://{addr}/"));
    crate::log_broadcast::broadcast_log(&format!("   最大并发任务: {max_concurrent}"));
    crate::log_broadcast::broadcast_log("═══════════════════════════════════════════");

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    println!("\n服务已停止");
    Ok(())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    println!("\n收到关闭信号，正在停止服务...");
}
