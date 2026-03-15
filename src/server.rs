use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use futures::future::join_all;
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, broadcast};
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};

use crate::config::{AppConfig, RegisterLogMode, RegisterPerfMode, S2aConfig, S2aExtraConfig};
use crate::db::RunHistoryDb;
use crate::email_service;
use crate::models::WorkflowReport;
use crate::proxy_pool::{ProxyPool, health_check, health_check_detailed, normalize_proxy, resolve_proxies, test_single_proxy, check_proxy_quality};
use crate::redis_cache::RedisCache;
use crate::services::{LiveCodexService, LiveRegisterService, S2aHttpService, S2aService};
use crate::workflow::{WorkflowOptions, WorkflowRunner};

// ─── Frontend: served from frontend/dist/ via ServeDir ──────────────────────

const TASK_FINISHED_KEEP: usize = 300;
const TASK_PRUNE_THRESHOLD: usize = 600;
const MAX_TARGET_COUNT: usize = 5000;
const MAX_REGISTER_WORKERS: usize = 512;
const MAX_RT_WORKERS: usize = 512;
const MAX_RT_RETRIES: usize = 20;

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
    #[serde(skip)]
    pub progress: Arc<crate::workflow::TaskProgress>,
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
        Self::prune_finished_tasks(&mut tasks);
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
            progress: Arc::new(crate::workflow::TaskProgress::new()),
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
        let mut tasks = self.tasks.lock().await;
        if let Some(t) = tasks.get_mut(task_id) {
            t.status = TaskStatus::Completed;
            t.report = Some(report);
        }
        Self::prune_finished_tasks(&mut tasks);
    }

    pub async fn set_failed(&self, task_id: &str, error: String) {
        let mut tasks = self.tasks.lock().await;
        if let Some(t) = tasks.get_mut(task_id) {
            t.status = TaskStatus::Failed;
            t.error = Some(error);
        }
        Self::prune_finished_tasks(&mut tasks);
    }

    pub async fn set_cancelled(&self, task_id: &str) {
        let mut tasks = self.tasks.lock().await;
        if let Some(t) = tasks.get_mut(task_id) {
            if matches!(t.status, TaskStatus::Pending | TaskStatus::Running) {
                t.cancel_flag.store(true, Ordering::SeqCst);
                t.status = TaskStatus::Cancelled;
            }
        }
        Self::prune_finished_tasks(&mut tasks);
    }

    pub async fn get(&self, task_id: &str) -> Option<TaskEntry> {
        self.tasks.lock().await.get(task_id).cloned()
    }

    async fn list_summaries(&self) -> Vec<TaskSummaryResp> {
        let mut list: Vec<TaskSummaryResp> = {
            let tasks = self.tasks.lock().await;
            tasks
                .values()
                .map(|t| TaskSummaryResp {
                    task_id: t.task_id.clone(),
                    status: t.status.to_string(),
                    team: t.team.clone(),
                    target: t.target,
                    created_at: t.created_at.clone(),
                })
                .collect()
        };
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

    pub async fn get_progress(&self, task_id: &str) -> Option<Arc<crate::workflow::TaskProgress>> {
        self.tasks
            .lock()
            .await
            .get(task_id)
            .map(|t| t.progress.clone())
    }

    pub async fn get_progress_snapshot(
        &self,
        task_id: &str,
    ) -> Option<(
        String,
        TaskStatus,
        usize,
        Arc<crate::workflow::TaskProgress>,
    )> {
        self.tasks.lock().await.get(task_id).map(|t| {
            (
                t.task_id.clone(),
                t.status.clone(),
                t.target,
                t.progress.clone(),
            )
        })
    }

    fn prune_finished_tasks(tasks: &mut HashMap<String, TaskEntry>) {
        if tasks.len() <= TASK_PRUNE_THRESHOLD {
            return;
        }

        let mut finished: Vec<(String, String)> = tasks
            .iter()
            .filter(|(_, task)| {
                matches!(
                    task.status,
                    TaskStatus::Completed | TaskStatus::Failed | TaskStatus::Cancelled
                )
            })
            .map(|(task_id, task)| (task_id.clone(), task.created_at.clone()))
            .collect();

        if finished.len() <= TASK_FINISHED_KEEP {
            return;
        }

        finished.sort_by(|a, b| b.1.cmp(&a.1));
        for (task_id, _) in finished.into_iter().skip(TASK_FINISHED_KEEP) {
            tasks.remove(&task_id);
        }
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
    pub redis_cache: Option<Arc<RedisCache>>,
    team_manage_health_locks: Arc<Mutex<HashMap<String, Instant>>>,
    /// 上次执行 sync_owner_registry 的时间，用于节流
    last_owner_registry_sync: Arc<Mutex<Option<Instant>>>,
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
    pub free_mode: Option<bool>,
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
    proxy_enabled: bool,
    proxy_refresh_urls: std::collections::HashMap<String, String>,
    email_domains: Vec<String>,
    chatgpt_mail_domains: Vec<String>,
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
    cleanup_timing: String,
}

#[derive(Serialize)]
struct ModeDefaultsResp {
    target_count: usize,
    register_workers: usize,
    rt_workers: usize,
    rt_retries: usize,
}

#[derive(Serialize)]
struct DefaultsResp {
    target_count: usize,
    register_workers: usize,
    rt_workers: usize,
    rt_retries: usize,
    team: ModeDefaultsResp,
    free: ModeDefaultsResp,
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
    chatgpt_mail_api_key: String,
    mail_max_concurrency: usize,
    register_log_mode: RegisterLogMode,
    register_perf_mode: RegisterPerfMode,
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
    free_group_ids: Option<Vec<i64>>,
    free_priority: Option<usize>,
    free_concurrency: Option<usize>,
    #[serde(default)]
    extra: S2aExtraConfig,
    bind_proxy: Option<bool>,
}

#[derive(Deserialize)]
struct UpdateDefaultsRequest {
    mode: Option<String>,
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
    chatgpt_mail_api_key: Option<String>,
    mail_max_concurrency: Option<usize>,
    register_log_mode: Option<RegisterLogMode>,
    register_perf_mode: Option<RegisterPerfMode>,
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
    cleanup_timing: Option<String>,
}

fn error_json(status: StatusCode, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn shared_http_client_10s() -> &'static rquest::Client {
    static CLIENT: OnceLock<rquest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        rquest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .connect_timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| rquest::Client::new())
    })
}

fn shared_http_client_8s() -> &'static rquest::Client {
    static CLIENT: OnceLock<rquest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        rquest::Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .connect_timeout(std::time::Duration::from_secs(4))
            .build()
            .unwrap_or_else(|_| rquest::Client::new())
    })
}

async fn run_db_blocking<T, F>(job: F) -> Result<T, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    tokio::task::spawn_blocking(job)
        .await
        .map_err(|e| format!("数据库后台任务失败: {e}"))?
}

// ─── Handlers ────────────────────────────────────────────────────────────────

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
    let common_target = cfg.defaults.target_count.unwrap_or(1);
    let common_reg = cfg.defaults.register_workers.unwrap_or(15);
    let common_rt = cfg.defaults.rt_workers.unwrap_or(10);
    let common_retry = cfg.defaults.rt_retries.unwrap_or(4);
    let team_defaults = ModeDefaultsResp {
        target_count: cfg.defaults.team_target_count.unwrap_or(common_target),
        register_workers: cfg.defaults.team_register_workers.unwrap_or(common_reg),
        rt_workers: cfg.defaults.team_rt_workers.unwrap_or(common_rt),
        rt_retries: cfg.defaults.team_rt_retries.unwrap_or(common_retry),
    };
    let free_defaults = ModeDefaultsResp {
        target_count: cfg.defaults.free_target_count.unwrap_or(common_target),
        register_workers: cfg.defaults.free_register_workers.unwrap_or(common_reg),
        rt_workers: cfg.defaults.free_rt_workers.unwrap_or(common_rt),
        rt_retries: cfg.defaults.free_rt_retries.unwrap_or(common_retry),
    };
    Json(FullConfigResponse {
        teams,
        defaults: DefaultsResp {
            target_count: common_target,
            register_workers: common_reg,
            rt_workers: common_rt,
            rt_retries: common_retry,
            team: team_defaults,
            free: free_defaults,
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
            chatgpt_mail_api_key: reg.chatgpt_mail_api_key.clone().unwrap_or_default(),
            mail_max_concurrency: reg.mail_max_concurrency.unwrap_or(50),
            register_log_mode: reg.register_log_mode.unwrap_or_default(),
            register_perf_mode: reg.register_perf_mode.unwrap_or_default(),
        },
        proxy_pool: cfg.proxy_pool.clone(),
        proxy_enabled: cfg.proxy_enabled.unwrap_or(true),
        proxy_refresh_urls: cfg.proxy_refresh_urls.clone(),
        email_domains: cfg.email_domains.clone(),
        chatgpt_mail_domains: cfg.chatgpt_mail_domains.clone(),
        d1_cleanup: D1CleanupResp {
            enabled: cfg.d1_cleanup.enabled.unwrap_or(false),
            account_id: cfg.d1_cleanup.account_id.clone().unwrap_or_default(),
            api_key: cfg.d1_cleanup.api_key.clone().unwrap_or_default(),
            databases: cfg.d1_cleanup.databases.clone().unwrap_or_default(),
            keep_percent: cfg.d1_cleanup.keep_percent.unwrap_or(0.1),
            batch_size: cfg.d1_cleanup.batch_size.unwrap_or(5000),
            cleanup_timing: cfg.d1_cleanup.cleanup_timing.map(|t| match t {
                crate::config::D1CleanupTiming::BeforeTask => "before_task".to_string(),
                crate::config::D1CleanupTiming::AfterTask => "after_task".to_string(),
            }).unwrap_or_else(|| "after_task".to_string()),
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
        free_group_ids: req.free_group_ids.unwrap_or_default(),
        free_priority: req.free_priority,
        free_concurrency: req.free_concurrency,
        extra: req.extra,
        bind_proxy: req.bind_proxy,
    });
    auto_save(&cfg, &state.config_path);
    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("号池 {} 已添加", req.name),
        }),
    ))
}

#[derive(Deserialize)]
struct UpdateS2aRequest {
    api_base: Option<String>,
    admin_key: Option<String>,
    concurrency: Option<usize>,
    priority: Option<usize>,
    group_ids: Option<Vec<i64>>,
    free_group_ids: Option<Vec<i64>>,
    free_priority: Option<Option<usize>>,
    free_concurrency: Option<Option<usize>>,
    extra: Option<S2aExtraConfig>,
    bind_proxy: Option<bool>,
}

async fn update_s2a_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateS2aRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    let team = cfg
        .s2a
        .iter_mut()
        .find(|t| t.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("未找到号池: {name}")))?;

    if let Some(v) = req.api_base {
        team.api_base = v;
    }
    if let Some(v) = req.admin_key {
        team.admin_key = v;
    }
    if let Some(v) = req.concurrency {
        team.concurrency = v;
    }
    if let Some(v) = req.priority {
        team.priority = v;
    }
    if let Some(v) = req.group_ids {
        team.group_ids = v;
    }
    if let Some(v) = req.free_group_ids {
        team.free_group_ids = v;
    }
    if let Some(v) = req.free_priority {
        team.free_priority = v;
    }
    if let Some(v) = req.free_concurrency {
        team.free_concurrency = v;
    }
    if let Some(v) = req.extra {
        team.extra = v;
    }
    if let Some(v) = req.bind_proxy {
        team.bind_proxy = Some(v);
    }
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("号池 {name} 已更新"),
    }))
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

async fn test_gptmail_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let key = cfg
        .register
        .chatgpt_mail_api_key
        .clone()
        .unwrap_or_default();
    drop(cfg);

    if key.is_empty() {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            "未配置 GPTMail API Key",
        ));
    }

    let client = shared_http_client_10s();

    let resp = client
        .get("https://mail.chatgpt.org.uk/api/stats")
        .header("X-API-Key", &key)
        .header("Accept", "application/json")
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

    if json.get("success").and_then(|v| v.as_bool()) != Some(true) {
        let err = json
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        return Err(error_json(
            StatusCode::BAD_GATEWAY,
            &format!("API 错误: {err}"),
        ));
    }

    Ok(Json(json))
}

#[derive(Serialize)]
struct S2aStatsResponse {
    active: usize,
    rate_limited: usize,
    available: usize,
    free_active: usize,
    free_rate_limited: usize,
    free_available: usize,
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

    let base = S2aHttpService::normalized_api_base(&team.api_base);

    // 构建 group 参数
    let group_param = team
        .group_ids
        .first()
        .map(|g| format!("&group={g}"))
        .unwrap_or_default();

    // 构建 free group 参数
    let free_group_param = team
        .free_group_ids
        .first()
        .map(|g| format!("&group={g}"))
        .unwrap_or_default();
    let has_free = !team.free_group_ids.is_empty();

    // 并发获取 active 和 rate_limited 数量
    let active_url = format!(
        "{base}/admin/accounts?page=1&page_size=1&status=active{group_param}&timezone=Asia%2FShanghai"
    );
    let rl_url = format!(
        "{base}/admin/accounts?page=1&page_size=1&status=rate_limited{group_param}&timezone=Asia%2FShanghai"
    );

    // free 分组的 URL（仅在配置了 free_group_ids 时使用）
    let free_active_url = format!(
        "{base}/admin/accounts?page=1&page_size=1&status=active{free_group_param}&timezone=Asia%2FShanghai"
    );
    let free_rl_url = format!(
        "{base}/admin/accounts?page=1&page_size=1&status=rate_limited{free_group_param}&timezone=Asia%2FShanghai"
    );

    // 并发获取所有统计（包括 free 分组）
    let client = shared_http_client_8s();

    let (active_res, rl_res, free_active_res, free_rl_res) = tokio::join!(
        fetch_s2a_total(&client, &active_url, &team.admin_key),
        fetch_s2a_total(&client, &rl_url, &team.admin_key),
        async {
            if has_free {
                fetch_s2a_total(&client, &free_active_url, &team.admin_key).await
            } else {
                Ok(0)
            }
        },
        async {
            if has_free {
                fetch_s2a_total(&client, &free_rl_url, &team.admin_key).await
            } else {
                Ok(0)
            }
        },
    );

    // 容错：单个请求失败不影响整体，返回 0
    let active = active_res.unwrap_or(0);
    let rate_limited = rl_res.unwrap_or(0);
    let free_active = free_active_res.unwrap_or(0);
    let free_rate_limited = free_rl_res.unwrap_or(0);

    Ok(Json(S2aStatsResponse {
        active,
        rate_limited,
        available: active.saturating_sub(rate_limited),
        free_active,
        free_rate_limited,
        free_available: free_active.saturating_sub(free_rate_limited),
    }))
}

async fn fetch_s2a_total(
    client: &rquest::Client,
    url: &str,
    admin_key: &str,
) -> anyhow::Result<usize> {
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

    let client = shared_http_client_10s();

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
    match req.mode.as_deref() {
        Some("team") => {
            if let Some(v) = req.target_count {
                cfg.defaults.team_target_count = Some(v);
            }
            if let Some(v) = req.register_workers {
                cfg.defaults.team_register_workers = Some(v);
            }
            if let Some(v) = req.rt_workers {
                cfg.defaults.team_rt_workers = Some(v);
            }
            if let Some(v) = req.rt_retries {
                cfg.defaults.team_rt_retries = Some(v);
            }
        }
        Some("free") => {
            if let Some(v) = req.target_count {
                cfg.defaults.free_target_count = Some(v);
            }
            if let Some(v) = req.register_workers {
                cfg.defaults.free_register_workers = Some(v);
            }
            if let Some(v) = req.rt_workers {
                cfg.defaults.free_rt_workers = Some(v);
            }
            if let Some(v) = req.rt_retries {
                cfg.defaults.free_rt_retries = Some(v);
            }
        }
        _ => {
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
        }
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
    if let Some(v) = req.chatgpt_mail_api_key {
        cfg.register.chatgpt_mail_api_key = Some(v);
    }
    if let Some(v) = req.mail_max_concurrency {
        cfg.register.mail_max_concurrency = Some(v);
    }
    if let Some(v) = req.register_log_mode {
        cfg.register.register_log_mode = Some(v);
    }
    if let Some(v) = req.register_perf_mode {
        cfg.register.register_perf_mode = Some(v);
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

async fn add_gptmail_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    if domain.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "域名不能为空"));
    }
    let mut cfg = state.config.write().await;
    if cfg.chatgpt_mail_domains.contains(&domain) {
        return Err(error_json(
            StatusCode::CONFLICT,
            &format!("域名已存在: {domain}"),
        ));
    }
    cfg.chatgpt_mail_domains.push(domain.clone());
    auto_save(&cfg, &state.config_path);
    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("GPTMail 域名 {domain} 已添加"),
        }),
    ))
}

async fn delete_gptmail_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    let mut cfg = state.config.write().await;
    let before = cfg.chatgpt_mail_domains.len();
    cfg.chatgpt_mail_domains.retain(|d| d != &domain);
    if cfg.chatgpt_mail_domains.len() == before {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("未找到域名: {domain}"),
        ));
    }
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("GPTMail 域名 {domain} 已删除"),
    }))
}

#[derive(Deserialize)]
struct ProxyRequest {
    proxy: String,
}

#[derive(Deserialize)]
struct ProxyTestRequest {
    proxy_url: String,
}

async fn add_proxy_handler(
    State(state): State<AppState>,
    Json(req): Json<ProxyRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let proxy = req.proxy.trim().to_string();
    if proxy.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "代理地址不能为空"));
    }
    let normalized = normalize_proxy(&proxy);
    let mut cfg = state.config.write().await;
    if cfg.proxy_pool.iter().any(|p| normalize_proxy(p) == normalized) {
        return Err(error_json(
            StatusCode::CONFLICT,
            &format!("代理已存在: {proxy}"),
        ));
    }
    cfg.proxy_pool.push(normalized.clone());
    auto_save(&cfg, &state.config_path);
    let masked = crate::util::mask_proxy(&normalized);
    crate::log_broadcast::broadcast_log(&format!("[代理] 添加: {masked}"));
    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("代理 {proxy} 已添加"),
        }),
    ))
}

/// 批量添加代理（一次写锁 + 一次保存）
async fn batch_add_proxy_handler(
    State(state): State<AppState>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let proxies: Vec<String> = req
        .get("proxies")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    if proxies.is_empty() {
        return Json(serde_json::json!({ "added": 0, "skipped": 0, "message": "无有效代理" }));
    }

    let mut cfg = state.config.write().await;
    let mut added = 0usize;
    let mut skipped = 0usize;
    for proxy in &proxies {
        let normalized = normalize_proxy(proxy);
        if cfg.proxy_pool.iter().any(|p| normalize_proxy(p) == normalized) {
            skipped += 1;
        } else {
            cfg.proxy_pool.push(normalized);
            added += 1;
        }
    }
    // 只保存一次
    if added > 0 {
        auto_save(&cfg, &state.config_path);
    }
    drop(cfg);

    crate::log_broadcast::broadcast_log(&format!(
        "[代理] 批量导入: {} 个添加, {} 个跳过 (共 {} 个)",
        added, skipped, proxies.len()
    ));

    Json(serde_json::json!({
        "added": added,
        "skipped": skipped,
        "message": format!("添加 {} 个，跳过 {} 个重复", added, skipped),
    }))
}

async fn delete_proxy_handler(
    State(state): State<AppState>,
    Json(req): Json<ProxyRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let proxy = req.proxy.trim().to_string();
    let normalized = normalize_proxy(&proxy);
    let mut cfg = state.config.write().await;
    let before = cfg.proxy_pool.len();
    cfg.proxy_pool.retain(|p| normalize_proxy(p) != normalized);
    if cfg.proxy_pool.len() == before {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("未找到代理: {proxy}"),
        ));
    }
    auto_save(&cfg, &state.config_path);
    let masked = crate::util::mask_proxy(&proxy);
    crate::log_broadcast::broadcast_log(&format!("[代理] 删除: {masked}"));
    Ok(Json(MsgResponse {
        message: format!("代理 {proxy} 已删除"),
    }))
}

async fn set_proxy_enabled_handler(
    State(state): State<AppState>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let enabled = req
        .get("enabled")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);
    let mut cfg = state.config.write().await;
    cfg.proxy_enabled = Some(enabled);
    auto_save(&cfg, &state.config_path);
    let msg = format!(
        "代理池已{}",
        if enabled { "启用" } else { "禁用（直连模式）" }
    );
    crate::log_broadcast::broadcast_log(&format!("[代理] {msg}"));
    Json(MsgResponse { message: msg })
}

/// 设置/清除某个代理的 IP 刷新 URL
async fn set_proxy_refresh_url_handler(
    State(state): State<AppState>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let proxy_url = req
        .get("proxy_url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    let refresh_url = req
        .get("refresh_url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();
    if proxy_url.is_empty() {
        return Json(MsgResponse {
            message: "缺少 proxy_url 参数".to_string(),
        });
    }
    let mut cfg = state.config.write().await;
    if refresh_url.is_empty() {
        cfg.proxy_refresh_urls.remove(&proxy_url);
    } else {
        cfg.proxy_refresh_urls.insert(proxy_url.clone(), refresh_url);
    }
    auto_save(&cfg, &state.config_path);
    let msg = if cfg.proxy_refresh_urls.contains_key(&proxy_url) {
        format!("代理 {} 的刷新 URL 已保存", proxy_url)
    } else {
        format!("代理 {} 的刷新 URL 已清除", proxy_url)
    };
    crate::log_broadcast::broadcast_log(&format!("[代理] {msg}"));
    Json(MsgResponse { message: msg })
}

/// 手动触发住宅代理 IP 刷新（指定代理或刷新所有已配置的）
async fn refresh_proxy_ip_handler(
    State(state): State<AppState>,
    Json(req): Json<serde_json::Value>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let proxy_url = req
        .get("proxy_url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .trim()
        .to_string();

    let refresh_urls: Vec<(String, String)> = {
        let cfg = state.config.read().await;
        if proxy_url.is_empty() {
            // 刷新所有配置了刷新 URL 的代理
            cfg.proxy_refresh_urls.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
        } else {
            match cfg.proxy_refresh_urls.get(&proxy_url) {
                Some(url) => vec![(proxy_url.clone(), url.clone())],
                None => {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("代理 {proxy_url} 未配置刷新 URL"),
                        }),
                    ));
                }
            }
        }
    };

    if refresh_urls.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "没有代理配置了刷新 URL".to_string(),
            }),
        ));
    }

    let client = rquest::Client::builder()
        .no_proxy()
        .timeout(std::time::Duration::from_secs(15))
        .build()
        .unwrap_or_else(|_| rquest::Client::new());

    let mut results = Vec::new();
    for (proxy, refresh_url) in &refresh_urls {
        crate::log_broadcast::broadcast_log(&format!("[代理] 刷新 IP: {proxy} → {refresh_url}"));
        match client.get(refresh_url).send().await {
            Ok(resp) => {
                let status = resp.status();
                let body = resp.text().await.unwrap_or_default();
                let success = status.is_success();
                let msg = if success { body.clone() } else { format!("HTTP {}: {body}", status.as_u16()) };
                crate::log_broadcast::broadcast_log(&format!(
                    "[代理] {} IP 刷新{}: {}",
                    proxy,
                    if success { "成功" } else { "失败" },
                    msg
                ));
                results.push(serde_json::json!({ "proxy": proxy, "success": success, "message": msg }));
            }
            Err(e) => {
                let msg = format!("请求失败: {e}");
                crate::log_broadcast::broadcast_log(&format!("[代理] {proxy} IP 刷新失败: {msg}"));
                results.push(serde_json::json!({ "proxy": proxy, "success": false, "message": msg }));
            }
        }
    }

    let all_ok = results.iter().all(|r| r["success"].as_bool().unwrap_or(false));
    let summary = if results.len() == 1 {
        results[0]["message"].as_str().unwrap_or("").to_string()
    } else {
        let ok_count = results.iter().filter(|r| r["success"].as_bool().unwrap_or(false)).count();
        format!("{}/{} 刷新成功", ok_count, results.len())
    };

    Ok(Json(serde_json::json!({
        "success": all_ok,
        "message": summary,
        "results": results,
    })))
}

async fn proxy_health_check_handler(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let (proxies, timeout_sec) = {
        let cfg = state.config.read().await;
        (cfg.proxy_pool.clone(), cfg.proxy_check_timeout_sec.unwrap_or(5))
    };
    crate::log_broadcast::broadcast_log(&format!(
        "[代理] 开始健康检测 {} 个代理",
        proxies.len()
    ));
    let results = health_check_detailed(&proxies, timeout_sec).await;
    let ok_count = results.iter().filter(|r| r.ok).count();
    crate::log_broadcast::broadcast_log(&format!(
        "[代理] 健康检测完成: {}/{} 可用",
        ok_count,
        results.len()
    ));
    Json(results)
}

/// 批量测试代理连通性（并发获取延迟、IP、地理信息）
async fn proxy_batch_test_handler(
    State(state): State<AppState>,
    Json(req): Json<serde_json::Value>,
) -> impl IntoResponse {
    let proxy_urls: Vec<String> = req
        .get("proxy_urls")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.trim().to_string()))
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

    let concurrency = req
        .get("concurrency")
        .and_then(|v| v.as_u64())
        .unwrap_or(5) as usize;

    if proxy_urls.is_empty() {
        return Json(serde_json::json!({ "results": [] }));
    }

    let timeout_sec = {
        let cfg = state.config.read().await;
        cfg.proxy_check_timeout_sec.unwrap_or(5).max(10)
    };

    crate::log_broadcast::broadcast_log(&format!(
        "[代理] 开始批量测试 {} 个代理 (并发={})",
        proxy_urls.len(),
        concurrency
    ));

    let semaphore = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency.clamp(1, 20)));
    let mut join_set = tokio::task::JoinSet::new();

    for url in proxy_urls {
        let sem = semaphore.clone();
        join_set.spawn(async move {
            let _permit = sem.acquire().await;
            let result = test_single_proxy(&url, timeout_sec).await;
            (url, result)
        });
    }

    let mut results = Vec::new();
    while let Some(join_result) = join_set.join_next().await {
        if let Ok((url, result)) = join_result {
            results.push(serde_json::json!({
                "proxy_url": url,
                "success": result.success,
                "message": result.message,
                "latency_ms": result.latency_ms,
                "ip_address": result.ip_address,
                "city": result.city,
                "region": result.region,
                "country": result.country,
                "country_code": result.country_code,
            }));
        }
    }

    let ok_count = results.iter().filter(|r| r.get("success").and_then(|v| v.as_bool()).unwrap_or(false)).count();
    crate::log_broadcast::broadcast_log(&format!(
        "[代理] 批量测试完成: {}/{} 可用",
        ok_count,
        results.len()
    ));

    Json(serde_json::json!({ "results": results }))
}

/// 测试单个代理连通性
async fn proxy_test_handler(
    State(state): State<AppState>,
    Json(req): Json<ProxyTestRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let proxy_url = req.proxy_url.trim().to_string();
    if proxy_url.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "代理地址不能为空"));
    }
    let timeout_sec = {
        let cfg = state.config.read().await;
        cfg.proxy_check_timeout_sec.unwrap_or(5)
    };
    let result = test_single_proxy(&proxy_url, timeout_sec.max(10)).await;
    Ok(Json(result))
}

/// 深度质量检测单个代理
async fn proxy_quality_check_handler(
    State(state): State<AppState>,
    Json(req): Json<ProxyTestRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let proxy_url = req.proxy_url.trim().to_string();
    if proxy_url.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "代理地址不能为空"));
    }
    let timeout_sec = {
        let cfg = state.config.read().await;
        cfg.proxy_check_timeout_sec.unwrap_or(5)
    };
    let result = check_proxy_quality(&proxy_url, timeout_sec.max(15)).await;
    Ok(Json(result))
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
    if let Some(v) = req.cleanup_timing {
        cfg.d1_cleanup.cleanup_timing = Some(match v.as_str() {
            "before_task" => crate::config::D1CleanupTiming::BeforeTask,
            _ => crate::config::D1CleanupTiming::AfterTask,
        });
    }
    auto_save(&cfg, &state.config_path);
    Json(MsgResponse {
        message: "D1 清理配置已更新".to_string(),
    })
}

async fn trigger_d1_cleanup_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let d1_cfg = {
        let cfg = state.config.read().await;
        cfg.d1_cleanup.clone()
    };
    match crate::d1_cleanup::run_cleanup(&d1_cfg).await {
        Ok(_) => Ok(Json(MsgResponse {
            message: "D1 清理完成".to_string(),
        })),
        Err(e) => Err(error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("D1 清理失败: {e}"),
        )),
    }
}

/// 自动持久化配置到文件（静默，不阻塞请求）
fn auto_save(config: &AppConfig, path: &std::path::Path) {
    let config_snapshot = config.clone();
    let path_buf = path.to_path_buf();
    let _ = tokio::task::spawn_blocking(move || match toml::to_string_pretty(&config_snapshot) {
        Ok(toml_str) => {
            if let Err(e) = std::fs::write(&path_buf, &toml_str) {
                println!("[自动保存] 写入失败: {e}");
            }
        }
        Err(e) => println!("[自动保存] 序列化失败: {e}"),
    });
}

async fn save_config_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg_snapshot = state.config.read().await.clone();
    let save_path = state.config_path.clone();
    let display_path = save_path.clone();

    tokio::task::spawn_blocking(move || -> Result<(), String> {
        let toml_str =
            toml::to_string_pretty(&cfg_snapshot).map_err(|e| format!("序列化失败: {e}"))?;
        std::fs::write(&save_path, &toml_str).map_err(|e| format!("写入文件失败: {e}"))?;
        Ok(())
    })
    .await
    .map_err(|e| {
        error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("保存任务执行失败: {e}"),
        )
    })?
    .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &e))?;

    Ok(Json(MsgResponse {
        message: format!("配置已保存到 {}", display_path.display()),
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

    let free_mode = req.free_mode.unwrap_or(false);
    let default_target = if free_mode {
        cfg.defaults
            .free_target_count
            .or(cfg.defaults.target_count)
            .unwrap_or(1)
    } else {
        cfg.defaults
            .team_target_count
            .or(cfg.defaults.target_count)
            .unwrap_or(1)
    };
    let default_reg_workers = if free_mode {
        cfg.defaults
            .free_register_workers
            .or(cfg.defaults.register_workers)
            .unwrap_or(15)
    } else {
        cfg.defaults
            .team_register_workers
            .or(cfg.defaults.register_workers)
            .unwrap_or(15)
    };
    let default_rt_workers = if free_mode {
        cfg.defaults
            .free_rt_workers
            .or(cfg.defaults.rt_workers)
            .unwrap_or(10)
    } else {
        cfg.defaults
            .team_rt_workers
            .or(cfg.defaults.rt_workers)
            .unwrap_or(10)
    };
    let default_rt_retries = if free_mode {
        cfg.defaults
            .free_rt_retries
            .or(cfg.defaults.rt_retries)
            .unwrap_or(4)
    } else {
        cfg.defaults
            .team_rt_retries
            .or(cfg.defaults.rt_retries)
            .unwrap_or(4)
    };
    let target = req.target.unwrap_or(default_target).max(1);
    let target = target.min(MAX_TARGET_COUNT);
    let register_workers = req
        .register_workers
        .unwrap_or(default_reg_workers)
        .clamp(1, MAX_REGISTER_WORKERS);
    let rt_workers = req
        .rt_workers
        .unwrap_or(default_rt_workers)
        .clamp(1, MAX_RT_WORKERS);
    let rt_retries = req
        .rt_retries
        .unwrap_or(default_rt_retries)
        .clamp(1, MAX_RT_RETRIES);
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
            free_mode,
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
    let tasks = state.task_manager.list_summaries().await;
    Json(TaskListResponse { tasks })
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

#[derive(Serialize)]
struct TaskProgressResponse {
    task_id: String,
    status: String,
    stage: String,
    reg_ok: usize,
    reg_failed: usize,
    rt_ok: usize,
    rt_failed: usize,
    s2a_ok: usize,
    s2a_failed: usize,
    target: usize,
}

async fn task_progress_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let (task_id, status, target, progress) = state
        .task_manager
        .get_progress_snapshot(&task_id)
        .await
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("任务不存在: {task_id}")))?;

    Ok(Json(TaskProgressResponse {
        task_id,
        status: status.to_string(),
        stage: progress.get_stage(),
        reg_ok: progress.reg_ok.load(Ordering::Relaxed),
        reg_failed: progress.reg_failed.load(Ordering::Relaxed),
        rt_ok: progress.rt_ok.load(Ordering::Relaxed),
        rt_failed: progress.rt_failed.load(Ordering::Relaxed),
        s2a_ok: progress.s2a_ok.load(Ordering::Relaxed),
        s2a_failed: progress.s2a_failed.load(Ordering::Relaxed),
        target,
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
    free_mode: bool,
    proxy_file: Option<PathBuf>,
) {
    task_manager.set_running(&task_id).await;

    {
        let rt = cfg.register_runtime();
        crate::log_broadcast::broadcast_log(&format!(
            "[配置] 邮件并发: {} | OTP重试: {} | OTP间隔: {}ms | 邮件超时: {}s | 请求超时: {}s",
            rt.mail_max_concurrency,
            rt.otp_max_retries,
            rt.otp_interval_ms,
            rt.mail_request_timeout_sec,
            rt.request_timeout_sec,
        ));
    }

    // 写入运行记录
    let run_id = task_id.clone();
    let new_run = crate::db::NewRun {
        id: run_id.clone(),
        schedule_name: None,
        trigger_type: "manual_task".to_string(),
        target_count: target,
        started_at: crate::util::beijing_now().to_rfc3339(),
    };
    if let Err(e) = run_history_db.enqueue_insert_run(new_run) {
        println!("[任务] 写入运行记录失败: {e}");
    }

    let cancel_flag = match task_manager.get_cancel_flag(&task_id).await {
        Some(flag) => flag,
        None => {
            task_manager
                .set_failed(&task_id, "内部错误: 找不到任务".to_string())
                .await;
            let _ =
                run_history_db.enqueue_fail_run(run_id.clone(), "内部错误: 找不到任务".to_string());
            return;
        }
    };

    if cancel_flag.load(Ordering::Relaxed) {
        let _ = run_history_db.enqueue_fail_run(run_id.clone(), "任务在启动前被取消".to_string());
        return;
    }

    let proxy_pool = match build_proxy_pool(&cfg, proxy_file.as_deref()).await {
        Ok(pool) => pool,
        Err(e) => {
            let msg = format!("代理初始化失败: {e}");
            task_manager.set_failed(&task_id, msg.clone()).await;
            let _ = run_history_db.enqueue_fail_run(run_id.clone(), msg);
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
        let gpt_domains = cfg.chatgpt_mail_domains.clone();
        let mail_concurrency = register_runtime.mail_max_concurrency;
        let reg_email = Arc::new(email_service::EmailService::new_chatgpt_org_uk(
            api_key.clone(),
            gpt_domains.clone(),
            mail_concurrency,
        ));
        let rt_email = Arc::new(email_service::EmailService::new_chatgpt_org_uk(
            api_key,
            gpt_domains,
            mail_concurrency,
        ));
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
        let mail_concurrency = register_runtime.mail_max_concurrency;
        let reg_email = Arc::new(email_service::EmailService::new_http(
            email_cfg.clone(),
            mail_concurrency,
        ));
        let rt_email = Arc::new(email_service::EmailService::new_http(
            email_cfg,
            mail_concurrency,
        ));
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
        // 兼容手动任务历史行为：补注册最多 5 轮；定时计划走 schedule 自身 rt_retries
        target_fill_max_rounds: 5,
        push_s2a,
        use_chatgpt_mail,
        free_mode,
        register_log_mode: register_runtime.register_log_mode,
        register_perf_mode: register_runtime.register_perf_mode,
    };

    let progress = task_manager.get_progress(&task_id).await;
    let started = std::time::Instant::now();
    let runner = WorkflowRunner::new(register_service, codex_service, s2a_service, proxy_pool);
    match runner
        .run_one_team(&cfg, &team, &options, cancel_flag.clone(), progress)
        .await
    {
        Ok(report) => {
            let completion = crate::db::RunCompletion {
                registered_ok: report.registered_ok,
                registered_failed: report.registered_failed,
                rt_ok: report.rt_ok,
                rt_failed: report.rt_failed,
                total_s2a_ok: report.s2a_ok,
                total_s2a_failed: report.s2a_failed,
                elapsed_secs: started.elapsed().as_secs_f64(),
                finished_at: crate::util::beijing_now().to_rfc3339(),
            };
            let _ = run_history_db.enqueue_complete_run(run_id.clone(), completion);
            task_manager.set_completed(&task_id, report).await;
        }
        Err(e) => {
            let msg = format!("{e:#}");
            crate::log_broadcast::broadcast_log(&format!(
                "[SIG-END][ERR] mode={} target_rt={} team={} reason={msg}",
                if free_mode { "free" } else { "team" },
                target,
                team.name
            ));
            let _ = run_history_db.enqueue_fail_run(run_id.clone(), msg.clone());
            task_manager.set_failed(&task_id, msg).await;
        }
    }
}

async fn build_proxy_pool(
    cfg: &AppConfig,
    proxy_file: Option<&std::path::Path>,
) -> anyhow::Result<Arc<ProxyPool>> {
    if !cfg.proxy_enabled.unwrap_or(true) {
        println!("[server] 代理池已禁用，使用直连模式");
        return Ok(Arc::new(ProxyPool::with_refresh_urls(vec![], cfg.proxy_refresh_urls.clone())));
    }
    let proxy_list = resolve_proxies(proxy_file, &cfg.proxy_pool)?;
    let check_timeout = cfg.proxy_check_timeout_sec.unwrap_or(5);
    let healthy_proxies = health_check(&proxy_list, check_timeout, proxy_file).await?;
    println!(
        "[server] 代理池初始化完成: {} 个可用代理",
        healthy_proxies.len()
    );
    Ok(Arc::new(ProxyPool::with_refresh_urls(healthy_proxies, cfg.proxy_refresh_urls.clone())))
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
    #[serde(default = "default_schedule_priority_serde")]
    priority: u32,
    register_workers: Option<usize>,
    rt_workers: Option<usize>,
    rt_retries: Option<usize>,
    #[serde(default = "default_true_serde")]
    push_s2a: bool,
    #[serde(default)]
    use_chatgpt_mail: bool,
    #[serde(default)]
    free_mode: bool,
    #[serde(default)]
    register_log_mode: Option<RegisterLogMode>,
    #[serde(default)]
    register_perf_mode: Option<RegisterPerfMode>,
    distribution: Vec<crate::config::DistributionEntry>,
}

fn default_true_serde() -> bool {
    true
}

fn default_batch_interval_serde() -> u64 {
    30
}

fn default_schedule_priority_serde() -> u32 {
    100
}

#[derive(Deserialize)]
struct UpdateScheduleRequest {
    name: Option<String>,
    start_time: Option<String>,
    end_time: Option<String>,
    enabled: Option<bool>,
    priority: Option<u32>,
    target_count: Option<usize>,
    batch_interval_mins: Option<u64>,
    register_workers: Option<usize>,
    rt_workers: Option<usize>,
    rt_retries: Option<usize>,
    push_s2a: Option<bool>,
    use_chatgpt_mail: Option<bool>,
    free_mode: Option<bool>,
    register_log_mode: Option<Option<RegisterLogMode>>,
    register_perf_mode: Option<Option<RegisterPerfMode>>,
    distribution: Option<Vec<crate::config::DistributionEntry>>,
}

#[derive(Deserialize)]
struct RunsQuery {
    page: Option<usize>,
    per_page: Option<usize>,
    schedule: Option<String>,
    trigger: Option<String>,
}

#[derive(Serialize)]
struct ScheduleWithStatus {
    #[serde(flatten)]
    config: crate::config::ScheduleConfig,
    running: bool,
    cooldown: bool,
    pending: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_info: Option<crate::scheduler::ScheduleRunInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pending_info: Option<crate::scheduler::SchedulePendingInfo>,
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
    let schedule_configs = {
        let cfg = state.config.read().await;
        cfg.schedule.clone()
    };
    let scheduler_snapshot = state.scheduler_state.snapshot().await;

    let mut schedules = Vec::with_capacity(schedule_configs.len());
    for config in schedule_configs {
        let run_info = scheduler_snapshot
            .active
            .get(&config.name)
            .cloned()
            .or_else(|| scheduler_snapshot.cooldown.get(&config.name).cloned());
        let pending_info = scheduler_snapshot.pending.get(&config.name).cloned();
        let running = scheduler_snapshot.current_running.as_deref() == Some(config.name.as_str());
        let cooldown = scheduler_snapshot.cooldown.contains_key(&config.name);
        let pending = pending_info.is_some();
        schedules.push(ScheduleWithStatus {
            running,
            cooldown,
            pending,
            config,
            run_info,
            pending_info,
        });
    }
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

    let register_workers = req
        .register_workers
        .map(|workers| workers.clamp(1, MAX_REGISTER_WORKERS));
    let rt_workers = req
        .rt_workers
        .map(|workers| workers.clamp(1, MAX_RT_WORKERS));
    let rt_retries = req
        .rt_retries
        .map(|retries| retries.clamp(1, MAX_RT_RETRIES));

    cfg.schedule.push(crate::config::ScheduleConfig {
        name: req.name.clone(),
        start_time: req.start_time,
        end_time: req.end_time,
        target_count: req.target_count.clamp(1, MAX_TARGET_COUNT),
        batch_interval_mins: req.batch_interval_mins,
        enabled: req.enabled,
        priority: req.priority,
        register_workers,
        rt_workers,
        rt_retries,
        push_s2a: req.push_s2a,
        use_chatgpt_mail: req.use_chatgpt_mail,
        free_mode: req.free_mode,
        register_log_mode: req.register_log_mode,
        register_perf_mode: req.register_perf_mode,
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
    let mut schedule_rename: Option<(String, String)> = None;
    // verify schedule exists
    cfg.schedule
        .iter()
        .find(|s| s.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("定时计划不存在: {name}")))?;

    if let Some(ref new_name) = req.name {
        let trimmed = new_name.trim().to_string();
        if trimmed.is_empty() {
            return Err(error_json(StatusCode::BAD_REQUEST, "名称不能为空"));
        }
        if trimmed != name && cfg.schedule.iter().any(|s| s.name == trimmed) {
            return Err(error_json(
                StatusCode::CONFLICT,
                &format!("定时计划已存在: {trimmed}"),
            ));
        }
        if trimmed != name {
            schedule_rename = Some((name.clone(), trimmed.clone()));
        }
        // re-borrow after the check
        cfg.schedule
            .iter_mut()
            .find(|s| s.name == name)
            .unwrap()
            .name = trimmed;
    }
    // re-borrow with (possibly new) name for remaining updates
    let actual_name = req.name.as_deref().map(|n| n.trim()).unwrap_or(&name);
    let sched = cfg
        .schedule
        .iter_mut()
        .find(|s| s.name == actual_name)
        .unwrap();

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
    if let Some(priority) = req.priority {
        sched.priority = priority;
    }
    if let Some(target_count) = req.target_count {
        sched.target_count = target_count.clamp(1, MAX_TARGET_COUNT);
    }
    if let Some(rw) = req.register_workers {
        sched.register_workers = Some(rw.clamp(1, MAX_REGISTER_WORKERS));
    }
    if let Some(rw) = req.rt_workers {
        sched.rt_workers = Some(rw.clamp(1, MAX_RT_WORKERS));
    }
    if let Some(rr) = req.rt_retries {
        sched.rt_retries = Some(rr.clamp(1, MAX_RT_RETRIES));
    }
    if let Some(ps) = req.push_s2a {
        sched.push_s2a = ps;
    }
    if let Some(cm) = req.use_chatgpt_mail {
        sched.use_chatgpt_mail = cm;
    }
    if let Some(fm) = req.free_mode {
        sched.free_mode = fm;
    }
    if let Some(log_mode) = req.register_log_mode {
        sched.register_log_mode = log_mode;
    }
    if let Some(perf_mode) = req.register_perf_mode {
        sched.register_perf_mode = perf_mode;
    }
    if let Some(dist) = req.distribution {
        let teams = cfg.effective_s2a_configs();
        if let Err(e) = crate::distribution::validate_distribution(&dist, &teams) {
            return Err(error_json(StatusCode::BAD_REQUEST, &e));
        }
        // re-borrow since we used cfg above
        let actual = req.name.as_deref().map(|n| n.trim()).unwrap_or(&name);
        cfg.schedule
            .iter_mut()
            .find(|s| s.name == actual)
            .unwrap()
            .distribution = dist;
    }
    auto_save(&cfg, &state.config_path);

    let display_name = req
        .name
        .as_deref()
        .map(|n| n.trim().to_string())
        .unwrap_or_else(|| name.clone());
    drop(cfg);
    if let Some((old_name, new_name)) = schedule_rename {
        if let Err(e) = state
            .run_history_db
            .enqueue_rename_schedule(old_name, new_name)
        {
            tracing::warn!("[调度] 同步重命名历史记录失败: {e}");
        }
    }
    Ok(Json(MsgResponse {
        message: format!("定时计划 {display_name} 已更新"),
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
    drop(cfg);
    let _ = state.scheduler_state.stop(&name).await;
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
    let enabled = sched.enabled;
    let cfg_snapshot = cfg.clone();
    drop(cfg);

    let status = if enabled { "已启用" } else { "已禁用" };

    // 禁用时，如果正在运行则停止
    if !enabled {
        state.scheduler_state.stop(&name).await;
    }
    auto_save(&cfg_snapshot, &state.config_path);

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
    let initial_schedule = cfg
        .schedule
        .iter()
        .find(|s| s.name == name)
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("定时计划不存在: {name}")))?
        .clone();
    drop(cfg);

    let (cancel_flag, batch_counter, next_batch_ts) = match state.scheduler_state.start(&name).await
    {
        Ok(handles) => handles,
        Err(crate::scheduler::ScheduleStartError::AlreadyRunning) => {
            return Err(error_json(
                StatusCode::CONFLICT,
                &format!("计划 {name} 已在运行中"),
            ));
        }
        Err(crate::scheduler::ScheduleStartError::Busy { running_name }) => {
            return Err(error_json(
                StatusCode::CONFLICT,
                &format!("当前有计划正在运行: {running_name}，请等待其结束后再手动启动 {name}"),
            ));
        }
    };

    let state_clone = state.clone();
    let db = state.run_history_db.clone();
    let schedule_name = name.clone();

    tokio::spawn(async move {
        // 手动触发的批次循环：只检查 cancel_flag，不检查时间窗口
        crate::log_broadcast::broadcast_log(&format!(
            "[手动触发] {} 批次循环开始 (每批 {} 个, 间隔 {} 分钟)",
            schedule_name, initial_schedule.target_count, initial_schedule.batch_interval_mins
        ));

        loop {
            if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                crate::log_broadcast::broadcast_log(&format!(
                    "[手动触发] {} 已停止",
                    schedule_name
                ));
                break;
            }

            let config_snapshot = state_clone.config.read().await.clone();
            let Some(schedule) = config_snapshot
                .schedule
                .iter()
                .find(|s| s.name == schedule_name)
                .cloned()
            else {
                crate::log_broadcast::broadcast_log(&format!(
                    "[手动触发] {} 已停止（计划已删除）",
                    schedule_name
                ));
                break;
            };
            if !schedule.enabled {
                crate::log_broadcast::broadcast_log(&format!(
                    "[手动触发] {} 已停止（计划已禁用）",
                    schedule_name
                ));
                break;
            }
            if !crate::scheduler::is_in_window(&schedule.start_time, &schedule.end_time) {
                crate::log_broadcast::broadcast_log(&format!(
                    "[手动触发] {} 时间窗口结束，自动停止",
                    schedule_name
                ));
                break;
            }

            let batch_num = batch_counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            next_batch_ts.store(0, std::sync::atomic::Ordering::Relaxed);
            crate::log_broadcast::broadcast_log(&format!(
                "[手动触发] {} 开始第 {} 批次",
                schedule_name, batch_num
            ));

            let runner = match build_workflow_runner(
                &config_snapshot,
                state_clone.proxy_file.as_deref(),
                schedule.use_chatgpt_mail,
            )
            .await
            {
                Ok(r) => r,
                Err(e) => {
                    crate::log_broadcast::broadcast_log(&format!(
                        "[手动触发] 构建 runner 失败 ({}): {e}",
                        schedule_name
                    ));
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
                    crate::log_broadcast::broadcast_log(&format!(
                        "[手动触发] {} 第 {} 批完成 | S2A: {} | 耗时: {:.1}s",
                        schedule_name, batch_num, report.total_s2a_ok, report.elapsed_secs
                    ));
                }
                Err(e) => {
                    crate::log_broadcast::broadcast_log(&format!(
                        "[手动触发] {} 第 {} 批失败: {e:#}",
                        schedule_name, batch_num
                    ));
                    if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                        break;
                    }
                }
            }

            // 批次间等待
            let wait_secs = schedule.batch_interval_mins.saturating_mul(60);
            let total_wait = std::time::Duration::from_secs(wait_secs);
            let check_interval = tokio::time::Duration::from_secs(5);
            let mut elapsed = std::time::Duration::ZERO;
            let mut should_continue = true;

            let next_ts = chrono::Utc::now().timestamp() as u64 + wait_secs;
            next_batch_ts.store(next_ts, std::sync::atomic::Ordering::Relaxed);

            while elapsed < total_wait {
                if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    should_continue = false;
                    break;
                }
                let Some(latest_schedule) =
                    crate::scheduler::load_schedule_config(&state_clone, &schedule_name).await
                else {
                    crate::log_broadcast::broadcast_log(&format!(
                        "[手动触发] {} 已停止（等待期间计划已删除）",
                        schedule_name
                    ));
                    should_continue = false;
                    break;
                };
                if !latest_schedule.enabled {
                    crate::log_broadcast::broadcast_log(&format!(
                        "[手动触发] {} 已停止（等待期间计划已禁用）",
                        schedule_name
                    ));
                    should_continue = false;
                    break;
                }
                if !crate::scheduler::is_in_window(
                    &latest_schedule.start_time,
                    &latest_schedule.end_time,
                ) {
                    crate::log_broadcast::broadcast_log(&format!(
                        "[手动触发] {} 等待期间时间窗口结束",
                        schedule_name
                    ));
                    should_continue = false;
                    break;
                }
                tokio::time::sleep(check_interval).await;
                elapsed += std::time::Duration::from_secs(5);
            }

            if cancel_flag.load(std::sync::atomic::Ordering::Relaxed) || !should_continue {
                break;
            }
        }

        state_clone.scheduler_state.remove(&schedule_name).await;
        crate::log_broadcast::broadcast_log(&format!(
            "[手动触发] {} 批次循环结束（共 {} 批）",
            schedule_name,
            batch_counter.load(std::sync::atomic::Ordering::Relaxed)
        ));
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(MsgResponse {
            message: format!("定时计划 {name} 已手动启动"),
        }),
    ))
}

/// 运行一次：只执行一个批次，完成即返回，不进入循环
async fn run_once_schedule_handler(
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
    let config_snapshot = cfg.clone();
    drop(cfg);

    let (cancel_flag, batch_counter, next_batch_ts) = match state.scheduler_state.start(&name).await
    {
        Ok(handles) => handles,
        Err(crate::scheduler::ScheduleStartError::AlreadyRunning) => {
            return Err(error_json(
                StatusCode::CONFLICT,
                &format!("计划 {name} 已在运行中"),
            ));
        }
        Err(crate::scheduler::ScheduleStartError::Busy { running_name }) => {
            return Err(error_json(
                StatusCode::CONFLICT,
                &format!("当前有计划正在运行: {running_name}，请等待其结束后再执行 {name}"),
            ));
        }
    };

    batch_counter.store(1, std::sync::atomic::Ordering::Relaxed);
    next_batch_ts.store(0, std::sync::atomic::Ordering::Relaxed);

    crate::log_broadcast::broadcast_log(&format!(
        "[运行一次] {} 开始执行单批次 (目标 {} 个)",
        name, schedule.target_count
    ));

    let runner = match build_workflow_runner(
        &config_snapshot,
        state.proxy_file.as_deref(),
        schedule.use_chatgpt_mail,
    )
    .await
    {
        Ok(runner) => runner,
        Err(e) => {
            state.scheduler_state.remove(&name).await;
            return Err(error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("构建 runner 失败: {e}"),
            ));
        }
    };

    let result = crate::distribution::run_distribution(
        &runner,
        &config_snapshot,
        &schedule,
        &state.run_history_db,
        "run-once",
        cancel_flag,
    )
    .await;

    state.scheduler_state.remove(&name).await;

    match result {
        Ok(report) => {
            crate::log_broadcast::broadcast_log(&format!(
                "[运行一次] {} 完成 | 注册: {}/{} | RT: {}/{} | S2A: {}/{} | 耗时: {:.1}s",
                name,
                report.registered_ok,
                report.registered_ok + report.registered_failed,
                report.rt_ok,
                report.rt_ok + report.rt_failed,
                report.total_s2a_ok,
                report.total_s2a_ok + report.total_s2a_failed,
                report.elapsed_secs
            ));
            Ok(Json(report))
        }
        Err(e) => {
            crate::log_broadcast::broadcast_log(&format!("[运行一次] {} 执行失败: {e:#}", name));
            Err(error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("执行失败: {e}"),
            ))
        }
    }
}

/// 停止一个运行中或等待中的计划
async fn stop_schedule_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    match state.scheduler_state.stop(&name).await {
        crate::scheduler::ScheduleStopOutcome::RunningCancelled => Ok(Json(MsgResponse {
            message: format!("定时计划 {name} 停止信号已发送"),
        })),
        crate::scheduler::ScheduleStopOutcome::PendingCancelled => Ok(Json(MsgResponse {
            message: format!("定时计划 {name} 的等待已取消"),
        })),
        crate::scheduler::ScheduleStopOutcome::CooldownCancelled => Ok(Json(MsgResponse {
            message: format!("定时计划 {name} 的后续批次已取消"),
        })),
        crate::scheduler::ScheduleStopOutcome::NotFound => Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("计划 {name} 未在运行、等待或准备中"),
        )),
    }
}

// ─── Run history handlers ───────────────────────────────────────────────────

async fn run_stats_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let stats = run_db_blocking({
        let db = state.run_history_db.clone();
        move || db.run_stats().map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| {
        error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("查询统计失败: {e}"),
        )
    })?;
    Ok(Json(stats))
}

async fn list_runs_handler(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<RunsQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let page = query.page.unwrap_or(1).max(1);
    let per_page = query.per_page.unwrap_or(20).clamp(1, 100);
    let schedule = query.schedule.as_deref();
    let trigger_filter = match query.trigger.as_deref() {
        Some("manual") => Some(crate::db::RunTriggerFilter::Manual),
        Some("scheduled") => Some(crate::db::RunTriggerFilter::Scheduled),
        _ => None,
    };

    let schedule = schedule.map(|s| s.to_string());
    let (runs, total) = run_db_blocking({
        let db = state.run_history_db.clone();
        move || {
            db.list_runs(page, per_page, schedule.as_deref(), trigger_filter)
                .map_err(|e| e.to_string())
        }
    })
    .await
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
    let detail = run_db_blocking({
        let db = state.run_history_db.clone();
        let run_id = run_id.clone();
        move || db.get_run(&run_id).map_err(|e| e.to_string())
    })
    .await
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
    use_chatgpt_mail: bool,
) -> anyhow::Result<WorkflowRunner> {
    let proxy_pool = build_proxy_pool(cfg, proxy_file).await?;
    let register_runtime = cfg.register_runtime();
    let codex_runtime = cfg.codex_runtime();

    crate::log_broadcast::broadcast_log(&format!(
        "[配置] 邮件并发: {} | OTP重试: {} | OTP间隔: {}ms | 邮件超时: {}s | 请求超时: {}s",
        register_runtime.mail_max_concurrency,
        register_runtime.otp_max_retries,
        register_runtime.otp_interval_ms,
        register_runtime.mail_request_timeout_sec,
        register_runtime.request_timeout_sec,
    ));

    let (register_service, codex_service): (
        Arc<dyn crate::services::RegisterService>,
        Arc<dyn crate::services::CodexService>,
    ) = if use_chatgpt_mail {
        let api_key = register_runtime.chatgpt_mail_api_key.clone();
        let gpt_domains = cfg.chatgpt_mail_domains.clone();
        let mail_concurrency = register_runtime.mail_max_concurrency;
        let reg_email = Arc::new(crate::email_service::EmailService::new_chatgpt_org_uk(
            api_key.clone(),
            gpt_domains.clone(),
            mail_concurrency,
        ));
        let rt_email = Arc::new(crate::email_service::EmailService::new_chatgpt_org_uk(
            api_key,
            gpt_domains,
            mail_concurrency,
        ));
        (
            Arc::new(LiveRegisterService::new(
                register_runtime.clone(),
                reg_email,
            )) as Arc<dyn crate::services::RegisterService>,
            Arc::new(LiveCodexService::new(codex_runtime.clone(), rt_email))
                as Arc<dyn crate::services::CodexService>,
        )
    } else {
        let email_cfg = crate::email_service::EmailServiceConfig {
            mail_api_base: register_runtime.mail_api_base.clone(),
            mail_api_path: register_runtime.mail_api_path.clone(),
            mail_api_token: register_runtime.mail_api_token.clone(),
            request_timeout_sec: register_runtime.mail_request_timeout_sec,
        };
        let mail_concurrency = register_runtime.mail_max_concurrency;
        let reg_email = Arc::new(crate::email_service::EmailService::new_http(
            email_cfg.clone(),
            mail_concurrency,
        ));
        let rt_email = Arc::new(crate::email_service::EmailService::new_http(
            email_cfg,
            mail_concurrency,
        ));
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
    let redis_cache = if let Some(redis_runtime) = cfg.redis_runtime() {
        match RedisCache::new(redis_runtime.clone()).await {
            Ok(cache) => {
                if let Err(error) = cache.ping().await {
                    tracing::warn!("Redis 已配置，但当前不可达；将自动回退 SQLite: {error}");
                } else {
                    tracing::info!("Redis 已启用: prefix={}", redis_runtime.key_prefix);
                }
                Some(Arc::new(cache))
            }
            Err(error) => {
                tracing::warn!("Redis 初始化失败；将回退 SQLite: {error}");
                None
            }
        }
    } else {
        None
    };

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
        redis_cache,
        team_manage_health_locks: Arc::new(Mutex::new(HashMap::new())),
        last_owner_registry_sync: Arc::new(Mutex::new(None)),
    };

    // 启动后台调度器
    crate::scheduler::start_scheduler(state.clone(), db);

    // SPA 静态文件服务：优先匹配 frontend/dist/ 中的文件，未命中则回退 index.html
    let spa = ServeDir::new("frontend/dist")
        .not_found_service(ServeFile::new("frontend/dist/index.html"));

    let app = Router::new()
        // Health
        .route("/health", get(health_handler))
        // Config management
        .route("/api/config", get(config_handler))
        .route("/api/config/s2a", post(add_s2a_handler))
        .route(
            "/api/config/s2a/{name}",
            delete(delete_s2a_handler).put(update_s2a_handler),
        )
        .route("/api/config/s2a/{name}/test", post(test_s2a_handler))
        .route("/api/config/s2a/{name}/stats", get(s2a_stats_handler))
        .route("/api/s2a/fetch-groups", post(fetch_s2a_groups_handler))
        .route("/api/config/defaults", put(update_defaults_handler))
        .route("/api/config/register", put(update_register_handler))
        .route("/api/test/gptmail", post(test_gptmail_handler))
        .route("/api/config/d1_cleanup", put(update_d1_cleanup_handler))
        .route("/api/d1_cleanup/trigger", post(trigger_d1_cleanup_handler))
        .route("/api/config/email_domains", post(add_email_domain_handler))
        .route(
            "/api/config/email_domains",
            delete(delete_email_domain_handler),
        )
        .route(
            "/api/config/gptmail_domains",
            post(add_gptmail_domain_handler).delete(delete_gptmail_domain_handler),
        )
        .route(
            "/api/config/proxy_pool",
            post(add_proxy_handler).delete(delete_proxy_handler),
        )
        .route("/api/config/proxy_pool/batch", post(batch_add_proxy_handler))
        .route("/api/config/proxy_enabled", put(set_proxy_enabled_handler))
        .route("/api/config/proxy_refresh_url", put(set_proxy_refresh_url_handler))
        .route("/api/proxy/refresh-ip", post(refresh_proxy_ip_handler))
        .route("/api/proxy/health-check", post(proxy_health_check_handler))
        .route("/api/proxy/batch-test", post(proxy_batch_test_handler))
        .route("/api/proxy/test", post(proxy_test_handler))
        .route("/api/proxy/quality-check", post(proxy_quality_check_handler))
        .route("/api/config/save", post(save_config_handler))
        // Task management
        .route("/api/tasks", post(create_task_handler))
        .route("/api/tasks", get(list_tasks_handler))
        .route("/api/tasks/{task_id}", get(get_task_handler))
        .route("/api/tasks/{task_id}/progress", get(task_progress_handler))
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
        .route(
            "/api/schedules/{name}/run-once",
            post(run_once_schedule_handler),
        )
        .route("/api/schedules/{name}/stop", post(stop_schedule_handler))
        // Run history
        .route("/api/runs", get(list_runs_handler))
        .route("/api/runs/stats", get(run_stats_handler))
        .route("/api/runs/{run_id}", get(get_run_handler))
        // Log stream (SSE)
        .route("/api/logs/stream", get(log_stream_handler))
        // Invite module
        .route("/api/invite/upload", post(invite_upload_handler))
        .route("/api/invite/uploads", get(list_invite_uploads_handler))
        .route("/api/invite/uploads/{id}", get(get_invite_upload_handler))
        .route("/api/invite/execute", post(execute_invite_handler))
        .route("/api/invite/tasks", get(list_invite_tasks_handler))
        .route("/api/invite/tasks/{id}", get(get_invite_task_handler))
        .route(
            "/api/invite/config",
            get(get_invite_config_handler).put(update_invite_config_handler),
        )
        // Team Manage module
        .route(
            "/api/team-manage/owners",
            get(team_manage_list_owners_handler),
        )
        .route(
            "/api/team-manage/owners/{accountId}/members",
            get(team_manage_get_members_handler),
        )
        .route(
            "/api/team-manage/owners/{accountId}/members/{userId}/kick",
            post(team_manage_kick_member_handler),
        )
        .route(
            "/api/team-manage/batch-kick",
            post(team_manage_batch_kick_handler),
        )
        .route(
            "/api/team-manage/owners/{accountId}/refresh",
            post(team_manage_refresh_members_handler),
        )
        .route(
            "/api/team-manage/batch-refresh-members",
            post(team_manage_batch_refresh_members_handler),
        )
        .route(
            "/api/team-manage/dashboard",
            get(team_manage_dashboard_handler),
        )
        .route(
            "/api/team-manage/owners/{accountId}/quota",
            get(team_manage_quota_handler),
        )
        .route(
            "/api/team-manage/member-quota",
            post(team_manage_member_quota_handler),
        )
        .route(
            "/api/team-manage/owners/{accountId}/invite",
            post(team_manage_invite_handler),
        )
        .route(
            "/api/team-manage/owners/batch-disable",
            post(team_manage_batch_disable_owners_handler),
        )
        .route(
            "/api/team-manage/owners/batch-restore",
            post(team_manage_batch_restore_owners_handler),
        )
        .route(
            "/api/team-manage/owners/batch-archive",
            post(team_manage_batch_archive_owners_handler),
        )
        .route(
            "/api/team-manage/owners/audits",
            get(team_manage_list_owner_audits_handler),
        )
        .route("/api/team-manage/health", get(team_manage_health_handler))
        .route(
            "/api/team-manage/proxy-settings",
            get(team_manage_get_proxy_settings).put(team_manage_set_proxy_settings),
        )
        .route(
            "/api/team-manage/batch-check",
            post(team_manage_batch_check_handler),
        )
        .route(
            "/api/team-manage/batch-invite",
            post(team_manage_batch_invite_handler),
        )
        .route(
            "/api/team-manage/batches",
            get(team_manage_list_batch_jobs_handler),
        )
        .route(
            "/api/team-manage/batches/{jobId}",
            get(team_manage_get_batch_job_handler),
        )
        .route(
            "/api/team-manage/batches/{jobId}/items",
            get(team_manage_get_batch_job_items_handler),
        )
        .route(
            "/api/team-manage/batches/{jobId}/retry-failed",
            post(team_manage_retry_failed_batch_items_handler),
        )
        .fallback_service(spa)
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    crate::log_broadcast::broadcast_log("═══════════════════════════════════════════");
    crate::log_broadcast::broadcast_log(&format!(
        "   autoteam2s2a 服务模式 v{}",
        env!("CARGO_PKG_VERSION")
    ));
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

// ─── Invite handlers ─────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct InviteUploadRequest {
    filename: Option<String>,
    accounts: serde_json::Value,
}

#[derive(Serialize)]
struct InviteUploadResponse {
    upload_id: String,
    owner_count: usize,
    owners: Vec<InviteOwnerBrief>,
}

#[derive(Serialize)]
struct InviteOwnerBrief {
    email: String,
    account_id: String,
    expires: Option<String>,
}

async fn invite_upload_handler(
    State(state): State<AppState>,
    Json(req): Json<InviteUploadRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let account_count = req.accounts.as_array().map_or(0, |a| a.len());
    let filename_preview = req.filename.as_deref().unwrap_or("upload.json");
    tracing::info!("[邀请上传] 开始处理: filename={filename_preview}, accounts={account_count}");

    let owners = crate::invite::parse_owners_json(&req.accounts)
        .map_err(|e| error_json(StatusCode::BAD_REQUEST, &format!("JSON 解析失败: {e}")))?;

    let upload_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let filename = req.filename.unwrap_or_else(|| "upload.json".to_string());
    let db_owners = crate::invite::owners_to_db(&owners);

    state
        .run_history_db
        .insert_invite_upload_sync(&upload_id, &filename, &db_owners)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("保存失败: {e}")))?;

    let owner_count = owners.len();
    tracing::info!("[邀请上传] 成功: upload_id={upload_id}, owner_count={owner_count}");

    let briefs: Vec<InviteOwnerBrief> = owners
        .iter()
        .map(|o| InviteOwnerBrief {
            email: o.email.clone(),
            account_id: o.account_id.clone(),
            expires: o.expires.clone(),
        })
        .collect();

    Ok((
        StatusCode::CREATED,
        Json(InviteUploadResponse {
            upload_id,
            owner_count,
            owners: briefs,
        }),
    ))
}

async fn list_invite_uploads_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let uploads = state
        .run_history_db
        .list_invite_uploads()
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    Ok(Json(uploads))
}

async fn get_invite_upload_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let detail = state
        .run_history_db
        .get_invite_upload_detail(&id)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "上传记录不存在"))?;
    Ok(Json(detail))
}

#[derive(Deserialize)]
struct ExecuteInviteRequest {
    upload_id: String,
    invite_count: Option<usize>,
    s2a_team: Option<String>,
    distribution: Option<Vec<crate::config::DistributionEntry>>,
    push_s2a: Option<bool>,
}

#[derive(Serialize)]
struct ExecuteInviteResponse {
    task_count: usize,
    task_ids: Vec<String>,
    message: String,
}

async fn execute_invite_handler(
    State(state): State<AppState>,
    Json(req): Json<ExecuteInviteRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let invite_runtime = cfg.invite_runtime();
    let invite_count = req
        .invite_count
        .unwrap_or(invite_runtime.default_invite_count)
        .clamp(1, 25);
    let push_s2a = req.push_s2a.unwrap_or(true);
    tracing::info!(
        "[邀请执行] 开始: upload_id={}, invite_count={invite_count}, push_s2a={push_s2a}",
        req.upload_id
    );

    // 构建号池分配方案：Vec<(S2aConfig, 百分比)>
    let team_assignments: Vec<(crate::config::S2aConfig, u8)> = if push_s2a {
        let available_teams = cfg.effective_s2a_configs();
        if let Some(ref dist) = req.distribution {
            // 多号池百分比分配模式
            if let Err(e) = crate::distribution::validate_distribution(dist, &available_teams) {
                return Err(error_json(StatusCode::BAD_REQUEST, &e));
            }
            dist.iter()
                .filter_map(|d| {
                    available_teams
                        .iter()
                        .find(|t| t.name == d.team)
                        .map(|t| (t.clone(), d.percent))
                })
                .collect()
        } else if let Some(ref name) = req.s2a_team {
            // 兼容旧的单号池模式
            available_teams
                .iter()
                .find(|t| t.name == *name)
                .map(|t| vec![(t.clone(), 100)])
                .unwrap_or_default()
        } else {
            available_teams
                .first()
                .map(|t| vec![(t.clone(), 100)])
                .unwrap_or_default()
        }
    } else {
        vec![]
    };

    let config_snapshot = cfg.clone();
    drop(cfg);

    // 获取未使用的 owners
    let unused_owners = state
        .run_history_db
        .get_unused_owners(&req.upload_id)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;

    if unused_owners.is_empty() {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            "该上传中没有可用的 Owner（全部已使用）",
        ));
    }

    // 按百分比将 owners 分配到各号池
    // owner_team_map: 每个 owner index → 对应的 S2aConfig
    let total_owners = unused_owners.len();
    let mut owner_team_map: Vec<Option<crate::config::S2aConfig>> = vec![None; total_owners];

    if !team_assignments.is_empty() {
        let mut offset = 0usize;
        for (i, (team_cfg, percent)) in team_assignments.iter().enumerate() {
            let count = if i + 1 < team_assignments.len() {
                (total_owners * (*percent as usize)) / 100
            } else {
                total_owners - offset
            };
            let end = (offset + count).min(total_owners);
            for idx in offset..end {
                owner_team_map[idx] = Some(team_cfg.clone());
            }
            offset = end;
        }
    }

    let mut task_ids = Vec::new();

    // 全局并发限制：最多 20 个 owner 同时执行邀请流程
    let invite_semaphore = Arc::new(tokio::sync::Semaphore::new(20));

    for (idx, (owner_db_id, owner_email, owner_account_id, access_token)) in
        unused_owners.into_iter().enumerate()
    {
        let team = owner_team_map[idx].clone();
        let task_id = uuid::Uuid::new_v4().to_string()[..8].to_string();

        // 生成邮箱种子
        let seeds: Vec<crate::models::AccountSeed> = (0..invite_count)
            .map(|_| crate::util::generate_account_seed(&config_snapshot.email_domains))
            .collect();

        // 写入任务记录
        let new_task = crate::db::NewInviteTask {
            id: task_id.clone(),
            upload_id: req.upload_id.clone(),
            owner_email: owner_email.clone(),
            owner_account_id: owner_account_id.clone(),
            s2a_team: team.as_ref().map(|t| t.name.clone()),
            invite_count,
            created_at: crate::util::beijing_now().to_rfc3339(),
        };
        let _ = state.run_history_db.enqueue_insert_invite_task(new_task);

        // 写入邮箱记录
        let email_inserts: Vec<crate::db::InviteEmailInsert> = seeds
            .iter()
            .map(|s| crate::db::InviteEmailInsert {
                email: s.account.clone(),
                password: s.password.clone(),
            })
            .collect();
        let _ = state
            .run_history_db
            .enqueue_insert_invite_emails(task_id.clone(), email_inserts);

        // 短暂等待 DB 写入
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let owner = crate::invite::TeamOwner {
            email: owner_email,
            account_id: owner_account_id,
            access_token,
            expires: None,
        };

        let progress = Arc::new(crate::models::InviteProgress::new());
        let db = state.run_history_db.clone();
        let invite_cfg = config_snapshot.invite_runtime();
        let max_members = invite_cfg.default_invite_count;
        let cfg_clone = config_snapshot.clone();
        let task_id_clone = task_id.clone();
        let sem = invite_semaphore.clone();

        tokio::spawn(async move {
            let _permit = sem.acquire().await;
            crate::invite::run_invite_workflow(
                task_id_clone,
                owner_db_id,
                owner,
                seeds,
                invite_cfg,
                cfg_clone,
                team,
                push_s2a,
                db,
                progress,
                max_members, // team 席位上限（不含 owner）
            )
            .await;
        });

        task_ids.push(task_id);
    }

    let created_count = task_ids.len();
    tracing::info!("[邀请执行] 任务已创建: task_count={created_count}");

    Ok((
        StatusCode::CREATED,
        Json(ExecuteInviteResponse {
            task_count: created_count,
            task_ids,
            message: "邀请任务已创建".to_string(),
        }),
    ))
}

async fn list_invite_tasks_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let tasks = state
        .run_history_db
        .list_invite_tasks(None)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    Ok(Json(tasks))
}

async fn get_invite_task_handler(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let detail = state
        .run_history_db
        .get_invite_task_detail(&id)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "邀请任务不存在"))?;
    Ok(Json(detail))
}

#[derive(Serialize)]
struct InviteConfigResponse {
    oai_client_version: String,
    default_invite_count: usize,
    request_timeout_sec: u64,
}

async fn get_invite_config_handler(State(state): State<AppState>) -> impl IntoResponse {
    let cfg = state.config.read().await;
    let rt = cfg.invite_runtime();
    Json(InviteConfigResponse {
        oai_client_version: rt.oai_client_version,
        default_invite_count: rt.default_invite_count,
        request_timeout_sec: rt.request_timeout_sec,
    })
}

#[derive(Deserialize)]
struct UpdateInviteConfigRequest {
    oai_client_version: Option<String>,
    default_invite_count: Option<usize>,
    request_timeout_sec: Option<u64>,
}

async fn update_invite_config_handler(
    State(state): State<AppState>,
    Json(req): Json<UpdateInviteConfigRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    if let Some(v) = req.oai_client_version {
        cfg.invite.oai_client_version = Some(v);
    }
    if let Some(v) = req.default_invite_count {
        cfg.invite.default_invite_count = Some(v.clamp(1, 25));
    }
    if let Some(v) = req.request_timeout_sec {
        cfg.invite.request_timeout_sec = Some(v.max(5));
    }
    let rt = cfg.invite_runtime();
    Ok(Json(InviteConfigResponse {
        oai_client_version: rt.oai_client_version,
        default_invite_count: rt.default_invite_count,
        request_timeout_sec: rt.request_timeout_sec,
    }))
}

// ─── Team Manage handlers ────────────────────────────────────────────────────

const TEAM_MANAGE_PAGE_SIZE_DEFAULT: usize = 20;
const TEAM_MANAGE_PAGE_SIZE_MAX: usize = 100;
const TEAM_MANAGE_CACHE_FRESH_MINS: i64 = 10;
const TEAM_MANAGE_CACHE_STALE_MINS: i64 = 30;

#[derive(Deserialize, Default)]
struct TeamManageOwnerListQuery {
    page: Option<usize>,
    page_size: Option<usize>,
    search: Option<String>,
    state: Option<String>,
    has_slots: Option<bool>,
    has_banned_member: Option<bool>,
    sort: Option<String>,
    order: Option<String>,
}

#[derive(Deserialize, Serialize, Clone, Default)]
struct TeamManageBatchFilters {
    search: Option<String>,
    state: Option<String>,
    has_slots: Option<bool>,
    has_banned_member: Option<bool>,
}

#[derive(Serialize, Clone)]
struct TeamManageOwnerListItem {
    email: String,
    account_id: String,
    access_token: Option<String>,
    member_count: Option<usize>,
    state: Option<String>,
    disabled_reason: Option<String>,
    owner_status: Option<String>,
    checked_at: Option<String>,
    cache_status: String,
    has_banned_member: bool,
    available_slots: Option<usize>,
    available_slots_cached: Option<usize>,
    last_health_checked_at: Option<String>,
    last_health_status: Option<String>,
}

#[derive(Serialize)]
struct TeamManageOwnersSummary {
    total_owners: usize,
    active_owners: usize,
    banned_owners: usize,
    expired_owners: usize,
    quarantined_owners: usize,
    owners_with_slots: usize,
    owners_with_banned_members: usize,
    fresh_cache_owners: usize,
    stale_cache_owners: usize,
    running_batch_jobs: usize,
}

#[derive(Serialize)]
struct TeamManageOwnersResponse {
    items: Vec<TeamManageOwnerListItem>,
    owners: Vec<TeamManageOwnerListItem>,
    page: usize,
    page_size: usize,
    total: usize,
    total_pages: usize,
    summary: TeamManageOwnersSummary,
}

#[derive(Serialize)]
struct TeamManageDashboardResponse {
    total_owners: usize,
    active_owners: usize,
    banned_owners: usize,
    expired_owners: usize,
    quarantined_owners: usize,
    owners_with_slots: usize,
    owners_with_banned_members: usize,
    fresh_cache_owners: usize,
    stale_cache_owners: usize,
    running_batch_jobs: usize,
    checked_recently: usize,
}

#[derive(Serialize, Deserialize, Clone)]
struct TeamManageMember {
    user_id: String,
    email: Option<String>,
    name: Option<String>,
    role: String,
    created_at: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct RedisOwnerMembersCacheEntry {
    members: Vec<TeamManageMember>,
    cached_at: String,
}

#[derive(Serialize)]
struct TeamManageMembersResponse {
    members: Vec<TeamManageMember>,
}

#[derive(Deserialize, Default)]
struct TeamManageMembersQuery {
    #[serde(default)]
    force_refresh: bool,
}

fn team_manage_cache_status(checked_at: Option<&str>) -> String {
    let Some(checked_at) = checked_at else {
        return "miss".to_string();
    };
    let Ok(ts) = DateTime::parse_from_rfc3339(checked_at) else {
        return "unknown".to_string();
    };
    let age = Utc::now()
        .signed_duration_since(ts.with_timezone(&Utc))
        .num_minutes();
    if age <= TEAM_MANAGE_CACHE_FRESH_MINS {
        "fresh".to_string()
    } else if age <= TEAM_MANAGE_CACHE_STALE_MINS {
        "stale".to_string()
    } else {
        "expired".to_string()
    }
}

#[allow(dead_code)]
fn team_manage_owner_runtime_state(owner_status: Option<&str>) -> String {
    match owner_status {
        Some("banned") => "banned".to_string(),
        Some(_) => "active".to_string(),
        None => "unknown".to_string(),
    }
}

fn team_manage_batch_job_summary(
    job: &crate::db::TeamManageBatchJobRecord,
) -> TeamManageBatchJobSummary {
    TeamManageBatchJobSummary {
        job_id: job.job_id.clone(),
        job_type: job.job_type.clone(),
        status: job.status.clone(),
        scope: job.scope.clone().unwrap_or_else(|| "manual".to_string()),
        s2a_team: serde_json::from_str::<serde_json::Value>(&job.payload_json)
            .ok()
            .and_then(|payload| {
                payload
                    .get("s2a_team")
                    .and_then(|value| value.as_str())
                    .map(|value| value.to_string())
            }),
        total_count: job.total_count,
        success_count: job.success_count,
        failed_count: job.failed_count,
        skipped_count: job.skipped_count,
        created_at: job.created_at.clone(),
        started_at: job.started_at.clone(),
        finished_at: job.finished_at.clone(),
    }
}

#[allow(dead_code)]
async fn team_manage_running_batch_jobs_count(state: &AppState) -> usize {
    state
        .run_history_db
        .list_team_manage_batch_jobs(None, Some(500))
        .map(|jobs| {
            jobs.into_iter()
                .filter(|job| matches!(job.status.as_str(), "pending" | "running"))
                .count()
        })
        .unwrap_or(0)
}

fn team_manage_max_members(cfg: &AppConfig) -> usize {
    cfg.invite_runtime().default_invite_count.max(1)
}

fn team_manage_cached_results_map(
    rows: Vec<(String, String, Option<String>, String)>,
) -> HashMap<String, OwnerHealthResult> {
    rows.into_iter()
        .map(|(account_id, owner_status, members_json, checked_at)| {
            let members = members_json
                .and_then(|value| serde_json::from_str::<Vec<MemberHealthInfo>>(&value).ok())
                .unwrap_or_default();
            let result = OwnerHealthResult {
                account_id: account_id.clone(),
                owner_status,
                members,
                checked_at,
                owner_quota: None,
            };
            (account_id, result)
        })
        .collect()
}

fn team_manage_owner_health_result_from_record(
    record: &crate::db::OwnerHealthRecord,
) -> OwnerHealthResult {
    OwnerHealthResult {
        account_id: record.account_id.clone(),
        owner_status: record.owner_status.clone(),
        members: record
            .members_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<Vec<MemberHealthInfo>>(value).ok())
            .unwrap_or_default(),
        checked_at: record.checked_at.clone(),
        owner_quota: None,
    }
}

fn team_manage_owner_health_cache_status(record: &crate::db::OwnerHealthRecord) -> String {
    if let Some(expires_at) = &record.expires_at {
        if let Ok(expires_at) = DateTime::parse_from_rfc3339(expires_at) {
            if Utc::now() > expires_at.with_timezone(&Utc) {
                return "expired".to_string();
            }
        }
    }
    if !record.cache_status.trim().is_empty() {
        return record.cache_status.clone();
    }
    team_manage_cache_status(Some(record.checked_at.as_str()))
}

fn team_manage_sort_key(sort: &str) -> String {
    match sort {
        "email" => "display_email",
        "members" => "member_count",
        "slots" => "available_slots",
        "checked_at" => "last_health_checked_at",
        "status" => "state",
        _ => sort,
    }
    .to_string()
}

async fn team_manage_resolve_batch_account_ids(
    state: &AppState,
    explicit_ids: &[String],
    scope: Option<&str>,
    filters: Option<&TeamManageBatchFilters>,
) -> Result<Vec<String>, (StatusCode, Json<ErrorResponse>)> {
    if matches!(scope, Some("filtered")) {
        let _ = state
            .run_history_db
            .sync_owner_registry_from_invite_owners()
            .map_err(|e| {
                error_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("同步 owner_registry 失败: {e}"),
                )
            })?;
        let filters = filters.cloned().unwrap_or_default();
        let mut page = 1usize;
        let mut total_pages = 1usize;
        let mut ids = Vec::new();
        while page <= total_pages {
            let result = state
                .run_history_db
                .list_owner_registry_page(&crate::db::OwnerRegistryQuery {
                    page,
                    page_size: 200,
                    search: filters.search.clone().filter(|value| !value.trim().is_empty()),
                    state: filters.state.clone().filter(|value| !value.trim().is_empty()),
                    has_slots: filters.has_slots,
                    has_banned_member: filters.has_banned_member,
                    sort_by: Some("updated_at".to_string()),
                    sort_order: Some("desc".to_string()),
                })
                .map_err(|e| {
                    error_json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("查询筛选结果失败: {e}"),
                    )
                })?;
            total_pages = result.total_pages.max(1);
            ids.extend(result.items.into_iter().map(|item| item.account_id));
            page += 1;
        }
        return Ok(ids);
    }
    Ok(explicit_ids.to_vec())
}

async fn team_manage_persist_health_result(state: &AppState, result: &OwnerHealthResult) {
    let now = crate::util::beijing_now();
    let expires_at = (now + chrono::Duration::minutes(TEAM_MANAGE_CACHE_FRESH_MINS)).to_rfc3339();
    let checked_at = result.checked_at.clone();
    let members_json = serde_json::to_string(&result.members).unwrap_or_default();
    let max_members = {
        let cfg = state.config.read().await;
        team_manage_max_members(&cfg)
    };
    let _ = state
        .run_history_db
        .enqueue_upsert_owner_health_record(crate::db::OwnerHealthUpsert {
            account_id: result.account_id.clone(),
            owner_status: result.owner_status.clone(),
            members_json,
            checked_at: checked_at.clone(),
            expires_at: Some(expires_at),
            cache_status: Some("fresh".to_string()),
            source: Some("live".to_string()),
            last_error: None,
        });
    let _ = state.run_history_db.update_owner_registry_health_summary(
        &result.account_id,
        Some(&checked_at),
        Some(&result.owner_status),
        result.members.len(),
        max_members.saturating_sub(result.members.len()),
        &now.to_rfc3339(),
    );
    if result.owner_status == "banned" {
        let _ = state.run_history_db.batch_update_owner_registry_state(
            &[result.account_id.clone()],
            "banned",
            Some("health_check_banned"),
            &now.to_rfc3339(),
        );
    }
    team_manage_cache_health_result_in_redis(state, result).await;
}

#[allow(dead_code)]
async fn team_manage_get_health_result_from_redis(
    state: &AppState,
    account_id: &str,
) -> Option<OwnerHealthResult> {
    let redis_cache = state.redis_cache.as_ref()?;
    match redis_cache
        .get_json::<OwnerHealthResult>(&redis_cache.owner_health_key(account_id))
        .await
    {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(
                "[TeamManage] 读取 Redis health cache 失败: account_id={}, error={}",
                account_id,
                error
            );
            None
        }
    }
}

async fn team_manage_cache_health_result_in_redis(
    state: &AppState,
    result: &OwnerHealthResult,
) {
    let Some(redis_cache) = state.redis_cache.as_ref() else {
        return;
    };
    if let Err(error) = redis_cache
        .set_json(
            &redis_cache.owner_health_key(&result.account_id),
            result,
            redis_cache.default_ttl_secs(),
        )
        .await
    {
        tracing::warn!(
            "[TeamManage] 写入 Redis health cache 失败: account_id={}, error={}",
            result.account_id,
            error
        );
    }
}

async fn team_manage_get_members_cache_from_redis(
    state: &AppState,
    account_id: &str,
) -> Option<RedisOwnerMembersCacheEntry> {
    let redis_cache = state.redis_cache.as_ref()?;
    match redis_cache
        .get_json::<RedisOwnerMembersCacheEntry>(&redis_cache.owner_members_key(account_id))
        .await
    {
        Ok(value) => value,
        Err(error) => {
            tracing::warn!(
                "[TeamManage] 读取 Redis members cache 失败: account_id={}, error={}",
                account_id,
                error
            );
            None
        }
    }
}

async fn team_manage_cache_members_in_redis(
    state: &AppState,
    account_id: &str,
    members: &[TeamManageMember],
    cached_at: &str,
) {
    let Some(redis_cache) = state.redis_cache.as_ref() else {
        return;
    };
    let entry = RedisOwnerMembersCacheEntry {
        members: members.to_vec(),
        cached_at: cached_at.to_string(),
    };
    if let Err(error) = redis_cache
        .set_json(
            &redis_cache.owner_members_key(account_id),
            &entry,
            redis_cache.default_ttl_secs(),
        )
        .await
    {
        tracing::warn!(
            "[TeamManage] 写入 Redis members cache 失败: account_id={}, error={}",
            account_id,
            error
        );
    }
}

async fn team_manage_try_acquire_health_lock(state: &AppState, account_id: &str) -> Option<String> {
    let mut locks = state.team_manage_health_locks.lock().await;
    locks.retain(|_, instant| instant.elapsed().as_secs() < 120);
    if locks.contains_key(account_id) {
        return None;
    }
    locks.insert(account_id.to_string(), Instant::now());
    drop(locks);

    let lock_owner = uuid::Uuid::new_v4().to_string();
    if let Some(redis_cache) = state.redis_cache.as_ref() {
        match redis_cache
            .try_acquire_lock(&redis_cache.health_lock_key(account_id), &lock_owner)
            .await
        {
            Ok(true) => {}
            Ok(false) => {
                state.team_manage_health_locks.lock().await.remove(account_id);
                return None;
            }
            Err(error) => {
                tracing::warn!(
                    "[TeamManage] Redis 健康检查锁失败，回退进程内锁: account_id={}, error={}",
                    account_id,
                    error
                );
            }
        }
    }
    Some(lock_owner)
}

async fn team_manage_release_health_lock(state: &AppState, account_id: &str, lock_owner: &str) {
    state
        .team_manage_health_locks
        .lock()
        .await
        .remove(account_id);

    if let Some(redis_cache) = state.redis_cache.as_ref() {
        if let Err(error) = redis_cache
            .release_lock(&redis_cache.health_lock_key(account_id), lock_owner)
            .await
        {
            tracing::warn!(
                "[TeamManage] Redis 释放健康检查锁失败: account_id={}, error={}",
                account_id,
                error
            );
        }
    }
}

async fn team_manage_list_owners_handler(
    State(state): State<AppState>,
    Query(query): Query<TeamManageOwnerListQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let TeamManageOwnerListQuery {
        page,
        page_size,
        search,
        state: query_state,
        has_slots,
        has_banned_member,
        sort,
        order,
    } = query;
    let page = page.unwrap_or(1).max(1);
    let page_size = page_size
        .unwrap_or(TEAM_MANAGE_PAGE_SIZE_DEFAULT)
        .clamp(1, TEAM_MANAGE_PAGE_SIZE_MAX);
    let search = search.unwrap_or_default().trim().to_lowercase();
    let state_filter = query_state.unwrap_or_default().trim().to_lowercase();
    let sort = sort.unwrap_or_else(|| "email".to_string()).to_lowercase();
    let order = order.unwrap_or_else(|| "asc".to_string()).to_lowercase();

    tracing::info!(
        "[TeamManage] 分页列出 owners: page={}, page_size={}, search='{}', state='{}'",
        page,
        page_size,
        search,
        state_filter
    );

    let _ = state
        .run_history_db
        .sync_owner_registry_from_invite_owners()
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("同步 owner_registry 失败: {e}"),
            )
        })?;
    let registry_page = state
        .run_history_db
        .list_owner_registry_page(&crate::db::OwnerRegistryQuery {
            page,
            page_size,
            search: if search.is_empty() { None } else { Some(search.clone()) },
            state: if state_filter.is_empty() {
                None
            } else {
                Some(state_filter.clone())
            },
            has_slots,
            has_banned_member,
            sort_by: Some(team_manage_sort_key(&sort)),
            sort_order: Some(order),
        })
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询 owner_registry 失败: {e}"),
            )
        })?;

    let health_records = state
        .run_history_db
        .get_owner_health_by_account_ids(
            &registry_page
                .items
                .iter()
                .map(|item| item.account_id.clone())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询健康缓存失败: {e}"),
            )
        })?;
    let health_map = health_records
        .into_iter()
        .map(|record| (record.account_id.clone(), record))
        .collect::<HashMap<_, _>>();

    let paged = registry_page
        .items
        .iter()
        .map(|record| {
            let health = health_map.get(&record.account_id);
            TeamManageOwnerListItem {
                email: record.display_email.clone(),
                account_id: record.account_id.clone(),
                access_token: record.latest_access_token.clone(),
                member_count: Some(record.member_count_cached),
                state: Some(record.state.clone()),
                disabled_reason: record.disabled_reason.clone(),
                owner_status: health
                    .map(|row| row.owner_status.clone())
                    .or_else(|| record.last_health_status.clone()),
                checked_at: record.last_health_checked_at.clone(),
                cache_status: health
                    .map(|row| row.cache_status.clone())
                    .unwrap_or_else(|| {
                        team_manage_cache_status(record.last_health_checked_at.as_deref())
                    }),
                has_banned_member: record.has_banned_member,
                available_slots: Some(record.available_slots_cached),
                available_slots_cached: Some(record.available_slots_cached),
                last_health_checked_at: record.last_health_checked_at.clone(),
                last_health_status: record.last_health_status.clone(),
            }
        })
        .collect::<Vec<_>>();

    let dashboard = state
        .run_history_db
        .get_team_manage_dashboard_summary()
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询 TeamManage 仪表盘失败: {e}"),
            )
        })?;
    let summary = TeamManageOwnersSummary {
        total_owners: dashboard.total_owners,
        active_owners: dashboard.active_owners,
        banned_owners: dashboard.banned_owners,
        expired_owners: dashboard.expired_owners,
        quarantined_owners: dashboard.quarantined_owners,
        owners_with_slots: dashboard.owners_with_slots,
        owners_with_banned_members: dashboard.owners_with_banned_members,
        fresh_cache_owners: dashboard.health_cache_fresh,
        stale_cache_owners: dashboard.health_cache_stale,
        running_batch_jobs: dashboard.running_batch_jobs,
    };

    Ok(Json(TeamManageOwnersResponse {
        owners: paged.clone(),
        items: paged,
        page: registry_page.page,
        page_size: registry_page.page_size,
        total: registry_page.total,
        total_pages: registry_page.total_pages,
        summary,
    }))
}

async fn fetch_chatgpt_members(
    account_id: &str,
    access_token: &str,
) -> Result<Vec<TeamManageMember>, String> {
    fetch_chatgpt_members_with_client(account_id, access_token, shared_http_client_15s()).await
}

async fn fetch_chatgpt_members_with_client(
    account_id: &str,
    access_token: &str,
    client: &rquest::Client,
) -> Result<Vec<TeamManageMember>, String> {

    let url = format!(
        "https://chatgpt.com/backend-api/accounts/{}/users?offset=0&limit=100&query=",
        account_id
    );

    let device_id = uuid::Uuid::new_v4().to_string();

    let resp = client
        .get(&url)
        .header("Accept", "*/*")
        .header("Accept-Language", "zh-CN,zh;q=0.9")
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Chatgpt-Account-Id", account_id)
        .header("Oai-Language", "zh-CN")
        .header("Oai-Device-Id", &device_id)
        .header("Referer", "https://chatgpt.com/admin/members")
        .header("Origin", "https://chatgpt.com")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")
        .send()
        .await
        .map_err(|e| format!("请求失败: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let preview = if body.len() > 300 {
            format!("{}...", &body[..300])
        } else {
            body
        };
        return Err(format!("HTTP {}: {}", status.as_u16(), preview));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("JSON 解析失败: {e}"))?;

    let items = body
        .get("items")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    let members: Vec<TeamManageMember> = items
        .into_iter()
        .map(|item| {
            let user_id = item
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let email = item
                .get("email")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let name = item
                .get("name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let role = item
                .get("role")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let created_at = item
                .get("created_time")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            TeamManageMember {
                user_id,
                email,
                name,
                role,
                created_at,
            }
        })
        .collect();

    Ok(members)
}

async fn team_manage_fetch_members_live(
    state: &AppState,
    account_id: &str,
) -> Result<Vec<TeamManageMember>, (StatusCode, Json<ErrorResponse>)> {
    let access_token = state
        .run_history_db
        .get_owner_token_by_account_id(account_id)
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询 token 失败: {e}"),
            )
        })?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "找不到对应的 Owner 或 access_token"))?;

    let members = fetch_chatgpt_members(account_id, &access_token)
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("获取成员失败: {e}")))?;
    let list = members
        .into_iter()
        .filter(|member| member.role != "account-owner")
        .collect::<Vec<_>>();
    let now = crate::util::beijing_now().to_rfc3339();
    let member_count = list.len();
    let max_members = {
        let cfg = state.config.read().await;
        team_manage_max_members(&cfg)
    };
    let members_json = serde_json::to_string(&list).unwrap_or_default();
    let _ = state.run_history_db.upsert_owner_member_cache(
        account_id,
        Some(&members_json),
        member_count,
        &now,
    );
    let _ = state.run_history_db.update_owner_registry_member_cache_summary(
        account_id,
        member_count,
        max_members.saturating_sub(member_count),
        &now,
    );
    team_manage_cache_members_in_redis(state, account_id, &list, &now).await;
    Ok(list)
}

fn team_manage_member_cache_status(cached_at: &str) -> String {
    if let Ok(ts) = DateTime::parse_from_rfc3339(cached_at) {
        let age = Utc::now()
            .signed_duration_since(ts.with_timezone(&Utc))
            .num_minutes();
        if age <= TEAM_MANAGE_CACHE_FRESH_MINS {
            return "fresh".to_string();
        }
        if age <= TEAM_MANAGE_CACHE_STALE_MINS {
            return "stale".to_string();
        }
        return "expired".to_string();
    }
    "expired".to_string()
}

async fn team_manage_get_members_handler(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Query(query): Query<TeamManageMembersQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!(
        "[TeamManage] 获取成员列表: account_id={account_id}, force_refresh={}",
        query.force_refresh
    );

    if !query.force_refresh {
        if let Some(cache) = team_manage_get_members_cache_from_redis(&state, &account_id).await {
            match team_manage_member_cache_status(&cache.cached_at).as_str() {
                "fresh" if !cache.members.is_empty() => {
                    return Ok(Json(TeamManageMembersResponse {
                        members: cache.members,
                    }));
                }
                "stale" if !cache.members.is_empty() => {
                    let refresh_state = state.clone();
                    let refresh_account_id = account_id.clone();
                    tokio::spawn(async move {
                        let _ = team_manage_fetch_members_live(&refresh_state, &refresh_account_id).await;
                    });
                    return Ok(Json(TeamManageMembersResponse {
                        members: cache.members,
                    }));
                }
                _ => {}
            }
        }
        if let Some(cache) = state
            .run_history_db
            .get_owner_member_cache(&account_id)
            .map_err(|e| {
                error_json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("查询成员缓存失败: {e}"),
                )
            })?
        {
            let members = cache
                .members_json
                .as_deref()
                .and_then(|value| serde_json::from_str::<Vec<TeamManageMember>>(value).ok())
                .unwrap_or_default();
            match team_manage_member_cache_status(&cache.cached_at).as_str() {
                "fresh" if !members.is_empty() => {
                    return Ok(Json(TeamManageMembersResponse { members }));
                }
                "stale" if !members.is_empty() => {
                    let refresh_state = state.clone();
                    let refresh_account_id = account_id.clone();
                    tokio::spawn(async move {
                        let _ = team_manage_fetch_members_live(&refresh_state, &refresh_account_id).await;
                    });
                    return Ok(Json(TeamManageMembersResponse { members }));
                }
                _ => {}
            }
        }
    }

    let members = team_manage_fetch_members_live(&state, &account_id).await?;

    tracing::info!(
        "[TeamManage] account_id={account_id} 共 {} 个成员",
        members.len()
    );
    Ok(Json(TeamManageMembersResponse { members }))
}

async fn team_manage_kick_member_handler(
    State(state): State<AppState>,
    Path((account_id, user_id)): Path<(String, String)>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!("[TeamManage] 踢除成员: account_id={account_id}, user_id={user_id}");

    let access_token = state
        .run_history_db
        .get_owner_token_by_account_id(&account_id)
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询 token 失败: {e}"),
            )
        })?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "找不到对应的 Owner 或 access_token"))?;

    // 读取代理配置
    let proxy_url = {
        let cfg = state.config.read().await;
        if cfg.team_manage_kick_use_proxy.unwrap_or(false) && !cfg.proxy_pool.is_empty() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static IDX: AtomicUsize = AtomicUsize::new(0);
            let i = IDX.fetch_add(1, Ordering::Relaxed);
            Some(cfg.proxy_pool[i % cfg.proxy_pool.len()].clone())
        } else {
            None
        }
    };

    let mut builder = rquest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .connect_timeout(std::time::Duration::from_secs(10));
    if let Some(ref pu) = proxy_url {
        if let Ok(p) = rquest::Proxy::all(pu) {
            builder = builder.proxy(p);
        }
    }
    let client = builder.build().unwrap_or_else(|_| rquest::Client::new());

    let url = format!(
        "https://chatgpt.com/backend-api/accounts/{}/users/{}",
        account_id, user_id
    );

    let device_id = uuid::Uuid::new_v4().to_string();

    let resp = client
        .delete(&url)
        .header("Accept", "*/*")
        .header("Accept-Language", "zh-CN,zh;q=0.9")
        .header("Authorization", format!("Bearer {}", access_token))
        .header("Chatgpt-Account-Id", &account_id)
        .header("Oai-Language", "zh-CN")
        .header("Oai-Device-Id", &device_id)
        .header("Referer", "https://chatgpt.com/admin/members")
        .header("Origin", "https://chatgpt.com")
        .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")
        .send()
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("请求失败: {e}")))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let preview = if body.len() > 300 {
            format!("{}...", &body[..300])
        } else {
            body
        };
        return Err(error_json(
            StatusCode::BAD_GATEWAY,
            &format!("踢除失败 HTTP {}: {}", status.as_u16(), preview),
        ));
    }

    tracing::info!("[TeamManage] 踢除成功: account_id={account_id}, user_id={user_id}");
    Ok(Json(
        serde_json::json!({ "success": true, "message": "成员已踢除" }),
    ))
}

// ─── 批量踢除成员 ────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct BatchKickRequest {
    items: Vec<BatchKickItem>,
}

#[derive(Deserialize)]
struct BatchKickItem {
    account_id: String,
    user_id: String,
    #[serde(default)]
    email: String,
}

async fn team_manage_batch_kick_handler(
    State(state): State<AppState>,
    Json(body): Json<BatchKickRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let total = body.items.len();
    if total == 0 {
        return Err(error_json(StatusCode::BAD_REQUEST, "items 不能为空"));
    }

    crate::log_broadcast::broadcast_log(&format!(
        "[批量清理] 开始: 共 {} 个成员待踢除",
        total
    ));

    // 按 owner 分组，复用 access_token
    let mut groups: std::collections::HashMap<String, Vec<(String, String)>> =
        std::collections::HashMap::new();
    for item in &body.items {
        groups
            .entry(item.account_id.clone())
            .or_default()
            .push((item.user_id.clone(), item.email.clone()));
    }

    crate::log_broadcast::broadcast_log(&format!(
        "[批量清理] 涉及 {} 个 Owner",
        groups.len()
    ));

    // 预取所有 owner 的 access_token
    let mut owner_tokens: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    let mut skipped_owners = 0usize;
    for account_id in groups.keys() {
        match state
            .run_history_db
            .get_owner_token_by_account_id(account_id)
        {
            Ok(Some(token)) => {
                owner_tokens.insert(account_id.clone(), token);
            }
            _ => {
                skipped_owners += 1;
                crate::log_broadcast::broadcast_log(&format!(
                    "[批量清理] {} 无 token，跳过该 owner 下所有成员",
                    account_id
                ));
            }
        }
    }

    // 预建代理客户端池（轮询复用）
    let proxy_clients: Vec<rquest::Client> = {
        let cfg = state.config.read().await;
        let kick_proxy = cfg.team_manage_kick_use_proxy.unwrap_or(false);
        let pool = &cfg.proxy_pool;
        if kick_proxy && !pool.is_empty() {
            crate::log_broadcast::broadcast_log(&format!(
                "[批量清理] 使用 {} 个代理",
                pool.len()
            ));
            pool.iter()
                .filter_map(|pu| {
                    let proxy = rquest::Proxy::all(pu).ok()?;
                    rquest::Client::builder()
                        .timeout(std::time::Duration::from_secs(15))
                        .connect_timeout(std::time::Duration::from_secs(10))
                        .proxy(proxy)
                        .build()
                        .ok()
                })
                .collect()
        } else {
            crate::log_broadcast::broadcast_log("[批量清理] 未配置代理，使用直连");
            vec![shared_http_client_15s().clone()]
        }
    };
    let proxy_clients = std::sync::Arc::new(proxy_clients);
    let proxy_idx = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    let success = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let failed = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let done = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

    // Owner 级并发 10，每 owner 内成员级并发 5
    let owner_sem = std::sync::Arc::new(tokio::sync::Semaphore::new(10));
    let mut owner_handles = Vec::new();

    for (account_id, members) in groups {
        let Some(access_token) = owner_tokens.get(&account_id).cloned() else {
            let skip_count = members.len();
            failed.fetch_add(skip_count, std::sync::atomic::Ordering::Relaxed);
            done.fetch_add(skip_count, std::sync::atomic::Ordering::Relaxed);
            continue;
        };

        let proxy_clients = proxy_clients.clone();
        let proxy_idx = proxy_idx.clone();
        let success = success.clone();
        let failed = failed.clone();
        let done = done.clone();
        let owner_sem = owner_sem.clone();

        owner_handles.push(tokio::spawn(async move {
            let _permit = owner_sem.acquire().await;

            for (user_id, email) in &members {
                let url = format!(
                    "https://chatgpt.com/backend-api/accounts/{}/users/{}",
                    account_id, user_id
                );
                let device_id = uuid::Uuid::new_v4().to_string();

                // 轮询选取代理客户端
                let idx = proxy_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let client = &proxy_clients[idx % proxy_clients.len()];

                let result = client
                    .delete(&url)
                    .header("Accept", "*/*")
                    .header("Accept-Language", "zh-CN,zh;q=0.9")
                    .header("Authorization", format!("Bearer {}", access_token))
                    .header("Chatgpt-Account-Id", &account_id)
                    .header("Oai-Language", "zh-CN")
                    .header("Oai-Device-Id", &device_id)
                    .header("Referer", "https://chatgpt.com/admin/members")
                    .header("Origin", "https://chatgpt.com")
                    .header("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36")
                    .send()
                    .await;

                let (ok, is_403) = match result {
                    Ok(resp) if resp.status().is_success() => {
                        crate::log_broadcast::broadcast_log(&format!(
                            "[批量清理] ✓ {} 踢除 {}",
                            account_id, email
                        ));
                        (true, false)
                    }
                    Ok(resp) => {
                        let status = resp.status();
                        let status_code = status.as_u16();
                        let body = resp.text().await.unwrap_or_default();
                        let preview = if body.len() > 200 { &body[..200] } else { &body };
                        crate::log_broadcast::broadcast_log(&format!(
                            "[批量清理] ✗ {} 踢除 {} 失败 HTTP {}: {}",
                            account_id, email, status_code, preview
                        ));
                        tracing::warn!(
                            "[批量清理] 踢除失败 {} {} HTTP {}: {}",
                            account_id, email, status_code, preview
                        );
                        (false, status_code == 403)
                    }
                    Err(e) => {
                        crate::log_broadcast::broadcast_log(&format!(
                            "[批量清理] ✗ {} 踢除 {} 失败: {}",
                            account_id, email, e
                        ));
                        tracing::warn!(
                            "[批量清理] 踢除失败 {} {}: {}",
                            account_id, email, e
                        );
                        (false, false)
                    }
                };

                if ok {
                    success.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                } else {
                    failed.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                done.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

                // 首次 403 → owner token 失效，跳过该 owner 剩余成员
                if is_403 {
                    let processed_so_far = members.iter().position(|(uid, _)| uid == user_id).unwrap_or(0) + 1;
                    let skip = members.len() - processed_so_far;
                    if skip > 0 {
                        failed.fetch_add(skip, std::sync::atomic::Ordering::Relaxed);
                        done.fetch_add(skip, std::sync::atomic::Ordering::Relaxed);
                        crate::log_broadcast::broadcast_log(&format!(
                            "[批量清理] ⚠ {} token 无效(403)，跳过剩余 {} 个成员",
                            account_id, skip
                        ));
                    }
                    break;
                }

                // 每完成一个检查是否需要输出进度
                let current_done = done.load(std::sync::atomic::Ordering::Relaxed);
                if current_done % 50 == 0 || current_done == total {
                    crate::log_broadcast::broadcast_log(&format!(
                        "[批量清理] 进度 {}/{} (成功 {}, 失败 {})",
                        current_done,
                        total,
                        success.load(std::sync::atomic::Ordering::Relaxed),
                        failed.load(std::sync::atomic::Ordering::Relaxed),
                    ));
                }
            }
        }));
    }

    for h in owner_handles {
        let _ = h.await;
    }

    let success_count = success.load(std::sync::atomic::Ordering::Relaxed);
    let failed_count = failed.load(std::sync::atomic::Ordering::Relaxed);

    crate::log_broadcast::broadcast_log(&format!(
        "[批量清理] 完成: 共 {} 个, 成功 {}, 失败 {}, 跳过 {} 个 owner(无token)",
        total, success_count, failed_count, skipped_owners
    ));

    tracing::info!(
        "[TeamManage] 批量清理完成: total={}, success={}, failed={}, skipped_owners={}",
        total, success_count, failed_count, skipped_owners
    );

    Ok(Json(serde_json::json!({
        "total": total,
        "success": success_count,
        "failed": failed_count,
        "skipped_owners": skipped_owners,
    })))
}

async fn team_manage_refresh_members_handler(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!("[TeamManage] 刷新成员列表: account_id={account_id}");
    let members = team_manage_fetch_members_live(&state, &account_id).await?;

    tracing::info!(
        "[TeamManage] 刷新完成: account_id={account_id}, 共 {} 个成员",
        members.len()
    );
    Ok(Json(TeamManageMembersResponse { members }))
}

#[derive(Deserialize)]
struct BatchRefreshMembersRequest {
    account_ids: Vec<String>,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    scope: Option<String>,
    #[serde(default)]
    filters: TeamManageBatchFilters,
}

#[derive(Serialize)]
struct BatchRefreshMembersResponse {
    total: usize,
    success: usize,
    failed: usize,
    refreshed: Vec<serde_json::Value>,
}

async fn team_manage_batch_refresh_members_handler(
    State(state): State<AppState>,
    Json(req): Json<BatchRefreshMembersRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let account_ids = team_manage_resolve_batch_account_ids(
        &state,
        &req.account_ids,
        req.scope.as_deref(),
        Some(&req.filters),
    )
    .await?;
    if account_ids.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "account_ids 不能为空"));
    }
    let concurrency = req.concurrency.max(1);
    crate::log_broadcast::broadcast_log(&format!(
        "[Team管理] 批量刷新成员: {} 个 owner, 并发={}",
        account_ids.len(),
        concurrency
    ));
    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut handles = Vec::new();
    for account_id in account_ids.clone() {
        let sem = semaphore.clone();
        let state = state.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await;
            match team_manage_fetch_members_live(&state, &account_id).await {
                Ok(members) => Ok((account_id, members.len())),
                Err(error) => Err((account_id, error.1 .0.error)),
            }
        }));
    }
    let mut success = 0usize;
    let mut failed = 0usize;
    let mut refreshed = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(Ok((account_id, member_count))) => {
                success += 1;
                crate::log_broadcast::broadcast_log(&format!(
                    "[Team管理] 刷新成员成功: {} ({}人)",
                    account_id, member_count
                ));
                refreshed.push(serde_json::json!({
                    "account_id": account_id,
                    "member_count": member_count,
                }));
            }
            Ok(Err((account_id, error))) => {
                failed += 1;
                crate::log_broadcast::broadcast_log(&format!(
                    "[Team管理][ERR] 刷新成员失败: {} - {}",
                    account_id, error
                ));
                refreshed.push(serde_json::json!({
                    "account_id": account_id,
                    "error": error,
                }));
            }
            Err(_) => {
                failed += 1;
            }
        }
    }
    crate::log_broadcast::broadcast_log(&format!(
        "[Team管理] 批量刷新完成: 成功 {}, 失败 {}",
        success, failed
    ));
    Ok(Json(BatchRefreshMembersResponse {
        total: account_ids.len(),
        success,
        failed,
        refreshed,
    }))
}

async fn team_manage_batch_update_owner_state(
    state: &AppState,
    req: &TeamManageBatchOwnerStateRequest,
    next_state: &str,
) -> Result<TeamManageBatchOwnerStateResponse, (StatusCode, Json<ErrorResponse>)> {
    let account_ids = team_manage_resolve_batch_account_ids(
        state,
        &req.account_ids,
        req.scope.as_deref(),
        Some(&req.filters),
    )
    .await?;
    if account_ids.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "account_ids 不能为空"));
    }
    crate::log_broadcast::broadcast_log(&format!(
        "[Team管理] 批量更新状态: {} 个 owner → {}",
        account_ids.len(),
        next_state
    ));
    let previous_states = state
        .run_history_db
        .get_owner_registry_states_by_account_ids(&account_ids)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?
        .into_iter()
        .collect::<HashMap<_, _>>();
    let now = crate::util::beijing_now().to_rfc3339();
    let affected = state
        .run_history_db
        .batch_update_owner_registry_state(
            &account_ids,
            next_state,
            req.reason.as_deref(),
            &now,
        )
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    let _ = state.run_history_db.insert_team_manage_owner_audits(
        account_ids
            .iter()
            .map(|account_id| crate::db::NewTeamManageOwnerAudit {
                account_id: account_id.clone(),
                action: format!("batch_{next_state}"),
                from_state: previous_states.get(account_id).cloned(),
                to_state: next_state.to_string(),
                reason: req.reason.clone(),
                scope: req.scope.clone(),
                batch_job_id: None,
                created_at: now.clone(),
            })
            .collect(),
    );
    crate::log_broadcast::broadcast_log(&format!(
        "[Team管理] 批量更新完成: {} 个 owner 已更新为 {}",
        affected, next_state
    ));
    Ok(TeamManageBatchOwnerStateResponse {
        affected,
        state: next_state.to_string(),
        message: format!("已更新 {} 个 Owner 为 {}", affected, next_state),
    })
}

async fn team_manage_batch_disable_owners_handler(
    State(state): State<AppState>,
    Json(req): Json<TeamManageBatchOwnerStateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(
        team_manage_batch_update_owner_state(&state, &req, "quarantined").await?,
    ))
}

async fn team_manage_batch_restore_owners_handler(
    State(state): State<AppState>,
    Json(req): Json<TeamManageBatchOwnerStateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(
        team_manage_batch_update_owner_state(&state, &req, "active").await?,
    ))
}

async fn team_manage_batch_archive_owners_handler(
    State(state): State<AppState>,
    Json(req): Json<TeamManageBatchOwnerStateRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(
        team_manage_batch_update_owner_state(&state, &req, "archived").await?,
    ))
}

async fn team_manage_list_owner_audits_handler(
    State(state): State<AppState>,
    Query(query): Query<TeamManageOwnerAuditQuery>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(10).clamp(1, 100);
    let total = state
        .run_history_db
        .count_team_manage_owner_audits(
            query.account_id.as_deref(),
            query.action.as_deref(),
            query.batch_job_id.as_deref(),
        )
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    let records = state
        .run_history_db
        .list_team_manage_owner_audits(
            query.account_id.as_deref(),
            query.action.as_deref(),
            query.batch_job_id.as_deref(),
            page,
            page_size,
        )
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    let total_pages = if total == 0 {
        1
    } else {
        (total + page_size - 1) / page_size
    };
    Ok(Json(TeamManageOwnerAuditListResponse {
        records,
        page,
        page_size,
        total,
        total_pages,
    }))
}

// ─── Codex Quota ─────────────────────────────────────────────────────────────

#[derive(Serialize, Deserialize, Clone)]
struct QuotaWindow {
    used_percent: f64,
    remaining_percent: f64,
    reset_after_seconds: u64,
    window_minutes: u64,
}

#[derive(Serialize, Deserialize, Clone)]
struct CodexQuota {
    five_hour: Option<QuotaWindow>,
    seven_day: Option<QuotaWindow>,
    status: String,
    model_used: Option<String>,
    error: Option<String>,
}

const CODEX_MODELS: &[&str] = &[
    "gpt-5.3-codex",
    "gpt-5.1-codex",
    "gpt-5-codex",
    "gpt-5.2-codex",
    "gpt-5",
];

async fn fetch_codex_quota(access_token: &str) -> CodexQuota {
    fetch_codex_quota_with_client(access_token, shared_http_client_10s()).await
}

async fn fetch_codex_quota_with_client(access_token: &str, client: &rquest::Client) -> CodexQuota {

    let device_id = uuid::Uuid::new_v4().to_string();

    for model in CODEX_MODELS {
        let body = serde_json::json!({
            "model": model,
            "instructions": "Reply with a single short token.",
            "input": [{"role": "user", "content": "hi"}],
            "store": false,
            "stream": true
        });

        let resp = match client
            .post("https://chatgpt.com/backend-api/codex/responses")
            .header("Accept", "*/*")
            .header("Authorization", format!("Bearer {}", access_token))
            .header("Content-Type", "application/json")
            .header("User-Agent", "codex-cli/0.104.0")
            .header("Oai-Device-Id", &device_id)
            .body(body.to_string())
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return CodexQuota {
                    five_hour: None,
                    seven_day: None,
                    status: "error".to_string(),
                    model_used: Some(model.to_string()),
                    error: Some(format!("请求失败: {e}")),
                };
            }
        };

        let status_code = resp.status().as_u16();

        // 封禁检测
        if matches!(status_code, 401 | 402 | 403) {
            return CodexQuota {
                five_hour: None,
                seven_day: None,
                status: "banned".to_string(),
                model_used: Some(model.to_string()),
                error: Some(format!("HTTP {status_code}")),
            };
        }

        // 限流检测
        if status_code == 429 {
            // 尝试从响应头解析额度信息
            let headers = resp.headers().clone();
            let parse_header = |name: &str| -> Option<f64> {
                headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|s| s.parse::<f64>().ok())
            };

            let primary_used = parse_header("x-codex-primary-used-percent");
            let primary_reset = parse_header("x-codex-primary-reset-after-seconds");
            let primary_window = parse_header("x-codex-primary-window-minutes");
            let secondary_used = parse_header("x-codex-secondary-used-percent");
            let secondary_reset = parse_header("x-codex-secondary-reset-after-seconds");
            let secondary_window = parse_header("x-codex-secondary-window-minutes");

            let five_hour = match (primary_used, primary_window) {
                (Some(used), Some(win)) if win <= 360.0 => Some(QuotaWindow {
                    used_percent: used,
                    remaining_percent: 100.0 - used,
                    reset_after_seconds: primary_reset.unwrap_or(0.0) as u64,
                    window_minutes: win as u64,
                }),
                _ => None,
            };
            let seven_day = match (secondary_used, secondary_window) {
                (Some(used), Some(win)) if win >= 10000.0 => Some(QuotaWindow {
                    used_percent: used,
                    remaining_percent: 100.0 - used,
                    reset_after_seconds: secondary_reset.unwrap_or(0.0) as u64,
                    window_minutes: win as u64,
                }),
                _ => None,
            };

            return CodexQuota {
                five_hour,
                seven_day,
                status: "rate_limited".to_string(),
                model_used: Some(model.to_string()),
                error: Some(format!("HTTP 429 限流")),
            };
        }

        // 先克隆 headers 再消费 body
        let headers = resp.headers().clone();

        // 模型不支持 → 降级
        if matches!(status_code, 400 | 404 | 422) {
            let body_text = resp.text().await.unwrap_or_default().to_lowercase();
            if body_text.contains("model not supported")
                || body_text.contains("invalid model")
                || body_text.contains("model_not_found")
            {
                tracing::debug!("[Codex Quota] 模型 {model} 不支持，尝试下一个");
                continue;
            }
        }

        // 解析响应头
        let headers = &headers;
        let parse_header = |name: &str| -> Option<f64> {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<f64>().ok())
        };

        let primary_used = parse_header("x-codex-primary-used-percent");
        let primary_reset = parse_header("x-codex-primary-reset-after-seconds");
        let primary_window = parse_header("x-codex-primary-window-minutes");

        let secondary_used = parse_header("x-codex-secondary-used-percent");
        let secondary_reset = parse_header("x-codex-secondary-reset-after-seconds");
        let secondary_window = parse_header("x-codex-secondary-window-minutes");

        let five_hour = match (primary_used, primary_window) {
            (Some(used), Some(win)) if win <= 360.0 => Some(QuotaWindow {
                used_percent: used,
                remaining_percent: 100.0 - used,
                reset_after_seconds: primary_reset.unwrap_or(0.0) as u64,
                window_minutes: win as u64,
            }),
            _ => None,
        };

        let seven_day = match (secondary_used, secondary_window) {
            (Some(used), Some(win)) if win >= 10000.0 => Some(QuotaWindow {
                used_percent: used,
                remaining_percent: 100.0 - used,
                reset_after_seconds: secondary_reset.unwrap_or(0.0) as u64,
                window_minutes: win as u64,
            }),
            _ => None,
        };

        return CodexQuota {
            five_hour,
            seven_day,
            status: if status_code >= 200 && status_code < 300 {
                "ok"
            } else {
                "error"
            }
            .to_string(),
            model_used: Some(model.to_string()),
            error: if status_code >= 200 && status_code < 300 {
                None
            } else {
                Some(format!("HTTP {status_code}"))
            },
        };
    }

    CodexQuota {
        five_hour: None,
        seven_day: None,
        status: "error".to_string(),
        model_used: None,
        error: Some("所有模型均不可用".to_string()),
    }
}

async fn team_manage_quota_handler(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!("[TeamManage] 查询额度: account_id={account_id}");

    let access_token = state
        .run_history_db
        .get_owner_token_by_account_id(&account_id)
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询 token 失败: {e}"),
            )
        })?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "找不到对应的 Owner 或 access_token"))?;

    let quota = fetch_codex_quota(&access_token).await;

    tracing::info!(
        "[TeamManage] 额度查询完成: account_id={account_id}, status={}, model={:?}",
        quota.status,
        quota.model_used
    );
    Ok(Json(quota))
}

// ─── Member Quota (子号额度查询) ─────────────────────────────────────────────

#[derive(Deserialize)]
struct MemberQuotaRequest {
    email: String,
}

/// 用 refresh_token 换 access_token
fn shared_http_client_15s() -> &'static rquest::Client {
    static CLIENT: OnceLock<rquest::Client> = OnceLock::new();
    CLIENT.get_or_init(|| {
        rquest::Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .connect_timeout(std::time::Duration::from_secs(8))
            .build()
            .unwrap_or_else(|_| rquest::Client::new())
    })
}

async fn refresh_rt_to_at(refresh_token: &str) -> Result<String, String> {
    let client = shared_http_client_15s();

    let resp = client
        .post("https://auth.openai.com/oauth/token")
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header(
            "User-Agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        )
        .form(&[
            ("grant_type", "refresh_token"),
            ("refresh_token", refresh_token),
            ("client_id", "app_EMoamEEZ73f0CkXaXp7hrann"),
            (
                "redirect_uri",
                "com.openai.chat://auth0.openai.com/ios/com.openai.chat/callback",
            ),
        ])
        .send()
        .await
        .map_err(|e| format!("刷新 token 请求失败: {e}"))?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        let preview = if body.len() > 200 {
            format!("{}...", &body[..200])
        } else {
            body
        };
        return Err(format!(
            "刷新 token 失败 HTTP {}: {}",
            status.as_u16(),
            preview
        ));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("刷新 token JSON 解析失败: {e}"))?;

    body.get("access_token")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .ok_or_else(|| "刷新 token 响应缺少 access_token".to_string())
}

/// 从 S2A 号池中搜索邮箱对应的账号，返回 (access_token, refresh_token)
async fn search_s2a_for_email(config: &AppConfig, email: &str) -> Option<(String, Option<String>)> {
    let client = shared_http_client_8s();
    let encoded_email = urlencoding::encode(email);

    for team in &config.s2a {
        let base = crate::services::S2aHttpService::normalized_api_base(&team.api_base);
        let url = format!(
            "{base}/admin/accounts?page=1&page_size=1&search={encoded_email}&timezone=Asia%2FShanghai"
        );

        let key = team.admin_key.trim();
        let bearer = if key.to_ascii_lowercase().starts_with("bearer ") {
            key.to_string()
        } else {
            format!("Bearer {key}")
        };

        let resp = match client
            .get(&url)
            .header("Accept", "application/json")
            .header("Authorization", &bearer)
            .header("X-API-Key", key)
            .header("X-Admin-Key", key)
            .send()
            .await
        {
            Ok(r) => r,
            Err(_) => continue,
        };

        if !resp.status().is_success() {
            continue;
        }

        let json: serde_json::Value = match resp.json().await {
            Ok(j) => j,
            Err(_) => continue,
        };

        let items = json
            .pointer("/data/items")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        for item in items {
            let creds = item.get("credentials");
            let at = creds
                .and_then(|c| c.get("access_token"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let rt = creds
                .and_then(|c| c.get("refresh_token"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            if let Some(at) = at {
                return Some((at, rt));
            }
        }
    }
    None
}

async fn team_manage_member_quota_handler(
    State(state): State<AppState>,
    Json(req): Json<MemberQuotaRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let email = req.email.trim().to_string();
    tracing::info!("[TeamManage] 子号额度查询: email={email}");

    // 策略1: 本地 DB 中的 refresh_token → 刷新 AT → 查额度
    if let Ok(Some(rt)) = state.run_history_db.get_email_refresh_token(&email) {
        tracing::info!("[TeamManage] 从本地 DB 获取到 RT，刷新 AT...");
        match refresh_rt_to_at(&rt).await {
            Ok(at) => {
                let quota = fetch_codex_quota(&at).await;
                tracing::info!(
                    "[TeamManage] 子号额度查询完成(本地RT): email={email}, status={}",
                    quota.status
                );
                return Ok(Json(quota));
            }
            Err(e) => {
                tracing::warn!("[TeamManage] 本地 RT 刷新失败: {e}，尝试号池兜底");
            }
        }
    }

    // 策略2: 从 S2A 号池搜索
    let config = state.config.read().await;
    if let Some((at, rt_opt)) = search_s2a_for_email(&config, &email).await {
        // 优先用 RT 刷新 AT（号池中的 AT 可能过期）
        if let Some(rt) = &rt_opt {
            if let Ok(fresh_at) = refresh_rt_to_at(rt).await {
                let quota = fetch_codex_quota(&fresh_at).await;
                tracing::info!(
                    "[TeamManage] 子号额度查询完成(号池RT刷新): email={email}, status={}",
                    quota.status
                );
                return Ok(Json(quota));
            }
        }
        // 直接用号池 AT
        let quota = fetch_codex_quota(&at).await;
        tracing::info!(
            "[TeamManage] 子号额度查询完成(号池AT): email={email}, status={}",
            quota.status
        );
        return Ok(Json(quota));
    }

    // 区分：外部域名 vs 号池未找到
    let email_lower = email.to_lowercase();
    let is_pool_domain = {
        let cfg = state.config.read().await;
        cfg.email_domains
            .iter()
            .chain(cfg.chatgpt_mail_domains.iter())
            .any(|d| email_lower.ends_with(&d.to_lowercase()))
    };
    let reason = if is_pool_domain {
        format!("{email} 属于号池域名但未在号池中找到（可能已移除）")
    } else {
        let domain = email_lower.rsplit('@').next().unwrap_or("unknown");
        format!("{email} 为外部域名(@{domain})，非号池管理范围")
    };

    Err(error_json(
        StatusCode::NOT_FOUND,
        &reason,
    ))
}

// ─── Team Manage: 邀请并入库 ─────────────────────────────────────────────────

#[derive(Deserialize, Clone)]
struct TeamManageInviteRequest {
    s2a_team: String,
    invite_count: Option<usize>,
}

#[derive(Serialize, Clone)]
struct TeamManageInviteCreated {
    task_id: String,
    invite_count: usize,
    message: String,
}

async fn create_team_manage_invite_task(
    state: &AppState,
    account_id: &str,
    req: TeamManageInviteRequest,
) -> Result<TeamManageInviteCreated, (StatusCode, Json<ErrorResponse>)> {
    tracing::info!(
        "[TeamManage] 邀请并入库: account_id={account_id}, s2a_team={}",
        req.s2a_team
    );

    let (owner_db_id, owner_email, access_token) = state
        .run_history_db
        .get_owner_info_by_account_id(account_id)
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询 owner 失败: {e}"),
            )
        })?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "找不到对应的 Owner 或 access_token"))?;

    let cfg = state.config.read().await;
    let invite_runtime = cfg.invite_runtime();
    let invite_count = req
        .invite_count
        .unwrap_or(invite_runtime.default_invite_count)
        .clamp(1, 25);
    let available_teams = cfg.effective_s2a_configs();
    let team = available_teams
        .iter()
        .find(|candidate| candidate.name == req.s2a_team)
        .cloned()
        .ok_or_else(|| {
            error_json(
                StatusCode::BAD_REQUEST,
                &format!("号池 '{}' 不存在", req.s2a_team),
            )
        })?;
    let config_snapshot = cfg.clone();
    drop(cfg);

    let task_id = uuid::Uuid::new_v4().to_string()[..8].to_string();
    let seeds: Vec<crate::models::AccountSeed> = (0..invite_count)
        .map(|_| crate::util::generate_account_seed(&config_snapshot.email_domains))
        .collect();

    let new_task = crate::db::NewInviteTask {
        id: task_id.clone(),
        upload_id: format!("team-manage-{account_id}"),
        owner_email: owner_email.clone(),
        owner_account_id: account_id.to_string(),
        s2a_team: Some(team.name.clone()),
        invite_count,
        created_at: crate::util::beijing_now().to_rfc3339(),
    };
    let _ = state.run_history_db.enqueue_insert_invite_task(new_task);

    let email_inserts: Vec<crate::db::InviteEmailInsert> = seeds
        .iter()
        .map(|seed| crate::db::InviteEmailInsert {
            email: seed.account.clone(),
            password: seed.password.clone(),
        })
        .collect();
    let _ = state
        .run_history_db
        .enqueue_insert_invite_emails(task_id.clone(), email_inserts);

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let owner = crate::invite::TeamOwner {
        email: owner_email,
        account_id: account_id.to_string(),
        access_token,
        expires: None,
    };
    let progress = Arc::new(crate::models::InviteProgress::new());
    let db = state.run_history_db.clone();
    let invite_cfg = config_snapshot.invite_runtime();
    let max_members = invite_cfg.default_invite_count;
    let task_id_clone = task_id.clone();

    tokio::spawn(async move {
        crate::invite::run_invite_workflow(
            task_id_clone,
            owner_db_id,
            owner,
            seeds,
            invite_cfg,
            config_snapshot,
            Some(team),
            true,
            db,
            progress,
            max_members,
        )
        .await;
    });

    tracing::info!(
        "[TeamManage] 邀请任务已创建: task_id={task_id}, invite_count={invite_count}, max_members={max_members}"
    );

    Ok(TeamManageInviteCreated {
        task_id,
        invite_count,
        message: format!(
            "邀请任务已创建，将邀请 {} 个成员并入库到 {}",
            invite_count, req.s2a_team
        ),
    })
}

async fn team_manage_invite_handler(
    State(state): State<AppState>,
    Path(account_id): Path<String>,
    Json(req): Json<TeamManageInviteRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let created = create_team_manage_invite_task(&state, &account_id, req).await?;

    Ok((StatusCode::CREATED, Json(serde_json::json!(created))))
}

async fn team_manage_dashboard_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let _ = state
        .run_history_db
        .sync_owner_registry_from_invite_owners()
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("同步 owner_registry 失败: {e}"),
            )
        })?;
    let summary = state
        .run_history_db
        .get_team_manage_dashboard_summary()
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询 TeamManage 仪表盘失败: {e}"),
            )
        })?;

    Ok(Json(TeamManageDashboardResponse {
        total_owners: summary.total_owners,
        active_owners: summary.active_owners,
        banned_owners: summary.banned_owners,
        expired_owners: summary.expired_owners,
        quarantined_owners: summary.quarantined_owners,
        owners_with_slots: summary.owners_with_slots,
        owners_with_banned_members: summary.owners_with_banned_members,
        fresh_cache_owners: summary.health_cache_fresh,
        stale_cache_owners: summary.health_cache_stale,
        running_batch_jobs: summary.running_batch_jobs,
        checked_recently: summary.health_cache_fresh,
    }))
}

fn team_manage_batch_request_fingerprint(req: &TeamManageBatchInviteRequest) -> String {
    let mut ids = req.account_ids.clone();
    ids.sort();
    format!(
        "{}|{}|{:?}|{:?}|{}|{}|{}|{}",
        req.s2a_team,
        ids.join(","),
        req.strategy,
        req.fixed_count,
        req.skip_banned,
        req.skip_expired,
        req.skip_quarantined,
        req.only_with_slots
    )
}

fn team_manage_batch_job_s2a_team(payload_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(payload_json)
        .ok()
        .and_then(|payload| {
            payload
                .get("s2a_team")
                .and_then(|value| value.as_str())
                .map(|value| value.to_string())
        })
}

fn team_manage_batch_job_item_from_record(
    item: &crate::db::TeamManageBatchItemRecord,
) -> TeamManageBatchJobItem {
    TeamManageBatchJobItem {
        account_id: item.account_id.clone(),
        status: item.status.clone(),
        invite_count: item
            .result_json
            .as_deref()
            .and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok())
            .and_then(|payload| payload.get("invite_count").and_then(|value| value.as_u64()))
            .unwrap_or(0) as usize,
        child_task_id: item.child_task_id.clone(),
        message: item.result_json.as_deref().and_then(|value| {
            serde_json::from_str::<serde_json::Value>(value)
                .ok()
                .and_then(|payload| {
                    payload
                        .get("message")
                        .and_then(|value| value.as_str())
                        .map(|value| value.to_string())
                })
                .or_else(|| Some(value.to_string()))
        }),
        error: item.error.clone(),
        updated_at: item
            .finished_at
            .clone()
            .or_else(|| item.started_at.clone())
            .unwrap_or_default(),
    }
}

fn team_manage_batch_job_from_records(
    job: &crate::db::TeamManageBatchJobRecord,
    items: &[crate::db::TeamManageBatchItemRecord],
) -> TeamManageBatchJob {
    TeamManageBatchJob {
        job_id: job.job_id.clone(),
        job_type: job.job_type.clone(),
        status: job.status.clone(),
        scope: job.scope.clone().unwrap_or_else(|| "manual".to_string()),
        s2a_team: team_manage_batch_job_s2a_team(&job.payload_json),
        total_count: job.total_count,
        success_count: job.success_count,
        failed_count: job.failed_count,
        skipped_count: job.skipped_count,
        created_at: job.created_at.clone(),
        started_at: job.started_at.clone(),
        finished_at: job.finished_at.clone(),
        items: items
            .iter()
            .map(team_manage_batch_job_item_from_record)
            .collect(),
    }
}

async fn team_manage_get_batch_job_from_db(
    state: &AppState,
    job_id: &str,
) -> Result<Option<TeamManageBatchJob>, (StatusCode, Json<ErrorResponse>)> {
    let job = state
        .run_history_db
        .get_team_manage_batch_job(job_id)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    let Some(job) = job else {
        return Ok(None);
    };
    let items = state
        .run_history_db
        .get_team_manage_batch_job_items(job_id)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    Ok(Some(team_manage_batch_job_from_records(&job, &items)))
}

async fn team_manage_sync_batch_job_cache(state: &AppState, job_id: &str) {
    let Some(redis_cache) = state.redis_cache.as_ref() else {
        return;
    };
    match team_manage_get_batch_job_from_db(state, job_id).await {
        Ok(Some(job)) => {
            if let Err(error) = redis_cache
                .set_json(
                    &redis_cache.batch_job_key(job_id),
                    &job,
                    redis_cache.batch_progress_ttl_secs(),
                )
                .await
            {
                tracing::warn!(
                    "[TeamManage] 写入 Redis batch job cache 失败: job_id={}, error={}",
                    job_id,
                    error
                );
            }
        }
        Ok(None) => {
            let _ = redis_cache.delete(&redis_cache.batch_job_key(job_id)).await;
        }
        Err(_) => {
            tracing::warn!("[TeamManage] 同步 Redis batch job cache 失败: job_id={}", job_id);
        }
    }
}

async fn team_manage_live_member_count(
    state: &AppState,
    account_id: &str,
) -> Result<usize, String> {
    let access_token = state
        .run_history_db
        .get_owner_token_by_account_id(account_id)
        .map_err(|e| format!("查询 token 失败: {e}"))?
        .ok_or_else(|| "找不到对应的 Owner 或 access_token".to_string())?;
    let members = fetch_chatgpt_members(account_id, &access_token).await?;
    Ok(members
        .into_iter()
        .filter(|member| member.role != "account-owner")
        .count())
}

async fn team_manage_get_batch_job(
    state: &AppState,
    job_id: &str,
) -> Result<Option<TeamManageBatchJob>, (StatusCode, Json<ErrorResponse>)> {
    if let Some(redis_cache) = state.redis_cache.as_ref() {
        match redis_cache
            .get_json::<TeamManageBatchJob>(&redis_cache.batch_job_key(job_id))
            .await
        {
            Ok(Some(job)) => return Ok(Some(job)),
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(
                    "[TeamManage] 读取 Redis batch job cache 失败: job_id={}, error={}",
                    job_id,
                    error
                );
            }
        }
    }
    let job = team_manage_get_batch_job_from_db(state, job_id).await?;
    if job.is_some() {
        team_manage_sync_batch_job_cache(state, job_id).await;
    }
    Ok(job)
}

async fn team_manage_update_batch_job_item(
    state: &AppState,
    job_id: &str,
    account_id: &str,
    status: &str,
    invite_count: usize,
    child_task_id: Option<String>,
    message: Option<String>,
    error: Option<String>,
) {
    let items = match state.run_history_db.get_team_manage_batch_job_items(job_id) {
        Ok(items) => items,
        Err(_) => return,
    };
    let Some(item) = items.iter().find(|item| item.account_id == account_id) else {
        return;
    };
    let now = crate::util::beijing_now().to_rfc3339();
    let result_json = serde_json::json!({
        "message": message,
        "invite_count": invite_count,
    })
    .to_string();
    let _ = state.run_history_db.update_team_manage_batch_item(
        item.id,
        crate::db::TeamManageBatchItemUpdate {
            status: Some(status.to_string()),
            child_task_id,
            result_json: Some(result_json),
            error,
            started_at: if item.started_at.is_none() {
                Some(now.clone())
            } else {
                None
            },
            finished_at: if matches!(status, "completed" | "failed" | "skipped") {
                Some(now.clone())
            } else {
                None
            },
        },
    );

    let refreshed_items = match state.run_history_db.get_team_manage_batch_job_items(job_id) {
        Ok(items) => items,
        Err(_) => return,
    };
    let success_count = refreshed_items
        .iter()
        .filter(|item| item.status == "completed")
        .count();
    let failed_count = refreshed_items
        .iter()
        .filter(|item| item.status == "failed")
        .count();
    let skipped_count = refreshed_items
        .iter()
        .filter(|item| item.status == "skipped")
        .count();
    let all_finished = refreshed_items
        .iter()
        .all(|item| matches!(item.status.as_str(), "completed" | "failed" | "skipped"));
    let next_status = if all_finished {
        if failed_count > 0 && success_count > 0 {
            "partial_success".to_string()
        } else if failed_count > 0 {
            "failed".to_string()
        } else {
            "completed".to_string()
        }
    } else {
        "running".to_string()
    };
    let existing_job = state.run_history_db.get_team_manage_batch_job(job_id).ok().flatten();
    let _ = state.run_history_db.update_team_manage_batch_job(
        job_id.to_string(),
        crate::db::TeamManageBatchJobUpdate {
            status: Some(next_status),
            total_count: Some(refreshed_items.len()),
            success_count: Some(success_count),
            failed_count: Some(failed_count),
            skipped_count: Some(skipped_count),
            started_at: if existing_job
                .as_ref()
                .and_then(|job| job.started_at.as_ref())
                .is_none()
            {
                Some(now.clone())
            } else {
                None
            },
            finished_at: if all_finished { Some(now) } else { None },
            error: None,
        },
    );
    team_manage_sync_batch_job_cache(state, job_id).await;
}

async fn team_manage_create_or_reuse_batch_job(
    state: &AppState,
    req: &TeamManageBatchInviteRequest,
) -> Result<(String, bool), (StatusCode, Json<ErrorResponse>)> {
    let fingerprint = team_manage_batch_request_fingerprint(req);
    if let Some(job) = state
        .run_history_db
        .get_team_manage_batch_job_by_fingerprint(&fingerprint)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?
    {
        return Ok((job.job_id, false));
    }
    let job_id = format!("batch-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    team_manage_insert_batch_job(state, req, &job_id, Some(fingerprint))?;
    team_manage_sync_batch_job_cache(state, &job_id).await;
    Ok((job_id, true))
}

fn team_manage_insert_batch_job(
    state: &AppState,
    req: &TeamManageBatchInviteRequest,
    job_id: &str,
    fingerprint: Option<String>,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let now = crate::util::beijing_now().to_rfc3339();
    let payload_json = serde_json::to_string(req).map_err(|e| {
        error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("序列化批量任务失败: {e}"),
        )
    })?;
    state
        .run_history_db
        .insert_team_manage_batch_job(crate::db::NewTeamManageBatchJob {
            job_id: job_id.to_string(),
            job_type: "batch_invite".to_string(),
            scope: Some(req.scope.clone().unwrap_or_else(|| "manual".to_string())),
            status: "pending".to_string(),
            payload_json,
            total_count: req.account_ids.len(),
            success_count: 0,
            failed_count: 0,
            skipped_count: 0,
            created_by: None,
            created_at: now.clone(),
            started_at: None,
            finished_at: None,
            error: None,
            fingerprint,
        })
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    state
        .run_history_db
        .insert_team_manage_batch_items(
            req.account_ids
                .iter()
                .map(|account_id| crate::db::NewTeamManageBatchItem {
                    job_id: job_id.to_string(),
                    account_id: account_id.clone(),
                    item_type: "invite".to_string(),
                    status: "pending".to_string(),
                    child_task_id: None,
                    result_json: None,
                    error: None,
                    started_at: None,
                    finished_at: None,
                })
                .collect(),
        )
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    Ok(())
}

async fn team_manage_batch_invite_handler(
    State(state): State<AppState>,
    Json(req): Json<TeamManageBatchInviteRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut req = req;
    req.account_ids = team_manage_resolve_batch_account_ids(
        &state,
        &req.account_ids,
        req.scope.as_deref(),
        Some(&req.filters),
    )
    .await?;
    if req.account_ids.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "account_ids 不能为空"));
    }

    let (job_id, created) = team_manage_create_or_reuse_batch_job(&state, &req).await?;
    if !created {
        let existing = team_manage_get_batch_job(&state, &job_id)
            .await?
            .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "批量任务不存在"))?;
        let accepted = existing
            .items
            .iter()
            .filter(|item| item.status != "skipped")
            .count();
        let skipped = existing
            .items
            .iter()
            .filter(|item| item.status == "skipped")
            .count();
        return Ok((
            StatusCode::ACCEPTED,
            Json(serde_json::json!(TeamManageBatchInviteResponse {
                job_id,
                accepted,
                skipped,
                message: "复用已有批量任务".to_string(),
            })),
        ));
    }

    let accepted = req.account_ids.len();
    let req_for_task = req.clone();
    let state_clone = state.clone();
    let job_id_clone = job_id.clone();
    tokio::spawn(async move {
        let max_members = {
            let cfg = state_clone.config.read().await;
            team_manage_max_members(&cfg)
        };
        let cached_map = state_clone
            .run_history_db
            .get_all_owner_health()
            .map(team_manage_cached_results_map)
            .unwrap_or_default();
        // 批量获取 owner_registry 状态，用于跳过 seat_limited
        let registry_state_map: std::collections::HashMap<String, String> = state_clone
            .run_history_db
            .get_owner_registry_states_by_account_ids(&req_for_task.account_ids)
            .unwrap_or_default()
            .into_iter()
            .collect();

        crate::log_broadcast::broadcast_log(&format!(
            "[Team管理] 批量邀请任务开始: job={}, {} 个 owner",
            job_id_clone,
            req_for_task.account_ids.len()
        ));

        for account_id in req_for_task.account_ids {
            if req_for_task.skip_banned
                && cached_map
                    .get(&account_id)
                    .map(|cached| cached.owner_status == "banned")
                    .unwrap_or(false)
            {
                crate::log_broadcast::broadcast_log(&format!("[Team管理] 跳过封禁 owner: {}", account_id));
                team_manage_update_batch_job_item(
                    &state_clone,
                    &job_id_clone,
                    &account_id,
                    "skipped",
                    0,
                    None,
                    Some("已按规则跳过封禁 owner".to_string()),
                    None,
                )
                .await;
                continue;
            }
            // 跳过 seat_limited 状态的 owner（free trial 席位上限）
            if registry_state_map.get(&account_id).map(|s| s.as_str()) == Some("seat_limited") {
                crate::log_broadcast::broadcast_log(&format!("[Team管理] 跳过席位已满 owner: {}", account_id));
                team_manage_update_batch_job_item(
                    &state_clone,
                    &job_id_clone,
                    &account_id,
                    "skipped",
                    0,
                    None,
                    Some("席位已满(free trial)，已自动跳过".to_string()),
                    None,
                )
                .await;
                continue;
            }
            if req_for_task.skip_expired || req_for_task.skip_quarantined {
                crate::log_broadcast::broadcast_log(&format!("[Team管理] 跳过过期/隔离 owner: {}", account_id));
                team_manage_update_batch_job_item(
                    &state_clone,
                    &job_id_clone,
                    &account_id,
                    "skipped",
                    0,
                    None,
                    Some("当前轮次未接入 owner_registry，已跳过过期/隔离判断".to_string()),
                    None,
                )
                .await;
                continue;
            }

            let strategy = req_for_task
                .strategy
                .clone()
                .unwrap_or_else(|| "fill_to_limit".to_string());
            let invite_count = if strategy == "fixed_count" {
                req_for_task.fixed_count.unwrap_or(1).clamp(1, 25)
            } else {
                match team_manage_live_member_count(&state_clone, &account_id).await {
                    Ok(member_count) => max_members.saturating_sub(member_count),
                    Err(error) => {
                        // 检测席位上限错误，标记 owner 为 seat_limited
                        let err_lower = error.to_ascii_lowercase();
                        if err_lower.contains("maximum number of seats")
                            || err_lower.contains("seats allowed")
                            || err_lower.contains("seat limit")
                        {
                            let now = chrono::Utc::now().to_rfc3339();
                            let _ = state_clone.run_history_db.batch_update_owner_registry_state(
                                &[account_id.clone()],
                                "seat_limited",
                                Some("free_trial_seat_limit"),
                                &now,
                            );
                            crate::log_broadcast::broadcast_log(&format!(
                                "[Team管理] 已标记 owner {} 为 seat_limited", account_id
                            ));
                        }
                        team_manage_update_batch_job_item(
                            &state_clone,
                            &job_id_clone,
                            &account_id,
                            "failed",
                            0,
                            None,
                            None,
                            Some(error),
                        )
                        .await;
                        continue;
                    }
                }
            };

            if req_for_task.only_with_slots && invite_count == 0 {
                crate::log_broadcast::broadcast_log(&format!("[Team管理] 跳过无空位 owner: {}", account_id));
                team_manage_update_batch_job_item(
                    &state_clone,
                    &job_id_clone,
                    &account_id,
                    "skipped",
                    0,
                    None,
                    Some("当前 owner 已无空位".to_string()),
                    None,
                )
                .await;
                continue;
            }

            if invite_count == 0 {
                crate::log_broadcast::broadcast_log(&format!("[Team管理] 跳过未生成邀请: {}", account_id));
                team_manage_update_batch_job_item(
                    &state_clone,
                    &job_id_clone,
                    &account_id,
                    "skipped",
                    0,
                    None,
                    Some("未生成邀请任务".to_string()),
                    None,
                )
                .await;
                continue;
            }

            match create_team_manage_invite_task(
                &state_clone,
                &account_id,
                TeamManageInviteRequest {
                    s2a_team: req_for_task.s2a_team.clone(),
                    invite_count: Some(invite_count),
                },
            )
            .await
            {
                Ok(created) => {
                    crate::log_broadcast::broadcast_log(&format!(
                        "[Team管理] 邀请任务已派发: {} (task={}, 需邀请{}人)",
                        account_id, created.task_id, created.invite_count
                    ));
                    team_manage_update_batch_job_item(
                        &state_clone,
                        &job_id_clone,
                        &account_id,
                        "completed",
                        created.invite_count,
                        Some(created.task_id),
                        Some(created.message),
                        None,
                    )
                    .await;
                }
                Err((status, body)) => {
                    let error_msg = format!("{} {}", status.as_u16(), body.0.error);
                    crate::log_broadcast::broadcast_log(&format!(
                        "[Team管理][ERR] 邀请失败: {} - {}",
                        account_id, error_msg
                    ));
                    // 检测席位上限错误，标记 owner 为 seat_limited
                    let err_lower = error_msg.to_ascii_lowercase();
                    if err_lower.contains("maximum number of seats")
                        || err_lower.contains("seats allowed")
                        || err_lower.contains("seat limit")
                    {
                        let now = chrono::Utc::now().to_rfc3339();
                        let _ = state_clone.run_history_db.batch_update_owner_registry_state(
                            &[account_id.clone()],
                            "seat_limited",
                            Some("free_trial_seat_limit"),
                            &now,
                        );
                        crate::log_broadcast::broadcast_log(&format!(
                            "[Team管理] 已标记 owner {} 为 seat_limited", account_id
                        ));
                    }
                    team_manage_update_batch_job_item(
                        &state_clone,
                        &job_id_clone,
                        &account_id,
                        "failed",
                        invite_count,
                        None,
                        None,
                        Some(error_msg),
                    )
                    .await;
                }
            }
        }
        crate::log_broadcast::broadcast_log(&format!("[Team管理] 批量邀请任务完成: job={}", job_id_clone));
    });

    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!(TeamManageBatchInviteResponse {
            job_id,
            accepted,
            skipped: 0,
            message: "批量邀请任务已创建".to_string(),
        })),
    ))
}

async fn team_manage_list_batch_jobs_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut jobs = state
        .run_history_db
        .list_team_manage_batch_jobs(None, Some(100))
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?
        .iter()
        .map(team_manage_batch_job_summary)
        .collect::<Vec<_>>();
    jobs.sort_by(|left, right| right.created_at.cmp(&left.created_at));
    Ok(Json(TeamManageBatchJobListResponse { jobs }))
}

async fn team_manage_get_batch_job_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = team_manage_get_batch_job(&state, &job_id)
        .await?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "批量任务不存在"))?;
    Ok(Json(job))
}

async fn team_manage_get_batch_job_items_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = team_manage_get_batch_job(&state, &job_id)
        .await?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "批量任务不存在"))?;
    Ok(Json(TeamManageBatchJobItemsResponse { items: job.items }))
}

fn team_manage_batch_retry_mode(mode: Option<&str>) -> &'static str {
    match mode.unwrap_or("all").trim().to_ascii_lowercase().as_str() {
        "network" => "network",
        "recoverable" => "recoverable",
        _ => "all",
    }
}

fn team_manage_is_network_retryable_error(error: &str) -> bool {
    let normalized = error.to_ascii_lowercase();
    [
        "timeout",
        "timed out",
        "connection",
        "connect",
        "refused",
        "reset",
        "broken pipe",
        "dns",
        "temporary failure",
        "temporarily unavailable",
        "service unavailable",
        "gateway timeout",
        "bad gateway",
        "request timeout",
        "请求超时",
        "连接失败",
        "连接超时",
        "连接被拒绝",
        "连接重置",
        "服务不可用",
        "网关超时",
        "网络错误",
        "502",
        "503",
        "504",
        "408",
    ]
    .iter()
    .any(|keyword| normalized.contains(keyword))
}

fn team_manage_is_recoverable_error(error: &str) -> bool {
    if team_manage_is_network_retryable_error(error) {
        return true;
    }
    let normalized = error.to_ascii_lowercase();
    [
        "429",
        "rate limit",
        "too many requests",
        "retry later",
        "retryable",
        "temporarily busy",
        "temporarily blocked",
        "conflict",
        "concurrent",
        "busy",
        "稍后重试",
        "稍后再试",
        "限流",
        "频率",
        "并发",
        "冲突",
        "重试",
    ]
    .iter()
    .any(|keyword| normalized.contains(keyword))
}

fn team_manage_should_retry_batch_item(
    item: &crate::db::TeamManageBatchItemRecord,
    retry_mode: &str,
) -> bool {
    match retry_mode {
        "network" => item
            .error
            .as_deref()
            .map(team_manage_is_network_retryable_error)
            .unwrap_or(false),
        "recoverable" => item
            .error
            .as_deref()
            .map(team_manage_is_recoverable_error)
            .unwrap_or(false),
        _ => true,
    }
}

async fn team_manage_retry_failed_batch_items_handler(
    State(state): State<AppState>,
    Path(job_id): Path<String>,
    Json(req): Json<TeamManageBatchRetryRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let job = state
        .run_history_db
        .get_team_manage_batch_job(&job_id)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, "批量任务不存在"))?;
    if job.job_type != "batch_invite" {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            "当前仅支持重试批量邀请任务",
        ));
    }
    let retry_mode = team_manage_batch_retry_mode(req.retry_mode.as_deref());
    let failed_items = state
        .run_history_db
        .get_team_manage_batch_items_by_status(&job_id, "failed")
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;
    let failed_items = failed_items
        .into_iter()
        .filter(|item| team_manage_should_retry_batch_item(item, retry_mode))
        .collect::<Vec<_>>();
    if failed_items.is_empty() {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            &format!("当前重试模式 {retry_mode} 下没有可重试的失败子项"),
        ));
    }
    let mut payload: TeamManageBatchInviteRequest = serde_json::from_str(&job.payload_json)
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("解析任务 payload 失败: {e}")))?;
    payload.account_ids = failed_items
        .iter()
        .map(|item| item.account_id.clone())
        .collect();
    payload.scope = Some("manual".to_string());
    payload.filters = TeamManageBatchFilters::default();

    let retry_job_id = format!("batch-{}", &uuid::Uuid::new_v4().to_string()[..8]);
    team_manage_insert_batch_job(&state, &payload, &retry_job_id, None)?;
    team_manage_sync_batch_job_cache(&state, &retry_job_id).await;

    let retry_req = payload.clone();
    let state_clone = state.clone();
    let retry_job_id_clone = retry_job_id.clone();
    tokio::spawn(async move {
        let max_members = {
            let cfg = state_clone.config.read().await;
            team_manage_max_members(&cfg)
        };
        let cached_map = state_clone
            .run_history_db
            .get_all_owner_health()
            .map(team_manage_cached_results_map)
            .unwrap_or_default();

        for account_id in retry_req.account_ids {
            if retry_req.skip_banned
                && cached_map
                    .get(&account_id)
                    .map(|cached| cached.owner_status == "banned")
                    .unwrap_or(false)
            {
                team_manage_update_batch_job_item(
                    &state_clone,
                    &retry_job_id_clone,
                    &account_id,
                    "skipped",
                    0,
                    None,
                    Some("已按规则跳过封禁 owner".to_string()),
                    None,
                )
                .await;
                continue;
            }

            let strategy = retry_req
                .strategy
                .clone()
                .unwrap_or_else(|| "fill_to_limit".to_string());
            let invite_count = if strategy == "fixed_count" {
                retry_req.fixed_count.unwrap_or(1).clamp(1, 25)
            } else {
                match team_manage_live_member_count(&state_clone, &account_id).await {
                    Ok(member_count) => max_members.saturating_sub(member_count),
                    Err(error) => {
                        team_manage_update_batch_job_item(
                            &state_clone,
                            &retry_job_id_clone,
                            &account_id,
                            "failed",
                            0,
                            None,
                            None,
                            Some(error),
                        )
                        .await;
                        continue;
                    }
                }
            };

            if invite_count == 0 {
                team_manage_update_batch_job_item(
                    &state_clone,
                    &retry_job_id_clone,
                    &account_id,
                    "skipped",
                    0,
                    None,
                    Some("未生成邀请任务".to_string()),
                    None,
                )
                .await;
                continue;
            }

            match create_team_manage_invite_task(
                &state_clone,
                &account_id,
                TeamManageInviteRequest {
                    s2a_team: retry_req.s2a_team.clone(),
                    invite_count: Some(invite_count),
                },
            )
            .await
            {
                Ok(created) => {
                    team_manage_update_batch_job_item(
                        &state_clone,
                        &retry_job_id_clone,
                        &account_id,
                        "completed",
                        created.invite_count,
                        Some(created.task_id),
                        Some(created.message),
                        None,
                    )
                    .await;
                }
                Err((status, body)) => {
                    team_manage_update_batch_job_item(
                        &state_clone,
                        &retry_job_id_clone,
                        &account_id,
                        "failed",
                        invite_count,
                        None,
                        None,
                        Some(format!("{} {}", status.as_u16(), body.0.error)),
                    )
                    .await;
                }
            }
        }
    });

    Ok(Json(TeamManageBatchRetryResponse {
        job_id: retry_job_id,
        retried: failed_items.len(),
        retry_mode: retry_mode.to_string(),
        message: format!(
            "已创建重试任务，按 {retry_mode} 模式重试 {} 个失败子项",
            failed_items.len()
        ),
    }))
}

// ─── Team Manage 代理设置 ────────────────────────────────────────────────────

async fn team_manage_get_proxy_settings(
    State(state): State<AppState>,
) -> impl IntoResponse {
    let cfg = state.config.read().await;
    Json(serde_json::json!({
        "kick_use_proxy": cfg.team_manage_kick_use_proxy.unwrap_or(false),
        "check_use_proxy": cfg.team_manage_check_use_proxy.unwrap_or(false),
    }))
}

async fn team_manage_set_proxy_settings(
    State(state): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let mut cfg = state.config.write().await;
    if let Some(v) = body.get("kick_use_proxy").and_then(|v| v.as_bool()) {
        cfg.team_manage_kick_use_proxy = Some(v);
    }
    if let Some(v) = body.get("check_use_proxy").and_then(|v| v.as_bool()) {
        cfg.team_manage_check_use_proxy = Some(v);
    }
    auto_save(&cfg, &state.config_path);
    Json(serde_json::json!({
        "kick_use_proxy": cfg.team_manage_kick_use_proxy.unwrap_or(false),
        "check_use_proxy": cfg.team_manage_check_use_proxy.unwrap_or(false),
    }))
}

// ─── Health cache: 读取持久化的检查结果 ─────────────────────────────────────

async fn team_manage_health_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let rows = state
        .run_history_db
        .get_all_owner_health()
        .map_err(|e| error_json(StatusCode::INTERNAL_SERVER_ERROR, &format!("{e}")))?;

    let records: Vec<serde_json::Value> = rows
        .into_iter()
        .map(|(account_id, owner_status, members_json, checked_at)| {
            let members: serde_json::Value = members_json
                .and_then(|s| serde_json::from_str(&s).ok())
                .unwrap_or(serde_json::json!([]));
            serde_json::json!({
                "account_id": account_id,
                "owner_status": owner_status,
                "members": members,
                "checked_at": checked_at,
            })
        })
        .collect();

    Ok(Json(serde_json::json!({ "records": records })))
}

// ─── Batch check: 并发检查当前页 Owner ───────────────────────────────────────

#[derive(Deserialize)]
struct BatchCheckRequest {
    account_ids: Vec<String>,
    #[serde(default = "default_concurrency")]
    concurrency: usize,
    #[serde(default)]
    force_refresh: bool,
    #[serde(default = "default_prefer_cache")]
    prefer_cache: bool,
    scope: Option<String>,
    #[serde(default)]
    filters: TeamManageBatchFilters,
}

fn default_concurrency() -> usize {
    5
}

fn default_prefer_cache() -> bool {
    true
}

#[derive(Serialize)]
struct BatchCheckResponse {
    results: Vec<OwnerHealthResult>,
    cache_hits: usize,
    cache_misses: usize,
    stale_returns: usize,
    scheduled_refreshes: usize,
    scope: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct MemberHealthInfo {
    email: String,
    name: Option<String>,
    status: String,
    seven_day_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_id: Option<String>,
}

#[derive(Serialize, Deserialize, Clone)]
struct OwnerHealthResult {
    account_id: String,
    owner_status: String,
    members: Vec<MemberHealthInfo>,
    checked_at: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    owner_quota: Option<CodexQuota>,
}

#[derive(Serialize, Deserialize, Clone)]
struct TeamManageBatchJobItem {
    account_id: String,
    status: String,
    invite_count: usize,
    child_task_id: Option<String>,
    message: Option<String>,
    error: Option<String>,
    updated_at: String,
}

#[derive(Serialize, Deserialize, Clone)]
struct TeamManageBatchJob {
    job_id: String,
    job_type: String,
    status: String,
    scope: String,
    s2a_team: Option<String>,
    total_count: usize,
    success_count: usize,
    failed_count: usize,
    skipped_count: usize,
    created_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
    items: Vec<TeamManageBatchJobItem>,
}

#[derive(Serialize)]
struct TeamManageBatchJobSummary {
    job_id: String,
    job_type: String,
    status: String,
    scope: String,
    s2a_team: Option<String>,
    total_count: usize,
    success_count: usize,
    failed_count: usize,
    skipped_count: usize,
    created_at: String,
    started_at: Option<String>,
    finished_at: Option<String>,
}

#[derive(Serialize)]
struct TeamManageBatchJobListResponse {
    jobs: Vec<TeamManageBatchJobSummary>,
}

#[derive(Serialize)]
struct TeamManageBatchJobItemsResponse {
    items: Vec<TeamManageBatchJobItem>,
}

#[derive(Deserialize, Serialize, Clone)]
struct TeamManageBatchInviteRequest {
    account_ids: Vec<String>,
    s2a_team: String,
    strategy: Option<String>,
    fixed_count: Option<usize>,
    scope: Option<String>,
    #[serde(default)]
    filters: TeamManageBatchFilters,
    #[serde(default)]
    skip_banned: bool,
    #[serde(default)]
    skip_expired: bool,
    #[serde(default)]
    skip_quarantined: bool,
    #[serde(default)]
    only_with_slots: bool,
}

#[derive(Serialize)]
struct TeamManageBatchInviteResponse {
    job_id: String,
    accepted: usize,
    skipped: usize,
    message: String,
}

#[derive(Deserialize)]
struct TeamManageBatchOwnerStateRequest {
    account_ids: Vec<String>,
    scope: Option<String>,
    #[serde(default)]
    filters: TeamManageBatchFilters,
    reason: Option<String>,
}

#[derive(Serialize)]
struct TeamManageBatchOwnerStateResponse {
    affected: usize,
    state: String,
    message: String,
}

#[derive(Deserialize)]
struct TeamManageOwnerAuditQuery {
    account_id: Option<String>,
    action: Option<String>,
    batch_job_id: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
}

#[derive(Serialize)]
struct TeamManageOwnerAuditListResponse {
    records: Vec<crate::db::TeamManageOwnerAuditRecord>,
    page: usize,
    page_size: usize,
    total: usize,
    total_pages: usize,
}

#[derive(Deserialize, Default)]
struct TeamManageBatchRetryRequest {
    retry_mode: Option<String>,
}

#[derive(Serialize)]
struct TeamManageBatchRetryResponse {
    job_id: String,
    retried: usize,
    retry_mode: String,
    message: String,
}

async fn team_manage_batch_check_handler(
    State(state): State<AppState>,
    Json(req): Json<BatchCheckRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let BatchCheckRequest {
        account_ids,
        concurrency,
        force_refresh,
        prefer_cache,
        scope,
        filters,
    } = req;
    let account_ids = team_manage_resolve_batch_account_ids(
        &state,
        &account_ids,
        scope.as_deref(),
        Some(&filters),
    )
    .await?;
    let concurrency = concurrency.max(1);
    tracing::info!(
        "[TeamManage] 批量检查: {} 个 owner, 并发={}",
        account_ids.len(),
        concurrency
    );
    crate::log_broadcast::broadcast_log(&format!(
        "[Team管理] 批量检查: {} 个 owner, 并发={}",
        account_ids.len(),
        concurrency
    ));

    // 节流：30 秒内最多执行一次 sync_owner_registry
    {
        let mut last_sync = state.last_owner_registry_sync.lock().await;
        let should_sync = match *last_sync {
            None => true,
            Some(t) => t.elapsed().as_secs() >= 30,
        };
        if should_sync {
            let _ = state
                .run_history_db
                .sync_owner_registry_from_invite_owners()
                .map_err(|e| {
                    error_json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &format!("同步 owner_registry 失败: {e}"),
                    )
                })?;
            *last_sync = Some(Instant::now());
        }
    }
    let cached_records = state
        .run_history_db
        .get_owner_health_by_account_ids(&account_ids)
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("查询健康缓存失败: {e}"),
            )
        })?;
    let cached_map = cached_records
        .into_iter()
        .map(|record| (record.account_id.clone(), record))
        .collect::<HashMap<_, _>>();

    let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
    let mut handles = Vec::new();
    let mut results = Vec::new();
    let mut cache_hits = 0usize;
    let mut cache_misses = 0usize;
    let mut stale_returns = 0usize;
    let mut scheduled_refresh_ids = Vec::new();

    // 批量从 Redis MGET 读取健康缓存
    let redis_cache_map: HashMap<String, OwnerHealthResult> =
        if !force_refresh && prefer_cache {
            if let Some(redis_cache) = state.redis_cache.as_ref() {
                let keys: Vec<String> = account_ids
                    .iter()
                    .map(|id| redis_cache.owner_health_key(id))
                    .collect();
                match redis_cache.mget_json::<OwnerHealthResult>(&keys).await {
                    Ok(values) => account_ids
                        .iter()
                        .zip(values)
                        .filter_map(|(id, v)| v.map(|val| (id.clone(), val)))
                        .collect(),
                    Err(error) => {
                        tracing::warn!("[TeamManage] Redis MGET 批量读取失败: {error}");
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            }
        } else {
            HashMap::new()
        };

    for account_id in account_ids {
        if !force_refresh && prefer_cache {
            if let Some(cached) = redis_cache_map.get(&account_id) {
                match team_manage_cache_status(Some(cached.checked_at.as_str())).as_str() {
                    "fresh" => {
                        cache_hits += 1;
                        results.push(cached.clone());
                        continue;
                    }
                    "stale" => {
                        stale_returns += 1;
                        results.push(cached.clone());
                        scheduled_refresh_ids.push(account_id.clone());
                        continue;
                    }
                    "expired" => {}
                    _ => {}
                }
            }
            if let Some(cached) = cached_map.get(&account_id) {
                match team_manage_owner_health_cache_status(cached).as_str() {
                    "fresh" => {
                        cache_hits += 1;
                        results.push(team_manage_owner_health_result_from_record(cached));
                        continue;
                    }
                    "stale" => {
                        stale_returns += 1;
                        results.push(team_manage_owner_health_result_from_record(cached));
                        scheduled_refresh_ids.push(account_id.clone());
                        continue;
                    }
                    "expired" => {}
                    _ => {}
                }
            }
        }

        cache_misses += 1;
        let sem = semaphore.clone();
        let state = state.clone();
        let aid = account_id.clone();
        let handle = tokio::spawn(async move {
            let _permit = sem.acquire().await;
            let Some(lock_owner) = team_manage_try_acquire_health_lock(&state, &aid).await
            else {
                // 锁冲突 → 返回占位结果，让前端知道此 owner 被跳过
                crate::log_broadcast::broadcast_log(&format!(
                    "[健康检查] {} 锁冲突，已跳过",
                    aid
                ));
                return OwnerHealthResult {
                    account_id: aid,
                    owner_status: "lock_conflict".to_string(),
                    members: vec![],
                    checked_at: crate::util::beijing_now().to_rfc3339(),
                    owner_quota: None,
                };
            };
            // 单 owner 检查整体超时 60 秒，防止一个慢 owner 卡住整个 chunk
            let result = match tokio::time::timeout(
                std::time::Duration::from_secs(60),
                check_single_owner(&state, &aid),
            )
            .await
            {
                Ok(Some(r)) => r,
                Ok(None) => {
                    // check_single_owner 返回 None（无 token 等）
                    crate::log_broadcast::broadcast_log(&format!(
                        "[健康检查] {} 检查失败(无token或内部错误)",
                        aid
                    ));
                    OwnerHealthResult {
                        account_id: aid.clone(),
                        owner_status: "check_failed".to_string(),
                        members: vec![],
                        checked_at: crate::util::beijing_now().to_rfc3339(),
                        owner_quota: None,
                    }
                }
                Err(_) => {
                    tracing::warn!("[TeamManage] {} 健康检查超时(60s)，跳过", aid);
                    crate::log_broadcast::broadcast_log(&format!(
                        "[健康检查] {} 超时(60s)，已跳过",
                        aid
                    ));
                    OwnerHealthResult {
                        account_id: aid.clone(),
                        owner_status: "timeout".to_string(),
                        members: vec![],
                        checked_at: crate::util::beijing_now().to_rfc3339(),
                        owner_quota: None,
                    }
                }
            };
            team_manage_release_health_lock(&state, &aid, &lock_owner).await;
            result
        });
        handles.push(handle);
    }

    for handle in handles {
        match handle.await {
            Ok(r) => {
                // 只对正常检查成功的结果持久化到缓存（跳过 check_failed/timeout/lock_conflict）
                if !matches!(
                    r.owner_status.as_str(),
                    "check_failed" | "timeout" | "lock_conflict"
                ) {
                    team_manage_persist_health_result(&state, &r).await;
                }
                results.push(r);
            }
            Err(e) => {
                tracing::warn!("[TeamManage] 健康检查 task panic: {e}");
            }
        }
    }

    if !scheduled_refresh_ids.is_empty() {
        let refresh_state = state.clone();
        let refresh_ids = scheduled_refresh_ids.clone();
        tokio::spawn(async move {
            let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency));
            let mut handles = Vec::new();
            for account_id in refresh_ids {
                let sem = semaphore.clone();
                let state = refresh_state.clone();
                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await;
                    let Some(lock_owner) =
                        team_manage_try_acquire_health_lock(&state, &account_id).await
                    else {
                        return None;
                    };
                    let result = match tokio::time::timeout(
                        std::time::Duration::from_secs(60),
                        check_single_owner(&state, &account_id),
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(_) => {
                            tracing::warn!("[TeamManage] {} 后台刷新超时(60s)，跳过", account_id);
                            None
                        }
                    };
                    team_manage_release_health_lock(&state, &account_id, &lock_owner).await;
                    result
                });
                handles.push(handle);
            }
            for handle in handles {
                if let Ok(Some(result)) = handle.await {
                    team_manage_persist_health_result(&refresh_state, &result).await;
                }
            }
        });
    }

    tracing::info!("[TeamManage] 批量检查完成: {} 个结果", results.len());
    crate::log_broadcast::broadcast_log(&format!(
        "[Team管理] 批量检查完成: {} 个结果, 缓存命中 {}, 重算 {}",
        results.len(),
        cache_hits,
        cache_misses
    ));
    Ok(Json(BatchCheckResponse {
        results,
        cache_hits,
        cache_misses,
        stale_returns,
        scheduled_refreshes: scheduled_refresh_ids.len(),
        scope,
    }))
}

async fn check_single_owner(state: &AppState, account_id: &str) -> Option<OwnerHealthResult> {
    crate::log_broadcast::broadcast_log(&format!("[健康检查] 开始: {}", account_id));

    let access_token = state
        .run_history_db
        .get_owner_token_by_account_id(account_id)
        .ok()
        .flatten()?;

    // 构建代理客户端（如果启用了检查代理）
    let check_client: Option<rquest::Client> = {
        let cfg = state.config.read().await;
        if cfg.team_manage_check_use_proxy.unwrap_or(false) && !cfg.proxy_pool.is_empty() {
            use std::sync::atomic::{AtomicUsize, Ordering};
            static CHECK_IDX: AtomicUsize = AtomicUsize::new(0);
            let i = CHECK_IDX.fetch_add(1, Ordering::Relaxed);
            let pu = &cfg.proxy_pool[i % cfg.proxy_pool.len()];
            rquest::Proxy::all(pu)
                .ok()
                .and_then(|proxy| {
                    rquest::Client::builder()
                        .timeout(std::time::Duration::from_secs(15))
                        .connect_timeout(std::time::Duration::from_secs(10))
                        .proxy(proxy)
                        .build()
                        .ok()
                })
        } else {
            None
        }
    };

    // 1. 查 owner 额度
    let owner_quota = if let Some(ref c) = check_client {
        fetch_codex_quota_with_client(&access_token, c).await
    } else {
        fetch_codex_quota(&access_token).await
    };
    let owner_status = owner_quota.status.clone();
    crate::log_broadcast::broadcast_log(&format!(
        "[健康检查] {} owner_status={}",
        account_id, owner_status
    ));

    // 2. 拉成员列表
    let members_raw = if let Some(ref c) = check_client {
        fetch_chatgpt_members_with_client(account_id, &access_token, c).await
    } else {
        fetch_chatgpt_members(account_id, &access_token).await
    }
    .unwrap_or_default();
    let members_filtered: Vec<_> = members_raw
        .into_iter()
        .filter(|m| m.role != "account-owner")
        .collect();

    // 3. 查每个成员额度
    crate::log_broadcast::broadcast_log(&format!(
        "[健康检查] {} 成员数={}",
        account_id,
        members_filtered.len()
    ));

    let config = state.config.read().await;
    let config_clone = config.clone();
    drop(config);

    let mut member_tasks = Vec::new();
    for (idx, m) in members_filtered.iter().enumerate() {
        let email = match &m.email {
            Some(e) => e.clone(),
            None => continue,
        };
        let member_name = m.name.clone();
        let member_user_id = m.user_id.clone();
        let state_clone = state.clone();
        let config_clone = config_clone.clone();
        let account_id = account_id.to_string();

        member_tasks.push(tokio::spawn(async move {
            let mut status = "unknown".to_string();
            let mut seven_day_pct: Option<f64> = None;

            // 尝试查成员额度
            // 策略1: 本地 DB refresh_token
            let mut got_quota = false;
            if let Ok(Some(rt)) = state_clone.run_history_db.get_email_refresh_token(&email) {
                if let Ok(at) = refresh_rt_to_at(&rt).await {
                    let quota = fetch_codex_quota(&at).await;
                    status = quota.status.clone();
                    seven_day_pct = quota.seven_day.as_ref().map(|w| w.remaining_percent);
                    got_quota = true;
                }
            }

            // 策略2: S2A 号池搜索
            if !got_quota {
                if let Some((at, rt_opt)) = search_s2a_for_email(&config_clone, &email).await {
                    let effective_at = if let Some(rt) = &rt_opt {
                        refresh_rt_to_at(rt).await.unwrap_or(at.clone())
                    } else {
                        at
                    };
                    let quota = fetch_codex_quota(&effective_at).await;
                    status = quota.status.clone();
                    seven_day_pct = quota.seven_day.as_ref().map(|w| w.remaining_percent);
                    got_quota = true;
                }
            }

            if !got_quota {
                // 区分：外部域名 vs 号池未找到
                let email_lower = email.to_lowercase();
                let is_pool_domain = config_clone
                    .email_domains
                    .iter()
                    .chain(config_clone.chatgpt_mail_domains.iter())
                    .any(|d| email_lower.ends_with(&d.to_lowercase()));
                status = if is_pool_domain {
                    "pool_not_found".to_string() // 域名匹配但号池没找到（可能已移除）
                } else {
                    "external_domain".to_string() // 非号池域名（如 @gmail.com）
                };
            }

            let pct_str = seven_day_pct
                .map(|p| format!("{:.0}%", p))
                .unwrap_or("--".to_string());
            crate::log_broadcast::broadcast_log(&format!(
                "[健康检查] {} #{} {} status={} 7d={}",
                account_id,
                idx + 1,
                email,
                status,
                pct_str
            ));

            MemberHealthInfo {
                email,
                name: member_name,
                status,
                seven_day_pct: seven_day_pct.map(|p| (p * 10.0).round() / 10.0),
                user_id: Some(member_user_id),
            }
        }));
    }

    let mut member_infos = Vec::new();
    for result in join_all(member_tasks).await {
        match result {
            Ok(info) => member_infos.push(info),
            Err(e) => tracing::warn!("[健康检查] {} 成员任务失败: {}", account_id, e),
        }
    }

    crate::log_broadcast::broadcast_log(&format!(
        "[健康检查] {} 完成: {} 个成员已检查",
        account_id,
        member_infos.len()
    ));

    Some(OwnerHealthResult {
        account_id: account_id.to_string(),
        owner_status,
        members: member_infos,
        checked_at: crate::util::beijing_now().to_rfc3339(),
        owner_quota: Some(owner_quota),
    })
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    println!("\n收到关闭信号，正在停止服务...");
}
