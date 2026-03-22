use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::path::{Path as StdPath, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use axum::extract::{Path, State};
use axum::http::{
    HeaderMap, HeaderValue, StatusCode,
    header::{self, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_TYPE},
};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use futures::stream::Stream;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock, broadcast};
use tower_http::cors::CorsLayer;

use crate::config::{AppConfig, RegisterLogMode, RegisterPerfMode, S2aConfig, S2aExtraConfig};
use crate::db::RunHistoryDb;
use crate::email_service;
use crate::models::{AccountWithRt, WorkflowReport};
use crate::proxy_pool::{ProxyPool, health_check, resolve_proxies};
use crate::services::{LiveCodexService, LiveRegisterService, S2aHttpService, S2aService};
use crate::workflow::{WorkflowOptions, WorkflowRunner};

// ─── Embedded frontend ──────────────────────────────────────────────────────

const INDEX_HTML: &str = include_str!("../static/index.html");
const APP_CSS: &str = include_str!("../static/assets/css/app.css");
const APP_JS: &str = include_str!("../static/assets/js/app.js");
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
    #[serde(default)]
    pub mail_provider: Option<crate::config::MailProvider>,
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
    email_domains: Vec<String>,
    chatgpt_mail_domains: Vec<String>,
    tempmail_domains: Vec<String>,
    tempmail_lol_domains: Vec<String>,
    d1_cleanup: D1CleanupResp,
    site_title: String,
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
    tempmail_api_key: String,
    tempmail_lol_api_key: String,
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
    tempmail_api_key: Option<String>,
    tempmail_lol_api_key: Option<String>,
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
}

fn error_json(status: StatusCode, msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        status,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn find_task_rt_json_file(report: &WorkflowReport) -> Option<PathBuf> {
    report.output_files.iter().find_map(|path| {
        let candidate = PathBuf::from(path);
        let file_name = candidate.file_name()?.to_string_lossy();
        if file_name.contains("with-rt") && candidate.extension().is_some_and(|ext| ext == "json") {
            Some(candidate)
        } else {
            None
        }
    })
}

fn task_rt_download_name(task_id: &str) -> String {
    format!("task-{task_id}-rt.txt")
}

fn parse_rt_lines_from_file(path: &StdPath) -> Result<String, String> {
    let raw = std::fs::read(path).map_err(|e| format!("读取任务结果文件失败: {e}"))?;
    let accounts: Vec<AccountWithRt> =
        serde_json::from_slice(&raw).map_err(|e| format!("解析任务结果文件失败: {e}"))?;
    let lines: Vec<&str> = accounts
        .iter()
        .map(|acc| acc.refresh_token.trim())
        .filter(|rt| !rt.is_empty())
        .collect();
    if lines.is_empty() {
        return Err("该任务没有可导出的 RT".to_string());
    }
    Ok(format!("{}\n", lines.join("\n")))
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

async fn index_handler() -> impl IntoResponse {
    Html(INDEX_HTML)
}

async fn app_css_handler() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/css; charset=utf-8")], APP_CSS)
}

async fn app_js_handler() -> impl IntoResponse {
    (
        [(
            header::CONTENT_TYPE,
            "application/javascript; charset=utf-8",
        )],
        APP_JS,
    )
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
            tempmail_api_key: reg.tempmail_api_key.clone().unwrap_or_default(),
            tempmail_lol_api_key: reg.tempmail_lol_api_key.clone().unwrap_or_default(),
            mail_max_concurrency: reg.mail_max_concurrency.unwrap_or(50),
            register_log_mode: reg.register_log_mode.unwrap_or_default(),
            register_perf_mode: reg.register_perf_mode.unwrap_or_default(),
        },
        proxy_pool: cfg.proxy_pool.clone(),
        email_domains: cfg.email_domains.clone(),
        chatgpt_mail_domains: cfg.chatgpt_mail_domains.clone(),
        tempmail_domains: cfg.tempmail_domains.clone(),
        tempmail_lol_domains: cfg.tempmail_lol_domains.clone(),
        d1_cleanup: D1CleanupResp {
            enabled: cfg.d1_cleanup.enabled.unwrap_or(false),
            account_id: cfg.d1_cleanup.account_id.clone().unwrap_or_default(),
            api_key: cfg.d1_cleanup.api_key.clone().unwrap_or_default(),
            databases: cfg.d1_cleanup.databases.clone().unwrap_or_default(),
            keep_percent: cfg.d1_cleanup.keep_percent.unwrap_or(0.1),
            batch_size: cfg.d1_cleanup.batch_size.unwrap_or(5000),
        },
        site_title: cfg.server.site_title.clone().unwrap_or_default(),
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
    name: Option<String>,
    api_base: Option<String>,
    admin_key: Option<String>,
    concurrency: Option<usize>,
    priority: Option<usize>,
    group_ids: Option<Vec<i64>>,
    free_group_ids: Option<Vec<i64>>,
    free_priority: Option<Option<usize>>,
    free_concurrency: Option<Option<usize>>,
    extra: Option<S2aExtraConfig>,
}

async fn update_s2a_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateS2aRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    if !cfg.s2a.iter().any(|t| t.name == name) {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("未找到号池: {name}"),
        ));
    }

    // 支持改名：检查新名称是否冲突
    if let Some(ref v) = req.name {
        let trimmed = v.trim().to_string();
        if !trimmed.is_empty() && trimmed != name {
            let conflict = cfg.s2a.iter().any(|t| t.name == trimmed && t.name != name);
            if conflict {
                return Err(error_json(
                    StatusCode::CONFLICT,
                    &format!("号池名称 {trimmed} 已存在"),
                ));
            }
        }
    }
    let team = cfg.s2a.iter_mut().find(|t| t.name == name).unwrap();
    if let Some(ref v) = req.name {
        let trimmed = v.trim().to_string();
        if !trimmed.is_empty() {
            team.name = trimmed;
        }
    }
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
    let updated_name = team.name.clone();
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("号池 {updated_name} 已更新"),
    }))
}

/// 按索引更新号池（用于名称为空等无法按名称匹配的场景）
async fn update_s2a_by_index_handler(
    State(state): State<AppState>,
    Path(idx): Path<usize>,
    Json(req): Json<UpdateS2aRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    let s2a_len = cfg.s2a.len();
    if idx >= s2a_len {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("号池索引 {idx} 超出范围（共 {s2a_len} 个）"),
        ));
    }

    // 改名：检查冲突
    if let Some(ref v) = req.name {
        let trimmed = v.trim().to_string();
        if !trimmed.is_empty() {
            let conflict = cfg
                .s2a
                .iter()
                .enumerate()
                .any(|(i, t)| i != idx && t.name == trimmed);
            if conflict {
                return Err(error_json(
                    StatusCode::CONFLICT,
                    &format!("号池名称 {trimmed} 已存在"),
                ));
            }
            cfg.s2a[idx].name = trimmed;
        }
    }
    let team = &mut cfg.s2a[idx];
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
    let updated_name = team.name.clone();
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("号池 {updated_name} 已更新"),
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

/// 按索引删除号池（用于名称为空等无法按名称匹配的场景）
async fn delete_s2a_by_index_handler(
    State(state): State<AppState>,
    Path(idx): Path<usize>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let mut cfg = state.config.write().await;
    if idx >= cfg.s2a.len() {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("号池索引 {idx} 超出范围（共 {} 个）", cfg.s2a.len()),
        ));
    }
    let removed = cfg.s2a.remove(idx);
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("号池 #{idx} ({}) 已删除", removed.name),
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
    if let Some(v) = req.tempmail_api_key {
        cfg.register.tempmail_api_key = Some(v);
    }
    if let Some(v) = req.tempmail_lol_api_key {
        cfg.register.tempmail_lol_api_key = Some(v);
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

async fn add_tempmail_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    if domain.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "域名不能为空"));
    }
    let mut cfg = state.config.write().await;
    if cfg.tempmail_domains.contains(&domain) {
        return Err(error_json(
            StatusCode::CONFLICT,
            &format!("域名已存在: {domain}"),
        ));
    }
    cfg.tempmail_domains.push(domain.clone());
    auto_save(&cfg, &state.config_path);
    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("TempMail 域名 {domain} 已添加"),
        }),
    ))
}

async fn delete_tempmail_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    let mut cfg = state.config.write().await;
    let before = cfg.tempmail_domains.len();
    cfg.tempmail_domains.retain(|d| d != &domain);
    if cfg.tempmail_domains.len() == before {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("未找到域名: {domain}"),
        ));
    }
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("TempMail 域名 {domain} 已删除"),
    }))
}

async fn add_tempmail_lol_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    if domain.is_empty() {
        return Err(error_json(StatusCode::BAD_REQUEST, "域名不能为空"));
    }
    let mut cfg = state.config.write().await;
    if cfg.tempmail_lol_domains.contains(&domain) {
        return Err(error_json(
            StatusCode::CONFLICT,
            &format!("域名已存在: {domain}"),
        ));
    }
    cfg.tempmail_lol_domains.push(domain.clone());
    auto_save(&cfg, &state.config_path);
    Ok((
        StatusCode::CREATED,
        Json(MsgResponse {
            message: format!("TempMail.lol 域名 {domain} 已添加"),
        }),
    ))
}

async fn delete_tempmail_lol_domain_handler(
    State(state): State<AppState>,
    Json(req): Json<EmailDomainRequest>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let domain = req.domain.trim().to_string();
    let mut cfg = state.config.write().await;
    let before = cfg.tempmail_lol_domains.len();
    cfg.tempmail_lol_domains.retain(|d| d != &domain);
    if cfg.tempmail_lol_domains.len() == before {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("未找到域名: {domain}"),
        ));
    }
    auto_save(&cfg, &state.config_path);
    Ok(Json(MsgResponse {
        message: format!("TempMail.lol 域名 {domain} 已删除"),
    }))
}

async fn test_tempmail_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let key = cfg.register.tempmail_api_key.clone().unwrap_or_default();
    drop(cfg);

    if key.is_empty() {
        return Err(error_json(
            StatusCode::BAD_REQUEST,
            "未配置 TempMail API Key",
        ));
    }

    let client = shared_http_client_10s();

    let resp = client
        .get("https://mail.123nhh.de/api/mailboxes?page=1&size=1")
        .header("Authorization", format!("Bearer {key}"))
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

    let mailbox_count = json.get("total").and_then(|v| v.as_u64()).unwrap_or(0);

    Ok(Json(serde_json::json!({
        "success": true,
        "data": {
            "mailbox_count": mailbox_count
        }
    })))
}

async fn test_tempmail_lol_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    let key = cfg
        .register
        .tempmail_lol_api_key
        .clone()
        .unwrap_or_default();
    drop(cfg);

    let client = shared_http_client_10s();
    let mut req = client
        .post("https://api.tempmail.lol/v2/inbox/create")
        .header("Content-Type", "application/json")
        .body("{}");
    if !key.trim().is_empty() {
        req = req.header("Authorization", format!("Bearer {}", key.trim()));
    }

    let create_resp = req
        .send()
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("创建邮箱失败: {e}")))?;
    let create_status = create_resp.status();
    if !create_status.is_success() {
        let body = create_resp.text().await.unwrap_or_default();
        let preview: String = body.chars().take(160).collect();
        return Err(error_json(
            StatusCode::BAD_GATEWAY,
            &format!("创建邮箱 HTTP {create_status} {preview}"),
        ));
    }

    let created: serde_json::Value = create_resp
        .json()
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("创建邮箱解析失败: {e}")))?;
    let address = created
        .get("address")
        .and_then(|v| v.as_str())
        .ok_or_else(|| error_json(StatusCode::BAD_GATEWAY, "创建邮箱响应缺少 address"))?;
    let token = created
        .get("token")
        .and_then(|v| v.as_str())
        .ok_or_else(|| error_json(StatusCode::BAD_GATEWAY, "创建邮箱响应缺少 token"))?;

    let mut inbox_req = client.get(format!(
        "https://api.tempmail.lol/v2/inbox?token={}",
        urlencoding::encode(token)
    ));
    if !key.trim().is_empty() {
        inbox_req = inbox_req.header("Authorization", format!("Bearer {}", key.trim()));
    }
    let inbox_resp = inbox_req
        .send()
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("查询邮箱失败: {e}")))?;
    let inbox_status = inbox_resp.status();
    if !inbox_status.is_success() {
        let body = inbox_resp.text().await.unwrap_or_default();
        let preview: String = body.chars().take(160).collect();
        return Err(error_json(
            StatusCode::BAD_GATEWAY,
            &format!("查询邮箱 HTTP {inbox_status} {preview}"),
        ));
    }

    let inbox_json: serde_json::Value = inbox_resp
        .json()
        .await
        .map_err(|e| error_json(StatusCode::BAD_GATEWAY, &format!("查询邮箱解析失败: {e}")))?;
    let email_count = inbox_json
        .get("emails")
        .and_then(|v| v.as_array())
        .map(|v| v.len())
        .unwrap_or(0);

    Ok(Json(serde_json::json!({
        "success": true,
        "data": {
            "address": address,
            "email_count": email_count,
            "using_api_key": !key.trim().is_empty()
        }
    })))
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

async fn trigger_d1_cleanup_handler(
    State(state): State<AppState>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let cfg = state.config.read().await;
    if !cfg.d1_cleanup.enabled.unwrap_or(false) {
        return Err(error_json(StatusCode::BAD_REQUEST, "D1 邮件清理未启用"));
    }
    let d1_cfg = cfg.d1_cleanup.clone();
    drop(cfg);
    match crate::d1_cleanup::run_cleanup(&d1_cfg).await {
        Ok(()) => Ok(Json(MsgResponse {
            message: "D1 邮件清理完成".into(),
        })),
        Err(e) => Err(error_json(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("清理失败: {e}"),
        )),
    }
}

#[derive(Deserialize)]
struct UpdateSiteTitleRequest {
    site_title: String,
}

async fn update_site_title_handler(
    State(state): State<AppState>,
    Json(req): Json<UpdateSiteTitleRequest>,
) -> impl IntoResponse {
    let mut cfg = state.config.write().await;
    let title = req.site_title.trim().to_string();
    cfg.server.site_title = if title.is_empty() {
        None
    } else {
        Some(title.clone())
    };
    auto_save(&cfg, &state.config_path);
    Json(MsgResponse {
        message: format!(
            "站点标题已更新: {}",
            if title.is_empty() { "(默认)" } else { &title }
        ),
    })
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
    let mail_provider = req.mail_provider.unwrap_or_else(|| {
        if req.use_chatgpt_mail.unwrap_or(false) {
            crate::config::MailProvider::Chatgpt
        } else {
            crate::config::MailProvider::Kyx
        }
    });
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
            mail_provider,
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

async fn export_task_rt_handler(
    State(state): State<AppState>,
    Path(task_id): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, Json<ErrorResponse>)> {
    let task = state
        .task_manager
        .get(&task_id)
        .await
        .ok_or_else(|| error_json(StatusCode::NOT_FOUND, &format!("任务不存在: {task_id}")))?;

    let report = task
        .report
        .ok_or_else(|| error_json(StatusCode::CONFLICT, "任务尚未完成，暂时无法导出 RT"))?;

    let rt_json = find_task_rt_json_file(&report).ok_or_else(|| {
        error_json(
            StatusCode::NOT_FOUND,
            "未找到该任务的 RT 结果文件，请确认该任务已成功产出 RT",
        )
    })?;

    if !rt_json.exists() {
        return Err(error_json(
            StatusCode::NOT_FOUND,
            &format!("RT 结果文件不存在: {}", rt_json.display()),
        ));
    }

    let body = parse_rt_lines_from_file(&rt_json)
        .map_err(|msg| error_json(StatusCode::BAD_GATEWAY, &msg))?;

    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        CONTENT_DISPOSITION,
        HeaderValue::from_str(&format!(
            "attachment; filename=\"{}\"",
            task_rt_download_name(&task_id)
        ))
        .map_err(|e| {
            error_json(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("构造下载文件名失败: {e}"),
            )
        })?,
    );

    Ok((headers, body))
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
    mail_provider: crate::config::MailProvider,
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
    ) = match mail_provider {
        crate::config::MailProvider::Chatgpt => {
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
        }
        crate::config::MailProvider::Duckmail | crate::config::MailProvider::Tempmail => {
            let mail_concurrency = register_runtime.mail_max_concurrency;
            let reg_email = Arc::new(email_service::EmailService::new_tempmail(
                register_runtime.tempmail_api_key.clone(),
                cfg.tempmail_domains.clone(),
                mail_concurrency,
            ));
            let rt_email = Arc::new(email_service::EmailService::new_tempmail(
                register_runtime.tempmail_api_key.clone(),
                cfg.tempmail_domains.clone(),
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
        }
        crate::config::MailProvider::TempmailLol => {
            let mail_concurrency = register_runtime.mail_max_concurrency;
            let shared_email = Arc::new(email_service::EmailService::new_tempmail_lol(
                register_runtime.tempmail_lol_api_key.clone(),
                cfg.tempmail_lol_domains.clone(),
                mail_concurrency,
            ));
            (
                Arc::new(LiveRegisterService::new(
                    register_runtime.clone(),
                    Arc::clone(&shared_email),
                )) as Arc<dyn crate::services::RegisterService>,
                Arc::new(LiveCodexService::new(codex_runtime.clone(), shared_email))
                    as Arc<dyn crate::services::CodexService>,
            )
        }
        crate::config::MailProvider::Kyx => {
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
        }
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
        mail_provider,
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
    mail_provider: Option<crate::config::MailProvider>,
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
    mail_provider: Option<crate::config::MailProvider>,
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
        mail_provider: req.mail_provider,
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
    if let Some(mp) = req.mail_provider {
        sched.mail_provider = Some(mp);
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
                schedule.effective_mail_provider(),
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
        schedule.effective_mail_provider(),
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
    mail_provider: crate::config::MailProvider,
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
    ) = match mail_provider {
        crate::config::MailProvider::Chatgpt => {
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
        }
        crate::config::MailProvider::Duckmail | crate::config::MailProvider::Tempmail => {
            let mail_concurrency = register_runtime.mail_max_concurrency;
            let reg_email = Arc::new(crate::email_service::EmailService::new_tempmail(
                register_runtime.tempmail_api_key.clone(),
                cfg.tempmail_domains.clone(),
                mail_concurrency,
            ));
            let rt_email = Arc::new(crate::email_service::EmailService::new_tempmail(
                register_runtime.tempmail_api_key.clone(),
                cfg.tempmail_domains.clone(),
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
        }
        crate::config::MailProvider::TempmailLol => {
            let mail_concurrency = register_runtime.mail_max_concurrency;
            let shared_email = Arc::new(crate::email_service::EmailService::new_tempmail_lol(
                register_runtime.tempmail_lol_api_key.clone(),
                cfg.tempmail_lol_domains.clone(),
                mail_concurrency,
            ));
            (
                Arc::new(LiveRegisterService::new(
                    register_runtime.clone(),
                    Arc::clone(&shared_email),
                )) as Arc<dyn crate::services::RegisterService>,
                Arc::new(LiveCodexService::new(codex_runtime.clone(), shared_email))
                    as Arc<dyn crate::services::CodexService>,
            )
        }
        crate::config::MailProvider::Kyx => {
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
        }
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
        match rx.recv().await {
            Ok(msg) => Some((Ok(Event::default().data(msg)), rx)),
            Err(broadcast::error::RecvError::Lagged(n)) => {
                let msg = format!("[... 跳过 {} 条日志]", n);
                Some((Ok(Event::default().data(msg)), rx))
            }
            Err(broadcast::error::RecvError::Closed) => None,
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
        .route("/assets/css/app.css", get(app_css_handler))
        .route("/assets/js/app.js", get(app_js_handler))
        // Health
        .route("/health", get(health_handler))
        // Config management
        .route("/api/config", get(config_handler))
        .route("/api/config/s2a", post(add_s2a_handler))
        .route(
            "/api/config/s2a/by-index/{idx}",
            delete(delete_s2a_by_index_handler).put(update_s2a_by_index_handler),
        )
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
            "/api/config/tempmail_domains",
            post(add_tempmail_domain_handler).delete(delete_tempmail_domain_handler),
        )
        .route(
            "/api/config/tempmail_lol_domains",
            post(add_tempmail_lol_domain_handler).delete(delete_tempmail_lol_domain_handler),
        )
        .route("/api/test/tempmail", post(test_tempmail_handler))
        .route("/api/test/tempmail_lol", post(test_tempmail_lol_handler))
        .route("/api/config/save", post(save_config_handler))
        .route("/api/config/site_title", put(update_site_title_handler))
        // Task management
        .route("/api/tasks", post(create_task_handler))
        .route("/api/tasks", get(list_tasks_handler))
        .route("/api/tasks/{task_id}", get(get_task_handler))
        .route(
            "/api/tasks/{task_id}/export-rt",
            get(export_task_rt_handler),
        )
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

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    println!("\n收到关闭信号，正在停止服务...");
}
