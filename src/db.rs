use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::types::ToSql;
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct RunStats {
    pub total_runs: usize,
    pub completed: usize,
    pub failed: usize,
    pub running: usize,
    pub total_target: usize,
    pub total_reg_ok: usize,
    pub total_reg_failed: usize,
    pub total_rt_ok: usize,
    pub total_rt_failed: usize,
    pub total_s2a_ok: usize,
    pub total_s2a_failed: usize,
    pub total_elapsed_secs: f64,
    pub avg_secs_per_account: f64,
}

pub struct RunHistoryDb {
    conn: Mutex<Connection>,
    write_tx: mpsc::Sender<WriteCommand>,
}

#[derive(Debug, Clone, Copy)]
pub enum RunTriggerFilter {
    Manual,
    Scheduled,
}

enum WriteCommand {
    InsertRun(NewRun),
    InsertDistributions {
        run_id: String,
        entries: Vec<NewDistribution>,
    },
    CompleteRun {
        run_id: String,
        update: RunCompletion,
    },
    FailRun {
        run_id: String,
        error: String,
    },
    UpdateDistribution {
        run_id: String,
        team: String,
        assigned: usize,
        ok: usize,
        failed: usize,
    },
    RenameSchedule {
        old_name: String,
        new_name: String,
    },
    // ─── 邀请模块写命令 ───
    #[allow(dead_code)]
    InsertInviteUpload {
        id: String,
        filename: String,
        owner_count: usize,
        created_at: String,
    },
    #[allow(dead_code)]
    InsertInviteOwners {
        upload_id: String,
        owners: Vec<InviteOwnerInsert>,
    },
    InsertInviteTask(NewInviteTask),
    InsertInviteEmails {
        task_id: String,
        emails: Vec<InviteEmailInsert>,
    },
    UpdateInviteTask {
        task_id: String,
        update: InviteTaskUpdate,
    },
    UpdateInviteEmail {
        email_id: i64,
        update: InviteEmailUpdate,
    },
    MarkOwnerUsed {
        owner_id: i64,
    },
    ResetOwnerUsed {
        owner_id: i64,
    },
    UpsertOwnerHealth {
        account_id: String,
        owner_status: String,
        members_json: String,
        checked_at: String,
        expires_at: Option<String>,
        cache_status: String,
        source: Option<String>,
        last_error: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DistributionUpdateKey {
    run_id: String,
    team: String,
}

#[derive(Debug, Clone)]
struct DistributionUpdateValue {
    assigned: usize,
    ok: usize,
    failed: usize,
}

#[derive(Debug, Clone)]
pub struct NewRun {
    pub id: String,
    pub schedule_name: Option<String>,
    pub trigger_type: String,
    pub target_count: usize,
    pub started_at: String,
}

#[derive(Debug, Clone)]
pub struct NewDistribution {
    pub team_name: String,
    pub percent: u8,
}

#[derive(Debug, Clone)]
pub struct RunCompletion {
    pub registered_ok: usize,
    pub registered_failed: usize,
    pub rt_ok: usize,
    pub rt_failed: usize,
    pub total_s2a_ok: usize,
    pub total_s2a_failed: usize,
    pub elapsed_secs: f64,
    pub finished_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunRecord {
    pub id: String,
    pub schedule_name: Option<String>,
    pub trigger_type: String,
    pub status: String,
    pub target_count: usize,
    pub registered_ok: usize,
    pub registered_failed: usize,
    pub rt_ok: usize,
    pub rt_failed: usize,
    pub total_s2a_ok: usize,
    pub total_s2a_failed: usize,
    pub elapsed_secs: Option<f64>,
    pub error: Option<String>,
    pub started_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DistributionRecord {
    pub id: i64,
    pub run_id: String,
    pub team_name: String,
    pub percent: usize,
    pub assigned_count: usize,
    pub s2a_ok: usize,
    pub s2a_failed: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct RunDetail {
    pub run: RunRecord,
    pub distributions: Vec<DistributionRecord>,
}

// ─── 邀请模块数据结构 ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct InviteOwnerInsert {
    pub email: String,
    pub account_id: String,
    pub access_token: String,
    pub expires: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewInviteTask {
    pub id: String,
    pub upload_id: String,
    pub owner_email: String,
    pub owner_account_id: String,
    pub s2a_team: Option<String>,
    pub invite_count: usize,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct InviteEmailInsert {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Clone, Default)]
pub struct InviteTaskUpdate {
    pub status: Option<String>,
    pub invited_ok: Option<usize>,
    pub invited_failed: Option<usize>,
    pub reg_ok: Option<usize>,
    pub reg_failed: Option<usize>,
    pub rt_ok: Option<usize>,
    pub rt_failed: Option<usize>,
    pub s2a_ok: Option<usize>,
    pub s2a_failed: Option<usize>,
    pub error: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct InviteEmailUpdate {
    pub invite_status: Option<String>,
    pub reg_status: Option<String>,
    pub rt_status: Option<String>,
    pub s2a_status: Option<String>,
    pub refresh_token: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InviteUploadRecord {
    pub id: String,
    pub filename: String,
    pub owner_count: usize,
    pub unused_count: usize,
    pub owner_emails: String,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct InviteOwnerRecord {
    pub id: i64,
    pub upload_id: String,
    pub email: String,
    pub account_id: String,
    pub expires: Option<String>,
    pub used: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct InviteUploadDetail {
    pub upload: InviteUploadRecord,
    pub owners: Vec<InviteOwnerRecord>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InviteTaskRecord {
    pub id: String,
    pub upload_id: String,
    pub owner_email: String,
    pub owner_account_id: String,
    pub s2a_team: Option<String>,
    pub invite_count: usize,
    pub status: String,
    pub invited_ok: usize,
    pub invited_failed: usize,
    pub reg_ok: usize,
    pub reg_failed: usize,
    pub rt_ok: usize,
    pub rt_failed: usize,
    pub s2a_ok: usize,
    pub s2a_failed: usize,
    pub error: Option<String>,
    pub created_at: String,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InviteEmailRecord {
    pub id: i64,
    pub task_id: String,
    pub email: String,
    pub invite_status: String,
    pub reg_status: String,
    pub rt_status: String,
    pub s2a_status: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct InviteTaskDetail {
    pub task: InviteTaskRecord,
    pub emails: Vec<InviteEmailRecord>,
}

#[derive(Debug, Clone)]
pub struct OwnerHealthUpsert {
    pub account_id: String,
    pub owner_status: String,
    pub members_json: String,
    pub checked_at: String,
    pub expires_at: Option<String>,
    pub cache_status: Option<String>,
    pub source: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OwnerHealthRecord {
    pub account_id: String,
    pub owner_status: String,
    pub members_json: Option<String>,
    pub checked_at: String,
    pub expires_at: Option<String>,
    pub cache_status: String,
    pub source: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OwnerRegistryUpsert {
    pub account_id: String,
    pub display_email: String,
    pub latest_access_token: Option<String>,
    pub token_expires_at: Option<String>,
    pub source_upload_id: Option<String>,
    pub state: Option<String>,
    pub disabled_reason: Option<String>,
    pub last_health_checked_at: Option<String>,
    pub last_health_status: Option<String>,
    pub member_count_cached: Option<usize>,
    pub available_slots_cached: Option<usize>,
    pub last_invite_task_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct OwnerRegistryRecord {
    pub account_id: String,
    pub display_email: String,
    #[serde(skip_serializing)]
    #[allow(dead_code)]
    pub latest_access_token: Option<String>,
    pub token_expires_at: Option<String>,
    pub source_upload_id: Option<String>,
    pub state: String,
    pub disabled_reason: Option<String>,
    pub last_health_checked_at: Option<String>,
    pub last_health_status: Option<String>,
    pub member_count_cached: usize,
    pub available_slots_cached: usize,
    pub last_invite_task_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub has_banned_member: bool,
}

#[derive(Debug, Clone, Default)]
pub struct OwnerRegistryQuery {
    pub page: usize,
    pub page_size: usize,
    pub search: Option<String>,
    pub state: Option<String>,
    pub has_slots: Option<bool>,
    pub has_banned_member: Option<bool>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct OwnerRegistryPage {
    pub items: Vec<OwnerRegistryRecord>,
    pub total: usize,
    pub page: usize,
    pub page_size: usize,
    pub total_pages: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct OwnerMemberCacheRecord {
    pub account_id: String,
    pub members_json: Option<String>,
    pub member_count: usize,
    pub cached_at: String,
}

#[derive(Debug, Clone, Serialize, Default)]
pub struct TeamManageDashboardSummary {
    pub total_owners: usize,
    pub active_owners: usize,
    pub banned_owners: usize,
    pub expired_owners: usize,
    pub quarantined_owners: usize,
    pub archived_owners: usize,
    pub owners_with_slots: usize,
    pub owners_with_banned_members: usize,
    pub health_cache_fresh: usize,
    pub health_cache_stale: usize,
    pub running_batch_jobs: usize,
}

#[derive(Debug, Clone)]
pub struct NewTeamManageBatchJob {
    pub job_id: String,
    pub job_type: String,
    pub scope: Option<String>,
    pub status: String,
    pub payload_json: String,
    pub total_count: usize,
    pub success_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub created_by: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TeamManageBatchJobUpdate {
    pub status: Option<String>,
    pub total_count: Option<usize>,
    pub success_count: Option<usize>,
    pub failed_count: Option<usize>,
    pub skipped_count: Option<usize>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamManageBatchJobRecord {
    pub job_id: String,
    pub job_type: String,
    pub scope: Option<String>,
    pub status: String,
    pub payload_json: String,
    pub total_count: usize,
    pub success_count: usize,
    pub failed_count: usize,
    pub skipped_count: usize,
    pub created_by: Option<String>,
    pub created_at: String,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
    pub error: Option<String>,
    pub fingerprint: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewTeamManageBatchItem {
    pub job_id: String,
    pub account_id: String,
    pub item_type: String,
    pub status: String,
    pub child_task_id: Option<String>,
    pub result_json: Option<String>,
    pub error: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TeamManageBatchItemUpdate {
    pub status: Option<String>,
    pub child_task_id: Option<String>,
    pub result_json: Option<String>,
    pub error: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamManageBatchItemRecord {
    pub id: i64,
    pub job_id: String,
    pub account_id: String,
    pub item_type: String,
    pub status: String,
    pub child_task_id: Option<String>,
    pub result_json: Option<String>,
    pub error: Option<String>,
    pub started_at: Option<String>,
    pub finished_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct NewTeamManageOwnerAudit {
    pub account_id: String,
    pub action: String,
    pub from_state: Option<String>,
    pub to_state: String,
    pub reason: Option<String>,
    pub scope: Option<String>,
    pub batch_job_id: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TeamManageOwnerAuditRecord {
    pub id: i64,
    pub account_id: String,
    pub action: String,
    pub from_state: Option<String>,
    pub to_state: String,
    pub reason: Option<String>,
    pub scope: Option<String>,
    pub batch_job_id: Option<String>,
    pub created_at: String,
}

impl RunHistoryDb {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("创建数据库目录失败: {}", parent.display()))?;
        }
        let path_buf = path.to_path_buf();
        let conn = Connection::open(&path_buf)
            .with_context(|| format!("打开数据库失败: {}", path.display()))?;
        Self::init_schema(&conn)?;

        let (write_tx, write_rx) = mpsc::channel::<WriteCommand>();
        let writer_path = path_buf.clone();
        std::thread::Builder::new()
            .name("run-history-db-writer".to_string())
            .spawn(move || {
                if let Err(e) = Self::run_writer_loop(&writer_path, write_rx) {
                    eprintln!("[RunHistoryDb] 写线程退出: {e}");
                }
            })
            .with_context(|| "启动 RunHistoryDb 写线程失败".to_string())?;

        Ok(Self {
            conn: Mutex::new(conn),
            write_tx,
        })
    }

    fn init_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS runs (
                id              TEXT PRIMARY KEY,
                schedule_name   TEXT,
                trigger_type    TEXT NOT NULL,
                status          TEXT NOT NULL DEFAULT 'running',
                target_count    INTEGER NOT NULL,
                registered_ok   INTEGER DEFAULT 0,
                registered_failed INTEGER DEFAULT 0,
                rt_ok           INTEGER DEFAULT 0,
                rt_failed       INTEGER DEFAULT 0,
                total_s2a_ok    INTEGER DEFAULT 0,
                total_s2a_failed INTEGER DEFAULT 0,
                elapsed_secs    REAL,
                error           TEXT,
                started_at      TEXT NOT NULL,
                finished_at     TEXT
            );

            CREATE TABLE IF NOT EXISTS run_distributions (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id          TEXT NOT NULL REFERENCES runs(id),
                team_name       TEXT NOT NULL,
                percent         INTEGER NOT NULL,
                assigned_count  INTEGER DEFAULT 0,
                s2a_ok          INTEGER DEFAULT 0,
                s2a_failed      INTEGER DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_runs_started_at
                ON runs(started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_runs_schedule_started_at
                ON runs(schedule_name, started_at DESC);
            CREATE INDEX IF NOT EXISTS idx_runs_schedule_finished_at
                ON runs(schedule_name, finished_at DESC);
            CREATE INDEX IF NOT EXISTS idx_run_distributions_run_id_team
                ON run_distributions(run_id, team_name);

            -- 邀请模块: 上传记录
            CREATE TABLE IF NOT EXISTS invite_uploads (
                id           TEXT PRIMARY KEY,
                filename     TEXT NOT NULL,
                owner_count  INTEGER NOT NULL,
                created_at   TEXT NOT NULL
            );

            -- 邀请模块: Owner 账号
            CREATE TABLE IF NOT EXISTS invite_owners (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                upload_id     TEXT NOT NULL REFERENCES invite_uploads(id),
                email         TEXT NOT NULL,
                account_id    TEXT NOT NULL,
                access_token  TEXT NOT NULL,
                expires       TEXT,
                used          INTEGER DEFAULT 0
            );

            -- 邀请模块: 邀请任务
            CREATE TABLE IF NOT EXISTS invite_tasks (
                id               TEXT PRIMARY KEY,
                upload_id        TEXT NOT NULL,
                owner_email      TEXT NOT NULL,
                owner_account_id TEXT NOT NULL,
                s2a_team         TEXT,
                invite_count     INTEGER NOT NULL,
                status           TEXT NOT NULL DEFAULT 'pending',
                invited_ok       INTEGER DEFAULT 0,
                invited_failed   INTEGER DEFAULT 0,
                reg_ok           INTEGER DEFAULT 0,
                reg_failed       INTEGER DEFAULT 0,
                rt_ok            INTEGER DEFAULT 0,
                rt_failed        INTEGER DEFAULT 0,
                s2a_ok           INTEGER DEFAULT 0,
                s2a_failed       INTEGER DEFAULT 0,
                error            TEXT,
                created_at       TEXT NOT NULL,
                finished_at      TEXT
            );

            -- 邀请模块: 被邀请邮箱详情
            CREATE TABLE IF NOT EXISTS invite_emails (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id         TEXT NOT NULL REFERENCES invite_tasks(id),
                email           TEXT NOT NULL,
                password        TEXT NOT NULL,
                invite_status   TEXT DEFAULT 'pending',
                reg_status      TEXT DEFAULT 'pending',
                rt_status       TEXT DEFAULT 'pending',
                s2a_status      TEXT DEFAULT 'pending',
                refresh_token   TEXT,
                error           TEXT
            );

             CREATE INDEX IF NOT EXISTS idx_invite_owners_upload
                ON invite_owners(upload_id);
            CREATE INDEX IF NOT EXISTS idx_invite_tasks_upload
                ON invite_tasks(upload_id);
            CREATE INDEX IF NOT EXISTS idx_invite_emails_task
                ON invite_emails(task_id);

            -- TeamManage: Owner 运行态主表
            CREATE TABLE IF NOT EXISTS owner_registry (
                account_id               TEXT PRIMARY KEY,
                display_email            TEXT NOT NULL,
                latest_access_token      TEXT,
                token_expires_at         TEXT,
                source_upload_id         TEXT,
                state                    TEXT NOT NULL DEFAULT 'active',
                disabled_reason          TEXT,
                last_health_checked_at   TEXT,
                last_health_status       TEXT,
                member_count_cached      INTEGER NOT NULL DEFAULT 0,
                available_slots_cached   INTEGER NOT NULL DEFAULT 0,
                last_invite_task_id      TEXT,
                created_at               TEXT NOT NULL,
                updated_at               TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_owner_registry_state_updated
                ON owner_registry(state, updated_at DESC);
            CREATE INDEX IF NOT EXISTS idx_owner_registry_token_expires
                ON owner_registry(token_expires_at);
            CREATE INDEX IF NOT EXISTS idx_owner_registry_health_checked
                ON owner_registry(last_health_checked_at DESC);

            -- TeamManage: Owner 成员列表缓存
            CREATE TABLE IF NOT EXISTS owner_member_cache (
                account_id   TEXT PRIMARY KEY,
                members_json TEXT,
                member_count INTEGER NOT NULL DEFAULT 0,
                cached_at    TEXT NOT NULL
            );

            -- TeamManage: 批量治理父任务
            CREATE TABLE IF NOT EXISTS team_manage_batch_jobs (
                job_id         TEXT PRIMARY KEY,
                job_type       TEXT NOT NULL,
                scope          TEXT,
                status         TEXT NOT NULL,
                payload_json   TEXT NOT NULL,
                total_count    INTEGER NOT NULL DEFAULT 0,
                success_count  INTEGER NOT NULL DEFAULT 0,
                failed_count   INTEGER NOT NULL DEFAULT 0,
                skipped_count  INTEGER NOT NULL DEFAULT 0,
                created_by     TEXT,
                created_at     TEXT NOT NULL,
                started_at     TEXT,
                finished_at    TEXT,
                error          TEXT,
                fingerprint    TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_team_manage_batch_jobs_status_created
                ON team_manage_batch_jobs(status, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_team_manage_batch_jobs_fingerprint
                ON team_manage_batch_jobs(fingerprint);

            -- TeamManage: 批量治理子项
            CREATE TABLE IF NOT EXISTS team_manage_batch_items (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id        TEXT NOT NULL REFERENCES team_manage_batch_jobs(job_id),
                account_id    TEXT NOT NULL,
                item_type     TEXT NOT NULL,
                status        TEXT NOT NULL,
                child_task_id TEXT,
                result_json   TEXT,
                error         TEXT,
                started_at    TEXT,
                finished_at   TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_team_manage_batch_items_job_status
                ON team_manage_batch_items(job_id, status);
            CREATE INDEX IF NOT EXISTS idx_team_manage_batch_items_account_job
                ON team_manage_batch_items(account_id, job_id);

            CREATE TABLE IF NOT EXISTS team_manage_owner_audits (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                account_id    TEXT NOT NULL,
                action        TEXT NOT NULL,
                from_state    TEXT,
                to_state      TEXT NOT NULL,
                reason        TEXT,
                scope         TEXT,
                batch_job_id  TEXT,
                created_at    TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_team_manage_owner_audits_account_created
                ON team_manage_owner_audits(account_id, created_at DESC);
            CREATE INDEX IF NOT EXISTS idx_team_manage_owner_audits_created
                ON team_manage_owner_audits(created_at DESC);",
        )?;

        // Migration: 为旧数据库补充 refresh_token 列
        let _ = conn.execute_batch("ALTER TABLE invite_emails ADD COLUMN refresh_token TEXT;");

        // owner_health 表：缓存 Owner 健康检查结果
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS owner_health (
                account_id   TEXT PRIMARY KEY,
                owner_status TEXT NOT NULL DEFAULT 'unknown',
                members_json TEXT,
                checked_at   TEXT NOT NULL,
                expires_at   TEXT,
                cache_status TEXT NOT NULL DEFAULT 'fresh',
                source       TEXT DEFAULT 'live',
                last_error   TEXT
            );",
        )?;

        let _ = conn.execute_batch(
            "ALTER TABLE owner_health ADD COLUMN expires_at TEXT;
             ALTER TABLE owner_health ADD COLUMN cache_status TEXT NOT NULL DEFAULT 'fresh';
             ALTER TABLE owner_health ADD COLUMN source TEXT DEFAULT 'live';
             ALTER TABLE owner_health ADD COLUMN last_error TEXT;",
        );
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_owner_health_checked_at ON owner_health(checked_at DESC)",
            [],
        )?;

        Ok(())
    }

    fn run_writer_loop(path: &PathBuf, rx: mpsc::Receiver<WriteCommand>) -> Result<()> {
        let mut conn = Connection::open(path)
            .with_context(|| format!("打开写连接失败: {}", path.display()))?;
        Self::init_schema(&conn)?;

        let flush_interval = Self::distribution_flush_interval();
        let mut pending_updates: HashMap<DistributionUpdateKey, DistributionUpdateValue> =
            HashMap::new();

        loop {
            let recv_result = match flush_interval {
                Some(interval) => rx.recv_timeout(interval),
                None => rx.recv().map_err(|_| mpsc::RecvTimeoutError::Disconnected),
            };

            match recv_result {
                Ok(cmd) => match cmd {
                    WriteCommand::UpdateDistribution {
                        run_id,
                        team,
                        assigned,
                        ok,
                        failed,
                    } => {
                        pending_updates.insert(
                            DistributionUpdateKey { run_id, team },
                            DistributionUpdateValue {
                                assigned,
                                ok,
                                failed,
                            },
                        );
                        if flush_interval.is_none()
                            && let Err(e) = Self::flush_pending_distribution_updates(
                                &mut conn,
                                &mut pending_updates,
                            )
                        {
                            eprintln!("[RunHistoryDb] 立即刷入分发表失败: {e}");
                        }
                    }
                    other => {
                        if let Err(e) = Self::flush_pending_distribution_updates(
                            &mut conn,
                            &mut pending_updates,
                        ) {
                            eprintln!("[RunHistoryDb] 刷入分发表失败: {e}");
                        }
                        if let Err(e) = Self::apply_write_command(&mut conn, other) {
                            eprintln!("[RunHistoryDb] 执行写命令失败: {e}");
                        }
                    }
                },
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    if let Err(e) =
                        Self::flush_pending_distribution_updates(&mut conn, &mut pending_updates)
                    {
                        eprintln!("[RunHistoryDb] 定时刷入分发表失败: {e}");
                    }
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    if let Err(e) =
                        Self::flush_pending_distribution_updates(&mut conn, &mut pending_updates)
                    {
                        eprintln!("[RunHistoryDb] 退出前刷入分发表失败: {e}");
                    }
                    break;
                }
            }
        }
        Ok(())
    }

    fn distribution_flush_interval() -> Option<Duration> {
        let raw = std::env::var("RUN_HISTORY_DISTRIBUTION_FLUSH_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(200);
        if raw == 0 {
            None
        } else {
            Some(Duration::from_millis(raw))
        }
    }

    fn flush_pending_distribution_updates(
        conn: &mut Connection,
        pending: &mut HashMap<DistributionUpdateKey, DistributionUpdateValue>,
    ) -> Result<()> {
        if pending.is_empty() {
            return Ok(());
        }
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "UPDATE run_distributions SET assigned_count = ?3, s2a_ok = ?4, s2a_failed = ?5
                 WHERE run_id = ?1 AND team_name = ?2",
            )?;
            for (key, value) in pending.iter() {
                stmt.execute(params![
                    &key.run_id,
                    &key.team,
                    value.assigned as i64,
                    value.ok as i64,
                    value.failed as i64,
                ])?;
            }
        }
        tx.commit()?;
        pending.clear();
        Ok(())
    }

    fn apply_write_command(conn: &mut Connection, cmd: WriteCommand) -> Result<()> {
        match cmd {
            WriteCommand::InsertRun(run) => {
                conn.execute(
                    "INSERT INTO runs (id, schedule_name, trigger_type, status, target_count, started_at)
                     VALUES (?1, ?2, ?3, 'running', ?4, ?5)",
                    params![
                        run.id,
                        run.schedule_name,
                        run.trigger_type,
                        run.target_count as i64,
                        run.started_at,
                    ],
                )?;
            }
            WriteCommand::InsertDistributions { run_id, entries } => {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare(
                        "INSERT INTO run_distributions (run_id, team_name, percent) VALUES (?1, ?2, ?3)",
                    )?;
                    for entry in entries {
                        stmt.execute(params![run_id, entry.team_name, entry.percent as i64])?;
                    }
                }
                tx.commit()?;
            }
            WriteCommand::CompleteRun { run_id, update } => {
                conn.execute(
                    "UPDATE runs SET
                        status = 'completed',
                        registered_ok = ?2,
                        registered_failed = ?3,
                        rt_ok = ?4,
                        rt_failed = ?5,
                        total_s2a_ok = ?6,
                        total_s2a_failed = ?7,
                        elapsed_secs = ?8,
                        finished_at = ?9
                     WHERE id = ?1",
                    params![
                        run_id,
                        update.registered_ok as i64,
                        update.registered_failed as i64,
                        update.rt_ok as i64,
                        update.rt_failed as i64,
                        update.total_s2a_ok as i64,
                        update.total_s2a_failed as i64,
                        update.elapsed_secs,
                        update.finished_at,
                    ],
                )?;
            }
            WriteCommand::FailRun { run_id, error } => {
                conn.execute(
                    "UPDATE runs SET status = 'failed', error = ?2, finished_at = ?3 WHERE id = ?1",
                    params![run_id, error, crate::util::beijing_now().to_rfc3339()],
                )?;
            }
            WriteCommand::UpdateDistribution {
                run_id,
                team,
                assigned,
                ok,
                failed,
            } => {
                conn.execute(
                    "UPDATE run_distributions SET assigned_count = ?3, s2a_ok = ?4, s2a_failed = ?5
                     WHERE run_id = ?1 AND team_name = ?2",
                    params![run_id, team, assigned as i64, ok as i64, failed as i64,],
                )?;
            }
            WriteCommand::RenameSchedule { old_name, new_name } => {
                conn.execute(
                    "UPDATE runs SET schedule_name = ?2 WHERE schedule_name = ?1",
                    params![old_name, new_name],
                )?;
            }
            WriteCommand::InsertInviteUpload {
                id,
                filename,
                owner_count,
                created_at,
            } => {
                conn.execute(
                    "INSERT INTO invite_uploads (id, filename, owner_count, created_at) VALUES (?1, ?2, ?3, ?4)",
                    params![id, filename, owner_count as i64, created_at],
                )?;
            }
            WriteCommand::InsertInviteOwners { upload_id, owners } => {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare(
                        "INSERT INTO invite_owners (upload_id, email, account_id, access_token, expires) VALUES (?1, ?2, ?3, ?4, ?5)",
                    )?;
                    for o in owners {
                        stmt.execute(params![
                            upload_id,
                            o.email,
                            o.account_id,
                            o.access_token,
                            o.expires
                        ])?;
                    }
                }
                tx.commit()?;
            }
            WriteCommand::InsertInviteTask(task) => {
                conn.execute(
                    "INSERT INTO invite_tasks (id, upload_id, owner_email, owner_account_id, s2a_team, invite_count, status, created_at)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
                    params![
                        task.id, task.upload_id, task.owner_email, task.owner_account_id,
                        task.s2a_team, task.invite_count as i64, task.created_at,
                    ],
                )?;
            }
            WriteCommand::InsertInviteEmails { task_id, emails } => {
                let tx = conn.transaction()?;
                {
                    let mut stmt = tx.prepare(
                        "INSERT INTO invite_emails (task_id, email, password) VALUES (?1, ?2, ?3)",
                    )?;
                    for e in emails {
                        stmt.execute(params![task_id, e.email, e.password])?;
                    }
                }
                tx.commit()?;
            }
            WriteCommand::UpdateInviteTask { task_id, update } => {
                let mut sets = Vec::new();
                let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
                if let Some(ref s) = update.status {
                    sets.push("status = ?");
                    values.push(Box::new(s.clone()));
                }
                if let Some(v) = update.invited_ok {
                    sets.push("invited_ok = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(v) = update.invited_failed {
                    sets.push("invited_failed = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(v) = update.reg_ok {
                    sets.push("reg_ok = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(v) = update.reg_failed {
                    sets.push("reg_failed = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(v) = update.rt_ok {
                    sets.push("rt_ok = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(v) = update.rt_failed {
                    sets.push("rt_failed = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(v) = update.s2a_ok {
                    sets.push("s2a_ok = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(v) = update.s2a_failed {
                    sets.push("s2a_failed = ?");
                    values.push(Box::new(v as i64));
                }
                if let Some(ref e) = update.error {
                    sets.push("error = ?");
                    values.push(Box::new(e.clone()));
                }
                if let Some(ref f) = update.finished_at {
                    sets.push("finished_at = ?");
                    values.push(Box::new(f.clone()));
                }
                if !sets.is_empty() {
                    values.push(Box::new(task_id));
                    let sql = format!("UPDATE invite_tasks SET {} WHERE id = ?", sets.join(", "));
                    let params: Vec<&dyn rusqlite::types::ToSql> =
                        values.iter().map(|v| v.as_ref()).collect();
                    conn.execute(&sql, params.as_slice())?;
                }
            }
            WriteCommand::UpdateInviteEmail { email_id, update } => {
                let mut sets = Vec::new();
                let mut values: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
                if let Some(ref s) = update.invite_status {
                    sets.push("invite_status = ?");
                    values.push(Box::new(s.clone()));
                }
                if let Some(ref s) = update.reg_status {
                    sets.push("reg_status = ?");
                    values.push(Box::new(s.clone()));
                }
                if let Some(ref s) = update.rt_status {
                    sets.push("rt_status = ?");
                    values.push(Box::new(s.clone()));
                }
                if let Some(ref s) = update.s2a_status {
                    sets.push("s2a_status = ?");
                    values.push(Box::new(s.clone()));
                }
                if let Some(ref s) = update.refresh_token {
                    sets.push("refresh_token = ?");
                    values.push(Box::new(s.clone()));
                }
                if let Some(ref e) = update.error {
                    sets.push("error = ?");
                    values.push(Box::new(e.clone()));
                }
                if !sets.is_empty() {
                    values.push(Box::new(email_id));
                    let sql = format!("UPDATE invite_emails SET {} WHERE id = ?", sets.join(", "));
                    let params: Vec<&dyn rusqlite::types::ToSql> =
                        values.iter().map(|v| v.as_ref()).collect();
                    conn.execute(&sql, params.as_slice())?;
                }
            }
            WriteCommand::MarkOwnerUsed { owner_id } => {
                conn.execute(
                    "UPDATE invite_owners SET used = 1 WHERE id = ?1",
                    params![owner_id],
                )?;
            }
            WriteCommand::ResetOwnerUsed { owner_id } => {
                conn.execute(
                    "UPDATE invite_owners SET used = 0 WHERE id = ?1",
                    params![owner_id],
                )?;
            }
            WriteCommand::UpsertOwnerHealth {
                account_id,
                owner_status,
                members_json,
                checked_at,
                expires_at,
                cache_status,
                source,
                last_error,
            } => {
                conn.execute(
                    "INSERT INTO owner_health (
                        account_id, owner_status, members_json, checked_at,
                        expires_at, cache_status, source, last_error
                     )
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                     ON CONFLICT(account_id) DO UPDATE SET
                     owner_status = excluded.owner_status,
                     members_json = excluded.members_json,
                     checked_at = excluded.checked_at,
                     expires_at = excluded.expires_at,
                     cache_status = excluded.cache_status,
                     source = excluded.source,
                     last_error = excluded.last_error",
                    params![
                        account_id,
                        owner_status,
                        members_json,
                        checked_at,
                        expires_at,
                        cache_status,
                        source,
                        last_error
                    ],
                )?;
            }
        }
        Ok(())
    }

    fn send_write_command(&self, cmd: WriteCommand) -> Result<()> {
        self.write_tx
            .send(cmd)
            .map_err(|e| anyhow::anyhow!("写队列发送失败: {e}"))
    }

    pub fn enqueue_insert_run(&self, run: NewRun) -> Result<()> {
        self.send_write_command(WriteCommand::InsertRun(run))
    }

    pub fn enqueue_insert_distributions(
        &self,
        run_id: String,
        entries: Vec<NewDistribution>,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::InsertDistributions { run_id, entries })
    }

    pub fn enqueue_complete_run(&self, run_id: String, update: RunCompletion) -> Result<()> {
        self.send_write_command(WriteCommand::CompleteRun { run_id, update })
    }

    pub fn enqueue_fail_run(&self, run_id: String, error: String) -> Result<()> {
        self.send_write_command(WriteCommand::FailRun { run_id, error })
    }

    pub fn enqueue_update_distribution(
        &self,
        run_id: String,
        team: String,
        assigned: usize,
        ok: usize,
        failed: usize,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::UpdateDistribution {
            run_id,
            team,
            assigned,
            ok,
            failed,
        })
    }

    pub fn enqueue_rename_schedule(&self, old_name: String, new_name: String) -> Result<()> {
        self.send_write_command(WriteCommand::RenameSchedule { old_name, new_name })
    }

    #[allow(dead_code)]
    pub fn insert_run(&self, run: &NewRun) -> Result<String> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO runs (id, schedule_name, trigger_type, status, target_count, started_at)
             VALUES (?1, ?2, ?3, 'running', ?4, ?5)",
            params![
                run.id,
                run.schedule_name,
                run.trigger_type,
                run.target_count as i64,
                run.started_at,
            ],
        )?;
        Ok(run.id.clone())
    }

    #[allow(dead_code)]
    pub fn insert_distributions(&self, run_id: &str, entries: &[NewDistribution]) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO run_distributions (run_id, team_name, percent) VALUES (?1, ?2, ?3)",
            )?;
            for entry in entries {
                stmt.execute(params![run_id, entry.team_name, entry.percent as i64])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn complete_run(&self, run_id: &str, update: &RunCompletion) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE runs SET
                status = 'completed',
                registered_ok = ?2,
                registered_failed = ?3,
                rt_ok = ?4,
                rt_failed = ?5,
                total_s2a_ok = ?6,
                total_s2a_failed = ?7,
                elapsed_secs = ?8,
                finished_at = ?9
             WHERE id = ?1",
            params![
                run_id,
                update.registered_ok as i64,
                update.registered_failed as i64,
                update.rt_ok as i64,
                update.rt_failed as i64,
                update.total_s2a_ok as i64,
                update.total_s2a_failed as i64,
                update.elapsed_secs,
                update.finished_at,
            ],
        )?;
        Ok(())
    }

    /// 重命名计划时同步更新所有历史 run 记录的 schedule_name
    #[allow(dead_code)]
    pub fn rename_schedule(&self, old_name: &str, new_name: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(
            "UPDATE runs SET schedule_name = ?2 WHERE schedule_name = ?1",
            params![old_name, new_name],
        )?;
        Ok(affected)
    }

    #[allow(dead_code)]
    pub fn fail_run(&self, run_id: &str, error: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE runs SET status = 'failed', error = ?2, finished_at = ?3 WHERE id = ?1",
            params![run_id, error, crate::util::beijing_now().to_rfc3339()],
        )?;
        Ok(())
    }

    /// 查询某个计划最近一次完成/失败的 finished_at 时间戳（ISO 8601）
    pub fn last_finished_at(&self, schedule_name: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT finished_at FROM runs
             WHERE schedule_name = ?1 AND finished_at IS NOT NULL
             ORDER BY finished_at DESC LIMIT 1",
            params![schedule_name],
            |row| row.get::<_, String>(0),
        );
        match result {
            Ok(ts) => Ok(Some(ts)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    #[allow(dead_code)]
    pub fn update_distribution(
        &self,
        run_id: &str,
        team: &str,
        assigned: usize,
        ok: usize,
        failed: usize,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE run_distributions SET assigned_count = ?3, s2a_ok = ?4, s2a_failed = ?5
             WHERE run_id = ?1 AND team_name = ?2",
            params![run_id, team, assigned as i64, ok as i64, failed as i64,],
        )?;
        Ok(())
    }

    /// 聚合统计所有完成的运行记录
    pub fn run_stats(&self) -> Result<RunStats> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT
                COUNT(*) AS total_runs,
                SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END),
                SUM(CASE WHEN status='running' THEN 1 ELSE 0 END),
                SUM(target_count),
                SUM(registered_ok),
                SUM(registered_failed),
                SUM(rt_ok),
                SUM(rt_failed),
                SUM(total_s2a_ok),
                SUM(total_s2a_failed),
                SUM(elapsed_secs),
                AVG(CASE WHEN status='completed' AND target_count>0 THEN elapsed_secs/target_count END)
            FROM runs",
        )?;
        let stats = stmt.query_row([], |row| {
            Ok(RunStats {
                total_runs: row.get::<_, i64>(0).unwrap_or(0) as usize,
                completed: row.get::<_, i64>(1).unwrap_or(0) as usize,
                failed: row.get::<_, i64>(2).unwrap_or(0) as usize,
                running: row.get::<_, i64>(3).unwrap_or(0) as usize,
                total_target: row.get::<_, i64>(4).unwrap_or(0) as usize,
                total_reg_ok: row.get::<_, i64>(5).unwrap_or(0) as usize,
                total_reg_failed: row.get::<_, i64>(6).unwrap_or(0) as usize,
                total_rt_ok: row.get::<_, i64>(7).unwrap_or(0) as usize,
                total_rt_failed: row.get::<_, i64>(8).unwrap_or(0) as usize,
                total_s2a_ok: row.get::<_, i64>(9).unwrap_or(0) as usize,
                total_s2a_failed: row.get::<_, i64>(10).unwrap_or(0) as usize,
                total_elapsed_secs: row.get::<_, f64>(11).unwrap_or(0.0),
                avg_secs_per_account: row.get::<_, f64>(12).unwrap_or(0.0),
            })
        })?;
        Ok(stats)
    }

    pub fn list_runs(
        &self,
        page: usize,
        per_page: usize,
        schedule: Option<&str>,
        trigger: Option<RunTriggerFilter>,
    ) -> Result<(Vec<RunRecord>, usize)> {
        let conn = self.conn.lock().unwrap();

        let offset = (page.saturating_sub(1)) * per_page;
        let (total, rows) = match (schedule, trigger) {
            (Some(sched), Some(RunTriggerFilter::Manual)) => {
                let total = conn.query_row(
                    "SELECT COUNT(*) FROM runs WHERE schedule_name = ?1 AND (trigger_type = 'manual_task' OR trigger_type = 'manual')",
                    params![sched],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                let mut stmt = conn.prepare(
                    "SELECT id, schedule_name, trigger_type, status, target_count,
                            registered_ok, registered_failed, rt_ok, rt_failed,
                            total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                            started_at, finished_at
                     FROM runs
                     WHERE schedule_name = ?1 AND (trigger_type = 'manual_task' OR trigger_type = 'manual')
                     ORDER BY started_at DESC LIMIT ?2 OFFSET ?3",
                )?;
                let rows = stmt
                    .query_map(params![sched, per_page as i64, offset as i64], map_run_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                (total, rows)
            }
            (Some(sched), Some(RunTriggerFilter::Scheduled)) => {
                let total = conn.query_row(
                    "SELECT COUNT(*) FROM runs WHERE schedule_name = ?1 AND trigger_type = 'scheduled'",
                    params![sched],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                let mut stmt = conn.prepare(
                    "SELECT id, schedule_name, trigger_type, status, target_count,
                            registered_ok, registered_failed, rt_ok, rt_failed,
                            total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                            started_at, finished_at
                     FROM runs
                     WHERE schedule_name = ?1 AND trigger_type = 'scheduled'
                     ORDER BY started_at DESC LIMIT ?2 OFFSET ?3",
                )?;
                let rows = stmt
                    .query_map(params![sched, per_page as i64, offset as i64], map_run_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                (total, rows)
            }
            (Some(sched), None) => {
                let total = conn.query_row(
                    "SELECT COUNT(*) FROM runs WHERE schedule_name = ?1",
                    params![sched],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                let mut stmt = conn.prepare(
                    "SELECT id, schedule_name, trigger_type, status, target_count,
                            registered_ok, registered_failed, rt_ok, rt_failed,
                            total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                            started_at, finished_at
                     FROM runs WHERE schedule_name = ?1
                     ORDER BY started_at DESC LIMIT ?2 OFFSET ?3",
                )?;
                let rows = stmt
                    .query_map(params![sched, per_page as i64, offset as i64], map_run_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                (total, rows)
            }
            (None, Some(RunTriggerFilter::Manual)) => {
                let total = conn.query_row(
                    "SELECT COUNT(*) FROM runs WHERE (trigger_type = 'manual_task' OR trigger_type = 'manual')",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                let mut stmt = conn.prepare(
                    "SELECT id, schedule_name, trigger_type, status, target_count,
                            registered_ok, registered_failed, rt_ok, rt_failed,
                            total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                            started_at, finished_at
                     FROM runs
                     WHERE (trigger_type = 'manual_task' OR trigger_type = 'manual')
                     ORDER BY started_at DESC LIMIT ?1 OFFSET ?2",
                )?;
                let rows = stmt
                    .query_map(params![per_page as i64, offset as i64], map_run_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                (total, rows)
            }
            (None, Some(RunTriggerFilter::Scheduled)) => {
                let total = conn.query_row(
                    "SELECT COUNT(*) FROM runs WHERE trigger_type = 'scheduled'",
                    [],
                    |row| row.get::<_, i64>(0),
                )? as usize;
                let mut stmt = conn.prepare(
                    "SELECT id, schedule_name, trigger_type, status, target_count,
                            registered_ok, registered_failed, rt_ok, rt_failed,
                            total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                            started_at, finished_at
                     FROM runs
                     WHERE trigger_type = 'scheduled'
                     ORDER BY started_at DESC LIMIT ?1 OFFSET ?2",
                )?;
                let rows = stmt
                    .query_map(params![per_page as i64, offset as i64], map_run_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                (total, rows)
            }
            (None, None) => {
                let total = conn
                    .query_row("SELECT COUNT(*) FROM runs", [], |row| row.get::<_, i64>(0))?
                    as usize;
                let mut stmt = conn.prepare(
                    "SELECT id, schedule_name, trigger_type, status, target_count,
                            registered_ok, registered_failed, rt_ok, rt_failed,
                            total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                            started_at, finished_at
                     FROM runs ORDER BY started_at DESC LIMIT ?1 OFFSET ?2",
                )?;
                let rows = stmt
                    .query_map(params![per_page as i64, offset as i64], map_run_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                (total, rows)
            }
        };

        Ok((rows, total))
    }

    pub fn get_run(&self, run_id: &str) -> Result<Option<RunDetail>> {
        let conn = self.conn.lock().unwrap();

        let run = {
            let mut stmt = conn.prepare(
                "SELECT id, schedule_name, trigger_type, status, target_count,
                        registered_ok, registered_failed, rt_ok, rt_failed,
                        total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                        started_at, finished_at
                 FROM runs WHERE id = ?1",
            )?;
            let mut rows = stmt.query_map(params![run_id], map_run_row)?;
            match rows.next() {
                Some(Ok(r)) => r,
                _ => return Ok(None),
            }
        };

        let distributions = {
            let mut stmt = conn.prepare(
                "SELECT id, run_id, team_name, percent, assigned_count, s2a_ok, s2a_failed
                 FROM run_distributions WHERE run_id = ?1 ORDER BY id",
            )?;
            stmt.query_map(params![run_id], |row| {
                Ok(DistributionRecord {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    team_name: row.get(2)?,
                    percent: row.get::<_, i64>(3)? as usize,
                    assigned_count: row.get::<_, i64>(4)? as usize,
                    s2a_ok: row.get::<_, i64>(5)? as usize,
                    s2a_failed: row.get::<_, i64>(6)? as usize,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };

        Ok(Some(RunDetail { run, distributions }))
    }

    // ─── 邀请模块 ────────────────────────────────────────────────────────────

    #[allow(dead_code)]
    pub fn enqueue_insert_invite_upload(
        &self,
        id: String,
        filename: String,
        owner_count: usize,
        created_at: String,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::InsertInviteUpload {
            id,
            filename,
            owner_count,
            created_at,
        })
    }

    #[allow(dead_code)]
    pub fn enqueue_insert_invite_owners(
        &self,
        upload_id: String,
        owners: Vec<InviteOwnerInsert>,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::InsertInviteOwners { upload_id, owners })
    }

    pub fn enqueue_insert_invite_task(&self, task: NewInviteTask) -> Result<()> {
        self.send_write_command(WriteCommand::InsertInviteTask(task))
    }

    pub fn enqueue_insert_invite_emails(
        &self,
        task_id: String,
        emails: Vec<InviteEmailInsert>,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::InsertInviteEmails { task_id, emails })
    }

    pub fn enqueue_update_invite_task(
        &self,
        task_id: String,
        update: InviteTaskUpdate,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::UpdateInviteTask { task_id, update })
    }

    pub fn enqueue_update_invite_email(
        &self,
        email_id: i64,
        update: InviteEmailUpdate,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::UpdateInviteEmail { email_id, update })
    }

    pub fn enqueue_mark_owner_used(&self, owner_id: i64) -> Result<()> {
        self.send_write_command(WriteCommand::MarkOwnerUsed { owner_id })
    }

    pub fn enqueue_reset_owner_used(&self, owner_id: i64) -> Result<()> {
        self.send_write_command(WriteCommand::ResetOwnerUsed { owner_id })
    }

    #[allow(dead_code)]
    pub fn enqueue_upsert_owner_health(
        &self,
        account_id: String,
        owner_status: String,
        members_json: String,
        checked_at: String,
    ) -> Result<()> {
        self.send_write_command(WriteCommand::UpsertOwnerHealth {
            account_id,
            owner_status,
            members_json,
            checked_at,
            expires_at: None,
            cache_status: "fresh".to_string(),
            source: Some("live".to_string()),
            last_error: None,
        })
    }

    pub fn enqueue_upsert_owner_health_record(&self, record: OwnerHealthUpsert) -> Result<()> {
        self.send_write_command(WriteCommand::UpsertOwnerHealth {
            account_id: record.account_id,
            owner_status: record.owner_status,
            members_json: record.members_json,
            checked_at: record.checked_at,
            expires_at: record.expires_at,
            cache_status: record.cache_status.unwrap_or_else(|| "fresh".to_string()),
            source: record.source.or_else(|| Some("live".to_string())),
            last_error: record.last_error,
        })
    }

    pub fn get_owner_health_by_account_ids(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<OwnerHealthRecord>> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap();
        let placeholders = std::iter::repeat("?")
            .take(account_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT account_id, owner_status, members_json, checked_at, expires_at, cache_status, source, last_error
             FROM owner_health
             WHERE account_id IN ({})
             ORDER BY checked_at DESC",
            placeholders
        );
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn ToSql> = account_ids.iter().map(|id| id as &dyn ToSql).collect();
        let rows = stmt
            .query_map(params.as_slice(), map_owner_health_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    #[allow(dead_code)]
    pub fn get_owner_health_freshness(
        &self,
        account_id: &str,
    ) -> Result<Option<(String, Option<String>)>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT cache_status, expires_at FROM owner_health WHERE account_id = ?1",
            params![account_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?)),
        )
        .optional()
        .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub fn mark_owner_health_stale(&self, account_id: &str) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(
            "UPDATE owner_health SET cache_status = 'stale' WHERE account_id = ?1",
            params![account_id],
        )?;
        Ok(affected)
    }

    pub fn get_all_owner_health_records(&self) -> Result<Vec<OwnerHealthRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT account_id, owner_status, members_json, checked_at, expires_at, cache_status, source, last_error
             FROM owner_health
             ORDER BY checked_at DESC",
        )?;
        let rows = stmt
            .query_map([], map_owner_health_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_all_owner_health(&self) -> Result<Vec<(String, String, Option<String>, String)>> {
        let rows = self.get_all_owner_health_records()?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.account_id,
                    row.owner_status,
                    row.members_json,
                    row.checked_at,
                )
            })
            .collect())
    }

    /// 同步插入上传记录和 owners（用于上传接口立即返回结果）
    pub fn insert_invite_upload_sync(
        &self,
        id: &str,
        filename: &str,
        owners: &[InviteOwnerInsert],
    ) -> Result<()> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "INSERT INTO invite_uploads (id, filename, owner_count, created_at) VALUES (?1, ?2, ?3, ?4)",
            params![id, filename, owners.len() as i64, crate::util::beijing_now().to_rfc3339()],
        )?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO invite_owners (upload_id, email, account_id, access_token, expires) VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;
            for o in owners {
                stmt.execute(params![
                    id,
                    o.email,
                    o.account_id,
                    o.access_token,
                    o.expires
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_invite_uploads(&self) -> Result<Vec<InviteUploadRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT u.id, u.filename, u.owner_count, u.created_at,
                    COALESCE((SELECT COUNT(*) FROM invite_owners o
                              WHERE o.upload_id = u.id AND o.used = 0
                              AND (SELECT COUNT(*) FROM invite_tasks t
                                   WHERE t.upload_id = u.id AND t.owner_email = o.email AND t.status = 'failed') < 2
                    ), 0),
                    COALESCE((SELECT GROUP_CONCAT(o2.email, ', ') FROM invite_owners o2 WHERE o2.upload_id = u.id), '')
             FROM invite_uploads u ORDER BY u.created_at DESC",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(InviteUploadRecord {
                    id: row.get(0)?,
                    filename: row.get(1)?,
                    owner_count: row.get::<_, i64>(2)? as usize,
                    created_at: row.get(3)?,
                    unused_count: row.get::<_, i64>(4)? as usize,
                    owner_emails: row.get::<_, String>(5).unwrap_or_default(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_invite_upload_detail(&self, upload_id: &str) -> Result<Option<InviteUploadDetail>> {
        let conn = self.conn.lock().unwrap();
        let upload = {
            let mut stmt = conn.prepare(
                "SELECT u.id, u.filename, u.owner_count, u.created_at,
                        COALESCE((SELECT COUNT(*) FROM invite_owners o
                                  WHERE o.upload_id = u.id AND o.used = 0
                                  AND (SELECT COUNT(*) FROM invite_tasks t
                                       WHERE t.upload_id = u.id AND t.owner_email = o.email AND t.status = 'failed') < 2
                        ), 0),
                        COALESCE((SELECT GROUP_CONCAT(o2.email, ', ') FROM invite_owners o2 WHERE o2.upload_id = u.id), '')
                 FROM invite_uploads u WHERE u.id = ?1",
            )?;
            let mut rows = stmt.query_map(params![upload_id], |row| {
                Ok(InviteUploadRecord {
                    id: row.get(0)?,
                    filename: row.get(1)?,
                    owner_count: row.get::<_, i64>(2)? as usize,
                    created_at: row.get(3)?,
                    unused_count: row.get::<_, i64>(4)? as usize,
                    owner_emails: row.get::<_, String>(5).unwrap_or_default(),
                })
            })?;
            match rows.next() {
                Some(Ok(r)) => r,
                _ => return Ok(None),
            }
        };
        let owners = {
            let mut stmt = conn.prepare(
                "SELECT id, upload_id, email, account_id, expires, used FROM invite_owners WHERE upload_id = ?1 ORDER BY id",
            )?;
            stmt.query_map(params![upload_id], |row| {
                Ok(InviteOwnerRecord {
                    id: row.get(0)?,
                    upload_id: row.get(1)?,
                    email: row.get(2)?,
                    account_id: row.get(3)?,
                    expires: row.get(4)?,
                    used: row.get::<_, i64>(5)? != 0,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        Ok(Some(InviteUploadDetail { upload, owners }))
    }

    /// 根据邮箱列表查找已注册账号的密码（用于 team 满员恢复场景）
    pub fn find_passwords_by_emails(&self, emails: &[String]) -> Result<Vec<(String, String)>> {
        if emails.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap();
        let placeholders = std::iter::repeat("?")
            .take(emails.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT email, password FROM invite_emails WHERE email IN ({}) GROUP BY email",
            placeholders
        );
        let mut stmt = conn.prepare(&sql)?;
        let params: Vec<&dyn rusqlite::types::ToSql> = emails
            .iter()
            .map(|e| e as &dyn rusqlite::types::ToSql)
            .collect();
        let rows = stmt
            .query_map(params.as_slice(), |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// 获取指定 upload 中未使用的 owner（含 access_token）
    pub fn get_unused_owners(&self, upload_id: &str) -> Result<Vec<(i64, String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, email, account_id, access_token FROM invite_owners WHERE upload_id = ?1 AND used = 0 ORDER BY id",
        )?;
        let rows = stmt
            .query_map(params![upload_id], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// 列出所有有 access_token 的 owners（用于 Team 管理模块）
    /// 返回 (email, account_id, access_token) 的列表，每个 account_id 只取最新一条
    #[allow(dead_code)]
    pub fn list_all_owners_with_tokens(&self) -> Result<Vec<(String, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT email, account_id, access_token FROM invite_owners \
             WHERE access_token IS NOT NULL AND access_token != '' \
             GROUP BY account_id \
             ORDER BY id DESC",
        )?;
        let rows = stmt
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    /// 根据 account_id 获取 access_token
    pub fn get_owner_token_by_account_id(&self, account_id: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT access_token FROM invite_owners WHERE account_id = ?1 LIMIT 1",
            params![account_id],
            |row| row.get::<_, String>(0),
        );
        match result {
            Ok(token) => Ok(Some(token)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 根据 account_id 获取 owner 的 (id, email, access_token)
    pub fn get_owner_info_by_account_id(
        &self,
        account_id: &str,
    ) -> Result<Option<(i64, String, String)>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT id, email, access_token FROM invite_owners WHERE account_id = ?1 AND access_token IS NOT NULL AND access_token != '' ORDER BY id DESC LIMIT 1",
            params![account_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        );
        match result {
            Ok(info) => Ok(Some(info)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// 根据邮箱获取 refresh_token（从 invite_emails 表）
    pub fn get_email_refresh_token(&self, email: &str) -> Result<Option<String>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT refresh_token FROM invite_emails WHERE email = ?1 AND refresh_token IS NOT NULL AND refresh_token != '' ORDER BY id DESC LIMIT 1",
            params![email],
            |row| row.get::<_, String>(0),
        );
        match result {
            Ok(token) => Ok(Some(token)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn list_invite_tasks(&self, upload_id: Option<&str>) -> Result<Vec<InviteTaskRecord>> {
        let conn = self.conn.lock().unwrap();
        let (sql, has_filter) = match upload_id {
            Some(_) => (
                "SELECT id, upload_id, owner_email, owner_account_id, s2a_team, invite_count,
                        status, invited_ok, invited_failed, reg_ok, reg_failed, rt_ok, rt_failed,
                        s2a_ok, s2a_failed, error, created_at, finished_at
                 FROM invite_tasks WHERE upload_id = ?1 ORDER BY created_at DESC",
                true,
            ),
            None => (
                "SELECT id, upload_id, owner_email, owner_account_id, s2a_team, invite_count,
                        status, invited_ok, invited_failed, reg_ok, reg_failed, rt_ok, rt_failed,
                        s2a_ok, s2a_failed, error, created_at, finished_at
                 FROM invite_tasks ORDER BY created_at DESC",
                false,
            ),
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = if has_filter {
            stmt.query_map(params![upload_id.unwrap()], map_invite_task_row)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map([], map_invite_task_row)?
                .collect::<Result<Vec<_>, _>>()?
        };
        Ok(rows)
    }

    pub fn get_invite_task_detail(&self, task_id: &str) -> Result<Option<InviteTaskDetail>> {
        let conn = self.conn.lock().unwrap();
        let task = {
            let mut stmt = conn.prepare(
                "SELECT id, upload_id, owner_email, owner_account_id, s2a_team, invite_count,
                        status, invited_ok, invited_failed, reg_ok, reg_failed, rt_ok, rt_failed,
                        s2a_ok, s2a_failed, error, created_at, finished_at
                 FROM invite_tasks WHERE id = ?1",
            )?;
            let mut rows = stmt.query_map(params![task_id], map_invite_task_row)?;
            match rows.next() {
                Some(Ok(r)) => r,
                _ => return Ok(None),
            }
        };
        let emails = {
            let mut stmt = conn.prepare(
                "SELECT id, task_id, email, invite_status, reg_status, rt_status, s2a_status, error
                 FROM invite_emails WHERE task_id = ?1 ORDER BY id",
            )?;
            stmt.query_map(params![task_id], |row| {
                Ok(InviteEmailRecord {
                    id: row.get(0)?,
                    task_id: row.get(1)?,
                    email: row.get(2)?,
                    invite_status: row.get(3)?,
                    reg_status: row.get(4)?,
                    rt_status: row.get(5)?,
                    s2a_status: row.get(6)?,
                    error: row.get(7)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?
        };
        Ok(Some(InviteTaskDetail { task, emails }))
    }

    #[allow(dead_code)]
    pub fn upsert_owner_registry_from_snapshot(&self, record: OwnerRegistryUpsert) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO owner_registry (
                account_id, display_email, latest_access_token, token_expires_at, source_upload_id,
                state, disabled_reason, last_health_checked_at, last_health_status,
                member_count_cached, available_slots_cached, last_invite_task_id,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)
             ON CONFLICT(account_id) DO UPDATE SET
                display_email = excluded.display_email,
                latest_access_token = excluded.latest_access_token,
                token_expires_at = excluded.token_expires_at,
                source_upload_id = excluded.source_upload_id,
                state = excluded.state,
                disabled_reason = excluded.disabled_reason,
                last_health_checked_at = excluded.last_health_checked_at,
                last_health_status = excluded.last_health_status,
                member_count_cached = excluded.member_count_cached,
                available_slots_cached = excluded.available_slots_cached,
                last_invite_task_id = excluded.last_invite_task_id,
                updated_at = excluded.updated_at",
            params![
                record.account_id,
                record.display_email,
                record.latest_access_token,
                record.token_expires_at,
                record.source_upload_id,
                record.state.unwrap_or_else(|| "active".to_string()),
                record.disabled_reason,
                record.last_health_checked_at,
                record.last_health_status,
                record.member_count_cached.unwrap_or(0) as i64,
                record.available_slots_cached.unwrap_or(0) as i64,
                record.last_invite_task_id,
                record.created_at,
                record.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn sync_owner_registry_from_invite_owners(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let now = crate::util::beijing_now().to_rfc3339();
        let affected = conn.execute(
            "INSERT INTO owner_registry (
                account_id, display_email, latest_access_token, token_expires_at,
                source_upload_id, state, created_at, updated_at
             )
             SELECT o.account_id, o.email, o.access_token, o.expires, o.upload_id, 'active', ?1, ?2
             FROM invite_owners o
             INNER JOIN (
                SELECT account_id, MAX(id) AS max_id
                FROM invite_owners
                WHERE access_token IS NOT NULL AND access_token != ''
                GROUP BY account_id
             ) latest
                 ON latest.max_id = o.id
             ON CONFLICT(account_id) DO UPDATE SET
                display_email = excluded.display_email,
                latest_access_token = excluded.latest_access_token,
                token_expires_at = excluded.token_expires_at,
                source_upload_id = excluded.source_upload_id,
                updated_at = excluded.updated_at",
            params![now, now],
        )?;
        Ok(affected)
    }

    pub fn list_owner_registry_page(
        &self,
        query: &OwnerRegistryQuery,
    ) -> Result<OwnerRegistryPage> {
        let page = query.page.max(1);
        let page_size = query.page_size.max(1);
        let offset = (page - 1) * page_size;
        let total = self.count_owner_registry_by_filters(query)?;
        let conn = self.conn.lock().unwrap();
        let (where_sql, mut values) = build_owner_registry_where(query);
        let order_by = owner_registry_order_clause(query);
        values.push(Box::new(page_size as i64));
        values.push(Box::new(offset as i64));
        let sql = format!(
            "SELECT
                r.account_id,
                r.display_email,
                r.latest_access_token,
                r.token_expires_at,
                r.source_upload_id,
                r.state,
                r.disabled_reason,
                r.last_health_checked_at,
                r.last_health_status,
                r.member_count_cached,
                r.available_slots_cached,
                r.last_invite_task_id,
                r.created_at,
                r.updated_at,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM owner_health h
                        WHERE h.account_id = r.account_id
                          AND h.members_json LIKE '%\"status\":\"banned\"%'
                    ) THEN 1
                    ELSE 0
                END AS has_banned_member
             FROM owner_registry r
             {where_sql}
             ORDER BY {order_by}
             LIMIT ? OFFSET ?"
        );
        let params: Vec<&dyn ToSql> = values.iter().map(|value| value.as_ref()).collect();
        let mut stmt = conn.prepare(&sql)?;
        let items = stmt
            .query_map(params.as_slice(), map_owner_registry_row)?
            .collect::<Result<Vec<_>, _>>()?;
        let total_pages = if total == 0 {
            0
        } else {
            total.div_ceil(page_size)
        };
        Ok(OwnerRegistryPage {
            items,
            total,
            page,
            page_size,
            total_pages,
        })
    }

    pub fn count_owner_registry_by_filters(&self, query: &OwnerRegistryQuery) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let (where_sql, values) = build_owner_registry_where(query);
        let sql = format!("SELECT COUNT(*) FROM owner_registry r {where_sql}");
        let params: Vec<&dyn ToSql> = values.iter().map(|value| value.as_ref()).collect();
        let total = conn.query_row(&sql, params.as_slice(), |row| row.get::<_, i64>(0))? as usize;
        Ok(total)
    }

    #[allow(dead_code)]
    pub fn get_owner_registry_by_account_id(
        &self,
        account_id: &str,
    ) -> Result<Option<OwnerRegistryRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT
                r.account_id,
                r.display_email,
                r.latest_access_token,
                r.token_expires_at,
                r.source_upload_id,
                r.state,
                r.disabled_reason,
                r.last_health_checked_at,
                r.last_health_status,
                r.member_count_cached,
                r.available_slots_cached,
                r.last_invite_task_id,
                r.created_at,
                r.updated_at,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM owner_health h
                        WHERE h.account_id = r.account_id
                          AND h.members_json LIKE '%\"status\":\"banned\"%'
                    ) THEN 1
                    ELSE 0
                END AS has_banned_member
             FROM owner_registry r
             WHERE r.account_id = ?1",
            params![account_id],
            map_owner_registry_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn batch_update_owner_registry_state(
        &self,
        account_ids: &[String],
        state: &str,
        reason: Option<&str>,
        updated_at: &str,
    ) -> Result<usize> {
        if account_ids.is_empty() {
            return Ok(0);
        }
        let conn = self.conn.lock().unwrap();
        let placeholders = std::iter::repeat("?")
            .take(account_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "UPDATE owner_registry
             SET state = ?, disabled_reason = ?, updated_at = ?
             WHERE account_id IN ({})",
            placeholders
        );
        let mut values: Vec<Box<dyn ToSql>> = vec![
            Box::new(state.to_string()),
            Box::new(reason.map(|value| value.to_string())),
            Box::new(updated_at.to_string()),
        ];
        for account_id in account_ids {
            values.push(Box::new(account_id.clone()));
        }
        let params: Vec<&dyn ToSql> = values.iter().map(|value| value.as_ref()).collect();
        let affected = conn.execute(&sql, params.as_slice())?;
        Ok(affected)
    }

    pub fn update_owner_registry_health_summary(
        &self,
        account_id: &str,
        last_health_checked_at: Option<&str>,
        last_health_status: Option<&str>,
        member_count_cached: usize,
        available_slots_cached: usize,
        updated_at: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(
            "UPDATE owner_registry
             SET last_health_checked_at = ?2,
                 last_health_status = ?3,
                 member_count_cached = ?4,
                 available_slots_cached = ?5,
                 updated_at = ?6
             WHERE account_id = ?1",
            params![
                account_id,
                last_health_checked_at,
                last_health_status,
                member_count_cached as i64,
                available_slots_cached as i64,
                updated_at,
            ],
        )?;
        Ok(affected)
    }

    pub fn update_owner_registry_member_cache_summary(
        &self,
        account_id: &str,
        member_count_cached: usize,
        available_slots_cached: usize,
        updated_at: &str,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let affected = conn.execute(
            "UPDATE owner_registry
             SET member_count_cached = ?2,
                 available_slots_cached = ?3,
                 updated_at = ?4
             WHERE account_id = ?1",
            params![
                account_id,
                member_count_cached as i64,
                available_slots_cached as i64,
                updated_at,
            ],
        )?;
        Ok(affected)
    }

    pub fn upsert_owner_member_cache(
        &self,
        account_id: &str,
        members_json: Option<&str>,
        member_count: usize,
        cached_at: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO owner_member_cache (account_id, members_json, member_count, cached_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(account_id) DO UPDATE SET
                members_json = excluded.members_json,
                member_count = excluded.member_count,
                cached_at = excluded.cached_at",
            params![account_id, members_json, member_count as i64, cached_at],
        )?;
        Ok(())
    }

    pub fn get_owner_member_cache(
        &self,
        account_id: &str,
    ) -> Result<Option<OwnerMemberCacheRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT account_id, members_json, member_count, cached_at
             FROM owner_member_cache
             WHERE account_id = ?1",
            params![account_id],
            |row| {
                Ok(OwnerMemberCacheRecord {
                    account_id: row.get(0)?,
                    members_json: row.get(1)?,
                    member_count: row.get::<_, i64>(2)? as usize,
                    cached_at: row.get(3)?,
                })
            },
        )
        .optional()
        .map_err(Into::into)
    }

    #[allow(dead_code)]
    pub fn batch_get_owner_member_cache(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<OwnerMemberCacheRecord>> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap();
        let placeholders = std::iter::repeat("?")
            .take(account_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT account_id, members_json, member_count, cached_at
             FROM owner_member_cache
             WHERE account_id IN ({})",
            placeholders
        );
        let params: Vec<&dyn ToSql> = account_ids.iter().map(|id| id as &dyn ToSql).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params.as_slice(), |row| {
                Ok(OwnerMemberCacheRecord {
                    account_id: row.get(0)?,
                    members_json: row.get(1)?,
                    member_count: row.get::<_, i64>(2)? as usize,
                    cached_at: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_team_manage_dashboard_summary(&self) -> Result<TeamManageDashboardSummary> {
        let conn = self.conn.lock().unwrap();
        let owner_counts = conn.query_row(
            "SELECT
                COUNT(*) AS total_owners,
                SUM(CASE WHEN state = 'active' THEN 1 ELSE 0 END) AS active_owners,
                SUM(CASE WHEN state = 'banned' THEN 1 ELSE 0 END) AS banned_owners,
                SUM(CASE WHEN state = 'expired' THEN 1 ELSE 0 END) AS expired_owners,
                SUM(CASE WHEN state = 'quarantined' THEN 1 ELSE 0 END) AS quarantined_owners,
                SUM(CASE WHEN state = 'archived' THEN 1 ELSE 0 END) AS archived_owners,
                SUM(CASE WHEN available_slots_cached > 0 AND state != 'seat_limited' THEN 1 ELSE 0 END) AS owners_with_slots
             FROM owner_registry",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0).unwrap_or(0) as usize,
                    row.get::<_, i64>(1).unwrap_or(0) as usize,
                    row.get::<_, i64>(2).unwrap_or(0) as usize,
                    row.get::<_, i64>(3).unwrap_or(0) as usize,
                    row.get::<_, i64>(4).unwrap_or(0) as usize,
                    row.get::<_, i64>(5).unwrap_or(0) as usize,
                    row.get::<_, i64>(6).unwrap_or(0) as usize,
                ))
            },
        )?;
        let owners_with_banned_members = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM owner_registry r
                 WHERE EXISTS (
                    SELECT 1
                    FROM owner_health h
                    WHERE h.account_id = r.account_id
                      AND h.members_json LIKE '%\"status\":\"banned\"%'
                 )",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0) as usize;
        let health_cache = conn.query_row(
            "SELECT
                SUM(CASE WHEN cache_status = 'fresh' THEN 1 ELSE 0 END),
                SUM(CASE WHEN cache_status = 'stale' THEN 1 ELSE 0 END)
             FROM owner_health",
            [],
            |row| {
                Ok((
                    row.get::<_, i64>(0).unwrap_or(0) as usize,
                    row.get::<_, i64>(1).unwrap_or(0) as usize,
                ))
            },
        )?;
        let running_batch_jobs = conn
            .query_row(
                "SELECT COUNT(*)
                 FROM team_manage_batch_jobs
                 WHERE status IN ('pending', 'running')",
                [],
                |row| row.get::<_, i64>(0),
            )
            .unwrap_or(0) as usize;
        Ok(TeamManageDashboardSummary {
            total_owners: owner_counts.0,
            active_owners: owner_counts.1,
            banned_owners: owner_counts.2,
            expired_owners: owner_counts.3,
            quarantined_owners: owner_counts.4,
            archived_owners: owner_counts.5,
            owners_with_slots: owner_counts.6,
            owners_with_banned_members,
            health_cache_fresh: health_cache.0,
            health_cache_stale: health_cache.1,
            running_batch_jobs,
        })
    }

    pub fn insert_team_manage_batch_job(&self, job: NewTeamManageBatchJob) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO team_manage_batch_jobs (
                job_id, job_type, scope, status, payload_json,
                total_count, success_count, failed_count, skipped_count,
                created_by, created_at, started_at, finished_at, error, fingerprint
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                job.job_id,
                job.job_type,
                job.scope,
                job.status,
                job.payload_json,
                job.total_count as i64,
                job.success_count as i64,
                job.failed_count as i64,
                job.skipped_count as i64,
                job.created_by,
                job.created_at,
                job.started_at,
                job.finished_at,
                job.error,
                job.fingerprint,
            ],
        )?;
        Ok(())
    }

    pub fn insert_team_manage_batch_items(&self, items: Vec<NewTeamManageBatchItem>) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO team_manage_batch_items (
                    job_id, account_id, item_type, status, child_task_id,
                    result_json, error, started_at, finished_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            )?;
            for item in items {
                stmt.execute(params![
                    item.job_id,
                    item.account_id,
                    item.item_type,
                    item.status,
                    item.child_task_id,
                    item.result_json,
                    item.error,
                    item.started_at,
                    item.finished_at,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn update_team_manage_batch_job(
        &self,
        job_id: String,
        update: TeamManageBatchJobUpdate,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut sets = Vec::new();
        let mut values: Vec<Box<dyn ToSql>> = Vec::new();
        if let Some(ref value) = update.status {
            sets.push("status = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(value) = update.total_count {
            sets.push("total_count = ?");
            values.push(Box::new(value as i64));
        }
        if let Some(value) = update.success_count {
            sets.push("success_count = ?");
            values.push(Box::new(value as i64));
        }
        if let Some(value) = update.failed_count {
            sets.push("failed_count = ?");
            values.push(Box::new(value as i64));
        }
        if let Some(value) = update.skipped_count {
            sets.push("skipped_count = ?");
            values.push(Box::new(value as i64));
        }
        if let Some(ref value) = update.started_at {
            sets.push("started_at = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(ref value) = update.finished_at {
            sets.push("finished_at = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(ref value) = update.error {
            sets.push("error = ?");
            values.push(Box::new(value.clone()));
        }
        if sets.is_empty() {
            return Ok(());
        }
        values.push(Box::new(job_id));
        let sql = format!(
            "UPDATE team_manage_batch_jobs SET {} WHERE job_id = ?",
            sets.join(", ")
        );
        let params: Vec<&dyn ToSql> = values.iter().map(|value| value.as_ref()).collect();
        conn.execute(&sql, params.as_slice())?;
        Ok(())
    }

    pub fn update_team_manage_batch_item(
        &self,
        item_id: i64,
        update: TeamManageBatchItemUpdate,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut sets = Vec::new();
        let mut values: Vec<Box<dyn ToSql>> = Vec::new();
        if let Some(ref value) = update.status {
            sets.push("status = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(ref value) = update.child_task_id {
            sets.push("child_task_id = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(ref value) = update.result_json {
            sets.push("result_json = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(ref value) = update.error {
            sets.push("error = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(ref value) = update.started_at {
            sets.push("started_at = ?");
            values.push(Box::new(value.clone()));
        }
        if let Some(ref value) = update.finished_at {
            sets.push("finished_at = ?");
            values.push(Box::new(value.clone()));
        }
        if sets.is_empty() {
            return Ok(());
        }
        values.push(Box::new(item_id));
        let sql = format!(
            "UPDATE team_manage_batch_items SET {} WHERE id = ?",
            sets.join(", ")
        );
        let params: Vec<&dyn ToSql> = values.iter().map(|value| value.as_ref()).collect();
        conn.execute(&sql, params.as_slice())?;
        Ok(())
    }

    pub fn list_team_manage_batch_jobs(
        &self,
        status: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<TeamManageBatchJobRecord>> {
        let conn = self.conn.lock().unwrap();
        let rows = match (status, limit) {
            (Some(status), Some(limit)) => {
                let mut stmt = conn.prepare(
                    "SELECT job_id, job_type, scope, status, payload_json, total_count,
                            success_count, failed_count, skipped_count, created_by,
                            created_at, started_at, finished_at, error, fingerprint
                     FROM team_manage_batch_jobs
                     WHERE status = ?1
                     ORDER BY created_at DESC
                     LIMIT ?2",
                )?;
                stmt.query_map(params![status, limit as i64], map_team_manage_batch_job_row)?
                    .collect::<Result<Vec<_>, _>>()?
            }
            (Some(status), None) => {
                let mut stmt = conn.prepare(
                    "SELECT job_id, job_type, scope, status, payload_json, total_count,
                            success_count, failed_count, skipped_count, created_by,
                            created_at, started_at, finished_at, error, fingerprint
                     FROM team_manage_batch_jobs
                     WHERE status = ?1
                     ORDER BY created_at DESC",
                )?;
                stmt.query_map(params![status], map_team_manage_batch_job_row)?
                    .collect::<Result<Vec<_>, _>>()?
            }
            (None, Some(limit)) => {
                let mut stmt = conn.prepare(
                    "SELECT job_id, job_type, scope, status, payload_json, total_count,
                            success_count, failed_count, skipped_count, created_by,
                            created_at, started_at, finished_at, error, fingerprint
                     FROM team_manage_batch_jobs
                     ORDER BY created_at DESC
                     LIMIT ?1",
                )?;
                stmt.query_map(params![limit as i64], map_team_manage_batch_job_row)?
                    .collect::<Result<Vec<_>, _>>()?
            }
            (None, None) => {
                let mut stmt = conn.prepare(
                    "SELECT job_id, job_type, scope, status, payload_json, total_count,
                            success_count, failed_count, skipped_count, created_by,
                            created_at, started_at, finished_at, error, fingerprint
                     FROM team_manage_batch_jobs
                     ORDER BY created_at DESC",
                )?;
                stmt.query_map([], map_team_manage_batch_job_row)?
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        Ok(rows)
    }

    pub fn get_team_manage_batch_job(
        &self,
        job_id: &str,
    ) -> Result<Option<TeamManageBatchJobRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT job_id, job_type, scope, status, payload_json, total_count,
                    success_count, failed_count, skipped_count, created_by,
                    created_at, started_at, finished_at, error, fingerprint
             FROM team_manage_batch_jobs
             WHERE job_id = ?1",
            params![job_id],
            map_team_manage_batch_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_team_manage_batch_job_by_fingerprint(
        &self,
        fingerprint: &str,
    ) -> Result<Option<TeamManageBatchJobRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT job_id, job_type, scope, status, payload_json, total_count,
                    success_count, failed_count, skipped_count, created_by,
                    created_at, started_at, finished_at, error, fingerprint
             FROM team_manage_batch_jobs
             WHERE fingerprint = ?1
             ORDER BY created_at DESC
             LIMIT 1",
            params![fingerprint],
            map_team_manage_batch_job_row,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn get_team_manage_batch_job_items(
        &self,
        job_id: &str,
    ) -> Result<Vec<TeamManageBatchItemRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, account_id, item_type, status, child_task_id,
                    result_json, error, started_at, finished_at
             FROM team_manage_batch_items
             WHERE job_id = ?1
             ORDER BY id",
        )?;
        let rows = stmt
            .query_map(params![job_id], map_team_manage_batch_item_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_team_manage_batch_items_by_status(
        &self,
        job_id: &str,
        status: &str,
    ) -> Result<Vec<TeamManageBatchItemRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, account_id, item_type, status, child_task_id,
                    result_json, error, started_at, finished_at
             FROM team_manage_batch_items
             WHERE job_id = ?1 AND status = ?2
             ORDER BY id",
        )?;
        let rows = stmt
            .query_map(params![job_id, status], map_team_manage_batch_item_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn get_owner_registry_states_by_account_ids(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<(String, String)>> {
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap();
        let placeholders = std::iter::repeat("?")
            .take(account_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT account_id, state FROM owner_registry WHERE account_id IN ({})",
            placeholders
        );
        let params: Vec<&dyn ToSql> = account_ids.iter().map(|id| id as &dyn ToSql).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params.as_slice(), |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }

    pub fn insert_team_manage_owner_audits(
        &self,
        items: Vec<NewTeamManageOwnerAudit>,
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO team_manage_owner_audits (
                    account_id, action, from_state, to_state, reason, scope, batch_job_id, created_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            for item in items {
                stmt.execute(params![
                    item.account_id,
                    item.action,
                    item.from_state,
                    item.to_state,
                    item.reason,
                    item.scope,
                    item.batch_job_id,
                    item.created_at,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn count_team_manage_owner_audits(
        &self,
        account_id: Option<&str>,
        action: Option<&str>,
        batch_job_id: Option<&str>,
    ) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let mut conditions = Vec::new();
        let mut values: Vec<Box<dyn ToSql>> = Vec::new();

        if let Some(account_id) = account_id.filter(|value| !value.trim().is_empty()) {
            conditions.push("account_id LIKE ?");
            values.push(Box::new(format!("%{}%", account_id.trim())));
        }
        if let Some(action) = action.filter(|value| !value.trim().is_empty()) {
            conditions.push("action = ?");
            values.push(Box::new(action.trim().to_string()));
        }
        if let Some(batch_job_id) = batch_job_id.filter(|value| !value.trim().is_empty()) {
            conditions.push("batch_job_id = ?");
            values.push(Box::new(batch_job_id.trim().to_string()));
        }

        let mut sql = String::from("SELECT COUNT(*) FROM team_manage_owner_audits");
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }

        let params: Vec<&dyn ToSql> = values.iter().map(|value| value.as_ref() as &dyn ToSql).collect();
        let total = conn.query_row(&sql, params.as_slice(), |row| row.get::<_, i64>(0))?;
        Ok(total as usize)
    }

    pub fn list_team_manage_owner_audits(
        &self,
        account_id: Option<&str>,
        action: Option<&str>,
        batch_job_id: Option<&str>,
        page: usize,
        page_size: usize,
    ) -> Result<Vec<TeamManageOwnerAuditRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut conditions = Vec::new();
        let mut values: Vec<Box<dyn ToSql>> = Vec::new();

        if let Some(account_id) = account_id.filter(|value| !value.trim().is_empty()) {
            conditions.push("account_id LIKE ?");
            values.push(Box::new(format!("%{}%", account_id.trim())));
        }
        if let Some(action) = action.filter(|value| !value.trim().is_empty()) {
            conditions.push("action = ?");
            values.push(Box::new(action.trim().to_string()));
        }
        if let Some(batch_job_id) = batch_job_id.filter(|value| !value.trim().is_empty()) {
            conditions.push("batch_job_id = ?");
            values.push(Box::new(batch_job_id.trim().to_string()));
        }

        let offset = page.saturating_sub(1) * page_size;
        let mut sql = String::from(
            "SELECT id, account_id, action, from_state, to_state, reason, scope, batch_job_id, created_at
             FROM team_manage_owner_audits",
        );
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY created_at DESC LIMIT ? OFFSET ?");
        values.push(Box::new(page_size as i64));
        values.push(Box::new(offset as i64));

        let params: Vec<&dyn ToSql> = values.iter().map(|value| value.as_ref() as &dyn ToSql).collect();
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt
            .query_map(params.as_slice(), map_team_manage_owner_audit_row)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    }
}

fn map_run_row(row: &rusqlite::Row) -> rusqlite::Result<RunRecord> {
    Ok(RunRecord {
        id: row.get(0)?,
        schedule_name: row.get(1)?,
        trigger_type: row.get(2)?,
        status: row.get(3)?,
        target_count: row.get::<_, i64>(4)? as usize,
        registered_ok: row.get::<_, i64>(5)? as usize,
        registered_failed: row.get::<_, i64>(6)? as usize,
        rt_ok: row.get::<_, i64>(7)? as usize,
        rt_failed: row.get::<_, i64>(8)? as usize,
        total_s2a_ok: row.get::<_, i64>(9)? as usize,
        total_s2a_failed: row.get::<_, i64>(10)? as usize,
        elapsed_secs: row.get(11)?,
        error: row.get(12)?,
        started_at: row.get(13)?,
        finished_at: row.get(14)?,
    })
}

fn map_invite_task_row(row: &rusqlite::Row) -> rusqlite::Result<InviteTaskRecord> {
    Ok(InviteTaskRecord {
        id: row.get(0)?,
        upload_id: row.get(1)?,
        owner_email: row.get(2)?,
        owner_account_id: row.get(3)?,
        s2a_team: row.get(4)?,
        invite_count: row.get::<_, i64>(5)? as usize,
        status: row.get(6)?,
        invited_ok: row.get::<_, i64>(7)? as usize,
        invited_failed: row.get::<_, i64>(8)? as usize,
        reg_ok: row.get::<_, i64>(9)? as usize,
        reg_failed: row.get::<_, i64>(10)? as usize,
        rt_ok: row.get::<_, i64>(11)? as usize,
        rt_failed: row.get::<_, i64>(12)? as usize,
        s2a_ok: row.get::<_, i64>(13)? as usize,
        s2a_failed: row.get::<_, i64>(14)? as usize,
        error: row.get(15)?,
        created_at: row.get(16)?,
        finished_at: row.get(17)?,
    })
}

fn map_owner_health_row(row: &rusqlite::Row) -> rusqlite::Result<OwnerHealthRecord> {
    Ok(OwnerHealthRecord {
        account_id: row.get(0)?,
        owner_status: row.get(1)?,
        members_json: row.get(2)?,
        checked_at: row.get(3)?,
        expires_at: row.get(4)?,
        cache_status: row.get(5)?,
        source: row.get(6)?,
        last_error: row.get(7)?,
    })
}

fn map_owner_registry_row(row: &rusqlite::Row) -> rusqlite::Result<OwnerRegistryRecord> {
    Ok(OwnerRegistryRecord {
        account_id: row.get(0)?,
        display_email: row.get(1)?,
        latest_access_token: row.get(2)?,
        token_expires_at: row.get(3)?,
        source_upload_id: row.get(4)?,
        state: row.get(5)?,
        disabled_reason: row.get(6)?,
        last_health_checked_at: row.get(7)?,
        last_health_status: row.get(8)?,
        member_count_cached: row.get::<_, i64>(9)? as usize,
        available_slots_cached: row.get::<_, i64>(10)? as usize,
        last_invite_task_id: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
        has_banned_member: row.get::<_, i64>(14)? != 0,
    })
}

fn map_team_manage_batch_job_row(
    row: &rusqlite::Row,
) -> rusqlite::Result<TeamManageBatchJobRecord> {
    Ok(TeamManageBatchJobRecord {
        job_id: row.get(0)?,
        job_type: row.get(1)?,
        scope: row.get(2)?,
        status: row.get(3)?,
        payload_json: row.get(4)?,
        total_count: row.get::<_, i64>(5)? as usize,
        success_count: row.get::<_, i64>(6)? as usize,
        failed_count: row.get::<_, i64>(7)? as usize,
        skipped_count: row.get::<_, i64>(8)? as usize,
        created_by: row.get(9)?,
        created_at: row.get(10)?,
        started_at: row.get(11)?,
        finished_at: row.get(12)?,
        error: row.get(13)?,
        fingerprint: row.get(14)?,
    })
}

fn map_team_manage_batch_item_row(
    row: &rusqlite::Row,
) -> rusqlite::Result<TeamManageBatchItemRecord> {
    Ok(TeamManageBatchItemRecord {
        id: row.get(0)?,
        job_id: row.get(1)?,
        account_id: row.get(2)?,
        item_type: row.get(3)?,
        status: row.get(4)?,
        child_task_id: row.get(5)?,
        result_json: row.get(6)?,
        error: row.get(7)?,
        started_at: row.get(8)?,
        finished_at: row.get(9)?,
    })
}

fn map_team_manage_owner_audit_row(
    row: &rusqlite::Row,
) -> rusqlite::Result<TeamManageOwnerAuditRecord> {
    Ok(TeamManageOwnerAuditRecord {
        id: row.get(0)?,
        account_id: row.get(1)?,
        action: row.get(2)?,
        from_state: row.get(3)?,
        to_state: row.get(4)?,
        reason: row.get(5)?,
        scope: row.get(6)?,
        batch_job_id: row.get(7)?,
        created_at: row.get(8)?,
    })
}

fn build_owner_registry_where(query: &OwnerRegistryQuery) -> (String, Vec<Box<dyn ToSql>>) {
    let mut clauses = Vec::new();
    let mut values: Vec<Box<dyn ToSql>> = Vec::new();
    if let Some(search) = query
        .search
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        clauses.push("(r.display_email LIKE ? OR r.account_id LIKE ?)");
        let like = format!("%{search}%");
        values.push(Box::new(like.clone()));
        values.push(Box::new(like));
    }
    if let Some(state) = query
        .state
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        clauses.push("r.state = ?");
        values.push(Box::new(state.to_string()));
    } else {
        clauses.push("r.state != 'archived'");
    }
    if matches!(query.has_slots, Some(true)) {
        clauses.push("r.available_slots_cached > 0 AND r.state != 'seat_limited'");
    }
    if matches!(query.has_slots, Some(false)) {
        clauses.push("r.available_slots_cached = 0");
    }
    if matches!(query.has_banned_member, Some(true)) {
        clauses.push(
            "EXISTS (
                SELECT 1 FROM owner_health h
                WHERE h.account_id = r.account_id
                  AND h.members_json LIKE '%\"status\":\"banned\"%'
             )",
        );
    }
    if matches!(query.has_banned_member, Some(false)) {
        clauses.push(
            "NOT EXISTS (
                SELECT 1 FROM owner_health h
                WHERE h.account_id = r.account_id
                  AND h.members_json LIKE '%\"status\":\"banned\"%'
             )",
        );
    }
    let where_sql = if clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", clauses.join(" AND "))
    };
    (where_sql, values)
}

fn owner_registry_order_clause(query: &OwnerRegistryQuery) -> &'static str {
    let desc = !matches!(query.sort_order.as_deref(), Some("asc" | "ASC"));
    match query.sort_by.as_deref() {
        Some("display_email") if desc => "r.display_email DESC, r.updated_at DESC",
        Some("display_email") => "r.display_email ASC, r.updated_at DESC",
        Some("member_count") if desc => "r.member_count_cached DESC, r.updated_at DESC",
        Some("member_count") => "r.member_count_cached ASC, r.updated_at DESC",
        Some("available_slots") if desc => "r.available_slots_cached DESC, r.updated_at DESC",
        Some("available_slots") => "r.available_slots_cached ASC, r.updated_at DESC",
        Some("last_health_checked_at") if desc => {
            "r.last_health_checked_at DESC, r.updated_at DESC"
        }
        Some("last_health_checked_at") => "r.last_health_checked_at ASC, r.updated_at DESC",
        Some("state") if desc => "r.state DESC, r.updated_at DESC",
        Some("state") => "r.state ASC, r.updated_at DESC",
        _ => "r.updated_at DESC",
    }
}
