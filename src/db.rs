use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, mpsc};
use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::{Connection, params};
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
                ON run_distributions(run_id, team_name);",
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
    ) -> Result<(Vec<RunRecord>, usize)> {
        let conn = self.conn.lock().unwrap();

        let (count_sql, list_sql) = if schedule.is_some() {
            (
                "SELECT COUNT(*) FROM runs WHERE schedule_name = ?1",
                "SELECT id, schedule_name, trigger_type, status, target_count,
                        registered_ok, registered_failed, rt_ok, rt_failed,
                        total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                        started_at, finished_at
                 FROM runs WHERE schedule_name = ?1
                 ORDER BY started_at DESC LIMIT ?2 OFFSET ?3",
            )
        } else {
            (
                "SELECT COUNT(*) FROM runs",
                "SELECT id, schedule_name, trigger_type, status, target_count,
                        registered_ok, registered_failed, rt_ok, rt_failed,
                        total_s2a_ok, total_s2a_failed, elapsed_secs, error,
                        started_at, finished_at
                 FROM runs ORDER BY started_at DESC LIMIT ?1 OFFSET ?2",
            )
        };

        let total: usize = if let Some(sched) = schedule {
            conn.query_row(count_sql, params![sched], |row| row.get::<_, i64>(0))? as usize
        } else {
            conn.query_row(count_sql, [], |row| row.get::<_, i64>(0))? as usize
        };

        let offset = (page.saturating_sub(1)) * per_page;

        let mut stmt = conn.prepare(list_sql)?;
        let rows = if let Some(sched) = schedule {
            stmt.query_map(params![sched, per_page as i64, offset as i64], map_run_row)?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            stmt.query_map(params![per_page as i64, offset as i64], map_run_row)?
                .collect::<Result<Vec<_>, _>>()?
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
