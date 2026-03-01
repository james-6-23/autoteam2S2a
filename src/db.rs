use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use serde::Serialize;

pub struct RunHistoryDb {
    conn: Mutex<Connection>,
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
        let conn = Connection::open(path)
            .with_context(|| format!("打开数据库失败: {}", path.display()))?;

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
            );",
        )?;

        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

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

    pub fn insert_distributions(&self, run_id: &str, entries: &[NewDistribution]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "INSERT INTO run_distributions (run_id, team_name, percent) VALUES (?1, ?2, ?3)",
        )?;
        for entry in entries {
            stmt.execute(params![run_id, entry.team_name, entry.percent as i64])?;
        }
        Ok(())
    }

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

    pub fn fail_run(&self, run_id: &str, error: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE runs SET status = 'failed', error = ?2, finished_at = ?3 WHERE id = ?1",
            params![run_id, error, chrono::Local::now().to_rfc3339()],
        )?;
        Ok(())
    }

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
            params![
                run_id,
                team,
                assigned as i64,
                ok as i64,
                failed as i64,
            ],
        )?;
        Ok(())
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
