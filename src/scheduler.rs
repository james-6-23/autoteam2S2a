use std::cmp::Ordering as CmpOrdering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use chrono::NaiveTime;
use serde::Serialize;
use tokio::sync::Mutex;

use crate::config::ScheduleConfig;
use crate::db::RunHistoryDb;
use crate::log_broadcast::broadcast_log;
use crate::server::AppState;

const SCHEDULER_TICK_SECS: u64 = 5;
const RUNNER_RETRY_SECS: u64 = 30;

// ─── Scheduler state ─────────────────────────────────────────────────────────

struct ActiveEntry {
    cancel_flag: Arc<AtomicBool>,
    batch_num: Arc<AtomicU64>,
    /// 当前执行中为 0，进入冷却后写入下一批次开始时间。
    next_batch_ts: Arc<AtomicU64>,
}

struct CooldownEntry {
    batch_num: u64,
    next_batch_ts: u64,
}

struct PendingEntry {
    pending_since_ts: u64,
    blocked_by: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct ScheduleRunInfo {
    pub batch_num: u64,
    pub next_batch_at: Option<String>,
}

#[derive(Serialize, Clone)]
pub struct SchedulePendingInfo {
    pub pending_since: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blocked_by: Option<String>,
}

#[derive(Clone, Default)]
pub struct SchedulerSnapshot {
    pub active: HashMap<String, ScheduleRunInfo>,
    pub cooldown: HashMap<String, ScheduleRunInfo>,
    pub pending: HashMap<String, SchedulePendingInfo>,
    pub current_running: Option<String>,
}

#[derive(Default)]
struct SchedulerInner {
    active: HashMap<String, ActiveEntry>,
    cooldown: HashMap<String, CooldownEntry>,
    pending: HashMap<String, PendingEntry>,
    current_running: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ScheduleStartError {
    AlreadyRunning,
    Busy { running_name: String },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScheduleStopOutcome {
    RunningCancelled,
    PendingCancelled,
    CooldownCancelled,
    NotFound,
}

/// 跟踪当前执行中的计划、等待中的计划和冷却中的计划。
pub struct SchedulerState {
    inner: Mutex<SchedulerInner>,
}

impl SchedulerState {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SchedulerInner::default()),
        }
    }

    /// 尝试启动一个计划。只允许一个计划占用当前执行槽位。
    pub async fn start(
        &self,
        name: &str,
    ) -> Result<(Arc<AtomicBool>, Arc<AtomicU64>, Arc<AtomicU64>), ScheduleStartError> {
        let mut inner = self.inner.lock().await;
        if inner.active.contains_key(name) || inner.cooldown.contains_key(name) {
            return Err(ScheduleStartError::AlreadyRunning);
        }
        if let Some(running_name) = inner.current_running.clone() {
            return Err(ScheduleStartError::Busy { running_name });
        }

        let flag = Arc::new(AtomicBool::new(false));
        let batch_num = Arc::new(AtomicU64::new(0));
        let next_batch_ts = Arc::new(AtomicU64::new(0));
        inner.active.insert(
            name.to_string(),
            ActiveEntry {
                cancel_flag: flag.clone(),
                batch_num: batch_num.clone(),
                next_batch_ts: next_batch_ts.clone(),
            },
        );
        inner.pending.remove(name);
        inner.cooldown.remove(name);
        inner.current_running = Some(name.to_string());
        Ok((flag, batch_num, next_batch_ts))
    }

    /// 标记一个计划处于等待中。等待开始时间只在首次进入时记录。
    pub async fn mark_pending(&self, name: &str, blocked_by: Option<String>) -> bool {
        let mut inner = self.inner.lock().await;
        if inner.active.contains_key(name) || inner.cooldown.contains_key(name) {
            inner.pending.remove(name);
            return false;
        }

        let changed = inner
            .pending
            .get(name)
            .map(|entry| entry.blocked_by != blocked_by)
            .unwrap_or(true);
        if !changed {
            return false;
        }

        let pending_since_ts = inner
            .pending
            .get(name)
            .map(|entry| entry.pending_since_ts)
            .unwrap_or_else(current_unix_ts);
        inner.pending.insert(
            name.to_string(),
            PendingEntry {
                pending_since_ts,
                blocked_by,
            },
        );
        true
    }

    pub async fn clear_pending(&self, name: &str) -> bool {
        self.inner.lock().await.pending.remove(name).is_some()
    }

    pub async fn clear_cooldown(&self, name: &str) -> bool {
        self.inner.lock().await.cooldown.remove(name).is_some()
    }

    pub async fn clear_due_cooldowns(&self, now_ts: u64) {
        let mut inner = self.inner.lock().await;
        inner
            .cooldown
            .retain(|_, entry| entry.next_batch_ts > now_ts && entry.next_batch_ts > 0);
    }

    /// 将当前计划转为准备中/冷却中，并释放执行槽位。
    pub async fn enter_cooldown(&self, name: &str, batch_num: u64, next_batch_ts: u64) {
        let mut inner = self.inner.lock().await;
        inner.active.remove(name);
        inner.pending.remove(name);
        if inner.current_running.as_deref() == Some(name) {
            inner.current_running = None;
        }
        inner.cooldown.insert(
            name.to_string(),
            CooldownEntry {
                batch_num,
                next_batch_ts,
            },
        );
    }

    /// 停止一个正在运行、等待中或准备中的计划。
    pub async fn stop(&self, name: &str) -> ScheduleStopOutcome {
        let mut inner = self.inner.lock().await;
        if let Some(entry) = inner.active.get(name) {
            entry.cancel_flag.store(true, Ordering::SeqCst);
            return ScheduleStopOutcome::RunningCancelled;
        }
        if inner.pending.remove(name).is_some() {
            return ScheduleStopOutcome::PendingCancelled;
        }
        if inner.cooldown.remove(name).is_some() {
            return ScheduleStopOutcome::CooldownCancelled;
        }
        ScheduleStopOutcome::NotFound
    }

    /// 从状态中移除（执行结束时调用）。
    pub async fn remove(&self, name: &str) {
        let mut inner = self.inner.lock().await;
        inner.active.remove(name);
        inner.pending.remove(name);
        inner.cooldown.remove(name);
        if inner.current_running.as_deref() == Some(name) {
            inner.current_running = None;
        }
    }

    pub async fn snapshot(&self) -> SchedulerSnapshot {
        let inner = self.inner.lock().await;
        SchedulerSnapshot {
            active: inner
                .active
                .iter()
                .map(|(name, entry)| (name.clone(), Self::build_active_run_info(entry)))
                .collect(),
            cooldown: inner
                .cooldown
                .iter()
                .map(|(name, entry)| (name.clone(), Self::build_cooldown_run_info(entry)))
                .collect(),
            pending: inner
                .pending
                .iter()
                .map(|(name, entry)| (name.clone(), Self::build_pending_info(entry)))
                .collect(),
            current_running: inner.current_running.clone(),
        }
    }

    fn build_active_run_info(entry: &ActiveEntry) -> ScheduleRunInfo {
        let ts = entry.next_batch_ts.load(Ordering::Relaxed);
        ScheduleRunInfo {
            batch_num: entry.batch_num.load(Ordering::Relaxed),
            next_batch_at: format_beijing_time(ts),
        }
    }

    fn build_cooldown_run_info(entry: &CooldownEntry) -> ScheduleRunInfo {
        ScheduleRunInfo {
            batch_num: entry.batch_num,
            next_batch_at: format_beijing_time(entry.next_batch_ts),
        }
    }

    fn build_pending_info(entry: &PendingEntry) -> SchedulePendingInfo {
        SchedulePendingInfo {
            pending_since: format_beijing_time(entry.pending_since_ts)
                .unwrap_or_else(|| "--:--:--".to_string()),
            blocked_by: entry.blocked_by.clone(),
        }
    }
}

fn current_unix_ts() -> u64 {
    chrono::Utc::now().timestamp().max(0) as u64
}

fn format_beijing_time(ts: u64) -> Option<String> {
    if ts == 0 {
        return None;
    }
    chrono::DateTime::from_timestamp(ts as i64, 0).map(|dt| {
        let tz = chrono::FixedOffset::east_opt(8 * 3600).unwrap();
        dt.with_timezone(&tz).format("%H:%M:%S").to_string()
    })
}

// ─── Time window check ──────────────────────────────────────────────────────

/// 判断当前北京时间是否在 [start, end) 窗口内。支持跨日窗口（如 22:00-06:00）。
pub fn is_in_window(start_time: &str, end_time: &str) -> bool {
    let start = match NaiveTime::parse_from_str(start_time, "%H:%M") {
        Ok(t) => t,
        Err(_) => return false,
    };
    let end = match NaiveTime::parse_from_str(end_time, "%H:%M") {
        Ok(t) => t,
        Err(_) => return false,
    };
    let now = crate::util::beijing_now().time();

    if start <= end {
        now >= start && now < end
    } else {
        now >= start || now < end
    }
}

/// 校验时间格式 HH:MM
pub fn validate_time(time_str: &str) -> Result<(), String> {
    NaiveTime::parse_from_str(time_str, "%H:%M")
        .map(|_| ())
        .map_err(|_| format!("无效的时间格式 (需要 HH:MM): {time_str}"))
}

pub async fn load_schedule_config(state: &AppState, name: &str) -> Option<ScheduleConfig> {
    let cfg = state.config.read().await;
    cfg.schedule.iter().find(|s| s.name == name).cloned()
}

// ─── Scheduler loop ─────────────────────────────────────────────────────────

pub fn start_scheduler(state: AppState, db: Arc<RunHistoryDb>) {
    tokio::spawn(async move {
        scheduler_loop(state, db).await;
    });
}

fn compare_schedule_priority(
    left: &(usize, &ScheduleConfig),
    right: &(usize, &ScheduleConfig),
) -> CmpOrdering {
    left.1
        .priority
        .cmp(&right.1.priority)
        .then_with(|| right.0.cmp(&left.0))
}

fn pick_startable_schedule<'a>(
    schedules: &'a [ScheduleConfig],
    snapshot: &SchedulerSnapshot,
) -> Option<(usize, &'a ScheduleConfig)> {
    schedules
        .iter()
        .enumerate()
        .filter(|(_, schedule)| {
            schedule.enabled
                && is_in_window(&schedule.start_time, &schedule.end_time)
                && !snapshot.active.contains_key(&schedule.name)
                && !snapshot.cooldown.contains_key(&schedule.name)
        })
        .max_by(compare_schedule_priority)
}

fn spawn_scheduled_batch(
    state: &AppState,
    db: &Arc<RunHistoryDb>,
    schedule_name: String,
    cancel_flag: Arc<AtomicBool>,
    batch_num: Arc<AtomicU64>,
    next_ts: Arc<AtomicU64>,
) {
    let state_clone = state.clone();
    let db_clone = db.clone();
    tokio::spawn(async move {
        run_batch_once(
            state_clone,
            db_clone,
            schedule_name,
            cancel_flag,
            batch_num,
            next_ts,
        )
        .await;
    });
}

async fn scheduler_loop(state: AppState, db: Arc<RunHistoryDb>) {
    broadcast_log(&format!(
        "[调度器] 后台调度器已启动，每 {} 秒检查时间窗口",
        SCHEDULER_TICK_SECS
    ));

    loop {
        let schedules = {
            let cfg = state.config.read().await;
            cfg.schedule.clone()
        };

        state
            .scheduler_state
            .clear_due_cooldowns(current_unix_ts())
            .await;

        let snapshot = state.scheduler_state.snapshot().await;
        if let Some(running_name) = snapshot.current_running.clone() {
            match schedules
                .iter()
                .find(|schedule| schedule.name == running_name)
            {
                Some(schedule_cfg)
                    if schedule_cfg.enabled
                        && is_in_window(&schedule_cfg.start_time, &schedule_cfg.end_time) => {}
                Some(schedule_cfg) => {
                    broadcast_log(&format!(
                        "[调度器] 时间窗口已结束，停止计划: {} ({}-{})",
                        schedule_cfg.name, schedule_cfg.start_time, schedule_cfg.end_time
                    ));
                    let _ = state.scheduler_state.stop(&running_name).await;
                }
                None => {
                    broadcast_log(&format!("[调度器] 计划已删除，停止计划: {}", running_name));
                    let _ = state.scheduler_state.stop(&running_name).await;
                }
            }
        }

        let snapshot = state.scheduler_state.snapshot().await;
        for schedule_cfg in &schedules {
            let in_window = is_in_window(&schedule_cfg.start_time, &schedule_cfg.end_time);
            if !schedule_cfg.enabled || !in_window {
                state
                    .scheduler_state
                    .clear_pending(&schedule_cfg.name)
                    .await;
                state
                    .scheduler_state
                    .clear_cooldown(&schedule_cfg.name)
                    .await;
            }

            if snapshot.current_running.as_deref() == Some(schedule_cfg.name.as_str())
                && (!schedule_cfg.enabled || !in_window)
            {
                let _ = state.scheduler_state.stop(&schedule_cfg.name).await;
            }
        }

        let snapshot = state.scheduler_state.snapshot().await;
        if let Some(running_name) = snapshot.current_running.clone() {
            for schedule_cfg in &schedules {
                if !schedule_cfg.enabled
                    || !is_in_window(&schedule_cfg.start_time, &schedule_cfg.end_time)
                    || schedule_cfg.name == running_name
                {
                    continue;
                }
                state
                    .scheduler_state
                    .mark_pending(&schedule_cfg.name, Some(running_name.clone()))
                    .await;
            }
        } else if let Some((selected_index, selected_schedule)) =
            pick_startable_schedule(&schedules, &snapshot)
        {
            let blocker_name = match state.scheduler_state.start(&selected_schedule.name).await {
                Ok((cancel_flag, batch_num, next_ts)) => {
                    broadcast_log(&format!(
                        "[调度器] 进入时间窗口，启动计划: {} ({}-{}, 优先级 {})",
                        selected_schedule.name,
                        selected_schedule.start_time,
                        selected_schedule.end_time,
                        selected_schedule.priority
                    ));
                    spawn_scheduled_batch(
                        &state,
                        &db,
                        selected_schedule.name.clone(),
                        cancel_flag,
                        batch_num,
                        next_ts,
                    );
                    selected_schedule.name.clone()
                }
                Err(ScheduleStartError::Busy { running_name }) => running_name,
                Err(ScheduleStartError::AlreadyRunning) => selected_schedule.name.clone(),
            };

            for (index, schedule_cfg) in schedules.iter().enumerate() {
                if index == selected_index
                    || !schedule_cfg.enabled
                    || !is_in_window(&schedule_cfg.start_time, &schedule_cfg.end_time)
                    || snapshot.cooldown.contains_key(&schedule_cfg.name)
                {
                    continue;
                }
                state
                    .scheduler_state
                    .mark_pending(&schedule_cfg.name, Some(blocker_name.clone()))
                    .await;
            }
        } else {
            for schedule_cfg in &schedules {
                state
                    .scheduler_state
                    .clear_pending(&schedule_cfg.name)
                    .await;
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_secs(SCHEDULER_TICK_SECS)).await;
    }
}

// ─── Batch execution ─────────────────────────────────────────────────────────

fn compute_remaining_cooldown_secs(
    last_finished_at: Option<String>,
    interval_mins: u64,
) -> Option<u64> {
    let last_ts = last_finished_at?;
    let last_dt = chrono::DateTime::parse_from_rfc3339(&last_ts).ok()?;
    let now = chrono::Utc::now();
    let since = now.signed_duration_since(last_dt);
    let interval = chrono::Duration::seconds(interval_mins.saturating_mul(60) as i64);
    let remaining = interval - since;
    (remaining > chrono::Duration::zero()).then(|| remaining.num_seconds() as u64)
}

async fn run_batch_once(
    state: AppState,
    db: Arc<RunHistoryDb>,
    schedule_name: String,
    cancel_flag: Arc<AtomicBool>,
    batch_counter: Arc<AtomicU64>,
    next_batch_ts: Arc<AtomicU64>,
) {
    let Some(initial_schedule) = load_schedule_config(&state, &schedule_name).await else {
        broadcast_log(&format!(
            "[调度器] {} 批次未启动：计划不存在",
            schedule_name
        ));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    };

    let last_finished = tokio::task::spawn_blocking({
        let db = db.clone();
        let schedule_name = schedule_name.clone();
        move || db.last_finished_at(&schedule_name)
    })
    .await
    .ok()
    .and_then(Result::ok)
    .flatten();

    if let Some(remaining_secs) =
        compute_remaining_cooldown_secs(last_finished, initial_schedule.batch_interval_mins)
    {
        let next_ts = current_unix_ts().saturating_add(remaining_secs);
        next_batch_ts.store(next_ts, Ordering::Relaxed);
        state
            .scheduler_state
            .enter_cooldown(
                &schedule_name,
                batch_counter.load(Ordering::Relaxed),
                next_ts,
            )
            .await;
        broadcast_log(&format!(
            "[调度器] {} 准备中，还需等待 {} 秒后再执行下一批",
            schedule_name, remaining_secs
        ));
        return;
    }

    if cancel_flag.load(Ordering::Relaxed) {
        broadcast_log(&format!("[调度器] {} 已停止（执行前取消）", schedule_name));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    }

    let cfg = state.config.read().await.clone();
    let Some(schedule) = cfg
        .schedule
        .iter()
        .find(|s| s.name == schedule_name)
        .cloned()
    else {
        broadcast_log(&format!("[调度器] {} 已停止（计划已删除）", schedule_name));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    };

    if !schedule.enabled {
        broadcast_log(&format!("[调度器] {} 已停止（计划已禁用）", schedule_name));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    }
    if !is_in_window(&schedule.start_time, &schedule.end_time) {
        broadcast_log(&format!("[调度器] {} 时间窗口结束", schedule_name));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    }

    let batch_num = batch_counter.fetch_add(1, Ordering::Relaxed) + 1;
    next_batch_ts.store(0, Ordering::Relaxed);
    broadcast_log(&format!(
        "[调度器] {} 开始第 {} 批次（每批 RT 成功目标 {} 个）",
        schedule.name, batch_num, schedule.target_count
    ));

    let runner = match crate::server::build_workflow_runner(
        &cfg,
        state.proxy_file.as_deref(),
        schedule.use_chatgpt_mail,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => {
            let next_ts = current_unix_ts().saturating_add(RUNNER_RETRY_SECS);
            next_batch_ts.store(next_ts, Ordering::Relaxed);
            state
                .scheduler_state
                .enter_cooldown(&schedule_name, batch_num, next_ts)
                .await;
            broadcast_log(&format!(
                "[调度器] 构建 runner 失败 ({}): {e}，{} 秒后重试",
                schedule_name, RUNNER_RETRY_SECS
            ));
            return;
        }
    };

    let result = crate::distribution::run_distribution(
        &runner,
        &cfg,
        &schedule,
        &db,
        "scheduled",
        cancel_flag.clone(),
    )
    .await;

    match result {
        Ok(report) => {
            broadcast_log(&format!(
                "[调度器] {} 第 {} 批完成 | 注册: {} | RT: {} | S2A: {} | 耗时: {:.1}s",
                schedule.name,
                batch_num,
                report.registered_ok,
                report.rt_ok,
                report.total_s2a_ok,
                report.elapsed_secs
            ));
        }
        Err(e) => {
            broadcast_log(&format!(
                "[调度器] {} 第 {} 批失败: {e:#}",
                schedule_name, batch_num
            ));
            if cancel_flag.load(Ordering::Relaxed) {
                broadcast_log(&format!("[调度器] {} 已停止（运行中取消）", schedule_name));
                state.scheduler_state.remove(&schedule_name).await;
                return;
            }
        }
    }

    let Some(latest_schedule) = load_schedule_config(&state, &schedule_name).await else {
        broadcast_log(&format!("[调度器] {} 已停止（计划已删除）", schedule_name));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    };
    if !latest_schedule.enabled {
        broadcast_log(&format!("[调度器] {} 已停止（计划已禁用）", schedule_name));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    }
    if !is_in_window(&latest_schedule.start_time, &latest_schedule.end_time) {
        broadcast_log(&format!("[调度器] {} 时间窗口结束", schedule_name));
        state.scheduler_state.remove(&schedule_name).await;
        return;
    }

    let wait_secs = latest_schedule.batch_interval_mins.saturating_mul(60);
    let next_ts = current_unix_ts().saturating_add(wait_secs);
    next_batch_ts.store(next_ts, Ordering::Relaxed);
    state
        .scheduler_state
        .enter_cooldown(&schedule_name, batch_num, next_ts)
        .await;
    broadcast_log(&format!(
        "[调度器] {} 准备中，{} 分钟后执行下一批",
        schedule_name, latest_schedule.batch_interval_mins
    ));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DistributionEntry;

    fn make_schedule(name: &str, priority: u32) -> ScheduleConfig {
        ScheduleConfig {
            name: name.to_string(),
            start_time: "00:00".to_string(),
            end_time: "23:59".to_string(),
            target_count: 10,
            batch_interval_mins: 30,
            enabled: true,
            priority,
            register_workers: None,
            rt_workers: None,
            rt_retries: None,
            push_s2a: true,
            use_chatgpt_mail: false,
            free_mode: false,
            register_log_mode: None,
            register_perf_mode: None,
            distribution: Vec::<DistributionEntry>::new(),
        }
    }

    #[test]
    fn pick_startable_schedule_prefers_higher_priority() {
        let schedules = vec![make_schedule("low", 10), make_schedule("high", 200)];
        let selected = pick_startable_schedule(&schedules, &SchedulerSnapshot::default())
            .unwrap()
            .1;
        assert_eq!(selected.name, "high");
    }

    #[test]
    fn pick_startable_schedule_keeps_config_order_on_tie() {
        let schedules = vec![make_schedule("first", 100), make_schedule("second", 100)];
        let selected = pick_startable_schedule(&schedules, &SchedulerSnapshot::default())
            .unwrap()
            .1;
        assert_eq!(selected.name, "first");
    }

    #[tokio::test]
    async fn scheduler_state_releases_slot_when_entering_cooldown() {
        let state = SchedulerState::new();

        let (_, batch_num, _) = state.start("alpha").await.expect("alpha should start");
        batch_num.store(1, Ordering::Relaxed);
        state
            .enter_cooldown("alpha", 1, current_unix_ts() + 60)
            .await;

        let snapshot = state.snapshot().await;
        assert!(snapshot.active.is_empty());
        assert!(snapshot.cooldown.contains_key("alpha"));
        assert!(snapshot.current_running.is_none());

        assert!(state.start("beta").await.is_ok());
    }

    #[tokio::test]
    async fn stop_can_cancel_pending_and_cooldown() {
        let state = SchedulerState::new();

        state.mark_pending("beta", Some("alpha".to_string())).await;
        assert_eq!(
            state.stop("beta").await,
            ScheduleStopOutcome::PendingCancelled
        );

        let (_, batch_num, _) = state.start("alpha").await.expect("alpha should start");
        batch_num.store(1, Ordering::Relaxed);
        state
            .enter_cooldown("alpha", 1, current_unix_ts() + 60)
            .await;
        assert_eq!(
            state.stop("alpha").await,
            ScheduleStopOutcome::CooldownCancelled
        );
    }

    #[test]
    fn compute_remaining_cooldown_secs_respects_interval() {
        let finished_at = chrono::Utc::now().to_rfc3339();
        let remaining =
            compute_remaining_cooldown_secs(Some(finished_at), 1).expect("cooldown should exist");
        assert!(remaining <= 60 && remaining > 0);
    }
}
