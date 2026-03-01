use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use anyhow::Result;

use crate::config::{AppConfig, DistributionEntry, ScheduleConfig};
use crate::db::{NewDistribution, NewRun, RunCompletion, RunHistoryDb};
use crate::models::{AccountWithRt, DistributionReport, TeamDistResult};
use crate::storage::save_json_records;
use crate::workflow::{WorkflowOptions, WorkflowRunner};

/// 按百分比分割账号列表。前 N-1 个号池取 floor(total * percent / 100)，最后一个取剩余。
fn split_by_percentage(
    accounts: &[AccountWithRt],
    distribution: &[(String, u8)],
) -> Vec<(String, Vec<AccountWithRt>)> {
    if distribution.is_empty() || accounts.is_empty() {
        return Vec::new();
    }

    let total = accounts.len();
    let mut result = Vec::with_capacity(distribution.len());
    let mut offset = 0usize;

    for (i, (team, percent)) in distribution.iter().enumerate() {
        let count = if i + 1 < distribution.len() {
            // 前 N-1 个用 floor
            (total * (*percent as usize)) / 100
        } else {
            // 最后一个取剩余
            total - offset
        };
        let end = (offset + count).min(total);
        result.push((team.clone(), accounts[offset..end].to_vec()));
        offset = end;
    }

    result
}

/// 执行多 S2A 分发流程
pub async fn run_distribution(
    runner: &WorkflowRunner,
    cfg: &AppConfig,
    schedule: &ScheduleConfig,
    db: &Arc<RunHistoryDb>,
    trigger_type: &str,
    cancel_flag: Arc<AtomicBool>,
) -> Result<DistributionReport> {
    let workflow_started = Instant::now();
    let run_id = uuid::Uuid::new_v4().to_string()[..8].to_string();

    // 1. 在 SQLite 创建 run 记录
    db.insert_run(&NewRun {
        id: run_id.clone(),
        schedule_name: Some(schedule.name.clone()),
        trigger_type: trigger_type.to_string(),
        target_count: schedule.target_count,
        started_at: chrono::Local::now().to_rfc3339(),
    })?;

    // 插入 distribution plan
    let dist_entries: Vec<NewDistribution> = schedule
        .distribution
        .iter()
        .map(|d| NewDistribution {
            team_name: d.team.clone(),
            percent: d.percent,
        })
        .collect();
    db.insert_distributions(&run_id, &dist_entries)?;

    let options = WorkflowOptions {
        target_count: schedule.target_count,
        register_workers: schedule
            .register_workers
            .unwrap_or(cfg.defaults.register_workers.unwrap_or(15))
            .max(1),
        rt_workers: schedule
            .rt_workers
            .unwrap_or(cfg.defaults.rt_workers.unwrap_or(10))
            .max(1),
        rt_retry_max: schedule
            .rt_retries
            .unwrap_or(cfg.defaults.rt_retries.unwrap_or(4))
            .max(1),
        push_s2a: schedule.push_s2a,
        use_chatgpt_mail: schedule.use_chatgpt_mail,
    };

    // 2. 注册 + RT
    println!(
        "[分发] 开始注册 + RT，目标: {} 个账号",
        schedule.target_count
    );
    let reg_result = match runner
        .run_register_and_rt(cfg, &options, cancel_flag.clone())
        .await
    {
        Ok(r) => r,
        Err(e) => {
            let _ = db.fail_run(&run_id, &format!("{e:#}"));
            return Err(e);
        }
    };

    if cancel_flag.load(Ordering::Relaxed) {
        let _ = db.fail_run(&run_id, "用户取消");
        anyhow::bail!("运行已取消");
    }

    let rt_ok = reg_result.rt_success.len();
    let rt_failed = reg_result.rt_failed.len();
    let mut output_files = reg_result.output_files;

    // 3. 过滤 free 账号
    let (s2a_eligible, free_accounts): (Vec<AccountWithRt>, Vec<AccountWithRt>) = reg_result
        .rt_success
        .into_iter()
        .partition(|acc| !acc.plan_type.eq_ignore_ascii_case("free"));

    if !free_accounts.is_empty() {
        println!(
            "[分发] 跳过 {} 个 free 账号（plan_type=free 不入库）",
            free_accounts.len()
        );
        if let Some(path) = save_json_records("accounts-free-skipped", &free_accounts)? {
            output_files.push(path.display().to_string());
        }
    }

    // 4. 按百分比分割
    let dist_pairs: Vec<(String, u8)> = schedule
        .distribution
        .iter()
        .map(|d| (d.team.clone(), d.percent))
        .collect();
    let splits = split_by_percentage(&s2a_eligible, &dist_pairs);

    // 5. 对每个 S2A 号池顺序推送
    let teams = cfg.effective_s2a_configs();
    let mut total_s2a_ok = 0usize;
    let mut total_s2a_failed = 0usize;
    let mut team_results: Vec<TeamDistResult> = Vec::new();

    for (team_name, accounts) in &splits {
        if cancel_flag.load(Ordering::Relaxed) {
            break;
        }

        let percent = schedule
            .distribution
            .iter()
            .find(|d| d.team == *team_name)
            .map(|d| d.percent)
            .unwrap_or(0);

        let team_cfg = match teams.iter().find(|t| t.name == *team_name) {
            Some(t) => t,
            None => {
                println!("[分发] 未找到号池配置: {team_name}，跳过");
                team_results.push(TeamDistResult {
                    team_name: team_name.clone(),
                    percent,
                    assigned_count: accounts.len(),
                    s2a_ok: 0,
                    s2a_failed: accounts.len(),
                });
                total_s2a_failed += accounts.len();
                continue;
            }
        };

        println!(
            "[分发] 推送 {} 个账号到 {} ({}%)",
            accounts.len(),
            team_name,
            percent
        );

        let (ok, failed) = if options.push_s2a && !accounts.is_empty() {
            runner.push_to_s2a(team_cfg, accounts.clone()).await
        } else {
            (0, 0)
        };

        total_s2a_ok += ok;
        total_s2a_failed += failed;

        // 6. 更新 SQLite 中该号池的统计
        let _ = db.update_distribution(&run_id, team_name, accounts.len(), ok, failed);

        team_results.push(TeamDistResult {
            team_name: team_name.clone(),
            percent,
            assigned_count: accounts.len(),
            s2a_ok: ok,
            s2a_failed: failed,
        });
    }

    let elapsed = workflow_started.elapsed().as_secs_f32();

    // 7. 更新 run 总状态
    let _ = db.complete_run(
        &run_id,
        &RunCompletion {
            registered_ok: reg_result.total_registered,
            registered_failed: reg_result.total_reg_failed,
            rt_ok,
            rt_failed,
            total_s2a_ok,
            total_s2a_failed,
            elapsed_secs: elapsed as f64,
            finished_at: chrono::Local::now().to_rfc3339(),
        },
    );

    // D1 清理
    if cfg.d1_cleanup.enabled.unwrap_or(false) {
        println!("\n[分发] D1 邮件清理");
        if let Err(e) = crate::d1_cleanup::run_cleanup(&cfg.d1_cleanup).await {
            println!("D1 清理失败: {e}");
        }
    }

    Ok(DistributionReport {
        run_id,
        registered_ok: reg_result.total_registered,
        registered_failed: reg_result.total_reg_failed,
        rt_ok,
        rt_failed,
        total_s2a_ok,
        total_s2a_failed,
        team_results,
        output_files,
        elapsed_secs: elapsed,
        target_count: schedule.target_count,
    })
}

/// 校验分发配置的百分比之和是否为 100，且引用的号池都存在
pub fn validate_distribution(
    distribution: &[DistributionEntry],
    available_teams: &[crate::config::S2aConfig],
) -> Result<(), String> {
    if distribution.is_empty() {
        return Err("分发配置不能为空".to_string());
    }

    let total: u16 = distribution.iter().map(|d| d.percent as u16).sum();
    if total != 100 {
        return Err(format!("分发百分比总和必须为 100，当前为 {total}"));
    }

    for entry in distribution {
        if !available_teams.iter().any(|t| t.name == entry.team) {
            return Err(format!("号池不存在: {}", entry.team));
        }
        if entry.percent == 0 {
            return Err(format!("号池 {} 的百分比不能为 0", entry.team));
        }
    }

    Ok(())
}
