mod config;
mod d1_cleanup;
#[path = "email-service.rs"]
mod email_service;
mod fingerprint;
mod iban;
mod models;
mod proxy_pool;
mod services;
mod storage;
mod stripe;
mod util;
mod workflow;

use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use config::{AppConfig, S2aConfig};
use proxy_pool::{ProxyPool, health_check, resolve_proxies};
use services::{
    DryRunCodexService, DryRunRegisterService, DryRunS2aService, LiveCodexService,
    LiveRegisterService, S2aHttpService,
};
use workflow::{WorkflowOptions, WorkflowRunner};

#[derive(Debug, Parser)]
#[command(author, version, about = "autoteam2s2a v1.0.0 | Team 注册 + 支付 + RT")]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// 分析配置与单团队执行参数
    Analyze {
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long)]
        team: Option<String>,
        /// 从文件加载代理列表（每行一个代理地址）
        #[arg(long)]
        proxy_file: Option<PathBuf>,
    },
    /// 执行一团队 free 注册 -> RT -> S2A 入库（默认 live）
    Run {
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        /// 从文件加载代理列表（每行一个代理地址）
        #[arg(long)]
        proxy_file: Option<PathBuf>,
        #[arg(long)]
        team: Option<String>,
        #[arg(long)]
        target: Option<usize>,
        #[arg(long)]
        register_workers: Option<usize>,
        #[arg(long)]
        rt_workers: Option<usize>,
        #[arg(long)]
        rt_retries: Option<usize>,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        live: bool,
        #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
        push_s2a: bool,
        /// 使用 chatgpt.org.uk 邮箱系统 (自动生成邮箱)
        #[arg(long, default_value_t = false)]
        chatgpt_mail: bool,
    },
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    println!("autoteam2s2a v{}", env!("CARGO_PKG_VERSION"));
    let cli = Cli::parse();
    match cli.command {
        None => run_interactive().await,
        Some(Commands::Analyze {
            config,
            team,
            proxy_file,
        }) => analyze(config, team, proxy_file).await,
        Some(Commands::Run {
            config,
            proxy_file,
            team,
            target,
            register_workers,
            rt_workers,
            rt_retries,
            live,
            push_s2a,
            chatgpt_mail,
        }) => {
            let dry_run = !live;
            run(
                config,
                team,
                target,
                register_workers,
                rt_workers,
                rt_retries,
                dry_run,
                push_s2a,
                chatgpt_mail,
                proxy_file,
            )
            .await
        }
    }
}

async fn analyze(
    config_path: PathBuf,
    team_name: Option<String>,
    proxy_file: Option<PathBuf>,
) -> Result<()> {
    let cfg = AppConfig::load(&config_path)
        .with_context(|| format!("加载配置失败: {}", config_path.display()))?;
    let teams = cfg.effective_s2a_configs();
    let selected = select_team(&teams, team_name.as_deref())?;

    println!("配置文件: {}", config_path.display());

    // 解析和检测代理
    let proxy_list = resolve_proxies(proxy_file.as_deref(), &cfg.proxy_pool)?;
    let check_timeout = cfg.proxy_check_timeout_sec.unwrap_or(5);
    let healthy_proxies = health_check(&proxy_list, check_timeout, proxy_file.as_deref()).await?;

    println!("可用代理数量: {}", healthy_proxies.len());
    for (idx, p) in healthy_proxies.iter().take(3).enumerate() {
        println!("  proxy[{idx}]: {}", util::mask_proxy(p));
    }
    if healthy_proxies.len() > 3 {
        println!("  ...");
    }
    println!("S2A 配置数量: {}", teams.len());
    println!(
        "当前团队: {} | 并发: {} | 优先级: {} | 分组: {:?}",
        selected.name, selected.concurrency, selected.priority, selected.group_ids
    );
    println!(
        "建议参数: target={} register_workers={} rt_workers={} rt_retries={}",
        cfg.defaults.target_count.unwrap_or(10),
        cfg.defaults.register_workers.unwrap_or(5),
        cfg.defaults.rt_workers.unwrap_or(5),
        cfg.defaults.rt_retries.unwrap_or(4)
    );
    Ok(())
}

use std::sync::atomic::{AtomicBool, Ordering};

#[allow(clippy::too_many_arguments)]
async fn run(
    config_path: PathBuf,
    team_name: Option<String>,
    target: Option<usize>,
    register_workers: Option<usize>,
    rt_workers: Option<usize>,
    rt_retries: Option<usize>,
    dry_run: bool,
    push_s2a: bool,
    use_chatgpt_mail: bool,
    proxy_file: Option<PathBuf>,
) -> Result<()> {
    let cfg = AppConfig::load(&config_path)
        .with_context(|| format!("加载配置失败: {}", config_path.display()))?;
    let teams = cfg.effective_s2a_configs();
    let selected_team = select_team(&teams, team_name.as_deref())?.clone();

    let target_count = target.or(cfg.defaults.target_count).unwrap_or(1).max(1);
    let reg_workers = register_workers
        .or(cfg.defaults.register_workers)
        .unwrap_or(15)
        .max(1);
    let rt_workers = rt_workers.or(cfg.defaults.rt_workers).unwrap_or(10).max(1);
    let rt_retry_max = rt_retries.or(cfg.defaults.rt_retries).unwrap_or(4).max(1);

    // 解析代理来源 + 健康检测
    let proxy_list = resolve_proxies(proxy_file.as_deref(), &cfg.proxy_pool)?;
    let check_timeout = cfg.proxy_check_timeout_sec.unwrap_or(5);
    let healthy_proxies = health_check(&proxy_list, check_timeout, proxy_file.as_deref()).await?;
    let proxy_pool = Arc::new(ProxyPool::new(healthy_proxies));
    println!(
        "运行模式: {} | 代理模式: {} ({} 个可用) | 目标账号: {}",
        if dry_run { "dry-run" } else { "live" },
        proxy_pool.mode_name(),
        proxy_pool.len(),
        target_count
    );
    if target_count < reg_workers {
        println!(
            "提示: 目标账号数({target_count}) 小于注册并发({reg_workers})，本轮实际注册并发最多为 {target_count}"
        );
    }
    if target_count < rt_workers {
        println!(
            "提示: 目标账号数({target_count}) 小于 RT 并发({rt_workers})，本轮实际 RT 并发最多为 {target_count}"
        );
    }

    let register_runtime = cfg.register_runtime();
    let codex_runtime = cfg.codex_runtime();

    // 根据邮箱系统选择创建不同的 EmailService
    let (register_service, codex_service): (
        Arc<dyn services::RegisterService>,
        Arc<dyn services::CodexService>,
    ) = if dry_run {
        (
            Arc::new(DryRunRegisterService::default()),
            Arc::new(DryRunCodexService::default()),
        )
    } else if use_chatgpt_mail {
        let api_key = register_runtime.chatgpt_mail_api_key.clone();
        println!("邮箱系统: chatgpt.org.uk (自动生成邮箱)");
        let reg_email = Arc::new(email_service::EmailService::new_chatgpt_org_uk(
            api_key.clone(),
        ));
        let rt_email = Arc::new(email_service::EmailService::new_chatgpt_org_uk(api_key));
        (
            Arc::new(LiveRegisterService::new(
                register_runtime.clone(),
                reg_email,
            )) as Arc<dyn services::RegisterService>,
            Arc::new(LiveCodexService::new(codex_runtime.clone(), rt_email))
                as Arc<dyn services::CodexService>,
        )
    } else {
        let email_cfg = email_service::EmailServiceConfig {
            mail_api_base: register_runtime.mail_api_base.clone(),
            mail_api_path: register_runtime.mail_api_path.clone(),
            mail_api_token: register_runtime.mail_api_token.clone(),
            request_timeout_sec: register_runtime.mail_request_timeout_sec,
        };
        println!("邮箱系统: kyx-cloud (自定义域名)");
        let reg_email = Arc::new(email_service::EmailService::new_http(email_cfg.clone()));
        let rt_email = Arc::new(email_service::EmailService::new_http(email_cfg));
        (
            Arc::new(LiveRegisterService::new(
                register_runtime.clone(),
                reg_email,
            )) as Arc<dyn services::RegisterService>,
            Arc::new(LiveCodexService::new(codex_runtime.clone(), rt_email))
                as Arc<dyn services::CodexService>,
        )
    };
    let s2a_service: Arc<dyn services::S2aService> = if dry_run {
        Arc::new(DryRunS2aService::default())
    } else {
        Arc::new(S2aHttpService::new())
    };

    if !dry_run && register_runtime.mail_api_token.is_empty() {
        bail!("live 模式需要配置 register.mail_api_token");
    }
    if !dry_run && !push_s2a {
        println!("提示: 当前为 live 注册模式，但未开启 S2A 入库（--push-s2a）。");
    }
    if !dry_run && push_s2a {
        println!("预检: 测试 S2A 连接 [{}]...", selected_team.name);
        s2a_service.test_connection(&selected_team).await?;
        println!("预检: S2A 连接正常");
    }

    // 设置 Ctrl+C 双击退出：第1次优雅关闭，第2次强制退出
    let cancel_flag = Arc::new(AtomicBool::new(false));
    let flag_clone = cancel_flag.clone();
    tokio::spawn(async move {
        let _ = tokio::signal::ctrl_c().await;
        println!("\n收到 Ctrl+C，正在等待进行中的任务完成...");
        println!("再次按 Ctrl+C 强制退出");
        flag_clone.store(true, Ordering::SeqCst);
        // 等待第二次 Ctrl+C → 强制退出
        let _ = tokio::signal::ctrl_c().await;
        println!("\n强制退出！");
        std::process::exit(1);
    });

    let options = WorkflowOptions {
        target_count,
        register_workers: reg_workers,
        rt_workers,
        rt_retry_max,
        push_s2a,
        use_chatgpt_mail,
    };
    let runner = WorkflowRunner::new(register_service, codex_service, s2a_service, proxy_pool);
    let report = runner
        .run_one_team(&cfg, &selected_team, &options, cancel_flag.clone())
        .await?;

    if cancel_flag.load(Ordering::Relaxed) {
        println!("\n--- 中断模式：已将进行中的结果处理完毕 ---");
    }

    println!("完成统计:");
    println!("  注册成功: {}", report.registered_ok);
    println!("  注册失败: {}", report.registered_failed);
    println!("  RT 成功: {}", report.rt_ok);
    println!("  RT 失败: {}", report.rt_failed);
    println!("  S2A 成功: {}", report.s2a_ok);
    println!("  S2A 失败: {}", report.s2a_failed);
    let total_secs = report.elapsed_secs;
    let avg_secs = if report.target_count > 0 {
        total_secs / report.target_count as f32
    } else {
        0.0
    };
    println!("  总耗时: {:.1}s", total_secs);
    println!("  平均耗时: {:.1}s/账号", avg_secs);
    for file in report.output_files {
        println!("  输出文件: {file}");
    }

    Ok(())
}

fn select_team<'a>(teams: &'a [S2aConfig], team_name: Option<&str>) -> Result<&'a S2aConfig> {
    if teams.is_empty() {
        bail!("配置中没有可用的 S2A 团队");
    }
    if let Some(name) = team_name {
        return teams
            .iter()
            .find(|t| t.name == name)
            .ok_or_else(|| anyhow::anyhow!("未找到团队: {name}"));
    }
    Ok(&teams[0])
}

async fn run_interactive() -> Result<()> {
    println!("进入交互模式（无需输入大量命令参数）");
    loop {
        println!();
        println!("请选择操作:");
        println!("  [1] 分析配置");
        println!("  [2] 执行流程（注册 -> RT -> S2A）");
        println!("  [0] 退出");
        let choice = prompt_usize("输入编号", 2, 0, 2)?;

        match choice {
            0 => {
                println!("已退出。");
                return Ok(());
            }
            1 => {
                let (config_path, cfg) = prompt_config_and_load()?;
                let team_name = prompt_team_name(&cfg.effective_s2a_configs())?;
                analyze(config_path, team_name, None).await?;
            }
            2 => {
                let (config_path, cfg) = prompt_config_and_load()?;
                let teams = cfg.effective_s2a_configs();
                if teams.is_empty() {
                    bail!("当前配置中没有可用团队，无法执行 run 流程");
                }
                let team_name = prompt_team_name(&teams)?;

                let target = prompt_usize(
                    "目标账号数量",
                    cfg.defaults.target_count.unwrap_or(1).max(1),
                    1,
                    10000,
                )?;
                let register_workers = prompt_usize(
                    "注册并发数",
                    cfg.defaults.register_workers.unwrap_or(15).max(1),
                    1,
                    256,
                )?;
                let rt_workers = prompt_usize(
                    "RT 并发数",
                    cfg.defaults.rt_workers.unwrap_or(10).max(1),
                    1,
                    256,
                )?;
                let rt_retries = prompt_usize(
                    "RT 重试轮次",
                    cfg.defaults.rt_retries.unwrap_or(4).max(1),
                    1,
                    100,
                )?;
                let live_mode = true;
                let push_s2a = true;
                println!("已默认启用: live 模式 + S2A 入库");

                println!("选择邮箱系统:");
                println!("  [1] kyx-cloud (自定义域名)");
                println!("  [2] chatgpt.org.uk (自动生成邮箱)");
                let mail_choice = prompt_usize("邮箱系统", 1, 1, 2)?;
                let use_chatgpt_mail = mail_choice == 2;

                run(
                    config_path,
                    team_name,
                    Some(target),
                    Some(register_workers),
                    Some(rt_workers),
                    Some(rt_retries),
                    !live_mode,
                    push_s2a,
                    use_chatgpt_mail,
                    None, // 交互模式：自动解析代理
                )
                .await?;
            }
            _ => unreachable!(),
        }

        if !prompt_yes_no("是否继续交互", true)? {
            println!("已退出。");
            return Ok(());
        }
    }
}

fn prompt_config_and_load() -> Result<(PathBuf, AppConfig)> {
    let config_path = prompt_config_path()?;
    let cfg = AppConfig::load(&config_path)
        .with_context(|| format!("加载配置失败: {}", config_path.display()))?;
    Ok((config_path, cfg))
}

fn prompt_config_path() -> Result<PathBuf> {
    let config_path = PathBuf::from("config.toml");
    if config_path.is_file() {
        println!("已自动识别配置文件: {}", config_path.display());
        return Ok(config_path);
    }
    bail!("未找到 config.toml，请先在程序根目录放置该文件");
}

fn prompt_team_name(teams: &[S2aConfig]) -> Result<Option<String>> {
    if teams.is_empty() {
        return Ok(None);
    }
    if teams.len() == 1 {
        println!("已选择团队: {}", teams[0].name);
        return Ok(Some(teams[0].name.clone()));
    }

    println!();
    println!("可选团队:");
    for (idx, team) in teams.iter().enumerate() {
        println!(
            "  [{}] {} (并发={} 优先级={} 分组={:?})",
            idx + 1,
            team.name,
            team.concurrency,
            team.priority,
            team.group_ids
        );
    }
    let selected = prompt_usize("选择团队编号", 1, 1, teams.len())?;
    Ok(Some(teams[selected - 1].name.clone()))
}

fn prompt_yes_no(label: &str, default: bool) -> Result<bool> {
    let suffix = if default { "Y/n" } else { "y/N" };
    loop {
        let input = prompt_string(&format!("{label} [{suffix}]: "))?;
        if input.is_empty() {
            return Ok(default);
        }
        match input.to_ascii_lowercase().as_str() {
            "y" | "yes" | "1" | "true" => return Ok(true),
            "n" | "no" | "0" | "false" => return Ok(false),
            _ => println!("请输入 y 或 n。"),
        }
    }
}

fn prompt_usize(label: &str, default: usize, min: usize, max: usize) -> Result<usize> {
    loop {
        let input = prompt_string(&format!("{label}（默认 {default}）: "))?;
        if input.is_empty() {
            return Ok(default.clamp(min, max));
        }
        match input.parse::<usize>() {
            Ok(v) if v >= min && v <= max => return Ok(v),
            _ => println!("请输入 {min} 到 {max} 的整数。"),
        }
    }
}

fn prompt_string(label: &str) -> Result<String> {
    print!("{label}");
    io::stdout().flush().context("刷新终端输出失败")?;
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .context("读取终端输入失败")?;
    Ok(input.trim().to_string())
}
