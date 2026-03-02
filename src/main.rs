mod config;
mod d1_cleanup;
mod db;
mod distribution;
#[path = "email-service.rs"]
mod email_service;
mod fingerprint;
mod iban;
mod log_broadcast;
mod models;
mod proxy_pool;
mod scheduler;
mod server;
mod services;
mod storage;
mod stripe;
mod util;
mod workflow;

use std::io::{self, Write};
use std::path::{Path, PathBuf};
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
    /// 分析配置与单号池执行参数
    Analyze {
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        #[arg(long)]
        team: Option<String>,
        /// 从文件加载代理列表（每行一个代理地址）
        #[arg(long)]
        proxy_file: Option<PathBuf>,
    },
    /// 执行一号池 free 注册 -> RT -> S2A 入库（默认 live）
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
    /// 启动 HTTP 服务模式（无需配置文件，可通过管理面板配置）
    Serve {
        /// 配置文件路径（可选，不指定则使用默认配置）
        #[arg(short, long, default_value = "config.toml")]
        config: PathBuf,
        /// 监听地址
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        /// 监听端口
        #[arg(short, long, default_value_t = 3456)]
        port: u16,
        /// 从文件加载代理列表（每行一个代理地址）
        #[arg(long)]
        proxy_file: Option<PathBuf>,
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
        Some(Commands::Serve {
            config,
            host,
            port,
            proxy_file,
        }) => {
            let cfg = config::AppConfig::load_or_default(&config);
            let host = cfg.server.host.clone().unwrap_or(host);
            let port = cfg.server.port.unwrap_or(port);
            server::start_server(cfg, config.clone(), host, port, proxy_file).await
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
        "当前号池: {} | 并发: {} | 优先级: {} | 分组: {:?}",
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
            Arc::new(DryRunRegisterService),
            Arc::new(DryRunCodexService),
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
        Arc::new(DryRunS2aService)
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
        free_mode: !cfg.payment_enabled(),
    };
    let runner = WorkflowRunner::new(register_service, codex_service, s2a_service, proxy_pool);
    let report = runner
        .run_one_team(&cfg, &selected_team, &options, cancel_flag.clone(), None)
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
        bail!("配置中没有可用的 S2A 号池");
    }
    if let Some(name) = team_name {
        return teams
            .iter()
            .find(|t| t.name == name)
            .ok_or_else(|| anyhow::anyhow!("未找到号池: {name}"));
    }
    Ok(&teams[0])
}

async fn run_interactive() -> Result<()> {
    // 第一步：选择配置文件（多个时触发交互选择器）
    let (config_path, cfg) = prompt_config_and_load()?;
    let teams = cfg.effective_s2a_configs();
    if teams.is_empty() {
        bail!("当前配置中没有可用号池，无法执行");
    }
    let team_name = prompt_team_name(&teams)?;

    // 第二步：设置运行参数
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

    // 第三步：直接执行
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
        None,
    )
    .await
}

fn prompt_config_and_load() -> Result<(PathBuf, AppConfig)> {
    let config_path = prompt_config_path()?;
    let cfg = AppConfig::load(&config_path)
        .with_context(|| format!("加载配置失败: {}", config_path.display()))?;
    Ok((config_path, cfg))
}

fn prompt_config_path() -> Result<PathBuf> {
    let configs = scan_config_files(".")?;

    if configs.is_empty() {
        bail!("未找到任何配置文件\n请确保目录下存在 config.toml 或 config.*.toml 格式的配置文件");
    }

    if configs.len() == 1 {
        println!("已自动识别配置文件: {}", configs[0].display());
        return Ok(configs[0].clone());
    }

    // 多个配置文件：启动交互式选择器
    select_config_interactive(&configs)
}

/// 扫描目录下的配置文件，返回排序后的列表（config.toml 排在最前面）
fn scan_config_files(dir: &str) -> Result<Vec<PathBuf>> {
    let dir_path = Path::new(dir);
    let mut configs: Vec<PathBuf> = Vec::new();

    let default_config = dir_path.join("config.toml");
    if default_config.is_file() {
        configs.push(default_config);
    }

    // 扫描 config.*.toml 格式的文件
    let entries = std::fs::read_dir(dir_path)
        .with_context(|| format!("无法读取目录: {}", dir_path.display()))?;

    let mut extra_configs: Vec<PathBuf> = entries
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            if let Some(name) = p.file_name().and_then(|n| n.to_str()) {
                name.starts_with("config.")
                    && name.ends_with(".toml")
                    && name != "config.toml"
                    && name.matches('.').count() == 2
            } else {
                false
            }
        })
        .collect();

    extra_configs.sort();
    configs.extend(extra_configs);
    Ok(configs)
}

/// 从配置文件中提取 S2A 摘要信息用于显示
fn extract_config_summary(path: &Path) -> String {
    let Ok(cfg) = AppConfig::load(path) else {
        return "(解析失败)".to_string();
    };
    let teams = cfg.effective_s2a_configs();
    if teams.is_empty() {
        return "(无 S2A 配置)".to_string();
    }
    // 提取第一个 S2A 的域名和所有分组
    let first = &teams[0];
    let domain = first
        .api_base
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .split('/')
        .next()
        .unwrap_or(&first.api_base);

    let all_groups: Vec<String> = teams
        .iter()
        .flat_map(|t| t.group_ids.iter())
        .map(|id| id.to_string())
        .collect();

    let team_names: Vec<&str> = teams.iter().map(|t| t.name.as_str()).collect();

    if all_groups.is_empty() {
        format!("{} | {}", domain, team_names.join(","))
    } else {
        format!(
            "{} | {} | 分组: {:?}",
            domain,
            team_names.join(","),
            all_groups
        )
    }
}

/// 交互式配置文件选择器（方向键 + 彩色高亮）
fn select_config_interactive(configs: &[PathBuf]) -> Result<PathBuf> {
    use crossterm::{
        cursor,
        event::{self, Event, KeyCode, KeyEventKind},
        execute,
        style::{Color, Print, ResetColor, SetBackgroundColor, SetForegroundColor},
        terminal::{self, ClearType},
    };

    let total = configs.len();
    let mut selected: usize = 0;

    // 预解析每个配置文件的摘要信息
    let summaries: Vec<String> = configs.iter().map(|p| extract_config_summary(p)).collect();

    // 进入 raw mode 以捕获单个按键
    terminal::enable_raw_mode().context("无法启用终端 raw 模式")?;

    let result = (|| -> Result<PathBuf> {
        let mut stdout = io::stdout();

        loop {
            // 清屏并绘制 UI
            execute!(
                stdout,
                terminal::Clear(ClearType::All),
                cursor::MoveTo(0, 0)
            )?;

            // Banner
            execute!(
                stdout,
                SetForegroundColor(Color::DarkGrey),
                Print("  +================================================+\r\n"),
                Print("  |                                                |\r\n"),
                Print("  |      "),
                SetForegroundColor(Color::Cyan),
                Print("AutoTeam2S2A  Config Selector"),
                SetForegroundColor(Color::DarkGrey),
                Print("       |\r\n"),
                Print("  |                                                |\r\n"),
                Print("  +================================================+\r\n"),
                ResetColor,
                Print("\r\n"),
                SetForegroundColor(Color::DarkCyan),
                Print("  使用 ↑↓ 方向键选择配置文件，Enter 确认\r\n"),
                Print("  按 ESC 或 Q 退出\r\n"),
                ResetColor,
                Print("\r\n"),
            )?;

            // 列出配置文件
            for (i, cfg) in configs.iter().enumerate() {
                let name = cfg
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("unknown");
                let summary = &summaries[i];

                if i == selected {
                    // 选中项：绿色背景 + 文件名后跟摘要信息
                    execute!(
                        stdout,
                        SetForegroundColor(Color::Black),
                        SetBackgroundColor(Color::Green),
                        Print(format!("  >> {:<22}", name)),
                        SetForegroundColor(Color::DarkGreen),
                        SetBackgroundColor(Color::Green),
                        Print(format!(" {:<50}\r\n", summary)),
                        ResetColor,
                    )?;
                } else {
                    // 未选中项：灰色文件名 + 暗色摘要
                    execute!(
                        stdout,
                        SetForegroundColor(Color::Grey),
                        Print(format!("     {:<22}", name)),
                        SetForegroundColor(Color::DarkGrey),
                        Print(format!(" {}\r\n", summary)),
                        ResetColor,
                    )?;
                }
            }

            // 状态栏
            execute!(
                stdout,
                Print("\r\n"),
                SetForegroundColor(Color::DarkCyan),
                Print(format!(
                    "  共找到 {} 个配置文件    当前选择: {}/{}\r\n",
                    total,
                    selected + 1,
                    total
                )),
                ResetColor,
            )?;

            stdout.flush()?;

            // 等待按键事件
            if let Event::Key(key_event) = event::read()? {
                // 只处理 Press 事件（避免 Release 重复触发）
                if key_event.kind != KeyEventKind::Press {
                    continue;
                }
                match key_event.code {
                    KeyCode::Up => {
                        if selected > 0 {
                            selected -= 1;
                        } else {
                            selected = total - 1;
                        }
                    }
                    KeyCode::Down => {
                        if selected < total - 1 {
                            selected += 1;
                        } else {
                            selected = 0;
                        }
                    }
                    KeyCode::Enter => {
                        return Ok(configs[selected].clone());
                    }
                    KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q') => {
                        bail!("用户取消选择");
                    }
                    _ => {}
                }
            }
        }
    })();

    // 恢复终端状态
    terminal::disable_raw_mode().ok();

    // 打印选择结果
    if let Ok(ref path) = result {
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("unknown");
        println!("\n已选择配置文件: {}", name);
    }

    result
}

fn prompt_team_name(teams: &[S2aConfig]) -> Result<Option<String>> {
    if teams.is_empty() {
        return Ok(None);
    }
    if teams.len() == 1 {
        println!("已选择号池: {}", teams[0].name);
        return Ok(Some(teams[0].name.clone()));
    }

    println!();
    println!("可选号池:");
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
    let selected = prompt_usize("选择号池编号", 1, 1, teams.len())?;
    Ok(Some(teams[selected - 1].name.clone()))
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
