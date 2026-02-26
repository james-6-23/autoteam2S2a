# gpt-free-tool-rs

Rust 重构版（第一阶段）：

- 保留主流程语义：`注册 -> 获取RT -> 入库S2A`
- 多代理轮询组件（线程安全）
- 单团队执行模式（支持按名称选择团队）
- 默认 `dry-run`，用于验证并发、重试、文件落盘与流程编排

## 当前状态

已完成：

- 工程分层：`config / proxy_pool / services / workflow / storage / util`
- 多代理组件：轮询分发、代理掩码日志
- 一团队流水线：注册并发、RT 重试并发、S2A 并发（可选）
- `register` live 协议流程
- `codex` live 最小 OAuth/RT 流程（PKCE + 授权码 + token 交换，含 OTP/Sentinel 处理）
- 输出文件：
  - `free-accounts-*.json`
  - `accounts-free-with-rt-*.json`
  - `accounts-free-no-rt-*.json`
  - `accounts-s2a-failed-*.json`

待迁移：

- 更细粒度错误分类与熔断策略

## 快速开始

```bash
cargo check
cargo run
cargo run -- analyze --config config.example.toml
cargo run -- run --config config.example.toml --target 5 --register-workers 3 --rt-workers 3
```

说明：`cargo run` 无参数时会进入交互式 CLI，根据提示逐步选择配置与运行参数。

启用真实 S2A 入库（谨慎）：

```bash
cargo run -- run --config config.example.toml --target 1 --live --push-s2a
```

启用真实注册 + Codex RT 流程（不入库，仅联调）：

```bash
cargo run -- run --config config.example.toml --target 1 --register-workers 1 --rt-workers 1 --live
```

## 迁移策略建议

1. 先把 `register` 实现替换为 Rust 真正 API 调用（保持当前 trait 不变）。
2. 再替换 `codex` 实现（保持 RT 重试框架不变）。
3. 最后补齐 observability（结构化日志/指标）与回归测试。
