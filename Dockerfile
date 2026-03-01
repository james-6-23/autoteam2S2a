# ── Stage 1: Build ─────────────────────────────────────────────
FROM rust:1.86-bookworm AS builder

# rquest 底层 BoringSSL 编译依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake ninja-build golang-go g++ make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 先复制依赖文件，利用 Docker 层缓存
COPY Cargo.toml Cargo.lock ./

# 创建空项目骨架缓存依赖编译
RUN mkdir src && echo "fn main() {}" > src/main.rs && \
    cargo build --release 2>/dev/null || true && \
    rm -rf src

# 复制全部源码和静态资源
COPY src/ src/
COPY static/ static/

# 正式编译（touch 确保 cargo 重新编译 main.rs）
RUN touch src/main.rs && cargo build --release

# ── Stage 2: Runtime ───────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 只复制编译好的二进制
COPY --from=builder /app/target/release/autoteam2s2a /app/autoteam2s2a

# 创建数据目录
RUN mkdir -p /app/config /app/data /app/accounts

# 数据持久化（配置文件可选，不存在时使用默认配置通过管理面板配置）
VOLUME ["/app/config"]
VOLUME ["/app/data"]
VOLUME ["/app/accounts"]

EXPOSE 3456

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:3456/health || exit 1

ENTRYPOINT ["/app/autoteam2s2a"]
# config.toml 可选：不存在则零配置启动，管理员通过 Web 面板配置
CMD ["serve", "--config", "/app/config/config.toml", "--host", "0.0.0.0", "--port", "3456"]
