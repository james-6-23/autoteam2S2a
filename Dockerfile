# ── Stage 1: Chef — 生成依赖配方 ──────────────────────────────
FROM rust:1.88-bookworm AS chef

RUN cargo install cargo-chef

# rquest 底层 BoringSSL 编译依赖
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake ninja-build golang-go g++ make libclang-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# ── Stage 2: Planner — 分析依赖 ──────────────────────────────
FROM chef AS planner

COPY Cargo.toml Cargo.lock ./
COPY src/ src/

# 生成依赖配方（只包含依赖信息，不含业务代码）
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Builder — 编译 ───────────────────────────────────
FROM chef AS builder

# 先用配方只编译依赖（源码不变时完全命中缓存）
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 再复制源码和静态资源，编译业务代码
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY static/ static/

RUN cargo build --release

# ── Stage 4: Runtime ─────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/autoteam2s2a /app/autoteam2s2a

RUN mkdir -p /app/config /app/data /app/accounts

VOLUME ["/app/config"]
VOLUME ["/app/data"]
VOLUME ["/app/accounts"]

EXPOSE 3456

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:3456/health || exit 1

ENTRYPOINT ["/app/autoteam2s2a"]
CMD ["serve", "--config", "/app/config/config.toml", "--host", "0.0.0.0", "--port", "3456"]
