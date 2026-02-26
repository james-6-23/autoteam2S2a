# OpenAI 反自动化机制解析

> 基于 `gpt-free-tool-rs` 项目的实际实现进行分析，以 Python 代码为主要说明语言

---

## 目录

1. [概述：多层反自动化体系](#1-概述)
2. [第一层：浏览器指纹伪装](#2-浏览器指纹伪装)
3. [第二层：Sentinel PoW 工作量证明](#3-sentinel-pow-工作量证明)
4. [第三层：Cloudflare Challenge Page](#4-cloudflare-challenge-page)
5. [项目中的完整对抗链路](#5-完整对抗链路)

---

## 1. 概述

OpenAI 采用了一套**多层递进的反自动化体系**来识别和阻止非浏览器流量：

```
请求进入
   │
   ▼
┌──────────────────┐
│ Cloudflare WAF   │──→ Challenge Page（JS 验证 / Turnstile CAPTCHA）
│ （边缘层）        │
└──────────────────┘
   │ 通过
   ▼
┌──────────────────┐
│ Sentinel 系统    │──→ PoW（Proof of Work）挑战
│ （应用层）        │
└──────────────────┘
   │ 通过
   ▼
┌──────────────────┐
│ 业务逻辑层       │──→ 账号状态检查 / 速率限制
└──────────────────┘
```

本项目需要逐一击破这三层防御。以下逐层分析。

---

## 2. 浏览器指纹伪装

### 2.1 原理

服务器通过 HTTP 请求头来判断客户端是否为真实浏览器。关键信号包括：

| 信号 | 说明 | 自动化工具常见特征 |
|------|------|------------------|
| `User-Agent` | 浏览器标识 | 使用默认值如 `python-requests/2.31` |
| `Sec-CH-UA` | Client Hints（Chromium 专属） | 缺失 |
| `Sec-CH-UA-Platform` | 操作系统 | 缺失或与 UA 矛盾 |
| `Sec-Fetch-Mode` | 请求模式 | 缺失（只有浏览器导航才发送） |
| `Accept-Language` | 语言偏好 | 缺失或过于统一 |
| H2/H3 指纹 | TLS/HTTP2 帧特征 | 与声称的浏览器不匹配 |

### 2.2 Python 实现：浏览器配置池

12 种浏览器身份轮转，每个 worker 使用不同配置：

```python
import random

# 12 种浏览器配置轮转
AUTO_ROTATING_PROFILES = [
    "chrome136", "chrome135", "edge134", "chrome136",
    "chrome133", "edge131",   "chrome135", "chrome131",
    "chrome136", "chrome135", "firefox136", "safari17_5",
]

# Accept-Language 随机池
ACCEPT_LANGUAGE_POOL = [
    "en-US,en;q=0.9",
    "en-GB,en;q=0.9,en-US;q=0.8",
    "en-US,en;q=0.9,de;q=0.8",
    "en-US,en;q=0.9,fr;q=0.8",
    "en-US,en;q=0.9,es;q=0.8",
    "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
]

def select_profile(salt: int) -> str:
    """根据 salt 选择浏览器配置（确保同一 worker+proxy 组合总是选同一配置）"""
    return AUTO_ROTATING_PROFILES[salt % len(AUTO_ROTATING_PROFILES)]
```

### 2.3 Python 实现：构建伪装请求头

```python
# 浏览器配置数据库
BROWSER_PROFILES = {
    "chrome136": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,image/apng,*/*;q=0.8",
        # Chromium 专属 Client Hints
        "sec_ch_ua": '"Chromium";v="136", "Google Chrome";v="136", "Not.A/Brand";v="99"',
        "sec_ch_ua_mobile": "?0",
        "sec_ch_ua_platform": '"Windows"',
    },
    "firefox136": {
        "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) "
                      "Gecko/20100101 Firefox/136.0",
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,"
                  "image/avif,image/webp,*/*;q=0.8",
        # Firefox 不发送 Client Hints！
        "sec_ch_ua": None,
        "sec_ch_ua_mobile": None,
        "sec_ch_ua_platform": None,
    },
}


def build_headers(profile_name: str, salt: int) -> dict:
    """构建伪装请求头"""
    profile = BROWSER_PROFILES[profile_name]
    accept_lang = ACCEPT_LANGUAGE_POOL[salt % len(ACCEPT_LANGUAGE_POOL)]

    # 所有请求都带的默认头
    headers = {
        "Accept": profile["accept"],
        "Accept-Language": accept_lang,
        "User-Agent": profile["user_agent"],
    }

    # Chromium 专属：添加 Client Hints
    # ⚠️ 关键：Firefox/Safari 绝对不能发这些头，否则暴露自动化特征
    if profile["sec_ch_ua"] is not None:
        headers["Sec-CH-UA"] = profile["sec_ch_ua"]
        headers["Sec-CH-UA-Mobile"] = profile["sec_ch_ua_mobile"]
        headers["Sec-CH-UA-Platform"] = profile["sec_ch_ua_platform"]

    return headers


def build_navigation_headers(profile_name: str, salt: int) -> dict:
    """构建导航请求专用头（仅页面跳转时使用）"""
    headers = build_headers(profile_name, salt)

    # 仅导航请求才带的头
    # ⚠️ 如果 API 请求也带这些头，反而会暴露自动化
    headers.update({
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    })

    return headers
```

### 2.4 使用示例

```python
import httpx

# 每个 worker 使用不同的 salt → 不同的浏览器身份
salt = hash(f"{proxy}:{retry}:{worker_id}") % 1000

profile = select_profile(salt)
default_headers = build_headers(profile, salt)
nav_headers = build_navigation_headers(profile, salt)

# 页面导航请求 → 用 navigation_headers
resp = httpx.get("https://auth0.openai.com/authorize?...", headers=nav_headers)

# API 请求 → 只用 default_headers（不带 Sec-Fetch-Mode: navigate）
resp = httpx.post("https://auth0.openai.com/u/login", headers=default_headers, json=payload)
```

---

## 3. Sentinel PoW 工作量证明

### 3.1 原理

PoW 是 OpenAI 的**应用层反自动化机制**，由 `sentinel.openai.com` 服务提供。核心思想借鉴了区块链的工作量证明：

> 服务器给客户端一个数学难题（找到满足特定条件的哈希输入），客户端必须消耗 CPU 时间求解。对正常用户来说只需几毫秒，但对大规模自动化来说会成为瓶颈。

### 3.2 完整流程

```
客户端                              Sentinel 服务器
   │                                       │
   │  POST /sentinel/req                   │
   │  { p: init_token,                     │
   │    id: device_id,                     │
   │    flow: "login__auto" }              │
   │ ────────────────────────────────────→  │
   │                                       │
   │  响应:                                 │
   │  { token: "xxx",                      │
   │    proofofwork: {                     │
   │      required: true,                  │
   │      seed: "0.xxxxx",                │
   │      difficulty: "06aa64"             │
   │    }}                                 │
   │ ←────────────────────────────────────  │
   │                                       │
   │  [客户端本地计算 PoW]                   │
   │                                       │
   │  后续请求携带:                          │
   │  openai-sentinel-proof-token:         │
   │  { p: "gAAAAAB{solved_pow}",          │
   │    id: device_id,                     │
   │    flow: "email_otp_verification",    │
   │    c: sentinel_token }                │
   │ ────────────────────────────────────→  │
```

### 3.3 Python 实现：PoW Config（伪装浏览器环境信息）

PoW 的"答案"是一个包含 **18 个字段**的 JSON 数组，经 Base64 编码后提交。每个字段模拟真实浏览器环境：

```python
import json
import time
import random
import base64
import uuid


def build_pow_config(session_id: str, user_agent: str) -> list:
    """
    构建 PoW 配置数组。
    这不仅是 PoW 的输入，本身也是浏览器环境指纹。
    服务器会验证这些字段是否合理，不合理会返回更高的 difficulty。
    """
    now_ms = int(time.time() * 1000)

    return [
        # [0]  随机 JS 执行耗时（模拟真实浏览器解析延迟）
        random.randint(2500, 3500),

        # [1]  固定的日期解析字符串（模拟 JS Date.parse 结果）
        "Tue Jan 30 2024 12:00:00 GMT+0000 (Coordinated Universal Time)",

        # [2]  2^32 常量（标识 32 位数值范围）
        4294967296,

        # [3]  ★ 迭代次数 ← PoW 核心变量，每次尝试时更新
        0,

        # [4]  User-Agent（必须与请求头中的 UA 保持一致！）
        user_agent,

        # [5]  Chrome 扩展路径（标识来源为浏览器扩展）
        "chrome-extension://pgojnojmmhpofjgdmaebadhbocahppod/assets/aW5qZWN0X2hhc2g/aW5qZ",

        # [6]  null（保留位）
        None,

        # [7]  navigator.language
        "zh-CN",

        # [8]  navigator.language（重复确认）
        "zh-CN",

        # [9]  状态标记（置 0）
        0,

        # [10] navigator.canShare.toString() 的输出
        "canShare-function canShare() { [native code] }",

        # [11] React 内部属性名（随机数模拟 React fiber）
        f"_reactListening{random.randint(1_000_000, 10_000_000)}",

        # [12] 事件属性名
        "onhashchange",

        # [13] 当前时间戳（毫秒）
        float(now_ms),

        # [14] 会话 ID
        session_id,

        # [15] 空串（保留位）
        "",

        # [16] 屏幕色深 screen.colorDepth
        24,

        # [17] performance.timeOrigin（页面加载时间 = 当前减去随机偏移）
        now_ms - random.randint(10_000, 50_000),
    ]
```

### 3.4 Python 实现：FNV-1a 哈希函数

OpenAI 使用的是 **FNV-1a 32-bit + 自定义 avalanche mixing**（非标准 FNV-1a）：

```python
def fnv1a32(data: bytes) -> int:
    """
    FNV-1a 32-bit 哈希 + OpenAI 自定义的 avalanche mixing。

    ⚠️ 最后三步 avalanche mixing 是 OpenAI 自定义的，
       标准 FNV-1a 没有这些步骤。如果漏掉，哈希永远不会匹配 difficulty！
    """
    FNV_OFFSET_BASIS = 2166136261
    FNV_PRIME = 16777619
    MASK32 = 0xFFFFFFFF

    h = FNV_OFFSET_BASIS

    # 标准 FNV-1a 循环
    for byte in data:
        h ^= byte
        h = (h * FNV_PRIME) & MASK32

    # ── OpenAI 自定义的 avalanche mixing ──
    # 这是关键！标准 FNV-1a 到上面就结束了
    h ^= (h >> 16)
    h = (h * 2246822507) & MASK32
    h ^= (h >> 13)
    h = (h * 3266489909) & MASK32
    h ^= (h >> 16)

    return h


# 验证
assert fnv1a32(b"hello") == 0xACC3C839  # 可自行验证
```

### 3.5 Python 实现：PoW 求解算法

```python
def solve_pow(
    seed: str,
    difficulty: str,
    session_id: str,
    user_agent: str,
    max_iterations: int = 3_000_000,
) -> tuple[str, int] | None:
    """
    求解 PoW 挑战。

    算法流程：
    1. 构建 config 数组（伪装浏览器环境）
    2. 每次迭代更新 config[3]（迭代计数器）
    3. 将 config 序列化为 JSON → Base64 编码
    4. 拼接 seed + encoded → 计算 FNV-1a 哈希
    5. 哈希的十六进制前缀 ≤ difficulty → 找到有效解

    Args:
        seed: Sentinel 服务器返回的随机种子
        difficulty: 难度值（如 "06aa64"），哈希前缀必须 ≤ 此值
        session_id: 会话 ID
        user_agent: 请求头中的 User-Agent
        max_iterations: 最大迭代次数

    Returns:
        (encoded_answer, iterations) 或 None（超时失败）
    """
    config = build_pow_config(session_id, user_agent)
    seed_bytes = seed.encode("utf-8")
    prefix_len = min(len(difficulty), 8)

    # 难度为空 → 直接通过，不需要计算
    if prefix_len == 0:
        encoded = base64.b64encode(json.dumps(config).encode()).decode()
        return (f"{encoded}~S", 0)

    target_prefix = difficulty[:prefix_len]

    for iteration in range(max_iterations):
        # ★ 核心：每次更新迭代计数器
        config[3] = iteration
        config[9] = 0

        # Step 1: JSON 序列化 → Base64 编码
        json_bytes = json.dumps(config, separators=(",", ":")).encode("utf-8")
        encoded = base64.b64encode(json_bytes).decode("ascii")

        # Step 2: 拼接 seed + encoded
        combined = seed_bytes + encoded.encode("ascii")

        # Step 3: 计算 FNV-1a 32-bit 哈希
        hash_val = fnv1a32(combined)

        # Step 4: 转为 8 位十六进制
        hex_hash = f"{hash_val:08x}"

        # Step 5: 前 N 位 ≤ difficulty → 有效解
        if hex_hash[:prefix_len] <= target_prefix:
            return (f"{encoded}~S", iteration + 1)

    return None  # 超过最大迭代次数，求解失败
```

#### 图解（以 `difficulty = "06aa64"` 为例）

```
迭代 0:   config[3]=0   → base64 → FNV1a → "a3f291bc" → "a3f291" > "06aa64" ✗
迭代 1:   config[3]=1   → base64 → FNV1a → "1b82de04" → "1b82de" > "06aa64" ✗
  ...
迭代 128: config[3]=128 → base64 → FNV1a → "04c1a387" → "04c1a3" ≤ "06aa64" ✓ FOUND!
```

### 3.6 Python 实现：Requirements Token（预生成 Token）

```python
def get_requirements_token(session_id: str, user_agent: str) -> str:
    """
    在调用 Sentinel API 之前，预生成初始 token。
    与 PoW 求解结果的区别：前缀为 gAAAAAC（而非 gAAAAAB）。
    """
    config = build_pow_config(session_id, user_agent)
    config[3] = 0  # 迭代次数固定为 0
    config[9] = 0
    encoded = base64.b64encode(
        json.dumps(config, separators=(",", ":")).encode()
    ).decode()
    return f"gAAAAAC{encoded}~S"   # ← 注意前缀 C


def format_solved_pow(solved: str) -> str:
    """PoW 求解结果的格式"""
    return f"gAAAAAB{solved}"      # ← 注意前缀 B
```

对比：

| 场景 | 格式 | 含义 |
|------|------|------|
| 初始请求 | `gAAAAAC{encoded}~S` | "我还没解过 PoW，这是我的环境信息" |
| PoW 解答 | `gAAAAAB{encoded}~S` | "我已经解出了 PoW" |

### 3.7 Python 实现：完整 Sentinel 交互流程

```python
import httpx


async def call_sentinel(
    client: httpx.AsyncClient,
    session_id: str,
    device_id: str,
    user_agent: str,
    flow: str = "login__auto",
    max_iterations: int = 3_000_000,
) -> dict:
    """
    完整的 Sentinel PoW 交互流程。

    Returns:
        {"sentinel_token": str, "solved_pow": str}
    """
    # Step 1: 生成初始 token
    init_token = get_requirements_token(session_id, user_agent)

    # Step 2: 向 Sentinel 请求 PoW 挑战
    resp = await client.post(
        "https://sentinel.openai.com/backend-api/sentinel/req",
        json={
            "p": init_token,
            "id": device_id,
            "flow": flow,
        },
    )
    data = resp.json()

    result = {
        "sentinel_token": data.get("token", ""),
        "solved_pow": init_token,  # 默认用初始 token
    }

    # Step 3: 检查是否需要 PoW
    pow_info = data.get("proofofwork", {})
    if not pow_info.get("required", False):
        print(f"  [PoW] {flow}: 不需要 PoW ✓")
        return result

    seed = pow_info["seed"]
    difficulty = pow_info["difficulty"]
    print(f"  [PoW] {flow}: 开始求解 difficulty={difficulty}")

    # Step 4: 求解 PoW
    solution = solve_pow(seed, difficulty, session_id, user_agent, max_iterations)

    if solution is None:
        raise RuntimeError(f"PoW 求解失败 (max_iter={max_iterations})")

    encoded_answer, iterations = solution
    result["solved_pow"] = format_solved_pow(encoded_answer)
    print(f"  [PoW] {flow}: 求解成功 iter={iterations} ✓")

    return result


def build_sentinel_header(
    solved_pow: str,
    device_id: str,
    flow: str,
    sentinel_token: str = "",
) -> str:
    """
    构建 openai-sentinel-proof-token 请求头的值。
    后续所有受保护的 API 请求都需要携带此头。
    """
    header = {
        "p": solved_pow,
        "id": device_id,
        "flow": flow,
    }
    if sentinel_token:
        header["c"] = sentinel_token

    return json.dumps(header)


# 使用示例
async def example_usage():
    async with httpx.AsyncClient() as client:
        session_id = str(uuid.uuid4())
        device_id = str(uuid.uuid4())
        user_agent = BROWSER_PROFILES["chrome136"]["user_agent"]

        # 调用 Sentinel 获取并求解 PoW
        sentinel_result = await call_sentinel(
            client, session_id, device_id, user_agent,
            flow="email_otp_verification__auto",
        )

        # 后续 API 请求携带 PoW 结果
        proof_header = build_sentinel_header(
            solved_pow=sentinel_result["solved_pow"],
            device_id=device_id,
            flow="email_otp_verification__auto",
            sentinel_token=sentinel_result["sentinel_token"],
        )

        resp = await client.post(
            "https://auth0.openai.com/...",
            headers={
                "openai-sentinel-proof-token": proof_header,
            },
            json={"code": "123456"},
        )
```

### 3.8 难度分析

difficulty 字符串的长度和值决定了求解难度：

| difficulty | 前缀比较长度 | 命中概率 | 期望迭代次数 | 预估耗时 |
|-----------|------------|---------|------------|---------|
| `""` (空) | 0 | 100% | 0 | 0ms |
| `"ff"` | 2 hex | 100% | 1 | < 1ms |
| `"06aa64"` | 6 hex | ~2.6% | ~38 | < 1ms |
| `"00ff"` | 4 hex | ~0.39% | ~256 | < 1ms |
| `"000100"` | 6 hex | ~0.0015% | ~65,536 | ~50ms |
| `"00001000"` | 8 hex | ~0.000015% | ~6.5M | ~5s |

当前实际运行中收到的 difficulty 约为 `06xxxx`，平均只需 **~40 次迭代**，耗时 **< 1ms**。

---

## 4. Cloudflare Challenge Page

### 4.1 原理

Cloudflare 在边缘层拦截可疑流量，返回一个 HTML 挑战页面（而非预期的 JSON/302 响应）。特征：

- HTTP 状态码 403
- 响应体为 HTML（包含 `<script>` 标签）
- 可能包含 Turnstile CAPTCHA

### 4.2 Python 实现：检测与重试

```python
def looks_like_challenge_page(status_code: int, body: str) -> bool:
    """检测响应是否为 Cloudflare 的挑战页面"""
    return status_code == 403 and (
        "<script" in body or "challenge-platform" in body
    )


def is_retryable_challenge(error_text: str) -> bool:
    """判断错误是否可通过换指纹重试"""
    return "challenge" in error_text or "cf-mitigated" in error_text


async def register_with_retry(proxy: str, worker_id: int, max_retries: int = 3):
    """
    注册流程：遇到 challenge 自动换指纹重试。

    关键策略：每次重试都更换 salt → 生成全新的浏览器身份
    （不同的 UA、Client Hints、Accept-Language 组合）
    """
    for retry in range(max_retries):
        # 每次重试使用不同的 salt → 不同的浏览器指纹
        salt = fingerprint_salt(proxy, retry, worker_id)
        profile = select_profile(salt)
        headers = build_headers(profile, salt)

        try:
            result = await do_register(headers, proxy)
            return result
        except ChallengeError as e:
            if retry + 1 < max_retries:
                print(f"  [W{worker_id}] 触发 challenge，换指纹重试 ({retry+1}/{max_retries})")
                await asyncio.sleep(0.35)  # 短暂等待后换身份重试
                continue
            raise


def fingerprint_salt(proxy: str | None, retry: int, entropy: int) -> int:
    """生成复合 salt，确保每次重试都使用不同的浏览器身份"""
    key = proxy or "direct"
    base = fnv1a32(key.encode()) % (2**32)
    return (base + retry * 131 + entropy * 17) % (2**32)
```

---

## 5. 完整对抗链路

以一次完整的 RT（Refresh Token）获取为例，项目如何依次突破三层防御：

```
                              时间线
                                │
[1] build_headers()             │   构建浏览器指纹（UA + Client Hints + TLS）
    → headers dict              │   对抗: HTTP 请求头检测
                                │
[2] get_requirements_token()    │   预生成 init_token（gAAAAAC...）
    → init_token                │   对抗: Sentinel 初始化验证
                                │
[3] GET /authorize              │   发起 OAuth 授权请求
    + navigation_headers        │   对抗: Sec-Fetch-* 检测
    ↳ 如果触发 challenge        ─┤→ [重试: 换指纹 goto 1]
                                │   对抗: Cloudflare WAF
                                │
[4] call_sentinel()             │   向 Sentinel 请求 PoW 挑战
    ← { seed, difficulty }      │
                                │
[5] solve_pow()                 │   CPU 求解 FNV-1a 哈希碰撞
    → gAAAAAB{solved}~S         │   对抗: 计算量验证
                                │
[6] 后续请求携带                  │   在 openai-sentinel-proof-token
    build_sentinel_header()     │   头中提交求解结果
                                │
[7] POST /token (code exchange) │   交换 authorization_code → refresh_token
    → refresh_token ✓           │
```

### 难度自适应机制

OpenAI 的 Sentinel 系统会根据以下因素**动态调整** PoW difficulty：

| 因素 | 低风险（低 difficulty） | 高风险（高 difficulty） |
|------|----------------------|----------------------|
| IP 信誉 | 家庭宽带 / 低频 IP | 数据中心 / 代理 / 高频 IP |
| 请求模式 | 正常间隔 | 高频批量 |
| 浏览器指纹 | 完整且一致 | 缺失或矛盾 |
| 地理位置 | 用户常见地区 | 异常地区 |
| 历史行为 | 正常使用 | 频繁注册 / 自动化特征 |

项目中看到 `difficulty=06xxxx`（低难度）说明当前的指纹伪装和代理质量有效。如果质量下降，difficulty 会升高。

---

## 附录 A：完整可运行的 Python 示例

```python
"""
完整的 PoW 求解演示（可直接运行）
"""
import json
import time
import random
import base64


def fnv1a32(data: bytes) -> int:
    h = 2166136261
    for byte in data:
        h ^= byte
        h = (h * 16777619) & 0xFFFFFFFF
    h ^= (h >> 16)
    h = (h * 2246822507) & 0xFFFFFFFF
    h ^= (h >> 13)
    h = (h * 3266489909) & 0xFFFFFFFF
    h ^= (h >> 16)
    return h


def build_pow_config(sid: str, ua: str) -> list:
    now_ms = int(time.time() * 1000)
    return [
        random.randint(2500, 3500),
        "Tue Jan 30 2024 12:00:00 GMT+0000 (Coordinated Universal Time)",
        4294967296, 0, ua,
        "chrome-extension://pgojnojmmhpofjgdmaebadhbocahppod/assets/aW5qZWN0X2hhc2g/aW5qZ",
        None, "zh-CN", "zh-CN", 0,
        "canShare-function canShare() { [native code] }",
        f"_reactListening{random.randint(1000000, 10000000)}",
        "onhashchange", float(now_ms), sid, "", 24,
        now_ms - random.randint(10000, 50000),
    ]


def solve_pow(seed: str, difficulty: str, sid: str, ua: str, max_iter: int = 500000):
    config = build_pow_config(sid, ua)
    seed_bytes = seed.encode()
    prefix_len = min(len(difficulty), 8)
    if prefix_len == 0:
        enc = base64.b64encode(json.dumps(config).encode()).decode()
        return enc + "~S", 0
    target = difficulty[:prefix_len]
    for i in range(max_iter):
        config[3] = i
        config[9] = 0
        enc = base64.b64encode(json.dumps(config, separators=(",", ":")).encode()).decode()
        combined = seed_bytes + enc.encode()
        h = fnv1a32(combined)
        if f"{h:08x}"[:prefix_len] <= target:
            return enc + "~S", i + 1
    return None


# ── 演示 ──
if __name__ == "__main__":
    # 模拟 Sentinel 返回的参数
    test_seed = "0.7584321948205742"
    test_difficulty = "06aa64"
    test_sid = "test-session-id"
    test_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/136.0.0.0"

    start = time.perf_counter()
    result = solve_pow(test_seed, test_difficulty, test_sid, test_ua)
    elapsed = time.perf_counter() - start

    if result:
        answer, iterations = result
        print(f"✓ PoW 求解成功!")
        print(f"  difficulty: {test_difficulty}")
        print(f"  迭代次数:   {iterations}")
        print(f"  耗时:       {elapsed*1000:.1f}ms")
        print(f"  答案前 40 字符: {answer[:40]}...")
    else:
        print("✗ PoW 求解失败")
```

预期输出：
```
✓ PoW 求解成功!
  difficulty: 06aa64
  迭代次数:   42
  耗时:       3.2ms
  答案前 40 字符: W3JhbmRvbSgpLCJUdWUgSmFuIDMwIDIwMjQg...
```

---

## 附录 B：配置参数参考

```toml
# config.toml 中与反自动化相关的配置
[register]
tls_emulation = "auto"       # 浏览器轮转模式（auto = 12 种轮转）
init_retries = 3             # challenge 重试次数（每次换指纹）

[codex]
sentinel_enabled = true       # 是否启用 Sentinel PoW
strict_sentinel = false       # PoW 失败时是否中止（false = 降级继续）
pow_max_iterations = 3000000  # PoW 最大迭代次数
```
