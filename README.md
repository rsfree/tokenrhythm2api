# trpool — TokenRhythm 号池网关

[![Release](https://img.shields.io/github/v/release/rsfree/tokenrhythm2api)](https://github.com/rsfree/tokenrhythm2api/releases)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

把多个 `sk_tr_` 密钥池化成一个入口：客户端只认一把 key，网关负责选号、换号、冷却、记账。

参考 `workbuddy2api` 的号池骨架（候选过滤 → 短名单加权抽签 → 熔断冷却 → 粘性会话），
针对 tokenrhythm 的协议特点重写：**零外部依赖、单二进制、原生 SSE 透传**。

---

## 为什么不是"余额均衡"

上游**没有公开的余额查询接口**（`/v1/me`、`/v1/usage`、`/v1/balance` 全 404，响应头也无配额头）。
所以"剩余额度"只能反推：

```
剩余 = 充值额（人工填） − 累计成本（本地记账）
```

成本口径不靠估：优先取上游响应里的 `cost_cny`（结算口径），
没有时用 `/v1/models` 自带的单价 × usage 计算。实测两者完全一致：

| 请求 | usage | 本地按单价算 | 上游 `cost_cny` |
|---|---|---|---|
| 非流式 | 65 / 16 | ¥0.00004760 | ¥0.00004760 |
| 流式 | 68 / 32 | ¥0.00007040 | ¥0.00007040 |

**没填充值额的号**余额显示"未知"，不参与余额加权，也不触发保底——适合先跑起来再补数据。

---

## 安装

从 [Releases](https://github.com/rsfree/tokenrhythm2api/releases) 下载对应平台的单文件二进制：

```bash
# Linux amd64
curl -fsSL -o trpool https://github.com/rsfree/tokenrhythm2api/releases/latest/download/trpool-linux-amd64
chmod +x trpool && ./trpool -config config.json
```

或自行编译（需 Go 1.25+）：

```bash
go build -o trpool ./cmd/trpool
```

## 快速开始

```bash
# 1. 配置（密钥走环境变量，不落盘）
export TRPOOL_ADMIN_KEY="sk-trpadmin-$(openssl rand -hex 12)"
export TRPOOL_CLIENT_KEYS="sk-trpool-$(openssl rand -hex 16)"

# 2. 编译运行
go build -o trpool ./cmd/trpool
./trpool -config config.json

# 3. 打开面板
open "http://127.0.0.1:8787/panel"
```

首启也可以直接灌号（池子非空时忽略，不会重复导入）：

```bash
TRPOOL_KEYS="账号A=sk_tr_xxx=100,账号B=sk_tr_yyy=50" ./trpool -config config.json
#                                     ↑ 名称=密钥=充值额（¥，可省）
```

Docker（自己构建）：

```bash
export TRPOOL_ADMIN_KEY=... TRPOOL_CLIENT_KEYS=...
docker compose up -d
```

或用 CI 出的镜像：

```bash
docker run -d --name trpool --restart unless-stopped \
  -p 8787:8787 \
  -e TRPOOL_ADMIN_KEY="sk-trpadmin-..." \
  -e TRPOOL_CLIENT_KEYS="sk-trpool-..." \
  -v "$PWD/data:/data/data" \
  ghcr.io/rsfree/tokenrhythm2api:latest
```

> 镜像 tag 与 git tag 同版本但**不带 `v` 前缀**（`v0.0.4` → `:0.0.4`），另有 `:latest` 和 `:sha-<7位>`，
> 三者来自同一次构建（digest 相同）。
>
> 当前只构建 **linux/amd64**。在 arm64 机器上会走模拟层，能跑但慢；需要原生 arm64 镜像再加多平台构建。

---

## 客户端接入

把 `base_url` 指到网关、`api_key` 换成 client key 即可，协议完全透传。

```python
from openai import OpenAI

client = OpenAI(
    api_key="sk-trpool-xxxx",           # 网关下发的，不是 sk_tr_
    base_url="http://127.0.0.1:8787/v1",
)
print(client.chat.completions.create(
    model="qwen3.8-flash",
    messages=[{"role": "user", "content": "你好"}],
).choices[0].message.content)
```

支持的路径（全部原样反代）：

| 协议 | 路径 |
|---|---|
| OpenAI | `/v1/chat/completions`、`/v1/embeddings`、`/v1/models` |
| Anthropic | `/v1/messages` |
| 视频 | `/v1/videos/generations`、`/v1/videos/{id}`、`/v1/videos` |
| 决策 | `/v1/decision`、`/v1/systemone` |

`stream: true` 按 SSE 逐行透传，不改缓冲、不攒包。

---

## 调度

选号优先级从强到弱，每一层解决一个具体问题：

| 优先级 | 规则 | 为什么 |
|---|---|---|
| ① 视频任务粘性 | `task_id → key`，查询必须回创建者，冷却中也回 | 上游只认创建者，换号就 404 |
| ② 会话粘性 | 同一段对话尽量落在同一个号 | 命中上游 prompt cache |
| ③ batch 成员 | 只从"当前活跃"的号里选 | 集中使用少数号，让每个号更快用透 |
| ④ 在途最少 | 并发高峰把新请求推给空闲的号 | 避免全压一个号撞 `max_in_flight` |
| ⑤ 权重 | 余额因子 × 闲置补偿 | 余额多的多用，久置的补偿 |

### 会话粘性：省钱的那一项

上游各家的 prompt cache **按账号隔离**，会话一旦被打散到不同号，缓存永远命中不了。
价目表里的差价是实打实的：

| 模型 | 输入 | 缓存命中 |
|---|---|---|
| glm-5.1 | ¥8/M | ¥2/M |
| kimi-k2.6 | ¥6.5/M | ¥1.3/M |

指纹取法（按优先级）：

1. 显式会话标识 —— OpenAI 的 `user`、Anthropic 的 `metadata.user_id`
2. 否则用 `system` + 首条 `user` 的内容（`prefix_affinity`，默认开）

TTL 默认 30 分钟，表上限 2000 条（超了淘汰最久未用）。粘住的号出事（冷却 / 余额触底）会自动退让，
不会把会话卡死；下一次请求会重新粘到新号上。

### batch：控制同时用几个号

```json
"batch": { "mode": "auto", "size": 3, "min": 1, "max": 0, "target_inflight_per_key": 4 }
```

| 模式 | 行为 |
|---|---|
| `off` | 全池参与，最均衡 |
| `fixed` | 固定只动用 `size` 个号 |
| `auto` | `target = ceil(在途并发 / target_inflight_per_key)`，夹在 `[min, max]` 之间 |

auto 的直觉：低并发时集中用 1 个号（少切换、缓存集中），并发上来按需扩。
**成员失效（冷却/冻结/额度耗尽）会自动从候补里补齐**，不需要人工维护。
batch 是软约束——里面的号全满或被 `tried` 排除时会自动放开到全池，不会把请求卡死。

实机观测：空载 `target=1`，在途 3 且 `per_key=2` 时 `target=2`。

### 预热预取

启动时并发探一遍所有启用的号（默认 4 并发），**探出坏号直接写进状态**，让第一波业务请求
不必自己试错。只打 `/v1/models`，不消耗生成额度。

效果实测：某次启动时两个号已被上游 `ACCOUNT_SUSPENDED` 冻结，预热直接把它们判为 `exhausted`，
随后 3 个业务请求全部 `attempt=1` 一次成功。若没有这一步，每个请求都要先撞一次 403 再换号。

---

## 容错

| 上游返回 | 判定 | 号的后果 |
|---|---|---|
| 2xx | 成功 | 失败计数清零，回到 active |
| 401 | 密钥失效 | `invalid`，不自动恢复，需人工重置 |
| 402 | 额度不足 | `exhausted`，冷却 30 分钟后自动重试 |
| **403 + `ACCOUNT_SUSPENDED`** | 账号被临时冻结 | 冷却 30 分钟，换号继续 |
| **403 + `MODEL_ACCESS_DENIED`** | 模型未开通 / 未灰度 | **不罚号**，原样返回客户端，不重试 |
| **403 + 其它** | 认不出来 | 冷却 30 分钟（保守处理） |
| **429 + `RATE_LIMITED`** | 本平台次数配额（实测 30 次/约 15 秒窗口） | `cooling`，按 `retryAfterSeconds` 冷却；没给提示才 20s 起指数退避 |
| **429 + `UPSTREAM_RATE_LIMITED`** | 模型供应链限流 | **不罚号**，退避 ≤5s 后优先同号重试 |
| 5xx / 超时 | 上游故障 | 累计 3 次后熔断，冷却 30s 起、指数退避 |
| 4xx 其他 | 请求本身有问题 | **不罚号**，立即返回 |

**403 必须看 body 才能判** —— 同一个状态码，三种完全不同的意思：

- 把 `ACCOUNT_SUSPENDED`（账号冻结）当成"模型没权限"不重试 → 会话粘在一个冻结号上，次次撞墙；
- 把 `MODEL_ACCESS_DENIED`（模型灰度）当成"号的锅"去冷却 → 调一次没权限的模型能把全池冷却掉。

**重试边界**：只有上游响应头已到、但还没向客户端写出任何字节时才换号。
流式一旦开始刷字节就不再换——否则客户端会收到两个上游拼接的内容。

**视频任务粘性**：`POST /v1/videos/generations` 成功后记下 `task_id → key`，
后续 `GET /v1/videos/{id}` 强制回到创建者，即使那个号正在冷却。粘性默认保留 72h。

---

## 面板

`/panel`（Bearer `admin_key`，也支持 `?token=<admin_key>` 浏览器直开）。左侧导航分七页，
窄屏自动收成抽屉。

| 页 | 解决什么 |
|---|---|
| **总览** | 可用号/请求/消耗/今日四张卡；调度快照（batch 用量与目标、会话、在途、粘性任务）；**最近异常**——失败请求按时间倒序，带状态码、号名、模型与上游原话 |
| **账号池** | 号池表：状态、余额进度条、请求/错误/Token、当前权重、最后错误；操作：探活、重置、编辑、启停、删除。`B` = 当前 batch 成员，数字 = 粘在该号上的会话数 |
| **用量** | 今日/近 14 日消耗、单请求均价、Token 总量；日账表（按 Asia/Shanghai 切日）；**号 · 消耗构成** 与 **模型 · 近期成本**（后者基于请求环，标明窗口） |
| **容量规划** | 可用号数、理论 RPM（可用号 × 120）、安全在飞数（`2 QPS × p50 延迟`）、观测 p95；每号容量表（在途/上限进度、会话、平均耗时、冷却剩余、最近成功）。**这页是判断"要不要加号"的依据** |
| **模型与档位** | 上游价目表：输入/输出/缓存读单价、输出/输入倍率、是否按单价记账；支持搜索与一键刷新 |
| **配置** | 14 项调度参数热更新（保存即生效并写回 `config.json` 的 `pool` 段）；运行时只读信息（版本/监听/上游/数据目录/探活间隔/日志环）；客户端密钥查看与复制 |
| **运行日志** | 最近 400 条逐请求记录；按状态（全部/仅失败/仅成功）、号、模型、文本过滤；每条带耗时、tokens、成本、第几次尝试、备注，`trace` 按钮复制 trace_id 便于对上上游工单。**日志跨重启保留最近 200 条**，页头标出本进程启动时刻 |

排查动线：**总览看异常 → 点号名看该号状态 → 运行日志按号过滤看时间线 → 容量规划判断是不是撞了窗口**。

管理接口（均需 `Authorization: Bearer <admin_key>`）：

```
GET    /panel/api/state
GET    /panel/api/logs?n=400
GET    /panel/api/prices            # 价目表快照
POST   /panel/api/keys              {name, secret, weight, budget}
PATCH  /panel/api/keys/{id}         {name?, secret?, weight?, budget?, enabled?}
DELETE /panel/api/keys/{id}
POST   /panel/api/keys/{id}/reset
POST   /panel/api/keys/{id}/probe
POST   /panel/api/keys/probe        全部探活
PUT    /panel/api/pool              热更新调度参数
POST   /panel/api/prices            刷新价目表
```

---

## 运维要点

- **状态文件**：`data/state.json`（号、账本、粘性、**最近 200 条请求日志**），权限 600，
  原子写（临时文件 + rename）。删掉它会丢历史账本，但号会随配置重建。
  日志随状态一起落盘，是为了重启后还能查"刚才那个 429 是谁打的"；面板日志页会标注
  **本进程启动时刻**，避免把上个进程的记录当成刚发生的读时间线。
- **密钥不落盘**：`admin_key` / `client_keys` 优先读环境变量；写回 `config.json` 时只替换 `pool` 段，不会把密钥写进文件。
- **自动探活**：默认每 2 分钟用 `/v1/models` 探一遍 `cooling`/`invalid` 的号，探活成功自动复位。
  探活只打 `/v1/models`，不消耗生成额度。
- **并发**：单号默认 `max_in_flight=3`，全局默认不限。
  实测 tokenrhythm 的上限是**每号 30 次/约 15 秒滑动窗口（≈2 QPS）**，不是并发数——
  31 并发时正好 30 过 1 拒，而当时串行的 6 个也会被拒。所以安全在飞数 ≈ `2 × 响应耗时`，
  1.5s 响应时约 3 个；超了返 429 `RATE_LIMITED` + `retryAfterSeconds`（13~15s 自愈），网关按它精确冷却。
- **`include_usage` 注入**：流式 `/v1/chat/completions` 会强制 `stream_options.include_usage=true`，
  否则拿不到 usage、记账只能估算。多出来的那个 usage chunk 是 OpenAI 官方语义，SDK 会正常忽略。

---

## 目录

```
cmd/trpool/         入口、配置加载、后台任务（Tick/探活/价目表刷新/落盘）
internal/config/    配置结构与校验（支持 "30s" 这类时长写法）
internal/pool/      号池核心：状态机、选号、冷却、熔断、在途、粘性
internal/ledger/    价目表、usage 解析、成本计算、请求日志环、日账
internal/upstream/  上游传输层（只负责收发，不含重试）
internal/proxy/     网关：鉴权 → 选号 → 透传 → 换号 → 记账
internal/panel/     管理 API + 内嵌前端（go:embed）
```

## 已知限制

- 余额是**本地记账推算**，不是上游真实余额。充值/退款/赠送额需要手动改充值额校准。
- 未知模型按"收费"处理（保守），避免把余额打空；可在 `/v1/models` 刷新后恢复准确。
- 没有接入用户体系：`client_keys` 是静态列表，适合个人或小团队内部用。
- 单实例设计，多副本会各记各的账、粘性表也不共享。
