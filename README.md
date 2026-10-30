# trpool — TokenRhythm 号池网关

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

Docker：

```bash
export TRPOOL_ADMIN_KEY=... TRPOOL_CLIENT_KEYS=...
docker compose up -d
```

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

## 调度与容错

**选号**：`权重 = 手动权重 × 余额因子 × 闲置补偿`，取前 Top-K 再按权重抽签。

- 余额因子：满额 `2.0×` → 触底 `0.2×`
- 闲置补偿：`1 + 闲置小时 × 增益`，上限可配 —— 防止低余额号永远排不进短名单

**状态转移**：

| 上游返回 | 判定 | 号的后果 |
|---|---|---|
| 2xx | 成功 | 失败计数清零，回到 active |
| 401 | 密钥失效 | `invalid`，**不自动恢复**，需人工重置 |
| 402 | 额度不足 | `exhausted`，冷却 30 分钟后自动重试 |
| 403 | 无灰度权限（如 `MODEL_ACCESS_DENIED`） | **不罚号**，原样返回客户端，不重试 |
| 429 | 容量限流 | `cooling`，指数退避（20s 起，上限 10m） |
| 5xx / 超时 | 上游故障 | 累计 3 次后熔断，冷却 30s 起、指数退避 |
| 4xx 其他 | 请求本身有问题 | **不罚号**，立即返回 |

`403` 特殊对待是有原因的：tokenrhythm 的 403 表示"模型仅对灰度用户开放"，
如果把它当成号的故障，一个没权限的模型会把全池号一起冷却掉。

**重试边界**：只有上游响应头已到、但还没向客户端写出任何字节时才换号。
流式一旦开始刷字节就不再换——否则客户端会收到两个上游拼接的内容。

**视频任务粘性**：`POST /v1/videos/generations` 成功后把 `task_id → key` 记下来，
后续 `GET /v1/videos/{id}` 强制回到创建者，即使那个号正在冷却（上游只认创建者）。粘性默认保留 72h。

---

## 面板

`/panel`（Bearer `admin_key`）

- 号池表：状态、余额进度条、请求/错误/Token、当前权重、最后错误
- 操作：探活、重置、编辑（名称/充值额/权重）、启用停用、删除
- 策略热更新：改完立即生效并写回 `config.json` 的 `pool` 段
- 请求日志：最近 400 条，环形缓冲
- 价目表刷新：`POST /panel/api/prices`

管理接口（均需 `Authorization: Bearer <admin_key>`）：

```
GET    /panel/api/state
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

- **状态文件**：`data/state.json`（号、账本、粘性），权限 600，原子写（临时文件 + rename）。
  删掉它会丢历史账本，但号会随配置重建。
- **密钥不落盘**：`admin_key` / `client_keys` 优先读环境变量；写回 `config.json` 时只替换 `pool` 段，不会把密钥写进文件。
- **自动探活**：默认每 2 分钟用 `/v1/models` 探一遍 `cooling`/`invalid` 的号，探活成功自动复位。
  探活只打 `/v1/models`，不消耗生成额度。
- **并发**：单号默认 `max_in_flight=4`，全局默认不限。过大会把上游 429 提前触发。
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
