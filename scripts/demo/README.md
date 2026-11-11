# 本地演示环境（不需要真实上游 key）

用来在没有 `sk_tr_` 密钥的情况下把面板喂出真实数据：假上游会按密钥后缀返回
200 / 429 / 403 三种结局，于是冷却、模型级封禁、换号重试、记账、容量统计全都有现场。

## 三步

```bash
# 1. 假上游（默认 127.0.0.1:18999）
python scripts/demo/mock_upstream.py

# 2. 网关指向它，并灌四个号
TRPOOL_ADMIN_KEY=sk-trpadmin-demo \
TRPOOL_CLIENT_KEYS=sk-trpool-demo \
TRPOOL_UPSTREAM=http://127.0.0.1:18999 \
TRPOOL_KEYS="主号=sk_tr_mock_key_one=200,备号A=sk_tr_mock_key_two=120,备号B=sk_tr_mock_key_three=80,受限号=sk_tr_mock_key_four=40" \
  ./trpool -config /tmp/trpool-demo/config.json

# 3. 打流量，然后开 http://127.0.0.1:8787/panel/?token=sk-trpadmin-demo
python scripts/demo/drive.py
```

## 四个假号的性格

| 密钥 | 行为 | 面板上能看到什么 |
|---|---|---|
| `sk_tr_mock_key_one` | 永远 200，延迟 0.25~0.6s | 正常号、成本累计 |
| `sk_tr_mock_key_two` | 永远 200，延迟固定 0.9s | p95 被拉高，容量页差异明显 |
| `sk_tr_mock_key_three` | 429 `RATE_LIMITED` + `Retry-After: 13` | 号级冷却、换号重试（`次=2`） |
| `sk_tr_mock_key_four` | 只在 `glm-5.1` 上 403 `MODEL_ACCESS_DENIED` | 模型级封禁 `(号 × 模型)`，其余模型照常 |

另外两种批量号种，专门用来演示活跃窗口：

| 后缀 | 行为 | 面板上能看到什么 |
|---|---|---|
| `..._rateN` | 恒 429 `RATE_LIMITED` | 号级冷却 + 弹性扩容被触发 |
| `..._drainN` | 恒 402 `INSUFFICIENT_BALANCE` | 号转「余额耗尽 · 停用」，窗口**当场补一个**上来；不会自动复活，需补额度/点「恢复」 |
| `..._susp` | 恒 403 `ACCOUNT_SUSPENDED` | 号转「账号已冻结 · 停用」，不会自动复活；**不计入 429 压力**，不会顶起弹性 |

## 演示活跃窗口 + 弹性扩容

20 个号里放 6 个 429、1 个欠费，再把窗口开到 12：

```bash
TRPOOL_ADMIN_KEY=sk-trpadmin-demo \
TRPOOL_CLIENT_KEYS=sk-trpool-demo \
TRPOOL_UPSTREAM=http://127.0.0.1:18999 \
TRPOOL_KEYS="ok1=sk_tr_demo_ok01=200,ok2=sk_tr_demo_ok02=200,ok3=sk_tr_demo_ok03=200,ok4=sk_tr_demo_ok04=200,ok5=sk_tr_demo_ok05=200,ok6=sk_tr_demo_ok06=200,r1=sk_tr_demo_rate01=200,r2=sk_tr_demo_rate02=200,r3=sk_tr_demo_rate03=200,r4=sk_tr_demo_rate04=200,r5=sk_tr_demo_rate05=200,r6=sk_tr_demo_rate06=200,d1=sk_tr_demo_drain01=50" \
  ./trpool -config /tmp/trpool-win/config.json
```

```bash
# 窗口 12 + 弹性：429 占比超 10% 就每次加 2 个，稳定 30 秒退 1 个
curl -s -X PUT http://127.0.0.1:8787/panel/api/pool \
  -H "Authorization: Bearer sk-trpadmin-demo" -H 'content-type: application/json' \
  -d '{"strategy":"weighted_random","top_k":5,"retry_max":3,
       "batch":{"mode":"fixed","size":12,"min":1,"max":0,
                "elastic":{"enabled":true,"window":"60s","trigger_rate":0.1,
                           "min_samples":10,"min_hits":2,"step":2,"max_size":20,
                           "hold":"2m","stable":"5m","decay_step":1}}}'
```

> 窗口成员按**权重**挑。想让故障号进窗口，先给它们 PATCH 一个更高的
> `weight`（`PATCH /panel/api/keys/{id}`），再把 `batch.mode` 切 off → fixed 重建窗口。
> 否则窗口会先挑走健康号，一直看不到 429。

## 注意

- 假上游是 `ThreadingHTTPServer`，SSE 必须发 `Connection: close`：没有 `Content-Length`
  的 1.1 响应靠连接关闭表示结束，声明 keep-alive 会让客户端一直等更多字节。
- `drive.py` 会读完整响应体。只发请求不读 body，等于客户端中途断开——网关会记一条
  「客户端断开」的 note 且拿不到 usage（截图里 token 全是 0）。
- 演示数据只落在 `-config` 指定的目录，别指到生产数据目录上。
