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

## 注意

- 假上游是 `ThreadingHTTPServer`，SSE 必须发 `Connection: close`：没有 `Content-Length`
  的 1.1 响应靠连接关闭表示结束，声明 keep-alive 会让客户端一直等更多字节。
- `drive.py` 会读完整响应体。只发请求不读 body，等于客户端中途断开——网关会记一条
  「客户端断开」的 note 且拿不到 usage（截图里 token 全是 0）。
- 演示数据只落在 `-config` 指定的目录，别指到生产数据目录上。
