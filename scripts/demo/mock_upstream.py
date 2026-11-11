"""本地假上游：只为截图/联调造数据，不参与任何真实调用。

行为按 Authorization 里的假密钥后缀区分：
  ..._one     正常 200
  ..._two     正常 200（延迟更高，制造 p95 差异）
  ..._three   429 RATE_LIMITED + retryAfterSeconds
  ..._four    glm-5.1 上 403 MODEL_ACCESS_DENIED，其它模型 200
  ..._rateN   429 按号限流（演示弹性扩窗：这类号越多，429 占比越高）
  ..._drain   402 额度不足（演示"一个号没钱了，窗口自动补一个"）
  ..._susp    403 ACCOUNT_SUSPENDED 账号冻结（演示"冻结号长退避、不算 429 压力"）
"""
import json
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MODELS = [
    ("glm-5.1", 8, 24, 2),
    ("kimi-k2.6", 6.5, 18, 1.3),
    ("qwen3.8-flash", 1.2, 4.8, 0.3),
    ("qwen3.8-max", 20, 60, 5),
    ("deepseek-v3.2", 2, 8, 0.5),
]

VIDEOS = {}
lock = threading.Lock()


def prices():
    return {
        "data": [
            {
                "id": m,
                "currency": "CNY",
                "effective_pricing": {
                    "currency": "CNY", "unit": "per_1m_tokens",
                    "prompt": p, "completion": c, "cache_read": cr,
                },
            }
            for m, p, c, cr in MODELS
        ]
    }


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _json(self, code, obj, extra=None):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Trace-Id", "trace_mock_%d" % random.randint(1000, 9999))
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(body)

    def _auth(self):
        h = self.headers.get("Authorization", "")
        return h.replace("Bearer ", "").strip()

    def do_GET(self):
        p = self.path.split("?")[0]
        if p == "/v1/models":
            tok = self._auth()
            time.sleep(0.05)
            tag = tok.rsplit("_", 1)[-1]
            if tag == "three" or tag.startswith("rate"):
                # 429 号连探活都过不去，演示"探活失败不改状态"的保守处理
                return self._json(429, {"error": {"code": "RATE_LIMITED", "message": "配额已用完"}},
                                  {"Retry-After": "13"})
            return self._json(200, prices())
        if p.startswith("/v1/videos/"):
            vid = p.rsplit("/", 1)[-1]
            with lock:
                ok = vid in VIDEOS
            if not ok:
                return self._json(404, {"error": {"code": "NOT_FOUND", "message": "任务不存在"}})
            return self._json(200, {
                "id": vid, "status": "succeeded", "progress": 100,
                "usage": {"prompt_tokens": 0, "completion_tokens": 0},
                "billing": {"total_cost_cny": 0.42},
            })
        self._json(404, {"error": {"code": "NOT_FOUND", "message": p}})

    def do_POST(self):
        p = self.path.split("?")[0]
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            req = json.loads(raw or b"{}")
        except Exception:
            req = {}
        model = req.get("model", "")
        tok = self._auth()
        tag = tok.rsplit("_", 1)[-1] if tok else ""

        if p == "/v1/videos/generations":
            vid = "vid_%d" % random.randint(10000, 99999)
            with lock:
                VIDEOS[vid] = tok
            time.sleep(0.4)
            return self._json(200, {"id": vid, "status": "queued"})

        if p not in ("/v1/chat/completions", "/v1/messages"):
            return self._json(404, {"error": {"code": "NOT_FOUND", "message": p}})

        if tag.startswith("drain"):
            return self._json(402, {"error": {"code": "INSUFFICIENT_BALANCE",
                                              "message": "账号额度不足，请充值"}})
        if tag.startswith("susp"):
            return self._json(403, {"error": {"code": "ACCOUNT_SUSPENDED",
                                              "message": "账号已临时冻结"}})
        if tag == "three" or tag.startswith("rate"):
            return self._json(429, {"error": {"code": "RATE_LIMITED",
                                              "message": "该账号本窗口请求数已达上限"}},
                              {"Retry-After": "13"})
        if tag == "four" and model == "glm-5.1":
            return self._json(403, {"error": {"code": "MODEL_ACCESS_DENIED",
                                              "message": "该模型未对该账号开通"}})

        time.sleep(0.9 if tag == "two" else random.uniform(0.25, 0.6))

        pt = len(json.dumps(req.get("messages") or req.get("system") or "")) // 3 + 120
        ct = random.randint(90, 420)
        cached = pt // 3 if random.random() < 0.5 else 0
        price = next((x for x in MODELS if x[0] == model), None)
        if price:
            _, pi, po, pcr = price
            cost = round((pt - cached) / 1e6 * pi + cached / 1e6 * pcr + ct / 1e6 * po, 8)
        else:
            cost = 0.00012

        if req.get("stream"):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            # 必须 close：没有 Content-Length 的 1.1 响应靠"连接关闭"表示结束，
            # 若声明 keep-alive，客户端会一直等更多字节（表现为请求挂住）。
            self.send_header("Connection", "close")
            self.close_connection = True
            self.end_headers()
            for i, piece in enumerate(["这是", "一段", "流式", "输出", "。"]):
                frame = {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "model": model,
                         "choices": [{"index": 0, "delta": {"content": piece}, "finish_reason": None}]}
                self.wfile.write(b"data: " + json.dumps(frame, ensure_ascii=False).encode() + b"\n\n")
                self.wfile.flush()
                time.sleep(0.12)
            tail = {"id": "chatcmpl-mock", "object": "chat.completion.chunk", "model": model,
                    "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                    "usage": {"prompt_tokens": pt, "completion_tokens": ct,
                              "total_tokens": pt + ct,
                              "prompt_tokens_details": {"cached_tokens": cached}},
                    "cost_cny": cost}
            self.wfile.write(b"data: " + json.dumps(tail, ensure_ascii=False).encode() + b"\n\n")
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
            return

        return self._json(200, {
            "id": "chatcmpl-mock", "object": "chat.completion", "model": model,
            "choices": [{"index": 0, "finish_reason": "stop",
                         "message": {"role": "assistant",
                                     "content": "（mock）收到 %d 条消息。" % len(req.get("messages") or [])}}],
            "usage": {"prompt_tokens": pt, "completion_tokens": ct, "total_tokens": pt + ct,
                      "prompt_tokens_details": {"cached_tokens": cached}},
            "cost_cny": cost,
        })


if __name__ == "__main__":
    srv = ThreadingHTTPServer(("127.0.0.1", 18999), H)
    print("mock upstream on 127.0.0.1:18999", flush=True)
    srv.serve_forever()
