import json
import sys
import threading
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:18787"
KEY = "sk-trpool-demo"
MODELS = ["glm-5.1", "kimi-k2.6", "qwen3.8-flash", "qwen3.8-max", "deepseek-v3.2"]


def post(path, payload):
    body = json.dumps(payload, ensure_ascii=False).encode()
    req = urllib.request.Request(BASE + path, data=body, method="POST")
    req.add_header("Authorization", "Bearer " + KEY)
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            # 必须把 body 读完：流式响应不读就等于客户端中途断开，
            # 网关会记一条"客户端断开"并且拿不到 usage（截图上会全是 0 token）。
            r.read()
            return r.status, r.headers.get("X-Trpool-Attempt"), r.headers.get("X-Trpool-Key")
    except urllib.error.HTTPError as e:
        e.read()
        return e.code, e.headers.get("X-Trpool-Attempt"), e.headers.get("X-Trpool-Key")
    except Exception as e:  # noqa: BLE001
        return "ERR:%s" % e, None, None


results = []
lock = threading.Lock()


def worker(i):
    model = MODELS[i % len(MODELS)]
    payload = {"model": model,
               "messages": [{"role": "system", "content": "你是一个简洁的助手，回答控制在两句以内。"},
                            {"role": "user", "content": "第 %d 个问题：解释一下号池调度里的会话粘性。" % i}]}
    if i % 7 == 3:
        payload["stream"] = True
    if i % 4 == 0:
        payload["user"] = "conv-%d" % (i % 3)
    r = post("/v1/chat/completions", payload)
    with lock:
        results.append(r)
        print("%2d %-14s %s key=%s att=%s" % (i, model, r[0], r[2], r[1]), flush=True)


threads = [threading.Thread(target=worker, args=(i,)) for i in range(24)]
for t in threads[:4]:
    t.start()
for t in threads[4:]:
    t.start()
    t.join()
for t in threads[:4]:
    t.join()

st, att, key = post("/v1/videos/generations", {"model": "qwen3.8-max", "prompt": "海边日落", "duration": 5})
print("video create:", st, "key=", key, flush=True)

codes = {}
for r in results:
    codes[r[0]] = codes.get(r[0], 0) + 1
print("汇总:", codes, flush=True)
sys.stdout.flush()
