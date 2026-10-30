package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"trpool/internal/config"
	"trpool/internal/ledger"
	"trpool/internal/pool"
	"trpool/internal/upstream"
)

const clientKey = "sk-trpool-test"

type fixture struct {
	h      *Handler
	p      *pool.Pool
	lg     *ledger.Log
	prices *ledger.Prices
	up     *httptest.Server
	gw     *httptest.Server
}

func newFixture(t *testing.T, upstreamFn http.HandlerFunc, secrets ...string) *fixture {
	t.Helper()
	upSrv := httptest.NewServer(upstreamFn)

	cfg := config.Default()
	cfg.Upstream = upSrv.URL
	cfg.AdminKey = "adm"
	cfg.ClientKeys = []string{clientKey}
	cfg.Pool.RetryMax = 3
	cfg.Pool.MaxInFlight = 4
	cfg.Pool.RateCooldown = config.Duration(20 * time.Second)
	cfg.Pool.Strategy = "sequential" // 测试要可预测的选号顺序

	p := pool.New(cfg.Pool)
	for i, s := range secrets {
		if _, err := p.Add(string(rune('A'+i)), s, 1, 0); err != nil {
			t.Fatalf("add key: %v", err)
		}
	}
	prices := ledger.NewPrices()
	_ = prices.Load([]byte(`{"data":[{"id":"m","effective_pricing":{"currency":"CNY","prompt":"1.0","completion":"2.0"}}]}`))
	lg := ledger.NewLog(50)
	daily := ledger.NewDaily()
	up := upstream.New(cfg.Upstream, 10*time.Second, 30*time.Second)
	h := New(cfg, p, up, prices, lg, daily)

	gw := httptest.NewServer(h)
	t.Cleanup(func() { gw.Close(); upSrv.Close() })
	return &fixture{h: h, p: p, lg: lg, prices: prices, up: upSrv, gw: gw}
}

func (f *fixture) do(t *testing.T, method, path, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, f.gw.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+clientKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestRejectsMissingClientKey(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}, "sk_tr_x")
	resp, err := http.Get(f.gw.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无密钥应为 401，实际 %d", resp.StatusCode)
	}
}

// 核心场景：第一个号 429，网关要自动换第二个号并把结果交给客户端。
func TestRetryOnRateLimitSwitchesKey(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if strings.Contains(auth, "bad") {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(429)
			_, _ = w.Write([]byte(`{"code":"RATE_LIMITED","message":"容量限流","traceId":"t-bad"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":10,"completion_tokens":5},"cost_cny":"0.00002"}`))
	}, "sk_tr_bad", "sk_tr_good")

	resp := f.do(t, "POST", "/v1/chat/completions", `{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	if resp.StatusCode != 200 {
		t.Fatalf("应换号后成功，实际 %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), `"ok"`) {
		t.Fatalf("响应体不是成功的那个: %s", body)
	}
	// 坏号必须进冷却，好号必须记上一笔账。
	var cooled, ok bool
	for _, v := range f.p.Views() {
		if v.Name == "A" && v.State == "cooling" {
			cooled = true
		}
		if v.Name == "B" && v.Requests == 1 {
			ok = true
		}
	}
	if !cooled {
		t.Error("429 的号应进入 cooling")
	}
	if !ok {
		t.Error("成功的号应记录 1 次请求")
	}
}

// 401 必须把号打成 invalid，且换号继续。
func TestRetryOnUnauthorized(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.Header.Get("Authorization"), "dead") {
			w.WriteHeader(401)
			_, _ = w.Write([]byte(`{"code":"UNAUTHORIZED","message":"未认证"}`))
			return
		}
		_, _ = w.Write([]byte(`{"ok":true,"usage":{"prompt_tokens":1,"completion_tokens":1}}`))
	}, "sk_tr_dead", "sk_tr_alive")

	resp := f.do(t, "POST", "/v1/chat/completions", `{"model":"m","messages":[]}`)
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("应换号成功，实际 %d", resp.StatusCode)
	}
	for _, v := range f.p.Views() {
		if v.Name == "A" && v.State != "invalid" {
			t.Errorf("401 的号状态 = %s，期望 invalid", v.State)
		}
	}
}

// 403（模型未获灰度）不该被当成号的故障，也不该触发换号。
func TestForbiddenDoesNotCoolPool(t *testing.T) {
	calls := 0
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		w.WriteHeader(403)
		_, _ = w.Write([]byte(`{"code":"MODEL_ACCESS_DENIED","message":"当前模型仅对灰度用户开放"}`))
	}, "sk_tr_a", "sk_tr_b", "sk_tr_c")

	resp := f.do(t, "POST", "/v1/chat/completions", `{"model":"m","messages":[]}`)
	defer resp.Body.Close()
	if resp.StatusCode != 403 {
		t.Fatalf("应原样返回 403，实际 %d", resp.StatusCode)
	}
	if calls != 1 {
		t.Errorf("403 不该重试换号，上游被调用 %d 次", calls)
	}
	for _, v := range f.p.Views() {
		if v.State != "active" {
			t.Errorf("号 %s 被 403 打成了 %s", v.Name, v.State)
		}
	}
}

func TestStreamPassThroughAndUsage(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		// 断言网关注入了 include_usage
		var req map[string]any
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &req)
		if so, ok := req["stream_options"].(map[string]any); !ok || so["include_usage"] != true {
			t.Errorf("网关未注入 stream_options.include_usage: %s", raw)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl := w.(http.Flusher)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"你\"}}]}\n\n"))
		fl.Flush()
		_, _ = w.Write([]byte("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3},\"cost_cny\":\"0.00001\"}\n\n"))
		fl.Flush()
		_, _ = w.Write([]byte("data: [DONE]\n\n"))
	}, "sk_tr_a")

	resp := f.do(t, "POST", "/v1/chat/completions", `{"model":"m","stream":true,"messages":[]}`)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "[DONE]") {
		t.Fatalf("SSE 未完整透传: %s", body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Errorf("Content-Type = %q，期望 SSE", ct)
	}

	views := f.p.Views()
	if len(views) != 1 {
		t.Fatal("号数量异常")
	}
	if views[0].PromptTokens != 7 || views[0].CompletionTokens != 3 {
		t.Errorf("流式 usage 未记账: %+v", views[0])
	}
	if views[0].Spent <= 0 {
		t.Error("流式成本未记账")
	}
}

// 视频任务创建后，查询必须回到同一个号。
// 上游只认创建者：查询时 Authorization 与创建时不一致就 404，用它反向验证粘性。
func TestVideoStickyRoutesToCreator(t *testing.T) {
	var mu sync.Mutex
	owner := map[string]string{}

	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if r.Method == http.MethodPost {
			id := fmt.Sprintf("task-%d", time.Now().UnixNano())
			mu.Lock()
			owner[id] = auth
			mu.Unlock()
			_, _ = w.Write([]byte(`{"id":"` + id + `","status":"queued"}`))
			return
		}
		id := strings.TrimPrefix(r.URL.Path, "/v1/videos/")
		mu.Lock()
		want := owner[id]
		mu.Unlock()
		if want != "" && want == auth {
			_, _ = w.Write([]byte(`{"id":"` + id + `","status":"succeeded"}`))
			return
		}
		w.WriteHeader(404)
		_, _ = w.Write([]byte(`{"code":"NOT_FOUND","message":"任务不存在"}`))
	}, "sk_tr_a", "sk_tr_b")

	// 轮询让创建请求交替落在两个号上，这样"粘性是否生效"才可观测；
	// 若每次都选同一个号，粘性失效也会碰巧通过。
	poolCfg := f.p.Config()
	poolCfg.Strategy = "round_robin"
	f.p.SetConfig(poolCfg)

	for i := 0; i < 6; i++ {
		resp := f.do(t, "POST", "/v1/videos/generations", `{"model":"m","content":[{"type":"text","text":"x"}]}`)
		raw, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("创建任务失败 %d: %s", resp.StatusCode, raw)
		}
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(raw, &created); err != nil || created.ID == "" {
			t.Fatalf("创建响应缺少 id: %s", raw)
		}

		q := f.do(t, "GET", "/v1/videos/"+created.ID, "")
		body, _ := io.ReadAll(q.Body)
		q.Body.Close()
		if q.StatusCode != 200 {
			t.Fatalf("第 %d 次查询未粘回创建者，%d: %s", i+1, q.StatusCode, body)
		}
	}
}

func TestUpstreamAllFailReturnsLastError(t *testing.T) {
	f := newFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte(`{"code":"UPSTREAM","message":"上游炸了"}`))
	}, "sk_tr_a", "sk_tr_b")

	// 首轮每个号只失败一次，还没到熔断阈值（3），所以状态码必须是上游的 500，
	// 不能被网关自己的 503 顶掉。
	resp := f.do(t, "POST", "/v1/chat/completions", `{"model":"m","messages":[]}`)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 500 {
		t.Fatalf("应透传上游 500，实际 %d", resp.StatusCode)
	}

	// 再打两轮，累计 3 次失败后全部熔断。
	for i := 0; i < 2; i++ {
		r := f.do(t, "POST", "/v1/chat/completions", `{"model":"m","messages":[]}`)
		_, _ = io.Copy(io.Discard, r.Body)
		r.Body.Close()
	}
	for _, v := range f.p.Views() {
		if v.State == "active" {
			t.Errorf("连续 500 后号 %s 仍在 active（fail_streak=%d）", v.Name, v.FailStreak)
		}
	}
}
