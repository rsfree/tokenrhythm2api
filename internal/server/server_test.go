package server

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
	"trpool/internal/fault"
	"trpool/internal/livecfg"
	"trpool/internal/pool"
	"trpool/internal/reqlog"
	"trpool/internal/session"
	"trpool/internal/upstream"
	"trpool/internal/usage"
)

const (
	keyA     = "sk_tr_aaaa_0000000000"
	keyB     = "sk_tr_bbbb_0000000000"
	clientKy = "sk-trpool-test"
)

type hit struct {
	secret string
	path   string
	body   string
}

type fake struct {
	mu  sync.Mutex
	log []hit
	// behave 根据密钥与请求决定响应；requestNo 是该密钥被调用的第几次（1 基）。
	behave func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte)
	seen   map[string]int
}

func (f *fake) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	secret := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	f.mu.Lock()
	if f.seen == nil {
		f.seen = map[string]int{}
	}
	f.seen[secret]++
	n := f.seen[secret]
	f.log = append(f.log, hit{secret: secret, path: r.URL.Path, body: string(body)})
	f.mu.Unlock()

	if f.behave == nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"ok","choices":[{"message":{"content":"hi"}}]}`)
		return
	}
	f.behave(w, r, secret, n, body)
}

func (f *fake) hits() []hit {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]hit(nil), f.log...)
}

func (f *fake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.log)
}

type harness struct {
	srv   *Server
	pool  *pool.Pool
	log   *reqlog.Recorder
	daily *usage.Daily
	gw    *httptest.Server
	up    *httptest.Server
	fk    *fake
}

func newHarness(t *testing.T, tune func(*config.PoolConfig)) *harness {
	t.Helper()
	fk := &fake{}
	upSrv := httptest.NewServer(fk)

	cfg := config.Default()
	cfg.Upstream = upSrv.URL
	cfg.ClientKeys = []string{clientKy}
	cfg.AdminKey = "admin-test"
	cfg.Logging.RingSize = 100
	cfg.Pool.Strategy = "round_robin"
	cfg.Pool.MinPickGap = 0
	cfg.Pool.MaxInFlight = 0
	cfg.Pool.CreditFloor = 0
	cfg.Pool.RetryMax = 3
	cfg.Pool.UpstreamTimeout = config.Duration(5 * time.Second)
	cfg.Pool.Batch.Mode = "off"
	cfg.Pool.FallbackProbe = false
	cfg.Pool.RateCooldown = config.Duration(10 * time.Second)
	cfg.Pool.RateMax = config.Duration(time.Minute)
	cfg.Pool.ExhaustedCooldown = config.Duration(time.Minute)
	cfg.Pool.ModelBlockBase = config.Duration(time.Hour)
	if tune != nil {
		tune(&cfg.Pool)
	}

	live := livecfg.New(livecfg.Snapshot{AdminKey: cfg.AdminKey, ClientKeys: cfg.ClientKeys})
	sess := session.New(cfg.Pool.SessionSticky.TTL.Duration(), 100)
	p := pool.New(cfg.Pool, sess)
	for _, s := range []string{keyA, keyB} {
		if _, err := p.Add(strings.TrimPrefix(s, "sk_tr_")[:4], s, 1, 0); err != nil {
			t.Fatal(err)
		}
	}
	prices := usage.NewPrices()
	daily := usage.NewDaily()
	logs := reqlog.New(reqlog.Config{RingSize: 100})
	up := upstream.New(upSrv.URL)
	srv := New(Deps{Cfg: &cfg, Live: live, Pool: p, Up: up, Prices: prices, Log: logs, Daily: daily, Version: "test"})
	gw := httptest.NewServer(srv)

	t.Cleanup(func() {
		gw.Close()
		upSrv.Close()
	})
	return &harness{srv: srv, pool: p, log: logs, daily: daily, gw: gw, up: upSrv, fk: fk}
}

func (h *harness) do(t *testing.T, method, path, body string, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, h.gw.URL+path, rdr)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+clientKy)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

const chatBody = `{"model":"glm-5.1","messages":[{"role":"user","content":"hi"}],"stream":false}`

func TestUnauthorized(t *testing.T) {
	h := newHarness(t, nil)
	resp, err := http.Get(h.gw.URL + "/v1/models")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("无密钥应 401，实际 %d", resp.StatusCode)
	}
	if h.fk.count() != 0 {
		t.Fatal("未鉴权不该打上游")
	}
}

func TestHealthz(t *testing.T) {
	h := newHarness(t, nil)
	resp, body := h.do(t, http.MethodGet, "/healthz", "", nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `"service":"trpool"`) {
		t.Fatalf("healthz 异常: %d %s", resp.StatusCode, body)
	}
}

func TestRetryOn5xxSwitchesKey(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		if secret == keyA {
			w.WriteHeader(500)
			_, _ = io.WriteString(w, `{"error":{"message":"boom"}}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"ok","usage":{"prompt_tokens":3,"completion_tokens":4}}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("应换号成功，实际 %d %s", resp.StatusCode, body)
	}
	if got := h.fk.count(); got != 2 {
		t.Fatalf("应打两次上游（A 失败 + B 成功），实际 %d", got)
	}
	if st := h.srv.Stats().Snapshot(); st["retries"] != 1 || st["switched"] != 1 {
		t.Fatalf("重试统计不对: %+v", st)
	}
	hits := h.fk.hits()
	if hits[0].secret != keyA || hits[1].secret != keyB {
		t.Fatalf("应先在 A 失败再换 B，实际 %+v", hits)
	}
	if resp.Header.Get("X-Trpool-Attempt") != "2" {
		t.Fatalf("应标注第 2 次尝试，实际 %q", resp.Header.Get("X-Trpool-Attempt"))
	}
}

func TestClientErrorNoRetry(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(400)
		_, _ = io.WriteString(w, `{"error":{"message":"bad request"}}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 400 {
		t.Fatalf("应透传 400，实际 %d", resp.StatusCode)
	}
	if !strings.Contains(body, "bad request") {
		t.Fatalf("应保留上游原文，实际 %s", body)
	}
	if h.fk.count() != 1 {
		t.Fatalf("4xx 不该换号重试，上游被打了 %d 次", h.fk.count())
	}
}

func TestModelDeniedBlocksPairAndSwitchesKey(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		if secret == keyA {
			w.WriteHeader(403)
			_, _ = io.WriteString(w, `{"code":"MODEL_ACCESS_DENIED","message":"model not enabled"}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"ok"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("模型未开通应换号，实际 %d %s", resp.StatusCode, body)
	}
	var blocked []string
	for _, v := range h.pool.Views() {
		if len(v.ModelBlocks) > 0 {
			blocked = append(blocked, v.Name+":"+v.ModelBlocks[0].Model)
		}
		if v.Name == "aaaa" && v.State != "active" {
			t.Fatalf("不应罚整个号，state=%s", v.State)
		}
	}
	if len(blocked) != 1 || !strings.HasSuffix(blocked[0], "glm-5.1") {
		t.Fatalf("应只封 (号, 模型)，实际 %v", blocked)
	}
}

func TestRateLimitedCoolsKey(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		if secret == keyA {
			w.Header().Set("Retry-After", "30")
			w.WriteHeader(429)
			_, _ = io.WriteString(w, `{"code":"RATE_LIMITED","message":"too many"}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"ok"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 200 || !strings.Contains(body, `"ok"`) {
		t.Fatalf("应换号成功，实际 %d %s", resp.StatusCode, body)
	}
	var left int
	for _, v := range h.pool.Views() {
		if v.Name == "aaaa" {
			left = v.CooldownLeft
			if v.State != "cooling" {
				t.Fatalf("429 应让号进 cooling，实际 %s", v.State)
			}
		}
	}
	if left < 25 || left > 31 {
		t.Fatalf("应按 retryAfter=30 冷却，实际 %d", left)
	}
}

func TestAllKeysFailPreservesUpstreamBody(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 2 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(502)
		_, _ = io.WriteString(w, `{"error":{"message":"upstream exploded"}}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 502 {
		t.Fatalf("应保留上游 502，实际 %d", resp.StatusCode)
	}
	if !strings.Contains(body, "upstream exploded") {
		t.Fatalf("应保留上游原文，实际 %s", body)
	}
	if c := h.fk.count(); c != 2 {
		t.Fatalf("两次尝试后应放弃，实际 %d 次", c)
	}
	if h.srv.Stats().Snapshot()["failed"] != 1 {
		t.Fatal("失败计数未增加")
	}
}

func TestStreamPassthroughAndUsageAccounting(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		if fl != nil {
			fl.Flush()
		}
		_, _ = io.WriteString(w, "data: {\"id\":\"1\",\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":22},\"cost_cny\":\"0.0012\"}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}
	body := `{"model":"glm-5.1","messages":[{"role":"user","content":"hi"}],"stream":true}`
	resp, out := h.do(t, http.MethodPost, "/v1/chat/completions", body, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("流式应 200，实际 %d", resp.StatusCode)
	}
	if !strings.Contains(out, `"content":"hi"`) || !strings.Contains(out, "[DONE]") {
		t.Fatalf("SSE 没有被原样透传: %s", out)
	}
	var pt, ct int64
	var spent float64
	for _, v := range h.pool.Views() {
		pt += v.PromptTokens
		ct += v.CompletionTokens
		spent += v.Spent
	}
	if pt != 11 || ct != 22 {
		t.Fatalf("流式 usage 记账不对: pt=%d ct=%d", pt, ct)
	}
	if spent < 0.0011 || spent > 0.0013 {
		t.Fatalf("应以上游 cost_cny 结算，实际 %.6f", spent)
	}
	// 流式请求被注入 include_usage
	if !strings.Contains(h.fk.hits()[0].body, `"include_usage":true`) {
		t.Fatalf("流式请求应注入 include_usage: %s", h.fk.hits()[0].body)
	}
}

func TestNonStreamUsageAndCostFromPriceTable(t *testing.T) {
	h := newHarness(t, nil)
	// 价目表：输入 ¥1/M、输出 ¥2/M
	_ = h.srv.d.Prices.Load([]byte(`{"data":[{"id":"glm-5.1","pricing":{"prompt":"1","completion":"2","currency":"CNY"}}]}`), "test")
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		_, _ = io.WriteString(w, `{"id":"ok","usage":{"prompt_tokens":1000,"completion_tokens":500}}`)
	}
	resp, _ := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("应 200，实际 %d", resp.StatusCode)
	}
	var spent float64
	for _, v := range h.pool.Views() {
		spent += v.Spent
	}
	// 1000/1e6*1 + 500/1e6*2 = 0.002
	if spent < 0.00199 || spent > 0.00201 {
		t.Fatalf("按单价计算成本不对: %.6f", spent)
	}
}

func TestVideoStickyQueryGoesToCreator(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/videos/generations":
			_, _ = io.WriteString(w, `{"id":"vid-1","status":"queued"}`)
		case r.URL.Path == "/v1/videos/vid-1":
			_, _ = io.WriteString(w, `{"id":"vid-1","status":"done","billing":{"total_cost_cny":0.5}}`)
		default:
			w.WriteHeader(404)
		}
	}
	resp, body := h.do(t, http.MethodPost, "/v1/videos/generations", `{"model":"minimax-video-h3"}`, nil)
	if resp.StatusCode != 200 || !strings.Contains(body, "vid-1") {
		t.Fatalf("创建任务失败: %d %s", resp.StatusCode, body)
	}
	// 创建者被冷却后，查询仍必须回它。
	var creator string
	for _, v := range h.pool.Views() {
		if v.StickyTasks > 0 {
			creator = v.ID
			h.pool.MarkFail(v.ID, pool.Outcome{Kind: fault.Rate, RetryAfter: time.Minute, Message: "429"})
		}
	}
	if creator == "" {
		t.Fatal("任务粘性没建立")
	}
	h.do(t, http.MethodGet, "/v1/videos/vid-1", "", nil)
	hits := h.fk.hits()
	if len(hits) < 2 {
		t.Fatalf("查询没打上游: %+v", hits)
	}
	if hits[len(hits)-1].path != "/v1/videos/vid-1" {
		t.Fatalf("查询路径不对: %+v", hits[len(hits)-1])
	}
}

func TestSessionStickySameKey(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.SessionSticky.Enabled = true })
	body := `{"model":"glm-5.1","user":"u-1","messages":[{"role":"user","content":"hi"}]}`
	first := ""
	for i := 0; i < 3; i++ {
		h.do(t, http.MethodPost, "/v1/chat/completions", body, nil)
		hits := h.fk.hits()
		last := hits[len(hits)-1].secret
		if i == 0 {
			first = last
		} else if last != first {
			t.Fatalf("同一会话应粘同一号：%s → %s", first, last)
		}
	}
}

func TestPassthroughDisabledRejectsUpstreamKey(t *testing.T) {
	h := newHarness(t, nil)
	req, _ := http.NewRequest(http.MethodGet, h.gw.URL+"/v1/models", nil)
	req.Header.Set("Authorization", "Bearer "+keyA)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("未开启直连时 sk_tr_ 应 401，实际 %d", resp.StatusCode)
	}
}

func TestPassthroughMode(t *testing.T) {
	h := newHarness(t, nil)
	// 直接改 live 快照开启直连（模拟配置项）。
	h.srv.d.Live.Store(livecfg.Snapshot{AdminKey: "admin-test", ClientKeys: []string{clientKy}, AllowPassthrough: true})
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		_, _ = fmt.Fprintf(w, `{"seen":"%s"}`, secret)
	}
	resp, body := h.do(t, http.MethodGet, "/v1/models", "", map[string]string{"Authorization": "Bearer " + keyB})
	if resp.StatusCode != 200 || !strings.Contains(body, keyB) {
		t.Fatalf("直连应原样使用客户端密钥: %d %s", resp.StatusCode, body)
	}
	if len(h.pool.Views()) != 2 {
		t.Fatal("直连不该改动号池")
	}
}

func TestLogsRecorded(t *testing.T) {
	h := newHarness(t, nil)
	h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	logs := h.log.Recent(10)
	if len(logs) == 0 {
		t.Fatal("没有请求日志")
	}
	e := logs[0]
	if e.KeyName == "" || e.Model != "glm-5.1" || e.Status != 200 || e.RequestID == "" {
		t.Fatalf("日志字段不全: %+v", e)
	}
	if reqs, _, _, _, _ := h.daily.Total(); reqs != 1 {
		t.Fatalf("日账应记 1 次请求，实际 %d", reqs)
	}
}

func TestDailyCountsOnceDespiteRetries(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		if secret == keyA {
			w.WriteHeader(500)
			_, _ = io.WriteString(w, `{"error":"boom"}`)
			return
		}
		_, _ = io.WriteString(w, `{"id":"ok"}`)
	}
	h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if reqs, _, _, _, _ := h.daily.Total(); reqs != 1 {
		t.Fatalf("一次请求换号重试后日账仍应只记 1 次，实际 %d", reqs)
	}
	if len(h.log.Recent(10)) != 2 {
		t.Fatalf("日志应保留两次尝试，实际 %d", len(h.log.Recent(10)))
	}
}

func TestAdminStatusRequiresAdminKey(t *testing.T) {
	h := newHarness(t, nil)
	resp, _ := h.do(t, http.MethodGet, "/status", "", nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("client key 不该能看 /status，实际 %d", resp.StatusCode)
	}
	resp, body := h.do(t, http.MethodGet, "/status", "", map[string]string{"Authorization": "Bearer admin-test"})
	if resp.StatusCode != 200 {
		t.Fatalf("admin key 应可看 /status: %d %s", resp.StatusCode, body)
	}
}

func TestBodyTooLarge(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) {})
	h.srv.d.Cfg.Server.BodyLimitMB = 1
	huge := strings.Repeat("a", (1<<20)+10)
	body, _ := json.Marshal(map[string]any{"model": "m", "messages": []any{huge}})
	resp, _ := h.do(t, http.MethodPost, "/v1/chat/completions", string(body), nil)
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("超限应 413，实际 %d", resp.StatusCode)
	}
}
