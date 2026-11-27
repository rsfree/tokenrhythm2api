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
	"trpool/internal/redact"
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
	hdr    http.Header
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
	f.log = append(f.log, hit{secret: secret, path: r.URL.Path, body: string(body), hdr: r.Header.Clone()})
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
	// 公开端点不带令牌时只回活着，池子规模/欠费数/版本号都不给。
	for _, leak := range []string{"exhausted", "blocked", "healthy", "version", "uptime"} {
		if strings.Contains(body, `"`+leak+`"`) {
			t.Fatalf("无令牌的 healthz 不该暴露 %s：%s", leak, body)
		}
	}
	// 带管理令牌才给全量，运维脚本照旧能用。
	resp, body = h.do(t, http.MethodGet, "/healthz", "", map[string]string{"Authorization": "Bearer admin-test"})
	if resp.StatusCode != 200 || !strings.Contains(body, `"healthy"`) || !strings.Contains(body, `"version"`) {
		t.Fatalf("带 admin 令牌应回全量: %d %s", resp.StatusCode, body)
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
	// 渠道病（5xx）先在同一个号上重试一次，再换号：瞬时抖动换号救不了，
	// 反而把新号也拖进熔断统计。
	if got := h.fk.count(); got != 3 {
		t.Fatalf("应为 A 失败 → A 重试 → B 成功，实际 %d 次", got)
	}
	if st := h.srv.Stats().Snapshot(); st["retries"] != 2 || st["switched"] != 1 {
		t.Fatalf("重试统计不对: %+v", st)
	}
	hits := h.fk.hits()
	if hits[0].secret != keyA || hits[1].secret != keyA || hits[2].secret != keyB {
		t.Fatalf("应为 A → A → B，实际 %+v", hits)
	}
	if resp.Header.Get("X-Trpool-Attempt") != "" {
		t.Fatalf("不应再回 X-Trpool-Attempt（渠道信息不外泄），实际 %q", resp.Header.Get("X-Trpool-Attempt"))
	}
	// 第几次尝试改由日志承载：面板上还能看到，客户端不再看得到。
	logs := h.log.Recent(1)
	if len(logs) != 1 || logs[0].Attempt != 3 {
		t.Fatalf("日志里应记第 3 次尝试，实际 %+v", logs)
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
		t.Fatalf("请求病应回 400，实际 %d", resp.StatusCode)
	}
	if !strings.Contains(body, `"code":"invalid_request_error"`) {
		t.Fatalf("对外 code 应为 invalid_request_error，实际 %s", body)
	}
	// 上游原话不再出现在响应里：文案由我们的模板生成。
	if strings.Contains(body, "bad request") {
		t.Fatalf("不该透传上游原文，实际 %s", body)
	}
	// 请求病换谁都一样：30 个号的池子就是 30 次无效调用 30 次计费。
	if got := h.fk.count(); got != 1 {
		t.Fatalf("请求病不该换号重试，实际打了 %d 次", got)
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

// 全池 5xx：对外统一 api_error(500)。不保留上游的 502——502 在我们的契约里
// 只表示"压根没拿到上游响应"，拿它当"上游 502"用会让调用方分不清两种情况。
func TestAllKeysFailReturnsApiError(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 2 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(502)
		_, _ = io.WriteString(w, `{"error":{"message":"upstream exploded"}}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("上游 5xx 应归一为 api_error(500)，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"api_error"`) {
		t.Fatalf("对外 code 应为 api_error，实际 %s", body)
	}
	if strings.Contains(body, "upstream exploded") {
		t.Fatalf("不该透传上游原文，实际 %s", body)
	}
	if c := h.fk.count(); c != 2 {
		t.Fatalf("两次尝试后应放弃（A 重试一次后到上限），实际 %d 次", c)
	}
	if h.srv.Stats().Snapshot()["failed"] != 1 {
		t.Fatal("失败计数未增加")
	}
	// 运维视图要留住上游原文。
	var raw string
	for _, e := range h.log.Recent(10) {
		if strings.Contains(e.RawMessage, "upstream exploded") {
			raw = e.RawMessage
		}
	}
	if raw == "" {
		t.Fatalf("日志丢了上游原文，实际 %+v", h.log.Recent(10))
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

func TestAllKeysDeadReturnsQuota(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 2 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(402)
		_, _ = io.WriteString(w, `{"code":"INSUFFICIENT_BALANCE","message":"账户余额不足"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	// 对外契约按 OpenAI 归一：欠费 = 429 + insufficient_quota（不是上游的 402，
	// 也不是自造的 NO_CAPACITY）。code 明确告诉调用方"重试无效"。
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("全池欠费应对外 429 insufficient_quota，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"insufficient_quota"`) {
		t.Fatalf("对外 code 应为 insufficient_quota，实际 %s", body)
	}
	// 终态不给 Retry-After：给了时间等于说"等一会儿能恢复"。
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		t.Fatalf("insufficient_quota 不该带 Retry-After，实际 %q", ra)
	}
	// 池子规模、欠费/冻结分布、上游业务码都是内部信息，一个字都不该出现。
	for _, leak := range []string{"欠费", "冻结", "池内共", "号池", "余额", "INSUFFICIENT_BALANCE", "账户"} {
		if strings.Contains(body, leak) {
			t.Fatalf("错误体泄露了渠道内部信息（%q），实际 %s", leak, body)
		}
	}
	// 但面板/归档要能查到这一轮到底撞了什么（运维视图与对外视图是两套）。
	var raw string
	for _, e := range h.log.Recent(10) {
		if e.Canonical == "insufficient_quota" && e.RawCode == "INSUFFICIENT_BALANCE" {
			raw = e.RawCode
		}
	}
	if raw == "" {
		t.Fatalf("日志应保留上游原文与命中规则，实际 %+v", h.log.Recent(10))
	}
	// 不再回 X-Trpool-* 头：号名/尝试次数/上游 trace 都是渠道内部信息。
	for _, hk := range []string{"X-Trpool-Reason", "X-Trpool-Error", "X-Trpool-Key",
		"X-Trpool-Attempt", "X-Trpool-Trace"} {
		if v := resp.Header.Get(hk); v != "" {
			t.Fatalf("不该再返回 %s，实际 %q", hk, v)
		}
	}
	if c := h.fk.count(); c != 2 {
		t.Fatalf("两个号各撞一次，实际 %d 次", c)
	}
	for _, v := range h.pool.Views() {
		if v.State != "exhausted" || v.CooldownLeft != 0 {
			t.Fatalf("402 后号应停用且无冷却，实际 %s 冷却 %ds", v.State, v.CooldownLeft)
		}
	}
}

// 池里还有活号、只是这一轮抽到的都是死号：仍然不能透传 402（那是上游号的账），
// 也不能对外描述池子状态——"还有 1 个可用"同样是在告诉客户端背后有几个号。
func TestDeadRoundWithLiveKeysAsksRetry(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 1 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(403)
		_, _ = io.WriteString(w, `{"code":"ACCOUNT_SUSPENDED","message":"账号已冻结"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	// 账号被冻结属于"上游凭据不可用"：对外 401 authentication_error，
	// 内部仍按终态 blocked 处理（两套视图分开）。
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("全池冻结应回 401 authentication_error，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"authentication_error"`) {
		t.Fatalf("对外 code 应为 authentication_error，实际 %s", body)
	}
	for _, leak := range []string{"可用 1", "池内", "冻结", "本轮", "ACCOUNT_SUSPENDED"} {
		if strings.Contains(body, leak) {
			t.Fatalf("不该对外描述池子状态（%q），实际 %s", leak, body)
		}
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
	if len(h.log.Recent(10)) != 3 {
		t.Fatalf("日志应保留三次尝试（A 失败 → A 重试 → B 成功），实际 %d", len(h.log.Recent(10)))
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

// 上游的响应头一个都不许漏给客户端。
//
// 老实现是黑名单透传（只挡 hop-by-hop），实测 /v1/models 会把上游的
// Content-Security-Policy（里面列着它的前端域名、阿里云 OSS、飞书、验证码源）、
// X-Tr-Frontend-Commit（上游构建 commit）、X-Trace-Id、Server 一起送给调用方。
// 这里把那些头全塞进假上游，确认客户端只看到白名单里的几个。
func TestUpstreamHeadersDoNotLeak(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Security-Policy", "default-src 'self' https://tokenrhythm.studio https://static.tokenrhythm.studio")
		w.Header().Set("X-Tr-Frontend-Commit", "01e8b136f77666a17e325ae8a2979f60109d984e")
		w.Header().Set("X-Tr-Frontend-Cohort", "base")
		w.Header().Set("X-Trace-Id", "trace_deadbeef")
		w.Header().Set("Server", "Tengine")
		w.Header().Set("X-Powered-By", "Next.js")
		w.Header().Set("Strict-Transport-Security", "max-age=31536000")
		w.Header().Set("Permissions-Policy", "camera=()")
		w.Header().Set("X-RateLimit-Remaining", "999")
		w.Header().Set("Set-Cookie", "upstream_session=abc; Path=/")
		_, _ = io.WriteString(w, `{"id":"ok"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("应 200，实际 %d %s", resp.StatusCode, body)
	}
	for _, k := range []string{"Content-Security-Policy", "X-Tr-Frontend-Commit",
		"X-Tr-Frontend-Cohort", "X-Trace-Id", "Server", "X-Powered-By",
		"Strict-Transport-Security", "Permissions-Policy", "X-RateLimit-Remaining",
		"Set-Cookie"} {
		if v := resp.Header.Get(k); v != "" {
			t.Fatalf("上游头 %s 不该透传给客户端，实际 %q", k, v)
		}
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type 必须保留，实际 %q", ct)
	}
	if strings.TrimSpace(body) != `{"id":"ok"}` {
		t.Fatalf("响应体应原样透传，实际 %q", body)
	}
}

// Retry-After 是唯一允许透传的"上游语义头"：429/503 的退避行为依赖它。
func TestRetryAfterPassesThrough(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 1 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(429)
		_, _ = io.WriteString(w, `{"error":{"message":"rate limited"}}`)
	}
	resp, _ := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if got := resp.Header.Get("Retry-After"); got != "30" {
		t.Fatalf("Retry-After 应透传，实际 %q", got)
	}
}

// 流式也不能漏：SSE 只要 Content-Type，别把上游的 CSP / commit 带出去。
func TestStreamHeadersDoNotLeak(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Security-Policy", "default-src 'self' https://tokenrhythm.studio")
		w.Header().Set("X-Tr-Frontend-Commit", "deadbeef")
		w.Header().Set("X-Request-Id", "upstream-req-1")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}
	resp, _ := h.do(t, http.MethodPost, "/v1/chat/completions",
		`{"model":"m","stream":true,"messages":[{"role":"user","content":"hi"}]}`, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("流式应 200，实际 %d", resp.StatusCode)
	}
	for _, k := range []string{"Content-Security-Policy", "X-Tr-Frontend-Commit"} {
		if v := resp.Header.Get(k); v != "" {
			t.Fatalf("流式响应里不该出现上游头 %s，实际 %q", k, v)
		}
	}
	// X-Request-Id 是网关自己生成的那个，不能是上游回显的。
	if got := resp.Header.Get("X-Request-Id"); got == "upstream-req-1" {
		t.Fatal("X-Request-Id 不该取上游的值")
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("SSE 的 Content-Type 必须保留，实际 %q", ct)
	}
}

// ---------- 响应体脱敏 ----------

// 上游在自己的成功响应里塞了 cost_cny（这笔请求的进货价）和 trace_id（渠道链路 ID）。
// 客户端一个都不该看到，但本地记账必须用得上——脱敏只能发生在解析之后。
func TestSuccessBodyRedacted(t *testing.T) {
	h := newHarness(t, nil)
	h.srv.scrub = redact.New("https://tokenrhythm.studio")
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"chatcmpl-1","model":"glm-5.1","choices":[{"message":{"content":"hi"}}],`+
			`"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8},`+
			`"cost_cny":"0.00001234","billing_pending":false,"trace_id":"trace_leak_me"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("应 200，实际 %d %s", resp.StatusCode, body)
	}
	for _, leak := range []string{"cost_cny", "billing_pending", "trace_id", "trace_leak_me"} {
		if strings.Contains(body, leak) {
			t.Fatalf("响应体泄露了 %s：%s", leak, body)
		}
	}
	if !strings.Contains(body, `"total_tokens":8`) || !strings.Contains(body, "hi") {
		t.Fatalf("业务字段不能被误删：%s", body)
	}
	// 记账走的是上游 cost_cny（Direct）。如果脱敏在解析之前，这里会是 0。
	e := h.log.Recent(1)[0]
	if e.Cost < 0.0000123 || e.Cost > 0.0000124 {
		t.Fatalf("上游 cost_cny 应被记账（0.00001234），实际 %v", e.Cost)
	}
}

// 流式：末帧的 cost_cny / trace_id 也要抹掉，但 usage 与 [DONE] 必须原样。
func TestStreamBodyRedacted(t *testing.T) {
	h := newHarness(t, nil)
	h.srv.scrub = redact.New("https://tokenrhythm.studio")
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"choices\":[],\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":1,\"total_tokens\":3},\"cost_cny\":\"0.00000120\",\"trace_id\":\"trace_leak_sse\"}\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}
	reqBody := `{"model":"glm-5.1","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", reqBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("应 200，实际 %d %s", resp.StatusCode, body)
	}
	for _, leak := range []string{"cost_cny", "trace_id", "trace_leak_sse"} {
		if strings.Contains(body, leak) {
			t.Fatalf("SSE 泄露了 %s：%s", leak, body)
		}
	}
	if !strings.Contains(body, "[DONE]") || !strings.Contains(body, `"total_tokens":3`) {
		t.Fatalf("usage / [DONE] 不能被误伤：%s", body)
	}
	// 流式的 usage 同样要在脱敏前读到，否则 token 记账全是 0。
	e := h.log.Recent(1)[0]
	if e.PromptTokens != 2 || e.CompletionTokens != 1 {
		t.Fatalf("流式 usage 应记为 2/1，实际 %d/%d", e.PromptTokens, e.CompletionTokens)
	}
}

// 失败时连上游原话都不透传：4xx 里的域名、文档链接、traceId 一个都不许出去，
// 对外只有模板文案 + canonical code。
func TestErrorBodyRedacted(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 1 })
	h.srv.scrub = redact.New("https://tokenrhythm.studio")
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(400)
		_, _ = io.WriteString(w, `{"code":"MODEL_NOT_AVAILABLE","message":"模型不可用，详见 https://tokenrhythm.studio/docs/models","traceId":"trace_9e06b3c0"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	// 上游 400 + MODEL_NOT_AVAILABLE 语义是"模型不存在/未开通"→ 对外 404。
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("模型不存在应对外 404，实际 %d %s", resp.StatusCode, body)
	}
	for _, leak := range []string{"tokenrhythm", "traceId", "trace_9e06b3c0",
		"MODEL_NOT_AVAILABLE", "docs/models", "详见"} {
		if strings.Contains(body, leak) {
			t.Fatalf("错误体泄露了 %s：%s", leak, body)
		}
	}
	// 但语义要留住，否则客户端没法判断是"模型不存在"还是"网关挂了"。
	if !strings.Contains(body, `"code":"model_not_found"`) || !strings.Contains(body, "模型") {
		t.Fatalf("错误语义不能被一起抹掉：%s", body)
	}
}

// 网络错误：Go 的 *url.Error 形如 `Get "https://<上游>/v1/...": dial tcp ...`，
// 原样回给客户端等于把上游域名与端点路径送出去。
func TestNetworkErrorMessageHidesUpstream(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 1 })
	h.up.Close() // 端口关掉，制造连接失败
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("连接失败应 502，实际 %d %s", resp.StatusCode, body)
	}
	for _, leak := range []string{"http://", "https://", "127.0.0.1", "dial", "connection refused"} {
		if strings.Contains(body, leak) {
			t.Fatalf("网络错误体泄露了 %s：%s", leak, body)
		}
	}
	// 压根没拿到上游响应 → unknown_upstream_error（502），不是 5xx 渠道病。
	if !strings.Contains(body, `"code":"unknown_upstream_error"`) {
		t.Fatalf("应保留 canonical 错误码：%s", body)
	}
	// 细节不能丢，日志里要看得到真实原因（对外视图给结论，运维视图给原文）。
	var raw string
	for _, e := range h.log.Recent(5) {
		if strings.Contains(e.RawMessage, "连接上游失败") {
			raw = e.RawMessage
		}
	}
	if raw == "" {
		t.Fatalf("日志应保留原始网络错误，实际 %+v", h.log.Recent(5))
	}
}

// 出站方向的两个"自报身份"漏洞：UA 写死成 trpool/2 + 仓库地址，
// 以及把客户端的 X-Forwarded-For / X-Real-IP / Origin / Cookie 原样转给上游。
// 前者是"同一出口 IP 一池号同指纹"，后者是把终端用户 IP 与网关域名交上去。
func TestOutboundDoesNotLeakIdentity(t *testing.T) {
	h := newHarness(t, nil)
	ua := "OpenAI/Python 1.99.0"
	hdr := map[string]string{
		"User-Agent":        ua,
		"X-Forwarded-For":   "203.0.113.7, 10.0.0.1",
		"X-Real-IP":         "203.0.113.7",
		"Forwarded":         "for=203.0.113.7",
		"CF-Connecting-IP":  "203.0.113.7",
		"Via":               "1.1 tengine",
		"Origin":            "https://trpool.example.com",
		"Referer":           "https://trpool.example.com/panel/",
		"Cookie":            "trpool_admin=leak",
		"Anthropic-Version": "2023-06-01",
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, hdr)
	if resp.StatusCode != 200 {
		t.Fatalf("正常请求应 200，实际 %d %s", resp.StatusCode, body)
	}
	if h.fk.count() != 1 {
		t.Fatalf("应只有一次上游请求，实际 %d", h.fk.count())
	}
	got := h.fk.hits()[0].hdr

	if v := got.Get("User-Agent"); v != ua {
		t.Errorf("UA = %q，应透传客户端 UA", v)
	}
	for _, k := range []string{"X-Forwarded-For", "X-Real-Ip", "Forwarded", "Cf-Connecting-Ip",
		"Via", "Origin", "Referer", "Cookie"} {
		if v := got.Values(k); len(v) > 0 {
			t.Errorf("不该转发的头漏了 %s = %v", k, v)
		}
	}
	// 协议头要留着，否则上游会按"异常客户端"处理。
	if v := got.Get("Anthropic-Version"); v != "2023-06-01" {
		t.Errorf("Anthropic-Version = %q，应照常转发", v)
	}
	if v := got.Get("Authorization"); v != "Bearer "+keyA && v != "Bearer "+keyB {
		t.Errorf("Authorization = %q，应换成池子里的号", v)
	}
}
