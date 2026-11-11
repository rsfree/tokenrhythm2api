package panel

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"trpool/internal/config"
	"trpool/internal/livecfg"
	"trpool/internal/pool"
	"trpool/internal/reqlog"
	"trpool/internal/session"
	"trpool/internal/upstream"
	"trpool/internal/usage"
)

// TestPanelJSSyntax 用 node --check 做一次前端语法闸门。
//
// 前端是 go:embed 进来的，Go 编译完全看不见它：一个重复声明的变量能让整个面板白屏，
// 而后端测试全绿。这类问题只会在浏览器里暴露，所以在这里补一道最便宜的检查。
func TestPanelJSSyntax(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("未安装 node，跳过 JS 语法检查")
	}
	out, err := exec.Command(node, "--check", "web/app.js").CombinedOutput()
	if err != nil {
		t.Fatalf("web/app.js 语法错误: %v\n%s", err, out)
	}
}

type pHarness struct {
	p       *Panel
	pool    *pool.Pool
	live    *livecfg.Holder
	srv     *httptest.Server
	cfg     *config.Config
	cfgPath string
}

func newPanelHarness(t *testing.T) *pHarness {
	t.Helper()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(cfgPath, []byte(`{"listen":":8787","custom":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default()
	live := livecfg.New(livecfg.Snapshot{AdminKey: "adm", ClientKeys: []string{"cli"}})
	pc := cfg.Pool
	p := pool.New(pc, session.New(pc.SessionSticky.TTL.Duration(), 10))
	up := upstream.New("http://127.0.0.1:1")
	logs := reqlog.New(reqlog.Config{RingSize: 50})
	daily := usage.NewDaily()
	prices := usage.NewPrices()
	pan := New(Deps{
		Cfg: &cfg, Live: live, Pool: p, Prices: prices, Log: logs, Daily: daily, Up: up,
		Version: "test", ConfigPath: cfgPath,
		Stats:  func() map[string]int64 { return map[string]int64{"total": 0} },
		Uptime: func() time.Duration { return time.Second },
	})
	srv := httptest.NewServer(pan)
	t.Cleanup(srv.Close)
	return &pHarness{p: pan, pool: p, live: live, srv: srv, cfg: &cfg, cfgPath: cfgPath}
}

func (h *pHarness) call(t *testing.T, method, path, body string, admin bool) (*http.Response, string) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	url := h.srv.URL + path
	req, _ := http.NewRequest(method, url, rdr)
	if admin {
		req.Header.Set("Authorization", "Bearer adm")
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

func TestPanelAuth(t *testing.T) {
	h := newPanelHarness(t)
	if resp, _ := h.call(t, http.MethodGet, "/panel/api/state", "", false); resp.StatusCode != 401 {
		t.Fatalf("无令牌应 401，实际 %d", resp.StatusCode)
	}
	if resp, body := h.call(t, http.MethodGet, "/panel/api/state?token=adm", "", false); resp.StatusCode != 200 {
		t.Fatalf("?token= 应可用: %d %s", resp.StatusCode, body)
	}
}

func TestPanelStateShape(t *testing.T) {
	h := newPanelHarness(t)
	_, body := h.call(t, http.MethodGet, "/panel/api/state", "", true)
	var m map[string]any
	if err := json.Unmarshal([]byte(body), &m); err != nil {
		t.Fatalf("state 不是 JSON: %v %s", err, body)
	}
	for _, k := range []string{"keys", "pool", "batch", "totals", "prices", "capacity", "model_health", "days", "logs", "sessions"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("state 缺字段 %s: %s", k, body)
		}
	}
}

func TestPanelKeyLifecycle(t *testing.T) {
	h := newPanelHarness(t)
	resp, body := h.call(t, http.MethodPost, "/panel/api/keys", `{"name":"a","secret":"sk_tr_a_1","budget":10}`, true)
	if resp.StatusCode != 200 {
		t.Fatalf("添加失败: %d %s", resp.StatusCode, body)
	}
	var created struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal([]byte(body), &created)

	resp, body = h.call(t, http.MethodPatch, "/panel/api/keys/"+created.ID, `{"weight":2,"enabled":false}`, true)
	if resp.StatusCode != 200 {
		t.Fatalf("修改失败: %d %s", resp.StatusCode, body)
	}
	keys := h.pool.Views()
	if len(keys) != 1 || keys[0].Enabled || keys[0].Weight != 2 {
		t.Fatalf("修改没生效: %+v", keys)
	}

	// 补额 + 恢复
	resp, _ = h.call(t, http.MethodPost, "/panel/api/keys/"+created.ID+"/budget", `{"delta":5}`, true)
	if resp.StatusCode != 200 {
		t.Fatalf("补额失败: %d", resp.StatusCode)
	}
	if got := h.pool.Views()[0].Budget; got != 15 {
		t.Fatalf("补额后 budget=%v", got)
	}

	resp, _ = h.call(t, http.MethodDelete, "/panel/api/keys/"+created.ID, "", true)
	if resp.StatusCode != 200 || len(h.pool.Views()) != 0 {
		t.Fatalf("删除失败: %d", resp.StatusCode)
	}
}

func TestPanelPoolHotUpdateWritesConfig(t *testing.T) {
	h := newPanelHarness(t)
	body := `{"strategy":"round_robin","top_k":7,"max_in_flight":4,"retry_max":2,
	 "rate_cooldown":"15s","rate_cooldown_max":"5m","breaker_cooldown":"20s",
	 "exhausted_cooldown":"40m","upstream_timeout":"120s","video_timeout":"300s",
	 "sticky_ttl":"24h","key_qps":2,
	 "session_sticky":{"enabled":true,"ttl":"20m","max_entries":500,"prefix_affinity":true},
	 "batch":{"mode":"auto","size":3,"min":1,"max":0,"target_inflight_per_key":3}}`
	resp, out := h.call(t, http.MethodPut, "/panel/api/pool", body, true)
	if resp.StatusCode != 200 {
		t.Fatalf("热更新失败: %d %s", resp.StatusCode, out)
	}
	cur := h.pool.Config()
	if cur.Strategy != "round_robin" || cur.TopK != 7 || cur.RateCooldown.Duration() != 15*time.Second {
		t.Fatalf("热更新没生效: %+v", cur)
	}
	raw, err := os.ReadFile(h.cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	out2 := string(raw)
	if !strings.Contains(out2, `"strategy": "round_robin"`) || !strings.Contains(out2, `"custom": 1`) {
		t.Fatalf("配置写回不对: %s", out2)
	}
	if strings.Contains(out2, "admin_key") {
		t.Fatal("写回配置不该新增密钥字段")
	}
}

// 只改窗口大小不该把其它参数清零——手写 curl 只传一两个字段太常见了。
// 以前那版是"解到零值再逐项兜底"，漏兜的字段会被静默清零，
// 比如 breaker_threshold 变 0 之后熔断就再也不会触发。
func TestPanelPoolPartialUpdateKeepsOthers(t *testing.T) {
	h := newPanelHarness(t)
	before := h.pool.Config()
	body := `{"strategy":"weighted_random","batch":{"mode":"fixed","size":2}}`
	resp, out := h.call(t, http.MethodPut, "/panel/api/pool", body, true)
	if resp.StatusCode != 200 {
		t.Fatalf("局部热更新失败: %d %s", resp.StatusCode, out)
	}
	cur := h.pool.Config()
	if cur.BreakerThreshold != before.BreakerThreshold {
		t.Fatalf("breaker_threshold 被清零: %d → %d", before.BreakerThreshold, cur.BreakerThreshold)
	}
	if cur.RateMax != before.RateMax || cur.RetryMax != before.RetryMax {
		t.Fatalf("冷却/重试参数被改掉: %+v", cur)
	}
	if cur.Batch.Mode != "fixed" || cur.Batch.Size != 2 {
		t.Fatalf("窗口参数没生效: %+v", cur.Batch)
	}
	if cur.Batch.Elastic.Window.Duration() != 60*time.Second {
		t.Fatalf("弹性默认值应补齐，实际 %v", cur.Batch.Elastic.Window.Duration())
	}
}

func TestPanelRejectsBadStrategy(t *testing.T) {
	h := newPanelHarness(t)
	resp, _ := h.call(t, http.MethodPut, "/panel/api/pool", `{"strategy":"magic"}`, true)
	if resp.StatusCode != 400 {
		t.Fatalf("非法策略应 400，实际 %d", resp.StatusCode)
	}
}

func TestPanelLogsAndStatic(t *testing.T) {
	h := newPanelHarness(t)
	resp, body := h.call(t, http.MethodGet, "/panel/api/logs?n=10", "", true)
	if resp.StatusCode != 200 || !strings.Contains(body, `"source":"ring"`) {
		t.Fatalf("日志接口异常: %d %s", resp.StatusCode, body)
	}
	// 归档未启用时 history=1 应给出明确错误，而不是空列表。
	resp, _ = h.call(t, http.MethodGet, "/panel/api/logs?history=1", "", true)
	if resp.StatusCode != 400 {
		t.Fatalf("未启用归档应 400，实际 %d", resp.StatusCode)
	}

	// 静态资源与安全头
	resp, page := h.call(t, http.MethodGet, "/panel/", "", false)
	if resp.StatusCode != 200 || !strings.Contains(page, "<html") {
		t.Fatalf("首页异常: %d", resp.StatusCode)
	}
	if resp.Header.Get("Content-Security-Policy") == "" || resp.Header.Get("ETag") == "" {
		t.Fatal("缺 CSP 或 ETag")
	}
	resp, js := h.call(t, http.MethodGet, "/panel/app.js", "", false)
	if resp.StatusCode != 200 || len(js) == 0 {
		t.Fatal("app.js 没取到")
	}
}
