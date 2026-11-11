package panel

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"trpool/internal/config"
	"trpool/internal/fault"
	"trpool/internal/livecfg"
	"trpool/internal/pool"
	"trpool/internal/session"
	"trpool/internal/upstream"
)

const probeKey = "sk_tr_probe_0000000000"

func newProbePanel(t *testing.T, probeModel string, h http.HandlerFunc) (*Panel, *pool.Pool) {
	t.Helper()
	upSrv := httptest.NewServer(h)
	t.Cleanup(upSrv.Close)

	cfg := config.Default()
	cfg.Upstream = upSrv.URL
	cfg.AutoProbe.Model = probeModel
	cfg.AutoProbe.MaxTokens = 1
	live := livecfg.New(livecfg.Snapshot{ProbeModel: probeModel, ProbeMaxTokens: 1})
	p := pool.New(cfg.Pool, session.New(cfg.Pool.SessionSticky.TTL.Duration(), 10))
	if _, err := p.Add("probe", probeKey, 1, 0); err != nil {
		t.Fatal(err)
	}
	pan := New(Deps{Cfg: &cfg, Live: live, Pool: p, Up: upstream.New(upSrv.URL)})
	return pan, p
}

func stateOf(p *pool.Pool) string {
	for _, v := range p.Views() {
		return v.State
	}
	return ""
}

func idOf(p *pool.Pool) string {
	for _, v := range p.Views() {
		return v.ID
	}
	return ""
}

// 配了探活模型：探活就是真跑一次生成，402 必须当场把这个号停掉。
// 这条是"欠费号显示可用"的核心回归：老实现只打 GET /v1/models，
// 欠费号拉模型列表也是 200，探一次就把刚停用的号放回来了。
func TestProbeWithModelStopsExhaustedKey(t *testing.T) {
	pan, p := newProbePanel(t, "glm-flash", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/models" {
			t.Error("配了探活模型就不该只拉 /v1/models")
			_, _ = io.WriteString(w, `{"data":[]}`)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" {
			t.Errorf("探活应打 POST /v1/chat/completions，实际 %s %s", r.Method, r.URL.Path)
		}
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"max_tokens":1`) || !strings.Contains(string(body), `"glm-flash"`) {
			t.Errorf("探活请求体不对: %s", body)
		}
		w.WriteHeader(http.StatusPaymentRequired)
		_, _ = io.WriteString(w, `{"code":"INSUFFICIENT_BALANCE","message":"账户余额不足"}`)
	})

	res := pan.probeOne(context.Background(), idOf(p), probeKey)
	if res.OK {
		t.Fatal("402 不该算探活成功")
	}
	if res.Model != "glm-flash" {
		t.Fatalf("结果应标出用的探活模型，实际 %q", res.Model)
	}
	if got := stateOf(p); got != "exhausted" {
		t.Fatalf("探活撞到 402 应把号停用（exhausted），实际 %s", got)
	}
}

// 没配探活模型（只拉 /v1/models）：就算返回 200，也只能证明密钥有效，
// 不能把欠费/冻结的号放回可用——否则面板上又会出现"余额不足却可用"。
func TestModelsOnlyProbeDoesNotReviveStopped(t *testing.T) {
	pan, p := newProbePanel(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"data":[{"id":"glm-flash"}]}`)
	})
	id := idOf(p)
	p.MarkFail(id, pool.Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})
	if got := stateOf(p); got != "exhausted" {
		t.Fatalf("前置条件：应先被停用，实际 %s", got)
	}

	res := pan.probeOne(context.Background(), id, probeKey)
	if !res.OK {
		t.Fatalf("/v1/models 200 应算探活成功（密钥有效），实际 %s", res.Error)
	}
	if got := stateOf(p); got != "exhausted" {
		t.Fatalf("免费探活不得复活欠费号，实际 %s", got)
	}
}

// 但 invalid（401）是"密钥失效"，/v1/models 通了就该放回来——这是它的唯一自动恢复通道。
func TestModelsOnlyProbeRevivesInvalid(t *testing.T) {
	pan, p := newProbePanel(t, "", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"data":[]}`)
	})
	id := idOf(p)
	p.MarkFail(id, pool.Outcome{Kind: fault.Auth, Message: "401 UNAUTHORIZED"})
	if got := stateOf(p); got != "invalid" {
		t.Fatalf("前置条件：应先变 invalid，实际 %s", got)
	}
	pan.probeOne(context.Background(), id, probeKey)
	if got := stateOf(p); got != "active" {
		t.Fatalf("401 号探活通过应恢复可用，实际 %s", got)
	}
}

// 配了模型且真跑通了：这才是可以复活停用号的证据（有钱、模型通）。
func TestModelProbeRevivesStoppedKey(t *testing.T) {
	pan, p := newProbePanel(t, "glm-flash", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"id":"x","choices":[{"message":{"content":"p"}}]}`)
	})
	id := idOf(p)
	p.MarkFail(id, pool.Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})
	if got := stateOf(p); got != "exhausted" {
		t.Fatalf("前置条件：应先被停用，实际 %s", got)
	}
	if res := pan.probeOne(context.Background(), id, probeKey); !res.OK {
		t.Fatalf("生成成功应算探活通过，实际 %s", res.Error)
	}
	if got := stateOf(p); got != "active" {
		t.Fatalf("真跑通生成后应恢复可用，实际 %s", got)
	}
}

// 探活模型填错（403 MODEL_ACCESS_DENIED）是配置问题，不该罚号。
func TestProbeModelDeniedDoesNotPunishKey(t *testing.T) {
	pan, p := newProbePanel(t, "no-such-model", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"code":"MODEL_ACCESS_DENIED","message":"model not enabled"}`)
	})
	res := pan.probeOne(context.Background(), idOf(p), probeKey)
	if res.OK {
		t.Fatal("403 不该算探活成功")
	}
	if !strings.Contains(res.Error, "探活模型可能填错") {
		t.Fatalf("应提示探活模型可能填错，实际 %q", res.Error)
	}
	if got := stateOf(p); got != "active" {
		t.Fatalf("探活模型未开通不该罚号，实际状态 %s", got)
	}
}
