package panel

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"trpool/internal/config"
	"trpool/internal/fault"
	"trpool/internal/livecfg"
	"trpool/internal/pool"
	"trpool/internal/session"
)

const pruneAdmin = "adm-prune"

func newPrunePanel(t *testing.T) (*Panel, *pool.Pool) {
	t.Helper()
	cfg := config.Default()
	cfg.Pool.Batch.Mode = "off"
	p := pool.New(cfg.Pool, session.New(cfg.Pool.SessionSticky.TTL.Duration(), 10))
	live := livecfg.New(livecfg.Snapshot{AdminKey: pruneAdmin})
	return New(Deps{Cfg: &cfg, Live: live, Pool: p}), p
}

func pruneCall(t *testing.T, pan *Panel, body string) (int, pruneResp) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/panel/api/keys/prune", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+pruneAdmin)
	rec := httptest.NewRecorder()
	pan.ServeHTTP(rec, req)
	var out pruneResp
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

// 不传 states = 只清「停用号」（欠费 / 冻结 / 人工停用），invalid 不在默认范围里：
// 401 的号还可能被探活救回来，批量删不该捎带它。
func TestPruneDefaultStates(t *testing.T) {
	pan, p := newPrunePanel(t)
	dead, _ := p.Add("dead", "sk_tr_dead_0123456789", 1, 0)
	banned, _ := p.Add("banned", "sk_tr_banned_0123456789", 1, 0)
	invalid, _ := p.Add("invalid", "sk_tr_invalid_0123456789", 1, 0)
	live, _ := p.Add("live", "sk_tr_live_0123456789", 1, 0)
	p.MarkFail(dead.ID, pool.Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})
	p.MarkFail(banned.ID, pool.Outcome{Kind: fault.Banned, Message: "403 ACCOUNT_SUSPENDED"})
	p.MarkFail(invalid.ID, pool.Outcome{Kind: fault.Auth, Message: "401 UNAUTHORIZED"})

	code, out := pruneCall(t, pan, ``)
	if code != http.StatusOK {
		t.Fatalf("期望 200，实际 %d", code)
	}
	if out.Removed != 2 {
		t.Fatalf("默认应删掉欠费+冻结 2 个号，实际 %d（%v）", out.Removed, out.Names)
	}
	if !p.Has(invalid.ID) || !p.Has(live.ID) {
		t.Fatal("invalid / 可用号不该被默认清理删掉")
	}
	if p.Has(dead.ID) || p.Has(banned.ID) {
		t.Fatal("停用号应已删除")
	}
}

// 显式点选 invalid 才删它。
func TestPruneExplicitInvalid(t *testing.T) {
	pan, p := newPrunePanel(t)
	k, _ := p.Add("invalid", "sk_tr_invalid_0123456789", 1, 0)
	p.MarkFail(k.ID, pool.Outcome{Kind: fault.Auth, Message: "401 UNAUTHORIZED"})

	code, out := pruneCall(t, pan, `{"states":["invalid"]}`)
	if code != http.StatusOK || out.Removed != 1 {
		t.Fatalf("显式指定 invalid 应删掉 1 个，实际 %d / %d", code, out.Removed)
	}
	if p.Has(k.ID) {
		t.Fatal("号应已删除")
	}
}

// 拿 active 来批量删是可预见的误操作（一个请求就能清空整个池），必须拒掉。
func TestPruneRejectsActive(t *testing.T) {
	pan, p := newPrunePanel(t)
	k, _ := p.Add("live", "sk_tr_live_0123456789", 1, 0)
	code, _ := pruneCall(t, pan, `{"states":["active"]}`)
	if code != http.StatusBadRequest {
		t.Fatalf("清理 active 应返回 400，实际 %d", code)
	}
	if !p.Has(k.ID) {
		t.Fatal("被拒的请求不该动池子")
	}
}

// dry_run 是给脚本用的：先看清单，再决定删不删。
func TestPruneDryRunViaAPI(t *testing.T) {
	pan, p := newPrunePanel(t)
	k, _ := p.Add("dead", "sk_tr_dead_0123456789", 1, 0)
	p.MarkFail(k.ID, pool.Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})

	code, out := pruneCall(t, pan, `{"dry_run":true}`)
	if code != http.StatusOK || out.Matched != 1 || out.Removed != 0 {
		t.Fatalf("dry_run 应只报清单：code=%d matched=%d removed=%d", code, out.Matched, out.Removed)
	}
	if !p.Has(k.ID) {
		t.Fatal("dry_run 不该真删")
	}
}

// 没带管理令牌一律 401——批量删是最危险的一类接口。
func TestPruneRequiresAdmin(t *testing.T) {
	pan, p := newPrunePanel(t)
	k, _ := p.Add("dead", "sk_tr_dead_0123456789", 1, 0)
	p.MarkFail(k.ID, pool.Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})

	req := httptest.NewRequest(http.MethodPost, "/panel/api/keys/prune", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	pan.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("无令牌应 401，实际 %d", rec.Code)
	}
	if !p.Has(k.ID) {
		t.Fatal("未鉴权的请求不该删号")
	}
}
