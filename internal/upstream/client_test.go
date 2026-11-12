package upstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// capture 起一个假的 tokenrhythm，把收到的头整份留下来。
func capture(t *testing.T) (*httptest.Server, *http.Header) {
	t.Helper()
	got := &http.Header{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"ok"}`))
	}))
	t.Cleanup(srv.Close)
	return srv, got
}

func do(t *testing.T, c *Client, hdr http.Header) {
	t.Helper()
	res, err := c.Do(context.Background(), http.MethodPost, "/v1/chat/completions",
		[]byte(`{"model":"m"}`), "sk_tr_secret", hdr, false)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	res.Close()
	if res.Status != http.StatusOK {
		t.Fatalf("status = %d", res.Status)
	}
}

// 客户端没带 UA（面板探活、预热、价目表刷新）：用内置兜底，
// 但绝不能是"自报家门"的 trpool/2 + 仓库地址。
func TestUserAgentFallback(t *testing.T) {
	srv, got := capture(t)
	c := New(srv.URL)
	do(t, c, http.Header{})

	if ua := got.Get("User-Agent"); ua != defaultUA {
		t.Fatalf("UA = %q，应为兜底 %q", ua, defaultUA)
	}
	for _, bad := range []string{"trpool", "github.com", "rsfree"} {
		if ua := got.Get("User-Agent"); containsFold(ua, bad) {
			t.Fatalf("UA 仍带渠道标识 %q: %q", bad, ua)
		}
	}
}

// 默认策略是透传：上游看到的是调用方自己的 UA，不是一池子号的统一指纹。
func TestUserAgentPassthrough(t *testing.T) {
	srv, got := capture(t)
	c := New(srv.URL)
	hdr := http.Header{}
	hdr.Set("User-Agent", "OpenAI/Python 1.99.0")
	do(t, c, hdr)

	if ua := got.Get("User-Agent"); ua != "OpenAI/Python 1.99.0" {
		t.Fatalf("UA = %q，应透传客户端 UA", ua)
	}
}

// 显式配置时（上游挑 UA 的场合）压过客户端 UA。
func TestUserAgentForced(t *testing.T) {
	srv, got := capture(t)
	c := New(srv.URL, WithUserAgent("  my-agent/1.0  "))
	hdr := http.Header{}
	hdr.Set("User-Agent", "curl/8.4.0")
	do(t, c, hdr)

	if ua := got.Get("User-Agent"); ua != "my-agent/1.0" {
		t.Fatalf("UA = %q，应为配置值（且去掉首尾空白）", ua)
	}
}

// 客户端网络身份与入口身份不能出现在上游请求里，
// 其余协议相关头（SDK 版本头、幂等头）要照常带过去。
func TestInboundOnlyHeadersStripped(t *testing.T) {
	srv, got := capture(t)
	c := New(srv.URL)

	hdr := http.Header{}
	hdr.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.1")
	hdr.Set("X-Forwarded-Proto", "https")
	hdr.Set("X-Forwarded-Host", "trpool.example.com")
	hdr.Set("X-Real-IP", "203.0.113.7")
	hdr.Set("Forwarded", "for=203.0.113.7")
	hdr.Set("CF-Connecting-IP", "203.0.113.7")
	hdr.Set("TRUE-Client-IP", "203.0.113.7")
	hdr.Set("X-Originating-IP", "203.0.113.7")
	hdr.Set("Via", "1.1 tengine")
	hdr.Set("X-Via", "proxy")
	hdr.Set("Origin", "https://trpool.example.com")
	hdr.Set("Referer", "https://trpool.example.com/panel/")
	hdr.Set("Cookie", "trpool_admin=xxx")
	// 这些必须保留：SDK 指纹头与幂等/追踪头都属于正常 API 调用
	hdr.Set("OpenAI-Beta", "assistants=v2")
	hdr.Set("X-Request-Id", "req-123")
	hdr.Set("Anthropic-Version", "2023-06-01")
	hdr.Set("Idempotency-Key", "idem-1")

	do(t, c, hdr)

	leaked := []string{
		"X-Forwarded-For", "X-Forwarded-Proto", "X-Forwarded-Host", "X-Real-IP",
		"Forwarded", "Cf-Connecting-Ip", "True-Client-Ip", "X-Originating-Ip",
		"Via", "X-Via", "Origin", "Referer", "Cookie",
	}
	for _, k := range leaked {
		if v := got.Values(k); len(v) > 0 {
			t.Errorf("不该转发的头漏了 %s = %v", k, v)
		}
	}
	kept := map[string]string{
		"Openai-Beta":       "assistants=v2",
		"X-Request-Id":      "req-123",
		"Anthropic-Version": "2023-06-01",
		"Idempotency-Key":   "idem-1",
		"Content-Type":      "application/json",
	}
	for k, want := range kept {
		if v := got.Get(k); v != want {
			t.Errorf("%s = %q，应为 %q", k, v, want)
		}
	}
}

// 鉴权必须换成池子里的号，客户端自己的 Authorization 一律丢弃。
func TestAuthorizationReplaced(t *testing.T) {
	srv, got := capture(t)
	c := New(srv.URL)
	hdr := http.Header{}
	hdr.Set("Authorization", "Bearer sk-client-side")
	do(t, c, hdr)

	if v := got.Get("Authorization"); v != "Bearer sk_tr_secret" {
		t.Fatalf("Authorization = %q，应被池子密钥覆盖", v)
	}
}

func containsFold(s, sub string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(sub))
}
