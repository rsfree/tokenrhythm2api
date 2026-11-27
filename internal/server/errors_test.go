package server

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"trpool/internal/config"
)

// 429 里住着三种语义。只看状态码就会把"欠费"和"过载"都报成"你请求太快"。
func TestQuotaIn429IsNotRateLimit(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 2 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(http.StatusTooManyRequests)
		// 上游在 429 里报余额不足：code 比状态码可信。
		_, _ = io.WriteString(w, `{"code":"INSUFFICIENT_BALANCE","message":"账户余额不足"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("应为 429，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"insufficient_quota"`) {
		t.Fatalf("429 + 欠费 code 应归一为 insufficient_quota，实际 %s", body)
	}
	// 终态不给退避时间。
	if ra := resp.Header.Get("Retry-After"); ra != "" {
		t.Fatalf("insufficient_quota 不该带 Retry-After，实际 %q", ra)
	}
	// 欠费是账号级终态：号必须被停用，而不是按 429 冷却 20 秒后又被选中。
	for _, v := range h.pool.Views() {
		if v.State != "exhausted" {
			t.Fatalf("欠费号应停用，实际 %s", v.State)
		}
	}
}

// 同一个 429 上的过载语义：对外 503，而不是 429。
func TestOverloaded429Becomes503(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 2 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"code":"SERVER_OVERLOADED","message":"service busy"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("429 + 过载 code 应回 503，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"server_is_overloaded"`) {
		t.Fatalf("对外 code 应为 server_is_overloaded，实际 %s", body)
	}
	// 上游没给退避时间 → 用默认 30s，并归一成秒数。
	if ra := resp.Header.Get("Retry-After"); ra != "30" {
		t.Fatalf("Retry-After 应为默认的 30 秒，实际 %q", ra)
	}
}

// Retry-After 必须归一为秒：上游给 HTTP-date 或 "60s" 时，客户端解析不了就会立刻重试。
func TestRetryAfterNormalisedToSeconds(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 1 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Retry-After", "42s")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"code":"RATE_LIMITED","message":"too many requests"}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("限流应回 429，实际 %d %s", resp.StatusCode, body)
	}
	if ra := resp.Header.Get("Retry-After"); ra != "42" {
		t.Fatalf("Retry-After 应归一为 42 秒，实际 %q", ra)
	}
	if !strings.Contains(body, "42") {
		t.Fatalf("文案里应带上退避秒数，实际 %s", body)
	}
}

// 流已经开始输出后才发现上游报错：状态码改不了，只能把 error 帧换成我们的模板。
func TestStreamErrorFrameIsCanonicalized(t *testing.T) {
	h := newHarness(t, nil)
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n")
		_, _ = io.WriteString(w, "data: {\"error\":{\"code\":\"INSUFFICIENT_BALANCE\",\"message\":\"余额不足，请充值 https://tokenrhythm.studio/account/recharge trace_abc\"}}\n\n")
	}
	reqBody := `{"model":"glm-5.1","stream":true,"messages":[{"role":"user","content":"hi"}]}`
	_, body := h.do(t, http.MethodPost, "/v1/chat/completions", reqBody, nil)

	for _, leak := range []string{"tokenrhythm", "recharge", "trace_abc", "INSUFFICIENT_BALANCE", "余额不足"} {
		if strings.Contains(body, leak) {
			t.Fatalf("流式错误帧泄露了 %q：%s", leak, body)
		}
	}
	if !strings.Contains(body, `"code":"api_error"`) {
		t.Fatalf("流式错误帧应换成 canonical 文案，实际 %s", body)
	}
	// 正常的内容帧不能被误伤。
	if !strings.Contains(body, `"content":"hi"`) {
		t.Fatalf("正常帧被改坏了：%s", body)
	}
}

// 200 里带 error 成员：当成功处理会记一条空补全并计费，比报错更糟。
func TestSuccessWithErrorBodyIsFailure(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) { c.RetryMax = 1 })
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"error":{"code":"MODEL_NOT_AVAILABLE","message":"模型不可用"}}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("200 + error 应回 500 api_error，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"api_error"`) {
		t.Fatalf("对外 code 应为 api_error，实际 %s", body)
	}
	// 不能计费：一次"失败的成功"会让成本统计和 token 统计一起失真。
	if _, _, _, cost, _ := h.daily.Total(); cost != 0 {
		t.Fatalf("失败的响应不该产生成本，实际 %v", cost)
	}
	for _, v := range h.pool.Views() {
		if v.PromptTokens != 0 || v.CompletionTokens != 0 || v.Requests != 0 {
			t.Fatalf("不该记 token/请求，实际 %d/%d req=%d", v.PromptTokens, v.CompletionTokens, v.Requests)
		}
	}
}
