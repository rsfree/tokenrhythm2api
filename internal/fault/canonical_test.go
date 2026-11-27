package fault

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func mh(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

// 核心命题：状态码不是语义。同一个 429 里住着限流、欠费、过载三种结论。
func TestStatusIsNotSemantics(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   Canonical
		http   int
		kind   Kind
	}{
		{
			"429 过载（Kimi 风格 type）",
			429, `{"error":{"type":"engine_overloaded_error","message":"engine overloaded"}}`,
			CServerOverloaded, 503, Upstream,
		},
		{
			"429 欠费（Kimi exceeded_current_quota）",
			429, `{"error":{"type":"exceeded_current_quota_error","message":"available_balance 0"}}`,
			CInsufficientQuota, 429, Quota,
		},
		{
			"429 限流",
			429, `{"error":{"type":"rate_limit_reached_error","message":"rpm limit"}}`,
			CRateLimit, 429, Rate,
		},
		{
			"429 我方上游 code 过载",
			429, `{"code":"SERVER_OVERLOADED","message":"busy"}`,
			CServerOverloaded, 503, Upstream,
		},
		{
			"429 只有 message 说过载",
			429, `{"message":"server is busy, please retry later"}`,
			CServerOverloaded, 503, Upstream,
		},
		{
			"402 欠费（DeepSeek 风格）",
			402, `{"error":{"message":"Insufficient Balance"}}`,
			CInsufficientQuota, 429, Quota,
		},
		{
			"403 欠费（方舟 AccountOverdueError）",
			403, `{"error":{"code":"AccountOverdueError","message":"overdue"}}`,
			CInsufficientQuota, 429, Quota,
		},
		{
			"403 欠费（百炼 Arrearage）",
			403, `{"error":{"code":"Arrearage","message":"arrearage"}}`,
			CInsufficientQuota, 429, Quota,
		},
		{
			"403 权限（我方 code）",
			403, `{"code":"MODEL_ACCESS_DENIED","message":"no permission"}`,
			CPermissionDenied, 403, ModelDenied,
		},
		{
			"403 无任何线索",
			403, `{"message":"forbidden"}`,
			CPermissionDenied, 403, ModelDenied,
		},
		{
			"400 模型不存在（我方 MODEL_NOT_AVAILABLE）",
			400, `{"code":"MODEL_NOT_AVAILABLE","message":"模型不可用"}`,
			CModelNotFound, 404, Client,
		},
		{
			// /v1/messages 那条路径上游只回一句中文、没有 code：
			// 靠窄 hint 判成"模型不存在"，别再笼统算成请求参数错。
			"400 模型不可用（无 code）",
			400, `{"error":{"message":"模型不可用：no-such-model"}}`,
			CModelNotFound, 404, Client,
		},
		{
			"400 上下文超长",
			400, `{"error":{"message":"This model's maximum context length is 8192 tokens"}}`,
			CContextLength, 400, Client,
		},
		{
			"400 内容审核",
			400, `{"error":{"message":"rejected by content policy"}}`,
			CContentPolicy, 400, Client,
		},
		{
			"400 普通参数错",
			400, `{"error":{"message":"missing required parameter: model"}}`,
			CInvalidRequest, 400, Client,
		},
		{
			"401 密钥失效",
			401, `{"code":"INVALID_API_KEY","message":"bad key"}`,
			CAuthentication, 401, Auth,
		},
		{
			"403 账号冻结（我方 code）",
			403, `{"code":"ACCOUNT_SUSPENDED","message":"账号已冻结"}`,
			// 对外是"上游凭据不可用"，内部是冻结号（终态 blocked），两套视图分开。
			CAuthentication, 401, Banned,
		},
		{
			"429 我方供应链限流",
			429, `{"code":"UPSTREAM_RATE_LIMITED","message":"model busy"}`,
			CRateLimit, 429, UpstreamRate,
		},
		{
			"429 我方按号限流",
			429, `{"code":"RATE_LIMITED","message":"too many requests"}`,
			CRateLimit, 429, Rate,
		},
		{
			"500 上游内部错误",
			500, `{"message":"internal error"}`,
			CApiError, 500, Upstream,
		},
		{
			"503 服务不可用",
			503, `{"message":"service unavailable"}`,
			CServerOverloaded, 503, Upstream,
		},
		{
			"529 Anthropic 过载",
			529, `{"type":"overloaded_error","message":"overloaded"}`,
			CServerOverloaded, 503, Upstream,
		},
		{
			"408 上游超时",
			408, `{"message":"request timeout"}`,
			CUpstreamTimeout, 504, Upstream,
		},
		{
			"413 请求体过大",
			413, `<html>too large</html>`,
			CRequestTooLarge, 413, Client,
		},
		{
			"502 HTML 错误页",
			502, `<html><body>502 Bad Gateway</body></html>`,
			CApiError, 500, Upstream,
		},
		{
			"418 认不出来 → 兜底",
			418, `{"error":{"code":"teapot","message":"I am a teapot"}}`,
			CUnknown, 502, Upstream,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v := Classify(c.status, []byte(c.body), mh())
			if v.Canonical != c.want {
				t.Fatalf("canonical = %s（rule %s），应为 %s", v.Canonical, v.RuleID, c.want)
			}
			if v.HTTP() != c.http {
				t.Fatalf("对外 status = %d，应为 %d", v.HTTP(), c.http)
			}
			if v.Kind != c.kind {
				t.Fatalf("Kind = %s，应为 %s（调度语义与对外契约是两套）", v.Kind, c.kind)
			}
		})
	}
}

// 200 里带 error 成员不能当成功：会记一条空补全并计费。
func TestSuccessWithErrorBody(t *testing.T) {
	v := Classify(200, []byte(`{"error":{"code":"MODEL_NOT_AVAILABLE","message":"模型不可用"}}`), mh())
	if v.Canonical != CApiError || v.HTTP() != 500 {
		t.Fatalf("canonical = %s http=%d，应为 api_error/500（rule %s）", v.Canonical, v.HTTP(), v.RuleID)
	}
	// 正常响应不能被误判。
	ok := Classify(200, []byte(`{"choices":[{"message":{"content":"hi"}}],"usage":{"total_tokens":3}}`), mh())
	if ok.Canonical != "" {
		t.Fatalf("正常 200 被判成失败：%s", ok.Canonical)
	}
	// 嵌套的 "error" 是 null 时不算失败。
	ok2 := Classify(200, []byte(`{"choices":[{"message":{"content":"hi"}}],"error":null}`), mh())
	if ok2.Canonical != "" {
		t.Fatalf("error 为 null 被判成失败：%s", ok2.Canonical)
	}
}

// 请求病绝不能换号：30 个号的池子就是 30 次无效调用 30 次计费。
func TestRequestFaultsAreNotRetryable(t *testing.T) {
	for _, c := range []Canonical{
		CInvalidRequest, CContextLength, CContentPolicy, CRequestTooLarge, CModelNotFound,
	} {
		if SpecOf(c).Retryable {
			t.Errorf("%s 不该允许换号重试", c)
		}
		if SpecOf(c).Kind != Client {
			t.Errorf("%s 的 Kind 应为 client", c)
		}
	}
	for _, c := range []Canonical{
		CAuthentication, CPermissionDenied, CRateLimit, CInsufficientQuota,
		CServerOverloaded, CApiError, CUpstreamTimeout,
	} {
		if !SpecOf(c).Retryable {
			t.Errorf("%s 应该允许换号", c)
		}
	}
}

// 全池失败时的对外选码：请求病 > 欠费 > 认证 > 权限 > 渠道病 > 限流。
func TestPickFinal(t *testing.T) {
	v := func(c Canonical) Verdict { return Verdict{Canonical: c} }

	got, ok := PickFinal([]Verdict{v(CRateLimit), v(CInsufficientQuota), v(CApiError)})
	if !ok || got.Canonical != CInsufficientQuota {
		t.Fatalf("混合失败应返回 insufficient_quota，实际 %s", got.Canonical)
	}

	got, _ = PickFinal([]Verdict{v(CRateLimit), v(CApiError)})
	if got.Canonical != CApiError {
		t.Fatalf("限流 + 渠道病应返回渠道病，实际 %s", got.Canonical)
	}

	// 全池都是限流：仍返回限流，别伪造成别的（确实会自愈）。
	got, _ = PickFinal([]Verdict{v(CRateLimit), v(CRateLimit)})
	if got.Canonical != CRateLimit {
		t.Fatalf("全池限流应返回 rate_limit_exceeded，实际 %s", got.Canonical)
	}

	got, _ = PickFinal([]Verdict{v(CModelNotFound), v(CInsufficientQuota)})
	if got.Canonical != CModelNotFound {
		t.Fatalf("请求病应压过配额，实际 %s", got.Canonical)
	}

	if _, ok := PickFinal(nil); ok {
		t.Fatal("空列表不该挑出结论")
	}
}

// Retry-After：归一为秒、截断 900s、终态不带。
func TestRetryAfterNormalisation(t *testing.T) {
	cases := []struct {
		hdr  http.Header
		body string
		want time.Duration
	}{
		{mh("Retry-After", "60"), "", 60 * time.Second},
		{mh("Retry-After", "60s"), "", 60 * time.Second}, // 野生的带单位写法
		{mh("Retry-After", "1.9"), "", 1900 * time.Millisecond},
		{mh("Retry-After", "99999"), "", 900 * time.Second}, // 截断，别把客户端挂死
		{mh("Retry-After", "Wed, 21 Oct 2015 07:28:00 GMT"), "", 0},
		{mh(), `{"data":{"retryAfterSeconds":12}}`, 12 * time.Second},
	}
	for _, c := range cases {
		if got := RetryAfter(c.hdr, []byte(c.body)); got != c.want {
			t.Errorf("RetryAfter(%v,%s) = %v，应为 %v", c.hdr, c.body, got, c.want)
		}
	}

	// 限流：上游没给就用默认 60s，并写进文案。
	rl := Classify(429, []byte(`{"code":"RATE_LIMITED"}`), mh())
	if rl.RetryAfter != 60*time.Second {
		t.Fatalf("限流默认退避 = %v，应为 60s", rl.RetryAfter)
	}
	if !strings.Contains(rl.CallerMessage("req_1"), "60") {
		t.Fatalf("文案应含退避秒数：%s", rl.CallerMessage("req_1"))
	}

	// 终态：欠费号给个"等 N 秒"会让客户端误以为等一会儿能恢复。
	q := Classify(402, []byte(`{"error":{"message":"Insufficient Balance"}}`), mh("Retry-After", "60"))
	if q.RetryAfter != 0 {
		t.Fatalf("insufficient_quota 不该带 Retry-After，实际 %v", q.RetryAfter)
	}
	if SpecOf(CInsufficientQuota).RetryAfter {
		t.Fatal("insufficient_quota 的 spec 不该声明 Retry-After")
	}
}

// 对外文案只能来自模板：上游文本一个字都不能漏出去。
func TestCallerMessageNeverContainsUpstreamText(t *testing.T) {
	secret := "account aav2dfsmiw@mail.giie.eu.org has no refresh token, balance 0.00, see https://tokenrhythm.studio/account/recharge"
	tokens := []string{"aav2dfsmiw", "refresh token", "0.00", "tokenrhythm", "http", "@"}

	all := []Canonical{
		CInvalidRequest, CContextLength, CContentPolicy, CRequestTooLarge,
		CAuthentication, CPermissionDenied, CModelNotFound, CRateLimit,
		CInsufficientQuota, CServerOverloaded, CApiError, CUpstreamTimeout, CUnknown,
	}
	for _, c := range all {
		msg := Message(c, 60, "req_abcd1234")
		if len(msg) > 200 {
			t.Errorf("%s 文案过长（%d）：%s", c, len(msg), msg)
		}
		for _, tk := range tokens {
			if strings.Contains(msg, tk) {
				t.Errorf("%s 文案里出现了 %q：%s", c, tk, msg)
			}
		}
	}

	// 上游把整段话塞进 message，也不能出现在对外文案里。
	v := Classify(500, []byte(`{"error":{"message":"`+secret+`"}}`), mh())
	out := v.CallerMessage("req_abcd1234")
	for _, tk := range tokens {
		if strings.Contains(out, tk) {
			t.Fatalf("对外文案泄露了 %q：%s", tk, out)
		}
	}
	// 但运维侧必须留着原文，否则没法排障。
	if !strings.Contains(v.Message, "refresh token") {
		t.Fatalf("运维视图丢了上游原文：%q", v.Message)
	}
}

// 传输层错误：超时 = 504，连接失败 = 502，都不能落到 200。
func TestClassifyNet(t *testing.T) {
	v := ClassifyNet(fmt.Errorf("上游超时: %w", context.DeadlineExceeded))
	if v.Canonical != CUpstreamTimeout || v.HTTP() != 504 {
		t.Fatalf("超时应为 upstream_timeout/504，实际 %s/%d", v.Canonical, v.HTTP())
	}
	v = ClassifyNet(errors.New("连接上游失败: dial tcp 1.2.3.4:443: connect: connection refused"))
	if v.Canonical != CUnknown || v.HTTP() != 502 {
		t.Fatalf("连接失败应为 unknown_upstream_error/502，实际 %s/%d", v.Canonical, v.HTTP())
	}
}
