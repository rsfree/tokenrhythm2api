package fault

import (
	"net/http"
	"testing"
	"time"
)

func hdr(k, v string) http.Header {
	h := http.Header{}
	h.Set(k, v)
	return h
}

func TestClassify(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		body      string
		want      Kind
		retryable bool
		model     bool
	}{
		{"401", 401, `{}`, Auth, true, false},
		{"402", 402, `{}`, Quota, true, false},
		{"429 platform", 429, `{"code":"RATE_LIMITED"}`, Rate, true, false},
		{"429 supply", 429, `{"code":"UPSTREAM_RATE_LIMITED"}`, UpstreamRate, true, true},
		{"403 suspended", 403, `{"code":"ACCOUNT_SUSPENDED"}`, Banned, true, false},
		{"403 suspended 中文", 403, `{"code":"ACCOUNT_SUSPENDED","message":"账号已临时冻结"}`, Banned, true, false},
		{"403 banned", 403, `{"code":"ACCOUNT_BANNED"}`, Banned, true, false},
		// 线上真实返回：欠费
		{"402 insufficient", 402, `{"code":"INSUFFICIENT_BALANCE","message":"账户余额不足，请充值后重试。"}`, Quota, true, false},
		// 上游把"没钱"塞进 400/403 的情况也不能漏判成客户端错误
		{"403 insufficient", 403, `{"code":"INSUFFICIENT_BALANCE","message":"账户余额不足"}`, Quota, true, false},
		{"400 insufficient", 400, `{"code":"INSUFFICIENT_BALANCE"}`, Quota, true, false},
		{"403 model denied", 403, `{"code":"MODEL_ACCESS_DENIED"}`, ModelDenied, true, true},
		{"403 unknown", 403, `{"code":"WHATEVER"}`, Quota, true, false},
		{"500", 500, `oops`, Upstream, true, false},
		{"400", 400, `{"error":"bad"}`, Client, false, false},
		{"200", 200, `{}`, None, false, false},
	}
	for _, c := range cases {
		v := Classify(c.status, []byte(c.body), nil)
		if v.Kind != c.want || v.Retryable != c.retryable || v.ModelScoped != c.model {
			t.Errorf("%s: got kind=%s retryable=%v model=%v", c.name, v.Kind, v.Retryable, v.ModelScoped)
		}
	}
}

func TestRetryAfterHeaderAndBody(t *testing.T) {
	v := Classify(429, []byte(`{}`), hdr("Retry-After", "13"))
	if v.RetryAfter != 13*time.Second {
		t.Fatalf("应读 Retry-After 头，实际 %s", v.RetryAfter)
	}
	v = Classify(429, []byte(`{"data":{"retryAfterSeconds":7.5}}`), nil)
	if v.RetryAfter != 7500*time.Millisecond {
		t.Fatalf("应读 body 里的 retryAfterSeconds，实际 %s", v.RetryAfter)
	}
}

func TestExtractMessage(t *testing.T) {
	if got := ExtractMessage([]byte(`{"code":"E","message":"boom"}`)); got != "E boom" {
		t.Fatalf("got %q", got)
	}
	if got := ExtractMessage([]byte(`{"error":{"message":"inner"}}`)); got != "inner" {
		t.Fatalf("got %q", got)
	}
	if got := ExtractMessage([]byte(`not json`)); got != "not json" {
		t.Fatalf("got %q", got)
	}
}
