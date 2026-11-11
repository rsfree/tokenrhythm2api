// Package fault 把上游响应翻译成调度决策需要的那几种失败语义。
//
// 单独成包的原因：传输层（upstream）不该知道号池，号池（pool）也不该解析 HTTP body。
// 分类口径只此一份，server / scheduler / panel 都从这里取结论，避免"同一个 403
// 在三个地方有三种解释"。
package fault

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Kind 是失败类别。空值表示"没有失败"。
type Kind string

const (
	None Kind = ""

	// Client 是请求本身有问题（4xx 里认不出来的那些）：号没毛病，不重试。
	Client Kind = "client"
	// Rate 是本平台的次数配额（429 RATE_LIMITED / 403 ACCOUNT_SUSPENDED）：
	// 罚号，按上游给的 retryAfter 精确冷却。
	Rate Kind = "rate"
	// UpstreamRate 是模型供应链侧限流（429 UPSTREAM_RATE_LIMITED）：
	// 换号救不了，只对 (号, 模型) 短暂冷却，退避后优先同号重试。
	UpstreamRate Kind = "upstream_rate"
	// Auth 是密钥失效（401）：号进 invalid，需人工或探活恢复。
	Auth Kind = "auth"
	// Quota 是额度/权限类（402）：号进 exhausted，冷却后自动重试。
	Quota Kind = "quota"
	// Upstream 是 5xx / 网络 / 超时：喂熔断器，累计到阈值才冷却。
	Upstream Kind = "upstream"
	// ModelDenied 是 403 MODEL_ACCESS_DENIED：只封 (号, 模型)，换号可能能救。
	ModelDenied Kind = "model_denied"
)

// Verdict 是一次上游失败的完整结论。
type Verdict struct {
	Kind       Kind
	Status     int
	RetryAfter time.Duration
	Message    string
	// ModelScoped 表示这个失败只对当前模型成立：换号前先看别的号有没有该模型，
	// 换模型即可绕过。
	ModelScoped bool
	// Retryable 表示换号/重试有意义。false 时原样回给客户端。
	Retryable bool
}

// Classify 按状态码 + body 判定失败类别。
//
// 403 与 429 都必须看 body 才能判——同一个状态码，两种完全相反的意思：
//
//	403 ACCOUNT_SUSPENDED    账号被冻结     → 罚号、换号
//	403 MODEL_ACCESS_DENIED  模型未开通     → 只封这个模型，换号
//	403 其它                 认不出来       → 按额度问题保守处理
//	429 RATE_LIMITED         平台次数配额   → 罚号，按 retryAfter 冷却
//	429 UPSTREAM_RATE_LIMITED 模型供应链限流 → 不罚号，退避后同号重试
func Classify(status int, body []byte, hdr http.Header) Verdict {
	v := Verdict{Status: status, Message: ExtractMessage(body)}
	switch {
	case status == http.StatusUnauthorized:
		v.Kind, v.Retryable = Auth, true

	case status == http.StatusPaymentRequired:
		v.Kind, v.Retryable = Quota, true

	case status == http.StatusTooManyRequests:
		v.RetryAfter = RetryAfter(hdr, body)
		if HasAny(body, "UPSTREAM_RATE_LIMITED", "upstream_rate_limited") {
			v.Kind, v.Retryable, v.ModelScoped = UpstreamRate, true, true
			return v
		}
		v.Kind, v.Retryable = Rate, true

	case status == http.StatusForbidden:
		switch {
		case HasAny(body, "ACCOUNT_SUSPENDED", "account_suspended"):
			v.Kind, v.Retryable = Rate, true
			v.RetryAfter = RetryAfter(hdr, body)
		case HasAny(body, "MODEL_ACCESS_DENIED", "model_access_denied", "灰度", "not enabled", "no permission"):
			v.Kind, v.Retryable, v.ModelScoped = ModelDenied, true, true
		default:
			v.Kind, v.Retryable = Quota, true
		}

	case status >= 500:
		v.Kind, v.Retryable = Upstream, true

	case status >= 400:
		v.Kind, v.Retryable = Client, false

	default:
		v.Kind = None
	}
	return v
}

// RetryAfter 依次看 Retry-After 头与 body 里的 data.retryAfterSeconds，
// 都没有就返回 0，交给配置里的默认冷却。
func RetryAfter(hdr http.Header, body []byte) time.Duration {
	if hdr != nil {
		if v := strings.TrimSpace(hdr.Get("Retry-After")); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				return time.Duration(n) * time.Second
			}
			if t, err := http.ParseTime(v); err == nil {
				if d := time.Until(t); d > 0 {
					return d
				}
			}
		}
	}
	var m struct {
		Data struct {
			RetryAfterSeconds float64 `json:"retryAfterSeconds"`
		} `json:"data"`
	}
	if json.Unmarshal(body, &m) == nil && m.Data.RetryAfterSeconds > 0 {
		return time.Duration(m.Data.RetryAfterSeconds * float64(time.Second))
	}
	return 0
}

// ExtractMessage 从各家错误信封里抠一句人能读的话。
func ExtractMessage(body []byte) string {
	var m struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   *struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &m) == nil {
		if m.Message != "" {
			return strings.TrimSpace(m.Code + " " + m.Message)
		}
		if m.Error != nil && m.Error.Message != "" {
			return m.Error.Message
		}
	}
	s := strings.TrimSpace(string(body))
	if len(s) > 240 {
		s = s[:240] + "…"
	}
	return s
}

// HasAny 是 body 里是否出现任意一个标记（大小写原样，上游标记是大写常量）。
func HasAny(body []byte, marks ...string) bool {
	for _, m := range marks {
		if bytes.Contains(body, []byte(m)) {
			return true
		}
	}
	return false
}
