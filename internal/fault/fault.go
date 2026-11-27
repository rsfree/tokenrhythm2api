// Package fault 把上游响应翻译成两套结论：对外契约（Canonical）与调度决策（Kind）。
//
// 单独成包的原因：传输层（upstream）不该知道号池，号池（pool）也不该解析 HTTP body。
// 分类口径只此一份，server / scheduler / panel 都从这里取结论，避免"同一个 403
// 在三个地方有三种解释"。
//
// 两套结论不能混用：
//   - Canonical 是**对外唯一允许出现的错误码**（13 个，见 canonical.go），
//     调用方按它决定自己要不要重试；
//   - Kind 是**内部调度语义**，决定换号、冷却、熔断、是否终态停用。
//
// 混用的后果是双向的：拿对外 status 去决定重试（429 既可能是限流也可能余额耗尽），
// 或者拿内部 Kind 去回给客户端（号池状态属于内部经营数据）。
package fault

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
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
	// Rate 是本平台的次数配额（429 RATE_LIMITED）：罚号，按上游给的
	// retryAfter 精确冷却。多开号能加总配额，所以它算"扩容压力"。
	Rate Kind = "rate"
	// Banned 是账号被平台冻结/封禁（403 ACCOUNT_SUSPENDED）：这不是次数
	// 配额，多开号也救不回一个被封的账号，只能换号 + 长退避等它自己解开。
	// 因此它不计入 429 压力，也不该用 429 的秒级冷却反复去撞。
	Banned Kind = "banned"
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

// Verdict 是一次上游失败的完整结论：对外契约 + 内部调度 + 运维原文。
type Verdict struct {
	Kind      Kind
	Canonical Canonical
	// RuleID 是命中的规则名，未知 code 落 "fallback" 时要靠它补表。
	RuleID string

	// Status 是上游原始状态码，只进运维记录。
	Status int
	// RetryAfter 是归一后的冷却时长（仅 rate/overload/api_error 类有值）。
	RetryAfter time.Duration
	// RetryAfterHint 表示这个时长来自上游的明确指示（头或 body），
	// 而不是我们按 canonical 填的默认值。前者说明"上游自己知道要等多久"，
	// 这时候立刻在同一账号上重试是白撞。
	RetryAfterHint bool
	// Message / RawCode / RawType 是上游原文，**只进运维记录，不进对外响应**。
	Message string
	RawCode string
	RawType string

	// ModelScoped 表示这个失败只对当前模型成立：换号前先看别的号有没有该模型，
	// 换模型即可绕过。
	ModelScoped bool
	// Retryable 表示换号/重试有意义。false 时立刻回给客户端。
	Retryable bool
}

// HTTP 返回对外状态码。
func (v Verdict) HTTP() int { return SpecOf(v.Canonical).HTTP }

// Type 返回对外的 error.type。
func (v Verdict) Type() string { return SpecOf(v.Canonical).Type }

// CallerMessage 生成对外文案。
func (v Verdict) CallerMessage(requestID string) string {
	return Message(v.Canonical, int(v.RetryAfter/time.Second), requestID)
}

// Classify 按 `<status, code, type, message>` 判定失败语义。
//
// 判定顺序（禁止跳级）：provider 业务码 → code 前缀 → type → hint → status 兜底。
// 只看状态码会在第一次遇到"429 = 上游过载"时判错，那家上游的 429 里同时住着
// 限流、欠费与过载三种结论。
func Classify(status int, body []byte, hdr http.Header) Verdict {
	hasErr := status >= 200 && status < 300 && SuccessCarriesError(body)
	// 2xx/3xx 且响应体里没有 error 成员：这次没有失败，别硬套一个 canonical。
	if status > 0 && status < 400 && !hasErr {
		return Verdict{Kind: None, Status: status}
	}
	h := classifyRaw(status, body, hasErr)

	sp := SpecOf(h.Canonical)
	v := Verdict{
		Kind:        h.Kind,
		Canonical:   h.Canonical,
		RuleID:      h.RuleID,
		Status:      status,
		Message:     h.Raw.Message,
		RawCode:     h.Raw.Code,
		RawType:     h.Raw.Type,
		ModelScoped: h.ModelScoped,
		Retryable:   sp.Retryable,
	}
	if sp.RetryAfter {
		ra := RetryAfter(hdr, body)
		if ra > 0 {
			v.RetryAfterHint = true
		} else {
			ra = time.Duration(sp.RetryAfterDefault) * time.Second
		}
		v.RetryAfter = ra
	}
	return v
}

// ClassifyNet 把传输层错误归到 canonical。
//
// 超时 = 上游没在时限内响应（504）；连接失败/被重置 = 压根没拿到响应（502）。
// 两者的差别对调用方有意义：超时可以原样重试，连接失败更可能是地址/网络问题。
func ClassifyNet(err error) Verdict {
	c := CUnknown
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		c = CUpstreamTimeout
	} else {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			c = CUpstreamTimeout
		}
	}
	sp := SpecOf(c)
	return Verdict{
		Kind:      sp.Kind,
		Canonical: c,
		RuleID:    "transport",
		Message:   err.Error(),
		Retryable: sp.Retryable,
	}
}

// SuccessCarriesError 报告一个 2xx 响应体里是否还带着非空 error 成员。
//
// 部分中转站用 200 包错误。当成功处理会记一条空补全并计费，比报错更糟。
func SuccessCarriesError(body []byte) bool { return hasErrorField(body) }

// RetryAfter 依次看 Retry-After 头与 body 里的 data.retryAfterSeconds，
// 归一为秒并截断到 900s：上游可能给 HTTP-date（不少客户端解析不了，退化成
// 立即重试，一次 429 就变成雪崩），也可能给一个离谱的大数把客户端挂死。
func RetryAfter(hdr http.Header, body []byte) time.Duration {
	if hdr != nil {
		if v := strings.TrimSpace(hdr.Get("Retry-After")); v != "" {
			// "60s" 在野生的上游里很常见，先剥掉单位再解析。
			raw := strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(v, "s"), "S"))
			if n, err := strconv.ParseFloat(raw, 64); err == nil && n > 0 {
				return capRetryAfter(time.Duration(n * float64(time.Second)))
			}
			if t, err := http.ParseTime(v); err == nil {
				if d := time.Until(t); d > 0 {
					return capRetryAfter(d)
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
		return capRetryAfter(time.Duration(m.Data.RetryAfterSeconds * float64(time.Second)))
	}
	return 0
}

const retryAfterCap = 900 * time.Second

func capRetryAfter(d time.Duration) time.Duration {
	if d > retryAfterCap {
		return retryAfterCap
	}
	return d
}

// ExtractMessage 从各家错误信封里抠一句人能读的话（只给运维）。
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
