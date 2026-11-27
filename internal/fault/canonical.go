package fault

// 本文件是"对外错误契约"的唯一事实源：13 个 canonical code、各自的 HTTP 状态、
// 对外文案模板，以及从 `<status, code, type, message>` 到 canonical 的判定规则链。
//
// 为什么要单独一套东西：上游的 HTTP 状态码不是语义，只是传输层标签。实测同一家
// 上游的 429 里同时住着「限流」「余额耗尽」「服务过载」三种完全相反的结论，
// 而「余额耗尽」在不同上游可以是 402 / 403 / 429。任何 `status → 对外 status`
// 的对照表都会在第一次遇到 429 过载时把"上游挂了"报成"你请求太快"。
//
// 判定顺序（禁止跳级）：code 精确 → code 前缀 → type → message hint → status 兜底。
// 调度决策（Kind）与对外 code（Canonical）分开：对外契约面向调用方，
// Kind 面向号池，两者混用会同时弄坏两边。

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
)

// Canonical 是对外唯一允许出现的错误码。
type Canonical string

const (
	// 请求病：换号/重试都不会变好，N 个号就是 N 次无效调用 N 次计费。
	CInvalidRequest  Canonical = "invalid_request_error"
	CContextLength   Canonical = "context_length_exceeded"
	CContentPolicy   Canonical = "content_policy_violation"
	CRequestTooLarge Canonical = "request_too_large"

	// 账号/权限病。
	CAuthentication   Canonical = "authentication_error"
	CPermissionDenied Canonical = "permission_denied"
	CModelNotFound    Canonical = "model_not_found"

	// 配额病：限流会自愈，余额不会。
	CRateLimit         Canonical = "rate_limit_exceeded"
	CInsufficientQuota Canonical = "insufficient_quota"

	// 渠道病：整条上游在抖，计入熔断。
	CServerOverloaded Canonical = "server_is_overloaded"
	CApiError         Canonical = "api_error"
	CUpstreamTimeout  Canonical = "upstream_timeout"

	// 兜底：命中不了任何规则。落这里必须有人去补表，否则等于把映射权交给上游。
	CUnknown Canonical = "unknown_upstream_error"
)

// Spec 是一个 canonical code 的对外规格。
type Spec struct {
	HTTP int
	Type string
	Kind Kind
	// Retryable 表示"换号有意义"。false 时请求立刻回给客户端，不再走池子。
	Retryable   bool
	ModelScoped bool

	// RetryAfter 决定是否给客户端带 Retry-After 头。
	// insufficient_quota 是终态，给了时间会让客户端误以为等一会儿能恢复。
	RetryAfter bool
	// RetryAfterDefault 是上游没给时的默认秒数。
	RetryAfterDefault int

	// Message 是对外文案模板，可含 {retry_after} / {request_id}。
	// **上游返回的任何文本都不进入这里**——不是"过滤后透传"，是"不透传"。
	Message string
}

var specs = map[Canonical]Spec{
	CInvalidRequest: {
		HTTP: 400, Type: "invalid_request_error", Kind: Client,
		Message: "请求参数不合法，请检查后重试",
	},
	CContextLength: {
		HTTP: 400, Type: "invalid_request_error", Kind: Client,
		Message: "对话超出该模型的上下文窗口，请裁剪历史后重试",
	},
	CContentPolicy: {
		HTTP: 400, Type: "invalid_request_error", Kind: Client,
		Message: "请求被内容审核拦截",
	},
	CRequestTooLarge: {
		HTTP: 413, Type: "invalid_request_error", Kind: Client,
		Message: "请求体超过当前通道的上限，请减少输入后重试",
	},
	CAuthentication: {
		HTTP: 401, Type: "authentication_error", Kind: Auth, Retryable: true,
		Message: "上游凭据不可用，运维已收到通知",
	},
	CPermissionDenied: {
		HTTP: 403, Type: "permission_error", Kind: ModelDenied, Retryable: true, ModelScoped: true,
		Message: "当前通道无权使用所请求的模型",
	},
	CModelNotFound: {
		HTTP: 404, Type: "invalid_request_error", Kind: Client,
		Message: "所请求的模型在服务方不存在或未开通",
	},
	CRateLimit: {
		HTTP: 429, Type: "rate_limit_error", Kind: Rate, Retryable: true,
		RetryAfter: true, RetryAfterDefault: 60,
		Message: "请求过于频繁，请在 {retry_after} 秒后重试",
	},
	CInsufficientQuota: {
		HTTP: 429, Type: "insufficient_quota", Kind: Quota, Retryable: true,
		Message: "服务方额度已用尽，在服务方充值前重试无效",
	},
	CServerOverloaded: {
		HTTP: 503, Type: "service_unavailable_error", Kind: Upstream, Retryable: true,
		RetryAfter: true, RetryAfterDefault: 30,
		Message: "上游模型暂时过载，请稍后重试",
	},
	CApiError: {
		HTTP: 500, Type: "api_error", Kind: Upstream, Retryable: true,
		RetryAfter: true, RetryAfterDefault: 5,
		Message: "上游服务处理失败，请稍后重试",
	},
	CUpstreamTimeout: {
		HTTP: 504, Type: "timeout_error", Kind: Upstream, Retryable: true,
		Message: "上游未在时限内响应，请稍后重试",
	},
	CUnknown: {
		HTTP: 502, Type: "upstream_error", Kind: Upstream, Retryable: true,
		Message: "上游返回了无法归类的错误，运维已收到通知（ref {request_id}）",
	},
}

// SpecOf 取规格。未知 code 一律按兜底处理，绝不把未知值原样序列化出去。
func SpecOf(c Canonical) Spec {
	if s, ok := specs[c]; ok {
		return s
	}
	return specs[CUnknown]
}

// Message 按模板生成对外文案。上游文本永远不参与拼接。
func Message(c Canonical, retryAfter int, requestID string) string {
	m := SpecOf(c).Message
	if strings.Contains(m, "{retry_after}") {
		if retryAfter <= 0 {
			retryAfter = SpecOf(c).RetryAfterDefault
		}
		m = strings.ReplaceAll(m, "{retry_after}", strconv.Itoa(retryAfter))
	}
	m = strings.ReplaceAll(m, "{request_id}", requestID)
	return m
}

// ---------- 判定 ----------

// hint 表：只有拿不到 code/type 时才用，且必须整组命中。
// `quota` 这个词不能单独作为配额判据——上游既用它表示限速也表示余额耗尽。
var hints = map[string][]string{
	"quota": {
		"insufficient balance", "insufficient credit", "credit not enough",
		"not enough credit", "no balance", "balance is insufficient",
		"arrearage", "overdue", "欠费", "余额不足", "额度已用完",
		"quota has been exhausted", "free allocated quota exceeded",
		"exceeded your current quota",
	},
	"rate": {
		"rate limit", "too many requests", "throttl", "requests per minute",
		"tokens per minute", "frequency limit", "请求过于频繁", "限流", "并发",
	},
	"overloaded": {
		"overloaded", "over capacity", "server is busy", "temporarily unavailable",
		"capacity", "繁忙", "负载过高",
	},
	"context": {
		"context length", "maximum context", "context window", "input length",
		"too long", "exceeds the maximum", "上下文长度", "输入超长",
	},
	"content": {
		"content policy", "content management policy", "sensitive", "moderation",
		"flagged", "审核", "敏感",
	},
}

// Rule 是一条判定规则。字段之间是 AND，同一字段内是 OR。
type Rule struct {
	ID     string
	Status []int
	// Codes 是精确匹配（大小写不敏感）；CodePrefix 用于分层 code。
	Codes      []string
	CodePrefix []string
	Types      []string
	// Hints 是 hint 组名，多组之间 AND。
	Hints []string
	// RequiresError 只对 2xx 有意义：200 里带 error 成员也算失败。
	RequiresError bool

	Canonical Canonical
	// Kind 缺省取 Canonical 的 Kind；只有"对外一个语义、调度另一种处理"时才覆盖。
	Kind        Kind
	ModelScoped bool
}

// providerRules 是针对当前上游（tokenrhythm 中转站）实测到的业务码。
// 顺序即优先级：更具体的必须排在更宽泛的前面。
var providerRules = []Rule{
	{ID: "tr.quota.balance", Codes: []string{
		"INSUFFICIENT_BALANCE", "INSUFFICIENT_QUOTA", "QUOTA_EXHAUSTED",
		"BALANCE_INSUFFICIENT", "INSUFFICIENT_CREDIT",
	}, Canonical: CInsufficientQuota},
	// 对外是"上游凭据不可用"(401)，内部是冻结号（终态 blocked）：
	// 对外不能让调用方以为自己的 key 错了，内部也不能把它当普通失效号反复探活。
	{ID: "tr.account.suspended", Codes: []string{
		"ACCOUNT_SUSPENDED", "ACCOUNT_BANNED", "ACCOUNT_FROZEN", "ACCOUNT_DISABLED",
	}, Canonical: CAuthentication, Kind: Banned},
	{ID: "tr.model.denied", Codes: []string{
		"MODEL_ACCESS_DENIED", "MODEL_NOT_ENABLED", "MODEL_FORBIDDEN",
	}, Canonical: CPermissionDenied},
	{ID: "tr.model.missing", Codes: []string{
		"MODEL_NOT_AVAILABLE", "MODEL_NOT_FOUND", "MODEL_DOES_NOT_EXIST", "INVALID_MODEL",
	}, Canonical: CModelNotFound},
	// 模型供应链限流：会自愈，但换号救不了（只冷 号×模型），所以 Kind 单独一支。
	{ID: "tr.rate.upstream", Codes: []string{"UPSTREAM_RATE_LIMITED"},
		Canonical: CRateLimit, Kind: UpstreamRate, ModelScoped: true},
	{ID: "tr.rate.account", Codes: []string{"RATE_LIMITED", "TOO_MANY_REQUESTS"},
		Canonical: CRateLimit, Kind: Rate},
	{ID: "tr.auth", Codes: []string{
		"UNAUTHORIZED", "INVALID_API_KEY", "API_KEY_INVALID", "INVALID_TOKEN",
	}, Canonical: CAuthentication},
	{ID: "tr.overloaded", Codes: []string{
		"SERVER_OVERLOADED", "OVERLOADED", "SERVICE_UNAVAILABLE", "CAPACITY_EXCEEDED",
	}, Canonical: CServerOverloaded},
	{ID: "tr.context", Codes: []string{"CONTEXT_LENGTH_EXCEEDED", "CONTEXT_TOO_LONG"},
		Canonical: CContextLength},
	{ID: "tr.content", Codes: []string{
		"CONTENT_POLICY_VIOLATION", "CONTENT_FILTERED", "SENSITIVE_CONTENT",
	}, Canonical: CContentPolicy},
	{ID: "tr.invalid", Codes: []string{
		"INVALID_REQUEST", "INVALID_PARAMETER", "BAD_REQUEST", "MISSING_PARAMETER",
	}, Canonical: CInvalidRequest},
}

// vendorRules 是"上游把我们当透明管道、把厂商自己的错误体原样透出来"时用的。
// 这些 code/type 是各家文档里的固定值，比 status 可信，因此排在 status 兜底之前。
var vendorRules = []Rule{
	{ID: "vendor.overloaded", Codes: []string{
		"engine_overloaded_error", "serveroverloaded", "server_overloaded",
		"service_unavailable_error",
	}, Canonical: CServerOverloaded},
	{ID: "vendor.quota", Codes: []string{
		"exceeded_current_quota_error", "accountoverdueerror", "arrearage",
		"quotaexceeded", "insufficient_quota", "credit_balance_exhausted",
		"organization_spend_limit_exceeded", "project_spend_limit_exceeded",
	}, Canonical: CInsufficientQuota},
	// 百炼的免费额度耗尽藏在一个看起来像限流的 code 里，必须先于 Throttling 判定。
	{ID: "vendor.qwen_allocation_quota", CodePrefix: []string{"throttling.allocationquota"},
		Canonical: CInsufficientQuota},
	{ID: "vendor.throttling", CodePrefix: []string{"throttling"},
		Canonical: CRateLimit},
	{ID: "vendor.rate", Codes: []string{
		"rate_limit_reached_error", "ratelimitexceeded", "slow_down",
	}, Canonical: CRateLimit},
	{ID: "vendor.auth", Codes: []string{
		"invalid_authentication_error", "invalid_api_key", "authentication_error",
	}, Canonical: CAuthentication},
	{ID: "vendor.model", Codes: []string{"model_not_found", "invalidmodel"},
		Canonical: CModelNotFound},
	{ID: "vendor.context", Codes: []string{"context_length_exceeded"},
		Canonical: CContextLength},
}

// genericRules 是"没有 provider 证据时"的降级链。顺序照 taxonomy：
// 先排除过载与配额，再落限流；5xx 里再分超时与过载。
var genericRules = []Rule{
	{ID: "generic.401", Status: []int{401}, Canonical: CAuthentication},
	{ID: "generic.402", Status: []int{402}, Canonical: CInsufficientQuota},
	{ID: "generic.403_quota", Status: []int{403}, Hints: []string{"quota"}, Canonical: CInsufficientQuota},
	{ID: "generic.403", Status: []int{403}, Canonical: CPermissionDenied},
	{ID: "generic.404", Status: []int{404}, Canonical: CModelNotFound},
	{ID: "generic.413", Status: []int{413}, Canonical: CRequestTooLarge},
	{ID: "generic.422", Status: []int{422}, Canonical: CInvalidRequest},
	{ID: "generic.400_context", Status: []int{400}, Hints: []string{"context"}, Canonical: CContextLength},
	{ID: "generic.400_content", Status: []int{400}, Hints: []string{"content"}, Canonical: CContentPolicy},
	{ID: "generic.400", Status: []int{400}, Canonical: CInvalidRequest},
	{ID: "generic.429_quota", Status: []int{429}, Hints: []string{"quota"}, Canonical: CInsufficientQuota},
	{ID: "generic.429_overloaded", Status: []int{429}, Hints: []string{"overloaded"}, Canonical: CServerOverloaded},
	{ID: "generic.429", Status: []int{429}, Canonical: CRateLimit},
	{ID: "generic.408", Status: []int{408}, Canonical: CUpstreamTimeout},
	{ID: "generic.503", Status: []int{503}, Canonical: CServerOverloaded},
	{ID: "generic.504", Status: []int{504}, Canonical: CUpstreamTimeout},
	{ID: "generic.529", Status: []int{529}, Canonical: CServerOverloaded},
	{ID: "generic.5xx", Status: []int{500, 502, 505, 507, 508, 510, 511}, Canonical: CApiError},
}

// rawFields 是从上游信封里抠出来的三要素（只给运维看）。
type rawFields struct {
	Code    string
	Type    string
	Message string
}

type envelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Msg     string `json:"msg"`
	Type    string `json:"type"`
	TraceID string `json:"traceId"`
	Error   *struct {
		Code    string `json:"code"`
		Type    string `json:"type"`
		Message string `json:"message"`
		Msg     string `json:"msg"`
	} `json:"error"`
	ResponseMetadata *struct {
		Error *struct {
			Code    string `json:"Code"`
			Message string `json:"Message"`
		} `json:"Error"`
	} `json:"ResponseMetadata"`
}

func (e envelope) fields(body []byte) rawFields {
	var f rawFields
	if e.Error != nil {
		f.Code = strings.TrimSpace(e.Error.Code)
		f.Type = strings.TrimSpace(e.Error.Type)
		f.Message = firstNonEmpty(e.Error.Message, e.Error.Msg)
	}
	if f.Code == "" {
		f.Code = strings.TrimSpace(e.Code)
	}
	if f.Type == "" {
		f.Type = strings.TrimSpace(e.Type)
	}
	if f.Message == "" {
		f.Message = firstNonEmpty(e.Message, e.Msg)
	}
	if f.Code == "" && e.ResponseMetadata != nil && e.ResponseMetadata.Error != nil {
		f.Code = strings.TrimSpace(e.ResponseMetadata.Error.Code)
	}
	if f.Message == "" && e.ResponseMetadata != nil && e.ResponseMetadata.Error != nil {
		f.Message = strings.TrimSpace(e.ResponseMetadata.Error.Message)
	}
	if f.Message == "" && f.Code == "" {
		// 非 JSON（HTML 错误页、代理横幅）：原文只留给运维，不进对外文案。
		s := strings.TrimSpace(string(body))
		if len(s) > 240 {
			s = s[:240] + "…"
		}
		f.Message = s
	}
	return f
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if s := strings.TrimSpace(v); s != "" {
			return s
		}
	}
	return ""
}

// hasErrorField 报告 2xx 响应体里是否还带着非空 error 成员。
//
// 先做子串预检：正常响应几乎都没有 `"error"` 这个键，省掉一次完整 JSON 解析。
func hasErrorField(body []byte) bool {
	if !bytes.Contains(body, []byte(`"error"`)) && !bytes.Contains(body, []byte(`"Error"`)) {
		return false
	}
	var v any
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if dec.Decode(&v) != nil {
		return false
	}
	return walkError(v, 0)
}

func walkError(v any, depth int) bool {
	if depth > 3 {
		return false
	}
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			if strings.EqualFold(k, "error") && !emptyJSON(val) {
				return true
			}
			if walkError(val, depth+1) {
				return true
			}
		}
	case []any:
		for _, it := range t {
			if walkError(it, depth+1) {
				return true
			}
		}
	}
	return false
}

func emptyJSON(v any) bool {
	switch t := v.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(t) == ""
	case map[string]any:
		return len(t) == 0
	case []any:
		return len(t) == 0
	case bool:
		return !t
	}
	return false
}

func (r *Rule) match(status int, f rawFields, hasErr bool) bool {
	if r.RequiresError && !hasErr {
		return false
	}
	if len(r.Status) > 0 && !containsInt(r.Status, status) {
		return false
	}
	if len(r.Codes) > 0 || len(r.CodePrefix) > 0 {
		code := strings.ToLower(f.Code)
		typeName := strings.ToLower(f.Type)
		hit := false
		for _, c := range r.Codes {
			c = strings.ToLower(c)
			if code != "" && code == c || typeName != "" && typeName == c {
				hit = true
				break
			}
		}
		if !hit {
			for _, p := range r.CodePrefix {
				p = strings.ToLower(p)
				if code != "" && strings.HasPrefix(code, p) ||
					typeName != "" && strings.HasPrefix(typeName, p) {
					hit = true
					break
				}
			}
		}
		if !hit {
			return false
		}
	}
	if len(r.Types) > 0 {
		hit := false
		for _, t := range r.Types {
			if strings.EqualFold(f.Type, t) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	// 每组 hint 至少命中一个词；多组之间是 AND。
	msg := strings.ToLower(f.Message)
	for _, group := range r.Hints {
		hit := false
		for _, needle := range hints[group] {
			if strings.Contains(msg, strings.ToLower(needle)) {
				hit = true
				break
			}
		}
		if !hit {
			return false
		}
	}
	return true
}

func containsInt(xs []int, v int) bool {
	for _, x := range xs {
		if x == v {
			return true
		}
	}
	return false
}

// classifyRaw 是 Classify 的核心：返回 canonical、命中的规则 id 与原始三要素。
func classifyRaw(status int, body []byte, hasErr bool) hit {
	var env envelope
	_ = json.Unmarshal(body, &env)
	f := env.fields(body)

	// 2xx 里带 error 成员：不管里面写的是什么 code，这条响应本身就是坏的，
	// 不能当成功（会记一条空补全并计费）。必须排在所有 code 规则之前，
	// 否则一个"200 + MODEL_NOT_AVAILABLE"会被判成模型不存在（404）。
	if hasErr && status >= 200 && status < 300 {
		return hit{Canonical: CApiError, RuleID: "generic.2xx_with_error", Raw: f, Kind: Upstream}
	}

	for i := range providerRules {
		if providerRules[i].match(status, f, hasErr) {
			return providerRules[i].toHit(f)
		}
	}
	for i := range vendorRules {
		if vendorRules[i].match(status, f, hasErr) {
			return vendorRules[i].toHit(f)
		}
	}
	for i := range genericRules {
		if genericRules[i].match(status, f, hasErr) {
			return genericRules[i].toHit(f)
		}
	}
	return hit{Canonical: CUnknown, RuleID: "fallback", Raw: f, Kind: Upstream}
}

// hit 是一条规则的匹配结果。
type hit struct {
	Canonical   Canonical
	RuleID      string
	Raw         rawFields
	Kind        Kind
	ModelScoped bool
}

func (r *Rule) toHit(f rawFields) hit {
	c := r.Canonical
	if c == "" {
		c = CUnknown
	}
	k := r.Kind
	if k == "" {
		k = SpecOf(c).Kind
	}
	ms := r.ModelScoped || SpecOf(c).ModelScoped
	return hit{Canonical: c, RuleID: r.ID, Raw: f, Kind: k, ModelScoped: ms}
}
