// Package usage 负责"从上游响应里读出用量"和"把用量换算成人民币"。
//
// 上游没有公开余额接口（/v1/me、/v1/usage、/v1/balance 全 404，响应头也无配额头），
// 所以剩余额度只能反推：充值额（人工填）− 累计成本（本地记账）。
// 成本口径优先取上游响应自带的结算字段（cost_cny / billing.total_cost_cny），
// 没有时才用 /v1/models 的单价 × usage 计算。
package usage

import (
	"encoding/json"
	"strconv"
)

// Usage 是从响应体里抠出来的用量。
type Usage struct {
	Prompt     int64
	Completion int64
	Cached     int64
	// Direct 是上游直接给出的结算金额（cost_cny / billing.total_cost_cny），
	// 有它就不用单价推算，直接用上游口径。
	Direct    float64
	HasDirect bool
	// Estimated 表示没有 usage、按字节估算出来的，面板上要标出来。
	Estimated bool
}

func (u *Usage) Add(o Usage) {
	if o.Prompt > 0 {
		u.Prompt = o.Prompt
	}
	if o.Completion > 0 {
		u.Completion = o.Completion
	}
	if o.Cached > 0 {
		u.Cached = o.Cached
	}
	if o.HasDirect {
		u.Direct = o.Direct
		u.HasDirect = true
	}
}

func (u Usage) Tokens() int64 { return u.Prompt + u.Completion }

// Parse 解析一份完整响应体（非流式）。兼容 OpenAI chat/completions、
// Anthropic messages、视频任务查询，以及只有顶层 cost_cny 的形态。
func Parse(body []byte) (Usage, bool) {
	if len(body) == 0 {
		return Usage{}, false
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return Usage{}, false
	}
	return fromMap(m)
}

// ParseChunk 解析 SSE 的一帧（data: 后面的 JSON 对象）。
func ParseChunk(payload []byte) (Usage, bool) {
	if len(payload) == 0 {
		return Usage{}, false
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		return Usage{}, false
	}
	return fromMap(m)
}

func fromMap(m map[string]any) (Usage, bool) {
	var u Usage
	found := false

	if raw, ok := m["usage"].(map[string]any); ok {
		// OpenAI
		if v, ok := num(raw["prompt_tokens"]); ok {
			u.Prompt, found = int64(v), true
		}
		if v, ok := num(raw["completion_tokens"]); ok {
			u.Completion, found = int64(v), true
		}
		// Anthropic
		if v, ok := num(raw["input_tokens"]); ok {
			u.Prompt, found = int64(v), true
		}
		if v, ok := num(raw["output_tokens"]); ok {
			u.Completion, found = int64(v), true
		}
		if d, ok := raw["prompt_tokens_details"].(map[string]any); ok {
			if v, ok := num(d["cached_tokens"]); ok {
				u.Cached = int64(v)
			}
		}
		if v, ok := num(raw["cache_read_input_tokens"]); ok {
			u.Cached = int64(v)
		}
	}

	// 视频：billing.total_cost_cny 是上游最终结算额，最可信。
	if b, ok := m["billing"].(map[string]any); ok {
		if v, ok := num(b["total_cost_cny"]); ok && v > 0 {
			u.Direct, u.HasDirect, found = v, true, true
		}
	}
	if v, ok := num(m["total_cost_cny"]); ok && v > 0 && !u.HasDirect {
		u.Direct, u.HasDirect, found = v, true, true
	}
	// chat/messages 的响应顶层直接带 cost_cny（可能是字符串），这是上游结算口径。
	if v, ok := num(m["cost_cny"]); ok && v > 0 && !u.HasDirect {
		u.Direct, u.HasDirect, found = v, true, true
	}
	return u, found
}

func num(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case int:
		return float64(t), true
	case json.Number:
		f, err := t.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(t, 64)
		return f, err == nil
	}
	return 0, false
}
