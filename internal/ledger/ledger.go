// Package ledger 负责两件事：把上游价目表变成可计算的成本，把每笔请求记成账。
//
// 为什么不用上游余额接口：tokenrhythm 未公开余额端点（/v1/me、/v1/usage 等全 404，
// 响应头也无配额头）。所以"剩余额度"由本地记账反推：充值额 - 累计成本。
// 成本口径直接取上游 /v1/models 里的单价，不是我们估的。
package ledger

import (
	"encoding/json"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Price 是某模型的单价，单位：CNY / 百万 token。
type Price struct {
	ID        string  `json:"id"`
	Input     float64 `json:"input"`
	Output    float64 `json:"output"`
	CacheRead float64 `json:"cache_read"`
	Currency  string  `json:"currency"`
}

func (p Price) Paid() bool { return p.Input > 0 || p.Output > 0 }

// Prices 是价目表缓存，支持热更新。
type Prices struct {
	mu   sync.RWMutex
	m    map[string]Price
	at   time.Time
	note string
}

func NewPrices() *Prices {
	return &Prices{m: map[string]Price{}}
}

type modelEntry struct {
	ID        string      `json:"id"`
	Currency  string      `json:"currency"`
	Pricing   *priceBlock `json:"pricing"`
	Effective *priceBlock `json:"effective_pricing"`
	Discount  *priceBlock `json:"discount_pricing"`
}

type priceBlock struct {
	Currency   string `json:"currency"`
	Unit       string `json:"unit"`
	Prompt     string `json:"prompt"`
	Completion string `json:"completion"`
	CacheRead  string `json:"cache_read"`
}

func toF(s string) float64 {
	if s == "" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		return 0
	}
	return v
}

// Load 解析 GET /v1/models 的响应体。优先 effective（折后价），其次 pricing。
func (p *Prices) Load(body []byte) error {
	var resp struct {
		Data []modelEntry `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	m := make(map[string]Price, len(resp.Data))
	for _, e := range resp.Data {
		if e.ID == "" {
			continue
		}
		blk := e.Effective
		if blk == nil || (blk.Prompt == "" && blk.Completion == "") {
			blk = e.Pricing
		}
		if blk == nil {
			blk = e.Discount
		}
		if blk == nil {
			continue
		}
		cur := blk.Currency
		if cur == "" {
			cur = e.Currency
		}
		pr := Price{
			ID:        e.ID,
			Input:     toF(blk.Prompt),
			Output:    toF(blk.Completion),
			CacheRead: toF(blk.CacheRead),
			Currency:  cur,
		}
		m[strings.ToLower(e.ID)] = pr
	}
	p.mu.Lock()
	p.m = m
	p.at = time.Now()
	p.note = ""
	p.mu.Unlock()
	return nil
}

func (p *Prices) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.m)
}

func (p *Prices) UpdatedAt() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.at
}

// Get 支持模糊回落：请求名带厂商前缀（如 openai/gpt-x）时，去掉前缀再找一次。
func (p *Prices) Get(model string) (Price, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	key := strings.ToLower(strings.TrimSpace(model))
	if v, ok := p.m[key]; ok {
		return v, true
	}
	if i := strings.LastIndex(key, "/"); i >= 0 {
		if v, ok := p.m[key[i+1:]]; ok {
			return v, true
		}
	}
	return Price{}, false
}

// Paid 判断模型是否收费。未知模型按收费处理（宁可保守，避免把余额打空）。
func (p *Prices) Paid(model string) bool {
	pr, ok := p.Get(model)
	if !ok {
		return true
	}
	return pr.Paid()
}

// Usage 是从上游响应体里抠出来的用量。
type Usage struct {
	Prompt     int64
	Completion int64
	Cached     int64
	// Direct 是上游直接给出的结算金额（视频接口的 billing.total_cost_cny），
	// 有它就不用单价推算，直接用上游口径。
	Direct    float64
	HasDirect bool
	// Estimated 表示没有 usage、按字符估算出来的，面板上要标出来。
	Estimated bool
}

// ParseUsage 兼容四种口径：OpenAI chat/completions、Anthropic messages、
// 视频任务查询、以及只有 result 没有 usage 的兜底。
func ParseUsage(body []byte) (Usage, bool) {
	if len(body) == 0 {
		return Usage{}, false
	}
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return Usage{}, false
	}
	var u Usage
	found := false

	if raw, ok := m["usage"].(map[string]any); ok {
		// OpenAI
		if v, ok := num(raw["prompt_tokens"]); ok {
			u.Prompt = int64(v)
			found = true
		}
		if v, ok := num(raw["completion_tokens"]); ok {
			u.Completion = int64(v)
			found = true
		}
		// Anthropic
		if v, ok := num(raw["input_tokens"]); ok {
			u.Prompt = int64(v)
			found = true
		}
		if v, ok := num(raw["output_tokens"]); ok {
			u.Completion = int64(v)
			found = true
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
			u.Direct = v
			u.HasDirect = true
			found = true
		}
	}
	if v, ok := num(m["total_cost_cny"]); ok && v > 0 && !u.HasDirect {
		u.Direct = v
		u.HasDirect = true
		found = true
	}
	// chat/messages 的响应顶层直接带 cost_cny（字符串），这是上游结算口径，
	// 比我们用价目表推算更权威，有就优先用。
	if v, ok := num(m["cost_cny"]); ok && v > 0 && !u.HasDirect {
		u.Direct = v
		u.HasDirect = true
		found = true
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

// Cost 按价目表算一笔成本（CNY）。
func (p *Prices) Cost(model string, u Usage) float64 {
	if u.HasDirect {
		return u.Direct
	}
	pr, ok := p.Get(model)
	if !ok {
		return 0
	}
	prompt := float64(u.Prompt)
	cached := float64(u.Cached)
	if cached > prompt {
		cached = prompt
	}
	billable := prompt - cached
	cachePrice := pr.CacheRead
	if cachePrice <= 0 {
		cachePrice = pr.Input
	}
	cost := billable/1e6*pr.Input + cached/1e6*cachePrice + float64(u.Completion)/1e6*pr.Output
	if cost < 0 {
		return 0
	}
	return cost
}

// ---------- 请求日志环 ----------

type Entry struct {
	Time       time.Time `json:"time"`
	KeyID      string    `json:"key_id"`
	KeyName    string    `json:"key_name"`
	Model      string    `json:"model"`
	Path       string    `json:"path"`
	Status     int       `json:"status"`
	LatencyMS  int64     `json:"latency_ms"`
	Prompt     int64     `json:"prompt_tokens"`
	Completion int64     `json:"completion_tokens"`
	Cost       float64   `json:"cost"`
	Stream     bool      `json:"stream"`
	Attempt    int       `json:"attempt"`
	TraceID    string    `json:"trace_id,omitempty"`
	Error      string    `json:"error,omitempty"`
}

// Log 是定长的请求日志环，写满后覆盖最旧的。
type Log struct {
	mu   sync.RWMutex
	buf  []Entry
	size int
	pos  int
	full bool
}

func NewLog(size int) *Log {
	if size <= 0 {
		size = 400
	}
	return &Log{buf: make([]Entry, size), size: size}
}

func (l *Log) Add(e Entry) {
	l.mu.Lock()
	l.buf[l.pos] = e
	l.pos = (l.pos + 1) % l.size
	if l.pos == 0 {
		l.full = true
	}
	l.mu.Unlock()
}

// Recent 返回最近 n 条，按时间倒序（新的在前）。
func (l *Log) Recent(n int) []Entry {
	l.mu.RLock()
	defer l.mu.RUnlock()
	total := l.pos
	if l.full {
		total = l.size
	}
	if n <= 0 || n > total {
		n = total
	}
	out := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		idx := (l.pos - 1 - i + l.size) % l.size
		out = append(out, l.buf[idx])
	}
	return out
}

// ---------- 日统计 ----------

type Day struct {
	Date       string  `json:"date"`
	Requests   int64   `json:"requests"`
	Errors     int64   `json:"errors"`
	Prompt     int64   `json:"prompt_tokens"`
	Completion int64   `json:"completion_tokens"`
	Cost       float64 `json:"cost"`
}

type Daily struct {
	mu   sync.Mutex
	Days map[string]*Day `json:"days"`
}

func NewDaily() *Daily { return &Daily{Days: map[string]*Day{}} }

func (d *Daily) Add(now time.Time, prompt, completion int64, cost float64, isErr bool) {
	key := now.Format("2006-01-02")
	d.mu.Lock()
	defer d.mu.Unlock()
	day := d.Days[key]
	if day == nil {
		day = &Day{Date: key}
		d.Days[key] = day
	}
	day.Requests++
	if isErr {
		day.Errors++
	}
	day.Prompt += prompt
	day.Completion += completion
	day.Cost += cost
}

// Last 返回最近 n 天，按日期倒序。
func (d *Daily) Last(n int) []Day {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Day, 0, len(d.Days))
	for _, v := range d.Days {
		out = append(out, *v)
	}
	for i := 0; i < len(out); i++ {
		for j := i + 1; j < len(out); j++ {
			if out[j].Date > out[i].Date {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}
