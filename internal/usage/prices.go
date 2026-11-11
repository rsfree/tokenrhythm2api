package usage

import (
	"encoding/json"
	"sort"
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
	mu        sync.RWMutex
	m         map[string]Price
	at        time.Time
	via       string
	lastError string
}

func NewPrices() *Prices { return &Prices{m: map[string]Price{}} }

type modelEntry struct {
	ID        string      `json:"id"`
	Currency  string      `json:"currency"`
	Pricing   *priceBlock `json:"pricing"`
	Effective *priceBlock `json:"effective_pricing"`
	Discount  *priceBlock `json:"discount_pricing"`
}

type priceBlock struct {
	Currency   string  `json:"currency"`
	Unit       string  `json:"unit"`
	Prompt     flexNum `json:"prompt"`
	Completion flexNum `json:"completion"`
	CacheRead  flexNum `json:"cache_read"`
}

// flexNum 兼容上游把单价写成字符串（"0.8"）或数字（0.8）两种形态。
type flexNum float64

func (f *flexNum) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || v < 0 {
		*f = 0
		return nil
	}
	*f = flexNum(v)
	return nil
}

// Load 解析 GET /v1/models 的响应体。优先 effective（折后价），其次 pricing。
func (p *Prices) Load(body []byte, via string) error {
	var resp struct {
		Data []modelEntry `json:"data"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		p.mu.Lock()
		p.lastError = err.Error()
		p.mu.Unlock()
		return err
	}
	m := make(map[string]Price, len(resp.Data))
	for _, e := range resp.Data {
		if e.ID == "" {
			continue
		}
		blk := e.Effective
		if blk == nil || (blk.Prompt == 0 && blk.Completion == 0) {
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
		m[strings.ToLower(e.ID)] = Price{
			ID:        e.ID,
			Input:     float64(blk.Prompt),
			Output:    float64(blk.Completion),
			CacheRead: float64(blk.CacheRead),
			Currency:  cur,
		}
	}
	p.mu.Lock()
	p.m = m
	p.at = time.Now()
	p.via = via
	p.lastError = ""
	p.mu.Unlock()
	return nil
}

func (p *Prices) NoteError(msg string) {
	p.mu.Lock()
	p.lastError = msg
	p.mu.Unlock()
}

func (p *Prices) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.m)
}

// Snapshot 返回价目表快照，按模型名（大小写不敏感）排序：
// map 遍历无序，直接塞给前端会每次刷新都跳。
func (p *Prices) Snapshot() []Price {
	p.mu.RLock()
	out := make([]Price, 0, len(p.m))
	for _, v := range p.m {
		out = append(out, v)
	}
	p.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool {
		return strings.ToLower(out[i].ID) < strings.ToLower(out[j].ID)
	})
	return out
}

func (p *Prices) UpdatedAt() time.Time {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.at
}

func (p *Prices) Meta() map[string]any {
	p.mu.RLock()
	defer p.mu.RUnlock()
	m := map[string]any{
		"count":      len(p.m),
		"updated_at": p.at.Format(time.RFC3339),
		"via":        p.via,
	}
	if p.lastError != "" {
		m["last_error"] = p.lastError
	}
	return m
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

// Paid 判断模型是否收费。未知模型按收费处理（保守，避免把余额打空）。
func (p *Prices) Paid(model string) bool {
	pr, ok := p.Get(model)
	if !ok {
		return true
	}
	return pr.Paid()
}

// Cost 按价目表算一笔成本（CNY）。上游直接给了结算额就以上游为准。
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
