// Package pool 是号池的调度核心：状态机、选号、冷却、在途控制、视频任务粘性。
//
// 三条不变量（破坏了就会出脏账或幽灵请求）：
//  1. 只有 Active 且未冷却、未满在途的号才会被选中；
//  2. 状态转移只发生在 Release/NoteFail 里，选号过程是只读的；
//  3. 粘性任务（视频查询）永远回到创建它的号，哪怕那个号正在冷却。
package pool

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"sync"
	"time"

	"trpool/internal/config"
)

type State string

const (
	StateActive    State = "active"
	StateCooling   State = "cooling"
	StateInvalid   State = "invalid"   // 401，需人工重置
	StateExhausted State = "exhausted" // 402/403，额度或权限
	StateDisabled  State = "disabled"  // 人工摘号
)

// FailKind 决定状态怎么转。
type FailKind int

const (
	FailClient   FailKind = iota // 4xx 是请求本身的问题，不记号的账
	FailRate                     // 429
	FailAuth                     // 401
	FailQuota                    // 402/403
	FailUpstream                 // 5xx / 网络 / 超时
)

func (f FailKind) String() string {
	switch f {
	case FailRate:
		return "rate"
	case FailAuth:
		return "auth"
	case FailQuota:
		return "quota"
	case FailUpstream:
		return "upstream"
	default:
		return "client"
	}
}

// Key 是一个上游凭证的完整运行时状态。
type Key struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Secret string `json:"secret"`

	Weight  float64 `json:"weight"` // 手动权重倍率
	Enabled bool    `json:"enabled"`
	Budget  float64 `json:"budget"` // 充值额（CNY），0 = 未知

	State         State     `json:"state"`
	CooldownUntil time.Time `json:"cooldown_until"`
	FailStreak    int       `json:"fail_streak"`
	LastError     string    `json:"last_error"`
	LastErrorAt   time.Time `json:"last_error_at"`

	SpentCNY         float64   `json:"spent_cny"`
	Requests         int64     `json:"requests"`
	Errors           int64     `json:"errors"`
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	LastUsedAt       time.Time `json:"last_used_at"`
	LastOKAt         time.Time `json:"last_ok_at"`
	CreatedAt        time.Time `json:"created_at"`

	inflight int
}

// Balance 返回本地记账口径的剩余额度；Budget<=0 表示上游额度未知。
func (k *Key) Balance() float64 {
	if k.Budget <= 0 {
		return -1
	}
	b := k.Budget - k.SpentCNY
	if b < 0 {
		return 0
	}
	return b
}

func (k *Key) Masked() string {
	s := k.Secret
	if len(s) <= 14 {
		return "****"
	}
	return s[:10] + "..." + s[len(s)-4:]
}

// Lease 是一次选号的结果。Release 必须调用，否则在途计数会永久泄漏。
type Lease struct {
	KeyID  string
	Secret string
	Name   string
	Tried  bool // 内部用：本轮已试过
}

// Pool 持有全部号与调度状态。
type Pool struct {
	mu    sync.RWMutex
	keys  map[string]*Key
	order []string
	cfg   config.PoolConfig

	rr            int
	totalInflight int

	sticky   map[string]stickyEntry
	onChange func()
}

type stickyEntry struct {
	KeyID string    `json:"key_id"`
	At    time.Time `json:"at"`
}

var ErrNoKey = errors.New("池内无可用密钥")

func New(cfg config.PoolConfig) *Pool {
	return &Pool{
		keys:   map[string]*Key{},
		cfg:    cfg,
		sticky: map[string]stickyEntry{},
	}
}

func (p *Pool) SetOnChange(fn func()) { p.onChange = fn }

func (p *Pool) notify() {
	if p.onChange != nil {
		p.onChange()
	}
}

func (p *Pool) SetConfig(cfg config.PoolConfig) {
	p.mu.Lock()
	p.cfg = cfg
	p.mu.Unlock()
	p.notify()
}

func (p *Pool) Config() config.PoolConfig {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.cfg
}

// ---------- 增删改 ----------

func (p *Pool) Add(name, secret string, weight, budget float64) (*Key, error) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return nil, fmt.Errorf("密钥不能为空")
	}
	if !strings.HasPrefix(secret, "sk-") && !strings.HasPrefix(secret, "sk_") {
		return nil, fmt.Errorf("密钥形态不对：应以 sk- 或 sk_ 开头")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// 同一密钥只允许入池一次，否则调度会把同一份额度当两份用。
	for _, k := range p.keys {
		if k.Secret == secret {
			return nil, fmt.Errorf("该密钥已在池中（%s）", k.Name)
		}
	}
	if name == "" {
		name = fmt.Sprintf("key-%d", len(p.keys)+1)
	}
	if weight <= 0 {
		weight = 1
	}
	now := time.Now()
	k := &Key{
		ID:        newID(),
		Name:      name,
		Secret:    secret,
		Weight:    weight,
		Enabled:   true,
		Budget:    budget,
		State:     StateActive,
		CreatedAt: now,
	}
	p.keys[k.ID] = k
	p.order = append(p.order, k.ID)
	p.notify()
	return k, nil
}

// Update 只改允许外部改的字段；nil 表示不动。
func (p *Pool) Update(id string, name, secret *string, weight, budget *float64, enabled *bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return fmt.Errorf("号不存在: %s", id)
	}
	if name != nil && strings.TrimSpace(*name) != "" {
		k.Name = strings.TrimSpace(*name)
	}
	if secret != nil && strings.TrimSpace(*secret) != "" {
		s := strings.TrimSpace(*secret)
		for oid, o := range p.keys {
			if oid != id && o.Secret == s {
				return fmt.Errorf("该密钥已被 %s 使用", o.Name)
			}
		}
		if k.Secret != s {
			k.Secret = s
			k.State = StateActive
			k.FailStreak = 0
			k.CooldownUntil = time.Time{}
			k.LastError = ""
		}
	}
	if weight != nil && *weight > 0 {
		k.Weight = *weight
	}
	if budget != nil && *budget >= 0 {
		k.Budget = *budget
	}
	if enabled != nil {
		k.Enabled = *enabled
		if *enabled && k.State == StateDisabled {
			k.State = StateActive
		}
		if !*enabled {
			k.State = StateDisabled
		}
	}
	p.notify()
	return nil
}

func (p *Pool) Remove(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.keys[id] == nil {
		return false
	}
	delete(p.keys, id)
	for i, v := range p.order {
		if v == id {
			p.order = append(p.order[:i], p.order[i+1:]...)
			break
		}
	}
	for tid, s := range p.sticky {
		if s.KeyID == id {
			delete(p.sticky, tid)
		}
	}
	p.notify()
	return true
}

// Reset 人工恢复：把 invalid/exhausted/cooling 拉回 active。
func (p *Pool) Reset(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return false
	}
	k.State = StateActive
	k.CooldownUntil = time.Time{}
	k.FailStreak = 0
	k.LastError = ""
	if !k.Enabled {
		k.Enabled = true
	}
	p.notify()
	return true
}

// ---------- 选号 ----------

func (p *Pool) usable(k *Key, now time.Time) bool {
	if !k.Enabled || k.State == StateDisabled || k.State == StateInvalid || k.State == StateExhausted {
		return false
	}
	if k.State == StateCooling && now.Before(k.CooldownUntil) {
		return false
	}
	return true
}

func (p *Pool) inflightFull(k *Key) bool {
	return p.cfg.MaxInFlight > 0 && k.inflight >= p.cfg.MaxInFlight
}

// Acquire 选一个号并占用一个在途名额。
// modelPaid 决定是否触发余额保底（免费模型不拦）。
func (p *Pool) Acquire(model string, modelPaid bool, tried map[string]bool, stickyKeyID string) (*Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()

	// 粘性优先：视频查询必须回到原号，冷却中也要试——上游只认创建者。
	if stickyKeyID != "" {
		if k := p.keys[stickyKeyID]; k != nil && k.Enabled {
			k.inflight++
			p.totalInflight++
			return &Lease{KeyID: k.ID, Secret: k.Secret, Name: k.Name}, nil
		}
	}

	if p.cfg.MaxInFlightGlobal > 0 && p.totalInflight >= p.cfg.MaxInFlightGlobal {
		return nil, fmt.Errorf("全局在途已满（%d），请稍后重试", p.cfg.MaxInFlightGlobal)
	}

	cands := make([]*Key, 0, len(p.keys))
	for _, id := range p.order {
		k := p.keys[id]
		if k == nil || tried[id] || !p.usable(k, now) || p.inflightFull(k) {
			continue
		}
		if modelPaid && p.cfg.CreditFloor > 0 && k.Budget > 0 && k.Balance() < p.cfg.CreditFloor {
			continue // 余额触底，不再派收费模型
		}
		cands = append(cands, k)
	}
	if len(cands) == 0 {
		return nil, fmt.Errorf("%w（%s）", ErrNoKey, p.blockReasonLocked(now))
	}

	var chosen *Key
	switch p.cfg.Strategy {
	case "sequential":
		chosen = cands[0]
	case "round_robin":
		chosen = cands[p.rr%len(cands)]
		p.rr++
	default:
		chosen = p.weightedLocked(cands, now)
	}
	if chosen == nil {
		return nil, ErrNoKey
	}
	chosen.inflight++
	chosen.LastUsedAt = now
	p.totalInflight++
	return &Lease{KeyID: chosen.ID, Secret: chosen.Secret, Name: chosen.Name}, nil
}

// weightedLocked 实现「top-K 短名单 + 加权抽签」：先按权重降序截断，
// 再在短名单内按权重抽。只排序不抽签会让热点永远压在同一个号上。
func (p *Pool) weightedLocked(cands []*Key, now time.Time) *Key {
	type scored struct {
		k *Key
		w float64
	}
	ss := make([]scored, 0, len(cands))
	for _, k := range cands {
		ss = append(ss, scored{k, p.weightOf(k, now)})
	}
	sort.SliceStable(ss, func(i, j int) bool { return ss[i].w > ss[j].w })
	k := p.cfg.TopK
	if k <= 0 || k > len(ss) {
		k = len(ss)
	}
	top := ss[:k]
	var sum float64
	for _, s := range top {
		sum += s.w
	}
	if sum <= 0 {
		return top[rand.IntN(len(top))].k
	}
	r := rand.Float64() * sum
	for _, s := range top {
		if r < s.w {
			return s.k
		}
		r -= s.w
	}
	return top[len(top)-1].k
}

// weightOf 是调度的灵魂：余额越多越优先，越久没被用越补偿。
func (p *Pool) weightOf(k *Key, now time.Time) float64 {
	w := k.Weight
	if w <= 0 {
		w = 1
	}
	// 余额因子：满额 2.0x，触底 0.2x。未知额度（Budget=0）不参与。
	if k.Budget > 0 {
		ratio := k.Balance() / k.Budget
		if ratio < 0 {
			ratio = 0
		}
		w *= 0.2 + ratio*1.8
	}
	// 闲置补偿：久置的号权重上浮，防止低余额号永远排不进短名单。
	idleFrom := k.LastUsedAt
	if idleFrom.IsZero() {
		idleFrom = k.CreatedAt
	}
	if !idleFrom.IsZero() && p.cfg.IdleWeightPerHour > 0 {
		idle := now.Sub(idleFrom).Hours()
		if idle > 0 {
			gain := math.Min(idle*p.cfg.IdleWeightPerHour, p.cfg.IdleWeightMax)
			w *= 1 + gain
		}
	}
	return w
}

func (p *Pool) blockReasonLocked(now time.Time) string {
	if len(p.keys) == 0 {
		return "池为空"
	}
	var soonest time.Time
	valid := 0
	for _, k := range p.keys {
		if k.Enabled && k.State != StateDisabled {
			valid++
		}
		if k.State == StateCooling && k.CooldownUntil.After(now) {
			if soonest.IsZero() || k.CooldownUntil.Before(soonest) {
				soonest = k.CooldownUntil
			}
		}
	}
	if valid == 0 {
		return "全部号已禁用或失效"
	}
	if !soonest.IsZero() {
		return fmt.Sprintf("全部冷却中，最早 %s 恢复", soonest.Format("15:04:05"))
	}
	return "全部在途已满"
}

// Release 归还在途名额，并按 outcome 更新状态。
// err 为 nil 表示成功。
func (p *Pool) Release(l *Lease, kind *FailKind, msg string) {
	if l == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[l.KeyID]
	if k == nil {
		if p.totalInflight > 0 {
			p.totalInflight--
		}
		return
	}
	if k.inflight > 0 {
		k.inflight--
	}
	if p.totalInflight > 0 {
		p.totalInflight--
	}
	now := time.Now()
	if kind == nil {
		k.FailStreak = 0
		k.State = StateActive
		k.CooldownUntil = time.Time{}
		k.LastOKAt = now
		return
	}
	k.Errors++
	k.LastError = truncate(msg, 240)
	k.LastErrorAt = now
	p.applyFailLocked(k, *kind, now)
}

func (p *Pool) applyFailLocked(k *Key, kind FailKind, now time.Time) {
	cfg := p.cfg
	switch kind {
	case FailClient:
		return // 请求本身有问题，号没毛病
	case FailAuth:
		k.State = StateInvalid
		k.CooldownUntil = time.Time{}
	case FailQuota:
		k.State = StateExhausted
		k.CooldownUntil = now.Add(cfg.ExhaustedCooldown.Duration())
	case FailRate:
		k.FailStreak++
		d := cfg.RateCooldown.Duration() * time.Duration(1<<uint(min(k.FailStreak-1, 6)))
		if c := cfg.RateMax.Duration(); c > 0 && d > c {
			d = c
		}
		k.State = StateCooling
		k.CooldownUntil = now.Add(d)
	case FailUpstream:
		k.FailStreak++
		if k.FailStreak < cfg.BreakerThreshold {
			return // 还没到熔断线，只是记一笔
		}
		over := k.FailStreak - cfg.BreakerThreshold
		d := cfg.BreakerCooldown.Duration() * time.Duration(1<<uint(min(over, 6)))
		if c := cfg.BreakerMax.Duration(); c > 0 && d > c {
			d = c
		}
		k.State = StateCooling
		k.CooldownUntil = now.Add(d)
	}
}

// Tick 把自然到期的冷却号拉回 active，并清理过期粘性。
func (p *Pool) Tick() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	changed := false
	for _, k := range p.keys {
		if k.State == StateCooling && !now.Before(k.CooldownUntil) {
			k.State = StateActive
			k.CooldownUntil = time.Time{}
			changed = true
		}
		if k.State == StateExhausted && !k.CooldownUntil.IsZero() && !now.Before(k.CooldownUntil) {
			k.State = StateActive
			k.CooldownUntil = time.Time{}
			changed = true
		}
	}
	ttl := p.cfg.StickyTTL.Duration()
	if ttl > 0 {
		for tid, s := range p.sticky {
			if now.Sub(s.At) > ttl {
				delete(p.sticky, tid)
				changed = true
			}
		}
	}
	if changed {
		p.notify()
	}
}

// ---------- 记账 ----------

func (p *Pool) Record(id string, promptTok, completionTok int64, costCNY float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return
	}
	k.Requests++
	k.PromptTokens += promptTok
	k.CompletionTokens += completionTok
	k.SpentCNY += costCNY
	if k.Budget > 0 && k.SpentCNY > k.Budget {
		// 记账口径穿仓：上游已经不让用了，别等下一笔 402。
		k.SpentCNY = k.Budget
	}
	p.notify()
}

// ---------- 粘性（视频等异步任务） ----------

func (p *Pool) SetSticky(taskID, keyID string) {
	if taskID == "" || keyID == "" {
		return
	}
	p.mu.Lock()
	p.sticky[taskID] = stickyEntry{KeyID: keyID, At: time.Now()}
	p.mu.Unlock()
	p.notify()
}

func (p *Pool) StickyKey(taskID string) string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.sticky[taskID].KeyID
}

// StickyByKey 反查某号名下的任务，面板展示用。
func (p *Pool) StickyCount(keyID string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	n := 0
	for _, s := range p.sticky {
		if s.KeyID == keyID {
			n++
		}
	}
	return n
}

// ---------- 持久化 ----------

type Snapshot struct {
	Keys   []*Key                 `json:"keys"`
	Order  []string               `json:"order"`
	Sticky map[string]stickyEntry `json:"sticky"`
}

func (p *Pool) Snapshot() Snapshot {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := Snapshot{Order: append([]string(nil), p.order...), Sticky: map[string]stickyEntry{}}
	for _, k := range p.keys {
		c := *k
		out.Keys = append(out.Keys, &c)
	}
	for tid, s := range p.sticky {
		out.Sticky[tid] = s
	}
	return out
}

func (p *Pool) Restore(s Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = map[string]*Key{}
	p.order = nil
	for _, k := range s.Keys {
		if k == nil || k.Secret == "" {
			continue
		}
		if k.ID == "" {
			k.ID = newID()
		}
		if k.Weight <= 0 {
			k.Weight = 1
		}
		if k.CreatedAt.IsZero() {
			k.CreatedAt = time.Now()
		}
		switch {
		case k.State == "" && !k.Enabled:
			k.State = StateDisabled
		case k.State == "":
			k.State = StateActive
		}
		k.inflight = 0
		p.keys[k.ID] = k
		p.order = append(p.order, k.ID)
	}
	if len(p.order) == 0 {
		for id := range p.keys {
			p.order = append(p.order, id)
		}
	}
	if len(s.Sticky) > 0 {
		p.sticky = s.Sticky
	}
}

// ---------- 面板视图 ----------

type View struct {
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	Masked           string  `json:"masked"`
	Enabled          bool    `json:"enabled"`
	State            string  `json:"state"`
	Weight           float64 `json:"weight"`
	Budget           float64 `json:"budget"`
	Spent            float64 `json:"spent"`
	Balance          float64 `json:"balance"` // -1 = 未知
	Requests         int64   `json:"requests"`
	Errors           int64   `json:"errors"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	FailStreak       int     `json:"fail_streak"`
	CooldownLeft     int     `json:"cooldown_left"`
	LastError        string  `json:"last_error"`
	LastUsedAt       string  `json:"last_used_at"`
	LastOKAt         string  `json:"last_ok_at"`
	Inflight         int     `json:"inflight"`
	StickyTasks      int     `json:"sticky_tasks"`
	Share            float64 `json:"share"` // 消耗占比 0..1
	WeightNow        float64 `json:"weight_now"`
}

func (p *Pool) Views() []View {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var totalSpent float64
	for _, k := range p.keys {
		totalSpent += k.SpentCNY
	}
	out := make([]View, 0, len(p.order))
	for _, id := range p.order {
		k := p.keys[id]
		if k == nil {
			continue
		}
		v := View{
			ID: k.ID, Name: k.Name, Masked: k.Masked(), Enabled: k.Enabled,
			State: string(k.State), Weight: k.Weight, Budget: k.Budget,
			Spent: k.SpentCNY, Balance: k.Balance(),
			Requests: k.Requests, Errors: k.Errors,
			PromptTokens: k.PromptTokens, CompletionTokens: k.CompletionTokens,
			FailStreak: k.FailStreak, LastError: k.LastError,
			Inflight: k.inflight, WeightNow: p.weightOf(k, now),
		}
		if k.Budget > 0 && totalSpent > 0 {
			v.Share = k.SpentCNY / totalSpent
		}
		if k.LastUsedAt != zero {
			v.LastUsedAt = k.LastUsedAt.Format(time.RFC3339)
		}
		if k.LastOKAt != zero {
			v.LastOKAt = k.LastOKAt.Format(time.RFC3339)
		}
		if k.State == StateCooling || k.State == StateExhausted {
			if d := int(k.CooldownUntil.Sub(now).Seconds()); d > 0 {
				v.CooldownLeft = d
			}
		}
		n := 0
		for _, s := range p.sticky {
			if s.KeyID == id {
				n++
			}
		}
		v.StickyTasks = n
		out = append(out, v)
	}
	return out
}

func (p *Pool) Has(id string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.keys[id] != nil
}

func (p *Pool) SecretOf(id string) (string, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	k := p.keys[id]
	if k == nil {
		return "", false
	}
	return k.Secret, true
}

var zero = time.Time{}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

var idSeq struct {
	sync.Mutex
	n int
}

func newID() string {
	idSeq.Lock()
	defer idSeq.Unlock()
	idSeq.n++
	return fmt.Sprintf("k%03d%s", idSeq.n, randomSuffix())
}

func randomSuffix() string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, 4)
	for i := range b {
		b[i] = alphabet[rand.IntN(len(alphabet))]
	}
	return string(b)
}
