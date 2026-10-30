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

// PickRequest 描述一次选号的上下文。四个粘性层级从强到弱：
// 视频任务 > 会话 > batch 成员 > 全池。
type PickRequest struct {
	Model string
	// Paid 表示该模型收费；收费模型会被余额保底拦截。
	Paid bool
	// Tried 是本次请求已经试过的号，换号重试时不能重复挑。
	Tried map[string]bool
	// StickyKeyID 是视频异步任务的创建者，查询必须回到它（即使正在冷却）。
	StickyKeyID string
	// SessionKey 是会话指纹，用于命中上游 prompt cache；0 表示不启用。
	SessionKey uint64
}

type sessionEntry struct {
	KeyID string    `json:"key_id"`
	At    time.Time `json:"at"`
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

	sticky map[string]stickyEntry
	// sessions 是会话指纹 → 号的粘性表（保 prompt cache）。
	sessions map[uint64]sessionEntry
	// batch 是当前"活跃使用"的号集合，按权重从高到低维护。
	batch    []string
	onChange func()
}

type stickyEntry struct {
	KeyID string    `json:"key_id"`
	At    time.Time `json:"at"`
}

var ErrNoKey = errors.New("池内无可用密钥")

func New(cfg config.PoolConfig) *Pool {
	return &Pool{
		keys:     map[string]*Key{},
		cfg:      cfg,
		sticky:   map[string]stickyEntry{},
		sessions: map[uint64]sessionEntry{},
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

// Acquire 是唯一的选号入口。优先级从强到弱：
//
//	① 视频任务粘性 —— 上游只认创建者，冷却中也要回原号
//	② 会话粘性     —— 命中就直接回，这是吃到上游 prompt cache 的唯一办法
//	③ batch 成员   —— 只从"当前活跃"的号里选（软约束，全不可用会自动放宽）
//	④ 在途最少     —— 并发高峰把新请求推给空闲的号
//	⑤ 权重         —— 余额因子 × 闲置补偿
func (p *Pool) Acquire(req PickRequest) (*Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()

	// ① 视频任务粘性
	if req.StickyKeyID != "" {
		if k := p.keys[req.StickyKeyID]; k != nil && k.Enabled {
			return p.leaseLocked(k, now), nil
		}
	}

	if p.cfg.MaxInFlightGlobal > 0 && p.totalInflight >= p.cfg.MaxInFlightGlobal {
		return nil, fmt.Errorf("全局在途已满（%d），请稍后重试", p.cfg.MaxInFlightGlobal)
	}

	// ② 会话粘性
	if k := p.sessionHitLocked(req, now); k != nil {
		return p.leaseLocked(k, now), nil
	}

	// ③④⑤
	cands := p.candidatesLocked(req, now)
	if len(cands) == 0 {
		return nil, fmt.Errorf("%w（%s）", ErrNoKey, p.blockReasonLocked(now))
	}

	var chosen *Key
	switch p.cfg.Strategy {
	case "sequential":
		chosen = p.leastInflightLocked(cands, now)
	case "round_robin":
		chosen = cands[p.rr%len(cands)]
		p.rr++
	default:
		chosen = p.weightedLocked(cands, now)
	}
	if chosen == nil {
		return nil, ErrNoKey
	}

	p.noteSessionLocked(req.SessionKey, chosen.ID, now)
	return p.leaseLocked(chosen, now), nil
}

func (p *Pool) leaseLocked(k *Key, now time.Time) *Lease {
	k.inflight++
	k.LastUsedAt = now
	p.totalInflight++
	return &Lease{KeyID: k.ID, Secret: k.Secret, Name: k.Name}
}

// sessionHitLocked 命中会话粘性且该号此刻可用，就返回它。
// 任何一条不满足都返回 nil，退回常规选号——粘性不该成为可用性的阻碍。
func (p *Pool) sessionHitLocked(req PickRequest, now time.Time) *Key {
	if req.SessionKey == 0 || !p.cfg.SessionSticky.Enabled {
		return nil
	}
	e, ok := p.sessions[req.SessionKey]
	if !ok {
		return nil
	}
	if ttl := p.cfg.SessionSticky.TTL.Duration(); ttl > 0 && now.Sub(e.At) > ttl {
		return nil
	}
	k := p.keys[e.KeyID]
	if k == nil || req.Tried[k.ID] || !p.usable(k, now) || p.inflightFull(k) || p.floorBlocked(k, req.Paid) {
		return nil
	}
	return k
}

func (p *Pool) noteSessionLocked(key uint64, keyID string, now time.Time) {
	if key == 0 || keyID == "" || !p.cfg.SessionSticky.Enabled {
		return
	}
	if max := p.cfg.SessionSticky.MaxEntries; max > 0 && len(p.sessions) >= max {
		// 淘汰最久未使用的，代价是 O(n) 扫描；表很小且只在满时才走。
		var oldestKey uint64
		var oldestAt time.Time
		for k, v := range p.sessions {
			if oldestAt.IsZero() || v.At.Before(oldestAt) {
				oldestKey, oldestAt = k, v.At
			}
		}
		delete(p.sessions, oldestKey)
	}
	p.sessions[key] = sessionEntry{KeyID: keyID, At: now}
}

// floorBlocked 判断余额是否已触底到不该再派收费模型。
func (p *Pool) floorBlocked(k *Key, paid bool) bool {
	if !paid || p.cfg.CreditFloor <= 0 || k.Budget <= 0 {
		return false
	}
	return k.Balance() < p.cfg.CreditFloor
}

// candidatesLocked 先做硬过滤，再套 batch 软约束。
func (p *Pool) candidatesLocked(req PickRequest, now time.Time) []*Key {
	all := make([]*Key, 0, len(p.keys))
	for _, id := range p.order {
		k := p.keys[id]
		if k == nil || req.Tried[id] || !p.usable(k, now) || p.inflightFull(k) {
			continue
		}
		if p.floorBlocked(k, req.Paid) {
			continue
		}
		all = append(all, k)
	}
	if len(all) == 0 {
		return nil
	}

	target, on := p.batchTargetLocked()
	if !on {
		return all
	}
	p.syncBatchLocked(target, now)
	if len(p.batch) == 0 {
		return all
	}
	in := make([]*Key, 0, len(p.batch))
	for _, id := range p.batch {
		for _, k := range all {
			if k.ID == id {
				in = append(in, k)
				break
			}
		}
	}
	if len(in) > 0 {
		return in
	}
	// batch 内的号这一轮全满（或被 tried 排除）→ 放开到全池。
	// 宁可突破 batch 也别把请求卡死。
	return all
}

// batchTargetLocked 算出"现在该同时用几个号"。
func (p *Pool) batchTargetLocked() (int, bool) {
	mode := p.cfg.Batch.Mode
	if mode == "" || mode == "off" || len(p.keys) == 0 {
		return 0, false
	}
	n := len(p.keys)
	minS, maxS := p.cfg.Batch.Min, p.cfg.Batch.Max
	if minS <= 0 {
		minS = 1
	}
	if minS > n {
		minS = n
	}
	if maxS <= 0 || maxS > n {
		maxS = n
	}
	if maxS < minS {
		maxS = minS
	}

	switch mode {
	case "fixed":
		return clampInt(p.cfg.Batch.Size, minS, maxS), true
	default: // auto
		per := p.cfg.Batch.TargetInflightPerKey
		if per <= 0 {
			per = 4
		}
		need := (p.totalInflight + per - 1) / per
		return clampInt(need, minS, maxS), true
	}
}

// syncBatchLocked 把 batch 向目标大小收敛。
// 扩容：按权重从高到低补；缩容：只淘汰空闲号，正在跑请求的等它跑完再说。
func (p *Pool) syncBatchLocked(target int, now time.Time) {
	alive := make([]string, 0, len(p.batch))
	for _, id := range p.batch {
		if k := p.keys[id]; k != nil && p.usable(k, now) {
			alive = append(alive, id)
		}
	}
	p.batch = alive

	if len(p.batch) < target {
		in := make(map[string]bool, len(p.batch))
		for _, id := range p.batch {
			in[id] = true
		}
		type cand struct {
			id string
			w  float64
		}
		cs := make([]cand, 0, len(p.order))
		for _, id := range p.order {
			k := p.keys[id]
			if k == nil || in[id] || !p.usable(k, now) {
				continue
			}
			cs = append(cs, cand{id, p.weightOf(k, now)})
		}
		sort.Slice(cs, func(i, j int) bool { return cs[i].w > cs[j].w })
		for _, c := range cs {
			if len(p.batch) >= target {
				break
			}
			p.batch = append(p.batch, c.id)
		}
		return
	}

	excess := len(p.batch) - target
	if excess <= 0 {
		return
	}
	keep := make([]string, 0, len(p.batch))
	for i := len(p.batch) - 1; i >= 0; i-- {
		id := p.batch[i]
		if excess > 0 {
			if k := p.keys[id]; k != nil && k.inflight == 0 {
				excess--
				continue
			}
		}
		keep = append(keep, id)
	}
	for i, j := 0, len(keep)-1; i < j; i, j = i+1, j-1 {
		keep[i], keep[j] = keep[j], keep[i]
	}
	p.batch = keep
}

// leastInflightLocked 取在途最少的号，并列时按权重抽，避免总压同一个。
func (p *Pool) leastInflightLocked(cands []*Key, now time.Time) *Key {
	best := cands[0].inflight
	for _, k := range cands[1:] {
		if k.inflight < best {
			best = k.inflight
		}
	}
	tier := make([]*Key, 0, len(cands))
	for _, k := range cands {
		if k.inflight == best {
			tier = append(tier, k)
		}
	}
	if len(tier) == 1 {
		return tier[0]
	}
	return p.weightedLocked(tier, now)
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
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
	// 在途惩罚：并发高峰时把新请求推向空闲的号，而不是继续压满的号。
	if p.cfg.LeastInflight && k.inflight > 0 {
		w /= 1 + float64(k.inflight)*p.cfg.InflightPenalty
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

// MarkFail 供预热/探活这类"没走选号"的路径使用：把结论落成号的状态。
// 没有它，预热就只是打了个日志，第一波业务请求照样要自己试错一遍。
func (p *Pool) MarkFail(id string, kind FailKind, msg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return
	}
	k.Errors++
	k.LastError = truncate(msg, 240)
	k.LastErrorAt = time.Now()
	p.applyFailLocked(k, kind, time.Now())
	p.notify()
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
	if st := p.cfg.SessionSticky.TTL.Duration(); st > 0 {
		for key, e := range p.sessions {
			if now.Sub(e.At) > st {
				delete(p.sessions, key)
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
	// 会话粘性与 batch 是运行时状态，重启即重建：
	// 会话已经断了，硬留着只会把新对话指到不该指的号上。
	p.sessions = map[uint64]sessionEntry{}
	p.batch = nil
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
	InBatch          bool    `json:"in_batch"`
	Sessions         int     `json:"sessions"`
}

func (p *Pool) Views() []View {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	var totalSpent float64
	for _, k := range p.keys {
		totalSpent += k.SpentCNY
	}
	inBatch := make(map[string]bool, len(p.batch))
	for _, id := range p.batch {
		inBatch[id] = true
	}
	sessByKey := make(map[string]int, len(p.keys))
	for _, e := range p.sessions {
		sessByKey[e.KeyID]++
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
			InBatch: inBatch[id], Sessions: sessByKey[id],
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

// BatchState 是调度面板要看的"当前用了几个号"。
type BatchState struct {
	Mode          string   `json:"mode"`
	Target        int      `json:"target"`
	Size          int      `json:"size"`
	Members       []string `json:"members"`
	TotalInflight int      `json:"total_inflight"`
	Sessions      int      `json:"sessions"`
}

func (p *Pool) BatchState() BatchState {
	p.mu.RLock()
	defer p.mu.RUnlock()
	st := BatchState{
		Mode:          p.cfg.Batch.Mode,
		Size:          len(p.batch),
		Members:       append([]string(nil), p.batch...),
		TotalInflight: p.totalInflight,
		Sessions:      len(p.sessions),
	}
	if t, ok := p.batchTargetLocked(); ok {
		st.Target = t
	}
	return st
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
