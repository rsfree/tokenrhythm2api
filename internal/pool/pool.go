package pool

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"trpool/internal/config"
	"trpool/internal/session"
)

var ErrNoKey = errors.New("池内无可用密钥")

type stickyEntry struct {
	KeyID string    `json:"key_id"`
	At    time.Time `json:"at"`
}

// Pool 持有全部号与调度状态。
type Pool struct {
	mu    sync.Mutex
	keys  map[string]*Key
	order []string
	cfg   config.PoolConfig

	rr            int
	totalInflight int
	seq           uint64

	sticky map[string]stickyEntry
	batch  []string
	// elastic 是 429 驱动的窗口弹性状态（不落盘）。
	elastic elasticState

	sessions *session.Table
	onChange func()
}

func New(cfg config.PoolConfig, sess *session.Table) *Pool {
	if sess == nil {
		sess = session.New(cfg.SessionSticky.TTL.Duration(), cfg.SessionSticky.MaxEntries)
	}
	return &Pool{
		keys:     map[string]*Key{},
		cfg:      cfg,
		sticky:   map[string]stickyEntry{},
		sessions: sess,
	}
}

// SetOnChange 注册状态变更回调（落盘标脏、面板推送等）。
func (p *Pool) SetOnChange(fn func()) { p.onChange = fn }

func (p *Pool) notify() {
	if p.onChange != nil {
		p.onChange()
	}
}

// Sessions 暴露粘性表给面板/调度器（统计与过期清理）。
func (p *Pool) Sessions() *session.Table { return p.sessions }

func (p *Pool) SetConfig(cfg config.PoolConfig) {
	p.mu.Lock()
	p.cfg = cfg
	// 会话粘性参数热更新：表本身跟着换 TTL/上限，不让旧设置继续生效。
	p.sessions = session.New(cfg.SessionSticky.TTL.Duration(), cfg.SessionSticky.MaxEntries)
	now := time.Now()
	if cfg.Batch.Mode == "off" || cfg.Batch.Mode == "" {
		// 关掉窗口就把成员清掉，别让面板还挂着 B 标。
		p.batch = nil
		p.elastic.extra = 0
	} else {
		p.evalElasticLocked(now)
		p.refreshBatchLocked(now)
	}
	p.mu.Unlock()
	p.notify()
}

func (p *Pool) Config() config.PoolConfig {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cfg
}

// Names 返回池内全部号名（按入池顺序），批量导入靠它避开重名。
func (p *Pool) Names() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.order))
	for _, id := range p.order {
		if k := p.keys[id]; k != nil {
			out = append(out, k.Name)
		}
	}
	return out
}

// ---------- 增删改 ----------

// validateSecret 校验密钥形态。单个/批量入池共用同一套判定，
// 免得两条路径对什么算合法密钥各说各话。
func validateSecret(secret string) error {
	if secret == "" {
		return fmt.Errorf("密钥不能为空")
	}
	if !strings.HasPrefix(secret, "sk-") && !strings.HasPrefix(secret, "sk_") {
		return fmt.Errorf("密钥形态不对：应以 sk- 或 sk_ 开头")
	}
	return nil
}

func (p *Pool) Add(name, secret string, weight, budget float64) (*Key, error) {
	secret = strings.TrimSpace(secret)
	if err := validateSecret(secret); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	// 同一密钥只允许入池一次，否则调度会把同一份额度当两份用。
	for _, k := range p.keys {
		if k.Secret == secret {
			return nil, fmt.Errorf("该密钥已在池中（%s）", k.Name)
		}
	}
	k := p.addLocked(name, secret, weight, budget, time.Now())
	// 新号入池立刻参与窗口：批量导 100 个号时不用等下一笔请求才补齐。
	p.refreshBatchLocked(k.CreatedAt)
	p.notify()
	return k, nil
}

// addLocked 插入一个号，不做去重、不刷新窗口——调用方持锁并负责这两件事。
func (p *Pool) addLocked(name, secret string, weight, budget float64, now time.Time) *Key {
	if name == "" {
		name = fmt.Sprintf("key-%d", len(p.keys)+1)
	}
	if weight <= 0 {
		weight = 1
	}
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
	return k
}

// KeyDraft 是一条待入池的号（批量导入用）。
type KeyDraft struct {
	Name   string
	Secret string
	Weight float64
	Budget float64
}

// KeyDraftResult 是批量导入里单条的结果：Error 非空表示这条没进池。
type KeyDraftResult struct {
	Key   *Key
	Name  string
	Error error
}

// AddMany 批量入池：只加一次锁、只刷新一次窗口、只通知一次落盘。
//
// 单条失败不拖累其它行——粘 100 个号时中间混进一行手滑很正常，全废掉最难受。
// 因此返回与 drafts 等长的结果，逐条交代进没进池、为什么没进。
func (p *Pool) AddMany(drafts []KeyDraft) []KeyDraftResult {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	// secret -> 冲突说明；先灌入池子里已有的，再在循环里把本批已收的写进去，
	// 这样"与池中重复"和"本批第 N 行重复"能给出不同的提示。
	known := make(map[string]error, len(p.keys)+len(drafts))
	for _, k := range p.keys {
		known[k.Secret] = fmt.Errorf("该密钥已在池中（%s）", k.Name)
	}

	out := make([]KeyDraftResult, len(drafts))
	added := 0
	for i, d := range drafts {
		secret := strings.TrimSpace(d.Secret)
		res := KeyDraftResult{Name: strings.TrimSpace(d.Name)}
		if err := validateSecret(secret); err != nil {
			res.Error = err
			out[i] = res
			continue
		}
		if why, dup := known[secret]; dup {
			res.Error = why
			out[i] = res
			continue
		}
		k := p.addLocked(res.Name, secret, d.Weight, d.Budget, now)
		known[secret] = fmt.Errorf("与第 %d 行是同一个密钥", i+1)
		res.Key = k
		res.Name = k.Name
		out[i] = res
		added++
	}
	if added > 0 {
		p.refreshBatchLocked(now)
		p.notify()
	}
	return out
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
			// 换密钥等于换了一个号：旧的惩罚状态全部作废。
			k.State = StateActive
			k.CooldownUntil = time.Time{}
			k.CooldownReason = ""
			k.FailStreak = 0
			k.RateStreak = 0
			k.QuotaHits = 0
			k.BanHits = 0
			k.BreakerUntil = time.Time{}
			k.BreakerCount = 0
			k.ModelBlocks = nil
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
	p.refreshBatchLocked(time.Now())
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
	p.batch = removeString(p.batch, id)
	p.refreshBatchLocked(time.Now())
	p.notify()
	return true
}

// Reset 人工恢复：把 invalid/exhausted/cooling 拉回 active，并清掉熔断与模型封禁。
func (p *Pool) Reset(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return false
	}
	k.State = StateActive
	k.CooldownUntil = time.Time{}
	k.CooldownReason = ""
	k.FailStreak = 0
	k.RateStreak = 0
	k.QuotaHits = 0
	k.BanHits = 0
	k.BreakerUntil = time.Time{}
	k.BreakerCount = 0
	k.ModelBlocks = nil
	k.LastError = ""
	if !k.Enabled {
		k.Enabled = true
	}
	p.refreshBatchLocked(time.Now())
	p.notify()
	return true
}

// ResetModel 只解除某个 (号, 模型) 的封禁，不碰其它模型与账号级状态。
func (p *Pool) ResetModel(id, model string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil || model == "" {
		return false
	}
	delete(k.ModelBlocks, model)
	p.notify()
	return true
}

// AddBudget 给某个号追加充值额（人工校准用）。
func (p *Pool) AddBudget(id string, delta float64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return false
	}
	if k.Budget <= 0 {
		k.Budget = 0
	}
	k.Budget += delta
	if k.Budget < 0 {
		k.Budget = 0
	}
	p.refreshBatchLocked(time.Now())
	p.notify()
	return true
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
	// 记账刚好把余额跑空时，窗口立刻换人——"没钱了就补一个"靠这里兜住，
	// 不用等服务端发 402 回来。
	p.refreshBatchLocked(time.Now())
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
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.sticky[taskID].KeyID
}

func (p *Pool) StickyCount(keyID string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	n := 0
	for _, s := range p.sticky {
		if s.KeyID == keyID {
			n++
		}
	}
	return n
}

func (p *Pool) StickyTotal() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sticky)
}

// ---------- 周期任务 ----------

// Tick 把自然到期的冷却号拉回 active，清理过期模型封禁与粘性。
func (p *Pool) Tick() {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	changed := false
	for _, k := range p.keys {
		if (k.State == StateCooling || k.State == StateExhausted || k.State == StateBlocked) &&
			!k.CooldownUntil.IsZero() && !now.Before(k.CooldownUntil) {
			k.State = StateActive
			k.CooldownUntil = time.Time{}
			k.CooldownReason = ""
			changed = true
		}
		if !k.BreakerUntil.IsZero() && !now.Before(k.BreakerUntil) {
			k.BreakerUntil = time.Time{}
			changed = true
		}
		for model, b := range k.ModelBlocks {
			if !now.Before(b.Until) {
				delete(k.ModelBlocks, model)
				changed = true
			}
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
	// 弹性窗口与活跃窗口都在这里随时间长回来/退回去：
	// 没有流量的时候没人调 Release，只能靠 5 秒一次的巡检。
	if p.evalElasticLocked(now) {
		changed = true
	}
	if p.refreshBatchLocked(now) {
		changed = true
	}
	if changed {
		p.notify()
	}
}

// ---------- 查询 ----------

func (p *Pool) Has(id string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.keys[id] != nil
}

func (p *Pool) SecretOf(id string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return "", false
	}
	return k.Secret, true
}

// EnabledKeys 返回可参与探活/刷价的号（不看冷却，只看是否被人工停用或判死）。
func (p *Pool) EnabledKeys() []View {
	all := p.Views()
	out := make([]View, 0, len(all))
	for _, v := range all {
		if v.Enabled && v.State != "invalid" && v.State != "disabled" {
			out = append(out, v)
		}
	}
	return out
}

func (p *Pool) Counts() (total, healthy, cooling, invalid, exhausted, blocked, disabled int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	for _, k := range p.keys {
		total++
		switch {
		case !k.Enabled || k.State == StateDisabled:
			disabled++
		case k.State == StateInvalid:
			invalid++
		case k.State == StateExhausted:
			exhausted++
		case k.State == StateBlocked:
			blocked++
		case k.usable(now):
			healthy++
		case k.State == StateCooling:
			cooling++
		}
	}
	return
}

// ModelBlocked 报告该模型是否在全池范围内被模型级封禁挡住。
// 用来把"没有可用号"和"号都在、只是都没开通这个模型"区分开——两者的处置完全不同。
func (p *Pool) ModelBlocked(model string) (blocked bool, reason string, count int) {
	if model == "" {
		return false, "", 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var soonest time.Time
	for _, k := range p.keys {
		if !k.Enabled || k.State == StateDisabled {
			continue
		}
		b, ok := k.modelBlocked(now, model)
		if !ok {
			return false, "", 0
		}
		count++
		if reason == "" {
			reason = b.Reason
		}
		if soonest.IsZero() || b.Until.Before(soonest) {
			soonest = b.Until
		}
	}
	if count == 0 {
		return false, "", 0
	}
	if reason == "" {
		reason = "模型级封禁"
	}
	return true, reason, count
}

// ModelHealth 汇总当前所有模型级封禁，供面板"模型"页展示。
type ModelHealth struct {
	Model    string `json:"model"`
	Kind     string `json:"kind"`
	Keys     int    `json:"keys"`
	Earliest int    `json:"earliest_left"`
	Reason   string `json:"reason,omitempty"`
}

func (p *Pool) ModelHealth() []ModelHealth {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	agg := map[string]*ModelHealth{}
	for _, k := range p.keys {
		for model, b := range k.ModelBlocks {
			if !now.Before(b.Until) {
				continue
			}
			h := agg[model]
			if h == nil {
				h = &ModelHealth{Model: model, Kind: b.Kind, Earliest: b.Left(now), Reason: b.Reason}
				agg[model] = h
			}
			h.Keys++
			if left := b.Left(now); h.Earliest == 0 || left < h.Earliest {
				h.Earliest = left
			}
			if b.Kind == "denied" {
				h.Kind = "denied"
			}
		}
	}
	out := make([]ModelHealth, 0, len(agg))
	for _, h := range agg {
		out = append(out, *h)
	}
	sortModelHealth(out)
	return out
}

// ---------- 面板视图 ----------

func (p *Pool) Views() []View {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	var totalSpent float64
	for _, k := range p.keys {
		totalSpent += k.SpentCNY
	}
	inBatch := make(map[string]bool, len(p.batch))
	for _, id := range p.batch {
		inBatch[id] = true
	}
	sessByKey := p.sessions.ByKey()
	stickyByKey := make(map[string]int, len(p.keys))
	for _, s := range p.sticky {
		stickyByKey[s.KeyID]++
	}

	out := make([]View, 0, len(p.order))
	for _, id := range p.order {
		k := p.keys[id]
		if k == nil {
			continue
		}
		v := View{
			ID: k.ID, Name: k.Name, Masked: k.Masked(), Enabled: k.Enabled,
			State: string(k.stateLabel()), Weight: k.Weight, Budget: k.Budget,
			Spent: k.SpentCNY, Balance: k.Balance(),
			Requests: k.Requests, Errors: k.Errors,
			PromptTokens: k.PromptTokens, CompletionTokens: k.CompletionTokens,
			FailStreak: k.FailStreak, LastError: k.LastError,
			QuotaHits: k.QuotaHits, BanHits: k.BanHits,
			Inflight: k.inflight, WeightNow: p.weightOf(k, now),
			InBatch: inBatch[id], Sessions: sessByKey[id],
			StickyTasks: stickyByKey[id], CooldownReason: k.CooldownReason,
			ModelBlocks: make([]ModelBlock, 0, len(k.ModelBlocks)),
		}
		if k.Budget > 0 && totalSpent > 0 {
			v.Share = k.SpentCNY / totalSpent
		}
		if !k.LastUsedAt.IsZero() {
			v.LastUsedAt = k.LastUsedAt.Format(time.RFC3339)
		}
		if !k.LastOKAt.IsZero() {
			v.LastOKAt = k.LastOKAt.Format(time.RFC3339)
		}
		if k.State == StateCooling || k.State == StateExhausted || k.State == StateBlocked {
			if d := int(k.CooldownUntil.Sub(now).Seconds()); d > 0 {
				v.CooldownLeft = d
			}
		}
		if d := int(k.BreakerUntil.Sub(now).Seconds()); d > 0 {
			v.BreakerLeft = d
		}
		for _, b := range k.ModelBlocks {
			if now.Before(b.Until) {
				v.ModelBlocks = append(v.ModelBlocks, ModelBlock{
					Model: b.Model, Until: b.Until, Reason: b.Reason, Kind: b.Kind, Hits: b.Hits,
				})
			}
		}
		out = append(out, v)
	}
	return out
}

// BatchState 是调度面板要看的"当前用了几个号"。
type BatchState struct {
	Mode string `json:"mode"`
	// Base 是基准窗口（fixed 的 size / auto 的并发换算值），Target 是含弹性余量的当前目标。
	Base   int `json:"base"`
	Target int `json:"target"`
	Size   int `json:"size"`
	// Extra 是弹性多开出来的号数；ElasticCap 是弹性上限。
	Extra      int  `json:"extra"`
	Elastic    bool `json:"elastic"`
	ElasticCap int  `json:"elastic_cap"`
	// PoolSize / Available 让面板能解释"目标 12 实际 11"：可用的号本来就不够。
	PoolSize  int `json:"pool_size"`
	Available int `json:"available"`
	// RateRatio 是统计窗口内的 429 占比，RateHits/RateTotal 是它的分子分母。
	RateRatio   float64 `json:"rate_ratio"`
	RateHits    int     `json:"rate_hits"`
	RateTotal   int     `json:"rate_total"`
	WindowSec   int     `json:"window_sec"`
	LastRateAgo int     `json:"last_rate_ago"`
	// TriggerRate 是配置的扩容触发线，面板要拿它解释"为什么还没扩"。
	TriggerRate   float64  `json:"trigger_rate"`
	Members       []string `json:"members"`
	MemberNames   []string `json:"member_names"`
	TotalInflight int      `json:"total_inflight"`
	Sessions      int      `json:"sessions"`
	StickyTasks   int      `json:"sticky_tasks"`
}

func (p *Pool) BatchState() BatchState {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	st := BatchState{
		Mode:          p.cfg.Batch.Mode,
		Size:          len(p.batch),
		PoolSize:      len(p.keys),
		Members:       append([]string(nil), p.batch...),
		TotalInflight: p.totalInflight,
		Sessions:      p.sessions.Len(),
		StickyTasks:   len(p.sticky),
	}
	for _, id := range p.batch {
		if k := p.keys[id]; k != nil {
			st.MemberNames = append(st.MemberNames, k.Name)
		}
	}
	for _, k := range p.keys {
		if k.usableForBatch(now, p.cfg.CreditFloor) {
			st.Available++
		}
	}
	if base, ok := p.batchBase(); ok {
		st.Base = base
	}
	if t, ok := p.batchTarget(); ok {
		st.Target = t
	}
	ev := p.elasticViewLocked(now)
	st.Elastic = ev.enabled
	st.Extra = ev.extra
	st.ElasticCap = ev.cap
	st.RateRatio = ev.ratio
	st.RateHits = ev.hits
	st.RateTotal = ev.total
	st.WindowSec = ev.window
	st.LastRateAgo = ev.lastAgo
	st.TriggerRate = p.cfg.Batch.Elastic.TriggerRate
	return st
}

// ---------- 工具 ----------

func removeString(list []string, v string) []string {
	out := list[:0]
	for _, x := range list {
		if x != v {
			out = append(out, x)
		}
	}
	return out
}

func sortModelHealth(list []ModelHealth) {
	for i := 0; i < len(list); i++ {
		for j := i + 1; j < len(list); j++ {
			if list[j].Keys > list[i].Keys ||
				(list[j].Keys == list[i].Keys && list[j].Model < list[i].Model) {
				list[i], list[j] = list[j], list[i]
			}
		}
	}
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
