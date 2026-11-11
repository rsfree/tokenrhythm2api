package pool

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"
	"strings"
	"time"
)

// PickRequest 描述一次选号的上下文。粘性优先级从强到弱：
// 视频任务 > 会话 > 软偏好（同号重试）> batch 成员 > 全池。
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
	// PreferKeyID 是"还是这个号再试一次"的软偏好：只在它可用、没满载时照办。
	// 模型供应链限流时换号无意义，只会多烧一个号的配额。
	PreferKeyID string
}

// Acquire 是唯一的选号入口。
func (p *Pool) Acquire(req PickRequest) (*Lease, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()

	// ① 视频任务粘性：上游只认创建者，冷却中也要回原号。
	if req.StickyKeyID != "" {
		if k := p.keys[req.StickyKeyID]; k != nil && k.Enabled && k.State != StateDisabled {
			return p.leaseLocked(k, now), nil
		}
	}

	// ② 软偏好：上游按模型限流时，重试仍落回原号最省配额。
	if req.PreferKeyID != "" {
		if k := p.keys[req.PreferKeyID]; k != nil && k.Enabled &&
			!req.Tried[k.ID] && k.usableFor(now, req.Model) &&
			!p.inflightFull(k) && !p.floorBlocked(k, req.Paid) {
			return p.leaseLocked(k, now), nil
		}
	}

	if p.cfg.MaxInFlightGlobal > 0 && p.totalInflight >= p.cfg.MaxInFlightGlobal {
		return nil, fmt.Errorf("全局在途已满（%d），请稍后重试", p.cfg.MaxInFlightGlobal)
	}

	// ③ 会话粘性：命中就直接回，这是吃到上游 prompt cache 的唯一办法。
	// 粘住的条件必须包含"这个模型没被封"——否则一个未开通的模型会把会话卡死在同一个号上。
	if req.SessionKey != 0 && p.cfg.SessionSticky.Enabled {
		if keyID, ok := p.sessions.Lookup(req.SessionKey); ok {
			if k := p.keys[keyID]; k != nil && !req.Tried[keyID] &&
				k.usableFor(now, req.Model) && !p.inflightFull(k) && !p.floorBlocked(k, req.Paid) {
				return p.leaseLocked(k, now), nil
			}
			// 命中的号这次不能用：解绑，免得下一次还往它身上撞。
			p.sessions.Drop(req.SessionKey)
		}
	}

	// ④⑤ batch 成员 + 权重
	cands := p.candidatesLocked(req, now)
	if len(cands) == 0 {
		if l := p.fallbackLocked(req, now); l != nil {
			return l, nil
		}
		return nil, fmt.Errorf("%w（%s）", ErrNoKey, p.blockReasonLocked(req.Model, now))
	}

	var chosen *Key
	switch p.cfg.Strategy {
	case "sequential":
		chosen = p.leastInflightLocked(cands, now)
	case "round_robin":
		chosen = cands[p.rr%len(cands)]
		p.rr++
	default:
		chosen = p.weightedLocked(p.eligibleLocked(cands, now), now)
	}
	if chosen == nil {
		return nil, ErrNoKey
	}
	if req.SessionKey != 0 && p.cfg.SessionSticky.Enabled {
		p.sessions.Bind(req.SessionKey, chosen.ID)
	}
	return p.leaseLocked(chosen, now), nil
}

func (p *Pool) leaseLocked(k *Key, now time.Time) *Lease {
	k.inflight++
	k.lastPickAt = now
	k.LastUsedAt = now
	k.seq = p.nextSeq()
	p.totalInflight++
	return &Lease{KeyID: k.ID, Secret: k.Secret, Name: k.Name}
}

func (p *Pool) nextSeq() uint64 {
	p.seq++
	return p.seq
}

func (p *Pool) inflightFull(k *Key) bool {
	return p.cfg.MaxInFlight > 0 && k.inflight >= p.cfg.MaxInFlight
}

// floorBlocked 判断余额是否已触底到不该再派收费模型。
// 充值额未知（Budget<=0）的号不参与保底：没数据就按"能用"处理，
// 否则新导入的号会永远排不进候选。
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
		if k == nil || req.Tried[id] {
			continue
		}
		if !k.usableFor(now, req.Model) || p.inflightFull(k) {
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

	target, on := p.batchTarget()
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

// eligibleLocked 做"防惊群"过滤：同一账号在 MinPickGap 内不重复被选中。
// 全都刚用过时按单调序号取最旧者——Windows 上 time.Now() 精度有限，
// 用墙钟比较会同时相等、恒选第一个，反而把流量压在同一个号上。
func (p *Pool) eligibleLocked(cands []*Key, now time.Time) []*Key {
	gap := p.cfg.MinPickGap.Duration()
	if gap <= 0 {
		return cands
	}
	eligible := make([]*Key, 0, len(cands))
	for _, k := range cands {
		if k.lastPickAt.IsZero() || now.Sub(k.lastPickAt) >= gap {
			eligible = append(eligible, k)
		}
	}
	if len(eligible) > 0 {
		return eligible
	}
	oldest := cands[0]
	for _, k := range cands[1:] {
		if k.seq < oldest.seq {
			oldest = k
		}
	}
	return []*Key{oldest}
}

// fallbackLocked 全池不可用时的最后一条路：挑"最早恢复"的号试一次。
//
// 只跳过真正没有希望的号（invalid / exhausted / disabled / 模型被封 / 余额触底），
// cooling 与熔断中的号可能是"其实已经好了"——让一个真实请求去验证，比干等便宜。
func (p *Pool) fallbackLocked(req PickRequest, now time.Time) *Lease {
	if !p.cfg.FallbackProbe || p.cfg.Strategy != "weighted_random" {
		return nil
	}
	var best *Key
	var bestAt time.Time
	for _, id := range p.order {
		k := p.keys[id]
		if k == nil || req.Tried[id] || !k.Enabled {
			continue
		}
		switch k.State {
		case StateDisabled, StateInvalid, StateExhausted:
			continue
		}
		if p.inflightFull(k) || p.floorBlocked(k, req.Paid) {
			continue
		}
		if _, blocked := k.modelBlocked(now, req.Model); blocked {
			continue
		}
		at := k.expiry(now)
		if at.IsZero() {
			continue
		}
		if best == nil || at.Before(bestAt) {
			best, bestAt = k, at
		}
	}
	if best == nil {
		return nil
	}
	l := p.leaseLocked(best, now)
	l.Fallback = true
	l.FallbackReason = fmt.Sprintf("%s 恢复（%s）", bestAt.Format("15:04:05"), best.stateLabel())
	return l
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

// weightedLocked 实现「top-K 短名单 + 加权抽签」：先按权重降序截断，
// 再在短名单内按权重抽。只排序不抽签会让热点永远压在同一个号上。
func (p *Pool) weightedLocked(cands []*Key, now time.Time) *Key {
	if len(cands) == 0 {
		return nil
	}
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

// weightOf 是调度的灵魂：余额越多越优先，越久没被用越补偿，在途越多越降权。
func (p *Pool) weightOf(k *Key, now time.Time) float64 {
	w := k.Weight
	if w <= 0 {
		w = 1
	}
	// 余额因子：满额 2.0x，触底 0.2x。充值额未知的号按满额算——
	// 按 0 算会让新导入的号永远进不了短名单，形成"越新越不用"的死锁。
	if k.Budget > 0 {
		ratio := k.Balance() / k.Budget
		if ratio < 0 {
			ratio = 0
		}
		w *= 0.2 + ratio*1.8
	} else {
		w *= 2.0
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

// blockReasonLocked 把"为什么没号"说清楚，别让排障的人只看到一句 no capacity。
//
// 这里刻意按成因分桶而不是只回一句"全部冷却中"：401 失效和 429 冷却的处置方式
// 完全不同（前者只能换号/重置，后者等十几秒自愈），看到错误信息的当下就该知道该做哪件事。
func (p *Pool) blockReasonLocked(model string, now time.Time) string {
	if len(p.keys) == 0 {
		return "池为空，请先在面板添加密钥"
	}
	if blocked, reason, count := p.modelBlockedLocked(model, now); blocked {
		return fmt.Sprintf("全部 %d 个号都对模型 %s 被封禁：%s", count, model, truncate(reason, 80))
	}

	counts := map[string]int{}
	var soonest time.Time
	enabled := 0
	for _, k := range p.keys {
		if !k.Enabled || k.State == StateDisabled {
			continue // 人工摘掉的号不算故障，不该出现在"为什么没号"里
		}
		enabled++
		if at := k.expiry(now); !at.IsZero() && (soonest.IsZero() || at.Before(soonest)) {
			soonest = at
		}
		switch {
		case k.State == StateInvalid:
			counts["失效(401)"]++
		case k.State == StateExhausted:
			counts["额度耗尽"]++
		case !k.BreakerUntil.IsZero() && now.Before(k.BreakerUntil):
			counts["熔断中"]++
		case k.State == StateCooling && now.Before(k.CooldownUntil):
			counts["冷却中"]++
		case p.cfg.MaxInFlight > 0 && k.inflight >= p.cfg.MaxInFlight:
			counts["在途已满"]++
		default:
			if _, blocked := k.modelBlocked(now, model); blocked {
				counts["该模型被封禁"]++
			} else {
				counts["暂不可用"]++
			}
		}
	}
	if enabled == 0 {
		return "全部号已禁用，请先在面板启用"
	}

	parts := make([]string, 0, len(counts))
	for _, name := range []string{"失效(401)", "额度耗尽", "冷却中", "熔断中", "在途已满", "该模型被封禁", "暂不可用"} {
		if n := counts[name]; n > 0 {
			parts = append(parts, fmt.Sprintf("%d 个%s", n, name))
		}
	}
	msg := strings.Join(parts, "，")
	if msg == "" {
		msg = "暂不可用"
	}
	if !soonest.IsZero() {
		msg += fmt.Sprintf("，最早 %s 恢复", soonest.Format("15:04:05"))
	}
	return msg
}

// modelBlockedLocked 与 ModelBlocked 同口径，供已持锁的内部调用使用。
func (p *Pool) modelBlockedLocked(model string, now time.Time) (bool, string, int) {
	if model == "" {
		return false, "", 0
	}
	count := 0
	reason := ""
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
	}
	if count == 0 {
		return false, "", 0
	}
	return true, reason, count
}
