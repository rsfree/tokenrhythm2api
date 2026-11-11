package pool

import (
	"time"

	"trpool/internal/fault"
)

// Outcome 是一次请求结束后的调度结论。Kind 为空表示成功。
type Outcome struct {
	Kind       fault.Kind
	Model      string
	Message    string
	RetryAfter time.Duration
}

// Success 是成功结果的便捷构造。
func Success() Outcome { return Outcome{} }

// Release 归还在途名额，并按结果更新状态。
// 每个 Lease 只有第一次调用生效——重复 Release 不会把在途计数减成负数。
func (p *Pool) Release(l *Lease, o Outcome) {
	if l == nil || l.released {
		return
	}
	l.released = true

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

	if o.Kind == fault.None {
		k.FailStreak = 0
		k.RateStreak = 0
		k.QuotaHits = 0
		k.BanHits = 0
		k.BreakerUntil = time.Time{}
		k.BreakerCount = 0
		k.CooldownUntil = time.Time{}
		k.CooldownReason = ""
		k.State = StateActive
		k.LastOKAt = now
		// 这个模型通了，就把它之前的"未开通"负缓存撤掉。
		if o.Model != "" {
			delete(k.ModelBlocks, o.Model)
		}
		p.observe(now, false)
		p.evalElasticLocked(now)
		p.refreshBatchLocked(now)
		p.notify()
		return
	}

	k.Errors++
	k.LastError = truncate(o.Message, 240)
	k.LastErrorAt = now
	p.applyFailLocked(k, o, now, true)
	// 只有"按号限流"算压力：多开号能加总配额。
	// 模型侧限流是全池共享的上游瓶颈；欠费/冻结/失效则应该补号或换号，扩窗没用。
	p.observe(now, o.Kind == fault.Rate)
	p.evalElasticLocked(now)
	p.refreshBatchLocked(now)
	p.notify()
}

// ReleaseSuccess 是不关心 model 的成功归还。
func (p *Pool) ReleaseSuccess(l *Lease) { p.Release(l, Outcome{}) }

// MarkFail 供预热/探活这类"没走选号"的路径使用：把结论落成号的状态。
// 没有它，预热就只是打了个日志，第一波业务请求照样要自己试错一遍。
func (p *Pool) MarkFail(id string, o Outcome) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return
	}
	k.Errors++
	k.LastError = truncate(o.Message, 240)
	k.LastErrorAt = time.Now()
	// 探活不是业务流量：不放大退避（否则每点一次"探活"就把冻结号的
	// 冷却翻倍，越探越久），但仍然要让状态立刻生效。
	p.applyFailLocked(k, o, time.Now(), false)
	// 探活/预热不算业务流量，不进 429 占比的样本；
	// 但号刚被判定不可用时，窗口要立刻补一个上来。
	p.refreshBatchLocked(time.Now())
	p.notify()
}

// MarkOK 供探活成功使用：只清 invalid 与"最近错误"，不动冷却与熔断。
//
// 为什么不 Reset 整号：/v1/models 通了只说明"这个号能取模型列表"，
// 不代表 chat 可用（模型级灰度、账号级限流都可能只拦生成接口）。
// 让业务请求替我们验证，比探活提前放行安全。
func (p *Pool) MarkOK(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil {
		return
	}
	k.LastOKAt = time.Now()
	if k.State == StateInvalid {
		k.State = StateActive
		k.LastError = ""
	}
	p.refreshBatchLocked(time.Now())
	p.notify()
}

// ClearModelBlock 在某模型请求成功后解除该 (号, 模型) 的封禁。
func (p *Pool) ClearModelBlock(id, model string) {
	if model == "" {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil || len(k.ModelBlocks) == 0 {
		return
	}
	if _, ok := k.ModelBlocks[model]; ok {
		delete(k.ModelBlocks, model)
		p.notify()
	}
}

// ReenableIfCredits 在人工补额/校准后解冻"余额耗尽"类冷却。
// 只解 exhausted 且冷却已到期的那部分：软限流的恢复证据是上游重置时刻，不是余额。
func (p *Pool) ReenableIfCredits(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	k := p.keys[id]
	if k == nil || k.Budget <= 0 {
		return
	}
	if k.State == StateExhausted && k.Balance() >= p.cfg.CreditFloor {
		k.State = StateActive
		k.CooldownUntil = time.Time{}
		k.CooldownReason = ""
		p.refreshBatchLocked(time.Now())
		p.notify()
	}
}

// applyFailLocked 是状态转移的唯一权威实现。
//
// escalate 表示"这一笔算业务流量、要把退避次数 +1"。探活/预热传 false：
// 它们的目的是验证"好了没有"，不该因为多探了几次就把号关得更久。
func (p *Pool) applyFailLocked(k *Key, o Outcome, now time.Time, escalate bool) {
	switch o.Kind {
	case fault.Client:
		// 请求本身有问题，号没毛病：只记 last_error，不动状态。
		return

	case fault.Auth:
		k.State = StateInvalid
		k.CooldownUntil = time.Time{}
		k.CooldownReason = "密钥失效（401）"

	case fault.Quota:
		// 余额不足：充钱就能活，所以不是"永久报废"，但也不能每 30 分钟
		// 拉回来撞一次——100 个号 99 个没钱时，那会变成每半小时一轮的
		// 无效请求波。按连续欠费次数指数放大，封顶 ExhaustedMax。
		if escalate {
			k.QuotaHits++
		}
		d := backoffDur(p.cfg.ExhaustedCooldown.Duration(), p.cfg.ExhaustedMax.Duration(), k.QuotaHits)
		k.State = StateExhausted
		k.CooldownUntil = now.Add(d)
		k.CooldownReason = firstNonEmpty(o.Message, "余额不足（402）")

	case fault.Banned:
		// 403 ACCOUNT_SUSPENDED：账号级冻结。换号有意义（Retryable），
		// 但这个号只能等上游解封，按更长档位退避。
		if escalate {
			k.BanHits++
		}
		d := backoffDur(p.cfg.BannedCooldown.Duration(), p.cfg.BannedMax.Duration(), k.BanHits)
		// 上游明确给了恢复时间就至少等那么久（冻结类错误很少给，给了就别浪费）。
		if o.RetryAfter > d {
			d = o.RetryAfter
		}
		k.State = StateBlocked
		k.CooldownUntil = now.Add(d)
		k.CooldownReason = firstNonEmpty(o.Message, "账号被冻结（403）")

	case fault.Rate:
		// 上游给了恢复时间就照它来；只有"没给"的时候才指数退避。
		// 关键：已经在冷却中就不再延长——重试/并发探测反复撞 429 会把
		// 一个健康号越关越久，这是"全池被推到封顶"的经典成因。
		var d time.Duration
		switch {
		case o.RetryAfter > 0:
			d = o.RetryAfter
		case k.State == StateCooling && now.Before(k.CooldownUntil):
			return
		default:
			k.RateStreak++
			d = p.cfg.RateCooldown.Duration() * (1 << uint(minInt(k.RateStreak-1, 6)))
		}
		if d < time.Second {
			d = time.Second
		}
		if max := p.cfg.RateMax.Duration(); max > 0 && d > max {
			d = max
		}
		k.State = StateCooling
		k.CooldownUntil = now.Add(d)
		k.CooldownReason = firstNonEmpty(o.Message, "限流（429）")

	case fault.UpstreamRate:
		// 模型供应链限流：跟号无关，不罚号；只封 (号, 模型) 一小会儿，
		// 请求层退避后优先同号重试。
		d := p.cfg.ModelRateCooldown.Duration()
		if o.RetryAfter > d {
			d = o.RetryAfter
		}
		if max := p.cfg.RateMax.Duration(); max > 0 && d > max {
			d = max
		}
		if d <= 0 {
			d = 30 * time.Second
		}
		p.blockModelLocked(k, o.Model, "limited", now.Add(d), firstNonEmpty(o.Message, "模型侧限流"), 0)

	case fault.ModelDenied:
		// 只有这个号没开通这个模型：换号大概率能救，所以不罚号，只封 (号, 模型)。
		hits := 1
		if b, ok := k.ModelBlocks[o.Model]; ok {
			hits = b.Hits + 1
		}
		d := p.cfg.ModelBlockBase.Duration()
		for i := 1; i < hits && i <= 6; i++ {
			d *= 2
			if max := p.cfg.ModelBlockMax.Duration(); max > 0 && d >= max {
				d = max
				break
			}
		}
		if max := p.cfg.ModelBlockMax.Duration(); max > 0 && d > max {
			d = max
		}
		if d <= 0 {
			d = 6 * time.Hour
		}
		p.blockModelLocked(k, o.Model, "denied", now.Add(d), firstNonEmpty(o.Message, "模型未开通"), hits)

	case fault.Upstream:
		k.FailStreak++
		if k.FailStreak < p.cfg.BreakerThreshold {
			return // 还没到熔断线，只是记一笔
		}
		d := p.cfg.BreakerCooldown.Duration()
		for i := 0; i < k.BreakerCount; i++ {
			d *= 2
			if max := p.cfg.BreakerMax.Duration(); max > 0 && d >= max {
				d = max
				break
			}
		}
		if max := p.cfg.BreakerMax.Duration(); max > 0 && d > max {
			d = max
		}
		if d <= 0 {
			d = 30 * time.Second
		}
		k.BreakerUntil = now.Add(d)
		k.BreakerCount++
		k.FailStreak = 0
		k.CooldownReason = firstNonEmpty(o.Message, "连续上游失败，熔断")
	}
}

func (p *Pool) blockModelLocked(k *Key, model, kind string, until time.Time, reason string, hits int) {
	if model == "" {
		return
	}
	if k.ModelBlocks == nil {
		k.ModelBlocks = map[string]ModelBlock{}
	}
	// 已有更晚的封禁时不缩短（同一模型连续命中，取最保守的那个）。
	if old, ok := k.ModelBlocks[model]; ok && old.Until.After(until) && old.Kind == kind {
		if hits > 0 {
			old.Hits = hits
		}
		k.ModelBlocks[model] = old
		return
	}
	k.ModelBlocks[model] = ModelBlock{Model: model, Until: until, Reason: truncate(reason, 200), Kind: kind, Hits: hits}
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// backoffDur 按命中次数指数放大冷却：hits<=1 用起点，2 倍、4 倍…到 max 截断。
// hits 会随"欠费/冻结"次数累积，只在成功后清零，所以稳定欠费的号会越来越安静。
func backoffDur(base, max time.Duration, hits int) time.Duration {
	if base <= 0 {
		base = 30 * time.Second
	}
	if hits < 1 {
		hits = 1
	}
	d := base
	for i := 1; i < hits && i <= 8; i++ {
		d *= 2
		if max > 0 && d >= max {
			return max
		}
	}
	if max > 0 && d > max {
		d = max
	}
	return d
}
