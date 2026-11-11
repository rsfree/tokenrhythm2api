package pool

import (
	"strings"
	"testing"
	"time"

	"trpool/internal/config"
	"trpool/internal/fault"
	"trpool/internal/session"
)

func testPool(t *testing.T, tune func(*config.PoolConfig)) *Pool {
	t.Helper()
	cfg := config.Default().Pool
	cfg.MinPickGap = 0
	cfg.MaxInFlight = 0
	cfg.CreditFloor = 0
	cfg.FallbackProbe = false
	cfg.Batch.Mode = "off"
	if tune != nil {
		tune(&cfg)
	}
	return New(cfg, session.New(cfg.SessionSticky.TTL.Duration(), cfg.SessionSticky.MaxEntries))
}

func addKey(t *testing.T, p *Pool, name string, budget float64) *Key {
	t.Helper()
	k, err := p.Add(name, "sk_tr_"+name+"_0123456789", 1, budget)
	if err != nil {
		t.Fatalf("add %s: %v", name, err)
	}
	return k
}

func acquire(t *testing.T, p *Pool, req PickRequest) *Lease {
	t.Helper()
	l, err := p.Acquire(req)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	return l
}

func TestAddRejectsDuplicateSecret(t *testing.T) {
	p := testPool(t, nil)
	addKey(t, p, "a", 0)
	if _, err := p.Add("b", "sk_tr_a_0123456789", 1, 0); err == nil {
		t.Fatal("同一密钥应被拒绝")
	}
	if _, err := p.Add("c", "bad-prefix", 1, 0); err == nil {
		t.Fatal("形态不对的密钥应被拒绝")
	}
}

func TestAcquireEmptyPool(t *testing.T) {
	p := testPool(t, nil)
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("空池应报错")
	}
}

// 选不到号时的错误信息必须点名成因：401 失效和 429 冷却的处置完全相反，
// 只说一句"全部在途已满"会把人带到错误的方向。
func TestNoCapacityReasonIsSpecific(t *testing.T) {
	p := testPool(t, nil)
	a := addKey(t, p, "a", 0)
	p.MarkFail(a.ID, Outcome{Kind: fault.Auth, Model: "m", Message: "UNAUTHORIZED"})

	_, err := p.Acquire(PickRequest{Model: "m"})
	if err == nil {
		t.Fatal("唯一号失效后应选不到号")
	}
	if msg := err.Error(); !strings.Contains(msg, "失效(401)") {
		t.Fatalf("错误信息应点明 401 失效，实际 %q", msg)
	}

	b := addKey(t, p, "b", 0)
	p.MarkFail(b.ID, Outcome{Kind: fault.Rate, Model: "m", Message: "RATE_LIMITED", RetryAfter: 30 * time.Second})
	_, err = p.Acquire(PickRequest{Model: "m"})
	if err == nil {
		t.Fatal("两个号都不可用时应选不到号")
	}
	msg := err.Error()
	if !strings.Contains(msg, "冷却中") || !strings.Contains(msg, "失效(401)") {
		t.Fatalf("应同时点明冷却与失效，实际 %q", msg)
	}
	if !strings.Contains(msg, "最早") {
		t.Fatalf("有冷却时应给出恢复时刻，实际 %q", msg)
	}
}

func TestSessionSticky(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.SessionSticky.Enabled = true
		c.SessionSticky.TTL = config.Duration(time.Hour)
	})
	addKey(t, p, "a", 0)
	addKey(t, p, "b", 0)

	var first string
	for i := 0; i < 6; i++ {
		l := acquire(t, p, PickRequest{Model: "m", SessionKey: 42})
		if i == 0 {
			first = l.KeyID
		}
		if l.KeyID != first {
			t.Fatalf("同一会话应粘在同一号上：第 %d 次到了 %s，首轮 %s", i, l.KeyID, first)
		}
		p.ReleaseSuccess(l)
	}
}

func TestSessionDroppedWhenKeyCooling(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.SessionSticky.Enabled = true
		c.SessionSticky.TTL = config.Duration(time.Hour)
	})
	a := addKey(t, p, "a", 0)

	l := acquire(t, p, PickRequest{Model: "m", SessionKey: 7})
	if l.KeyID != a.ID {
		t.Fatalf("只有一个号，应选中 a，实际 %s", l.KeyID)
	}
	p.Release(l, Outcome{Kind: fault.Rate, Message: "429", RetryAfter: time.Minute})

	b := addKey(t, p, "b", 0)
	next := acquire(t, p, PickRequest{Model: "m", SessionKey: 7})
	if next.KeyID == a.ID {
		t.Fatal("会话绑定的号在冷却，应重新分配")
	}
	if next.KeyID != b.ID {
		t.Fatalf("应重新绑到 b，实际 %s", next.KeyID)
	}
	p.ReleaseSuccess(next)
}

func TestModelDeniedOnlyBlocksThatModel(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.ModelBlockBase = config.Duration(6 * time.Hour)
	})
	a := addKey(t, p, "a", 0)
	addKey(t, p, "b", 0)

	l := acquire(t, p, PickRequest{Model: "glm-5.1", PreferKeyID: a.ID})
	if l.KeyID != a.ID {
		t.Fatalf("软偏好应选中 a，实际 %s", l.KeyID)
	}
	p.Release(l, Outcome{Kind: fault.ModelDenied, Model: "glm-5.1", Message: "MODEL_ACCESS_DENIED"})

	for _, v := range p.Views() {
		if v.ID != a.ID {
			continue
		}
		if v.State != "active" {
			t.Fatalf("模型未开通不该罚整个号，state=%s", v.State)
		}
		if len(v.ModelBlocks) != 1 || v.ModelBlocks[0].Model != "glm-5.1" {
			t.Fatalf("应只有 glm-5.1 被封，实际 %+v", v.ModelBlocks)
		}
	}

	// 同模型换到另一个号。
	next := acquire(t, p, PickRequest{Model: "glm-5.1"})
	if next.KeyID == a.ID {
		t.Fatal("被封的号不该再接同一模型")
	}
	p.ReleaseSuccess(next)

	// 别的模型仍可用原号：软偏好指向 a 应该能成。
	other := acquire(t, p, PickRequest{Model: "kimi-k2", PreferKeyID: a.ID})
	if other.KeyID != a.ID {
		t.Fatalf("换模型应回到原号，实际 %s", other.KeyID)
	}
	p.ReleaseSuccess(other)
}

func TestModelDeniedBackoffGrows(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.ModelBlockBase = config.Duration(time.Hour)
		c.ModelBlockMax = config.Duration(10 * time.Hour)
	})
	a := addKey(t, p, "a", 0)
	addKey(t, p, "b", 0)

	l := acquire(t, p, PickRequest{Model: "m", PreferKeyID: a.ID})
	p.Release(l, Outcome{Kind: fault.ModelDenied, Model: "m"})
	first := a.ModelBlocks["m"].Until

	// 同号同模型再撞一次（探活/并发路径用 MarkFail）。
	p.MarkFail(a.ID, Outcome{Kind: fault.ModelDenied, Model: "m", Message: "MODEL_ACCESS_DENIED"})
	second := a.ModelBlocks["m"].Until
	if !second.After(first) {
		t.Fatalf("第二次封禁应更长：first=%s second=%s", first, second)
	}

	// 封禁期间同模型只能走 b。
	next := acquire(t, p, PickRequest{Model: "m"})
	if next.KeyID == a.ID {
		t.Fatal("模型封禁期间不该再选中")
	}
	p.ReleaseSuccess(next)
}

func TestRateCooldownDoesNotExtend(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.RateCooldown = config.Duration(5 * time.Second)
		c.RateMax = config.Duration(time.Minute)
	})
	k := addKey(t, p, "a", 0)

	p.MarkFail(k.ID, Outcome{Kind: fault.Rate, Message: "RATE_LIMITED"})
	first := k.CooldownUntil
	p.MarkFail(k.ID, Outcome{Kind: fault.Rate, Message: "RATE_LIMITED"})
	if !k.CooldownUntil.Equal(first) {
		t.Fatalf("冷却中再次 429 不应延长冷却：%s → %s", first, k.CooldownUntil)
	}
}

func TestRateCooldownUsesRetryAfter(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.RateCooldown = config.Duration(time.Second)
		c.RateMax = config.Duration(time.Minute)
	})
	k := addKey(t, p, "a", 0)
	l := acquire(t, p, PickRequest{Model: "m"})
	p.Release(l, Outcome{Kind: fault.Rate, Message: "RATE_LIMITED", RetryAfter: 12 * time.Second})

	left := time.Until(k.CooldownUntil)
	if left < 10*time.Second || left > 13*time.Second {
		t.Fatalf("应按上游 retryAfter 冷却（≈12s），实际 %s", left)
	}
}

func TestRateCooldownCap(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.RateCooldown = config.Duration(time.Second)
		c.RateMax = config.Duration(30 * time.Second)
	})
	k := addKey(t, p, "a", 0)
	l := acquire(t, p, PickRequest{Model: "m"})
	p.Release(l, Outcome{Kind: fault.Rate, RetryAfter: time.Hour})
	if left := time.Until(k.CooldownUntil); left > 31*time.Second {
		t.Fatalf("冷却应被封顶到 30s，实际 %s", left)
	}
}

func TestBreakerThreshold(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.BreakerThreshold = 2
		c.BreakerCooldown = config.Duration(10 * time.Second)
		c.BreakerMax = config.Duration(time.Minute)
	})
	k := addKey(t, p, "a", 0)

	p.Release(acquire(t, p, PickRequest{Model: "m"}), Outcome{Kind: fault.Upstream, Message: "500"})
	if !k.BreakerUntil.IsZero() {
		t.Fatal("第一次 5xx 不该熔断")
	}
	p.Release(acquire(t, p, PickRequest{Model: "m"}), Outcome{Kind: fault.Upstream, Message: "500"})
	if k.BreakerUntil.IsZero() {
		t.Fatal("达到阈值应熔断")
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("熔断期间不该有可用号")
	}
}

func TestAuthAndQuotaStates(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.ExhaustedCooldown = config.Duration(time.Minute) })
	a := addKey(t, p, "a", 0)
	b := addKey(t, p, "b", 0)

	p.Release(acquire(t, p, PickRequest{Model: "m", PreferKeyID: a.ID}), Outcome{Kind: fault.Auth, Message: "401"})
	invalid, exhausted := 0, 0
	for _, v := range p.Views() {
		switch v.State {
		case "invalid":
			invalid++
		case "exhausted":
			exhausted++
		}
	}
	if invalid != 1 {
		t.Fatalf("应有一个号进 invalid，实际 %d", invalid)
	}
	p.Release(acquire(t, p, PickRequest{Model: "m", PreferKeyID: b.ID}), Outcome{Kind: fault.Quota, Message: "402"})
	for _, v := range p.Views() {
		if v.State == "exhausted" {
			exhausted++
		}
	}
	if exhausted != 1 {
		t.Fatalf("应有一个号进 exhausted，实际 %d", exhausted)
	}

	for _, id := range []string{a.ID, b.ID} {
		p.Reset(id)
	}
	if total, healthy, _, _, _, _, _ := p.Counts(); total != 2 || healthy != 2 {
		t.Fatalf("重置后应全部健康，实际 total=%d healthy=%d", total, healthy)
	}
}

func TestCreditFloorBlocksPaidOnly(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.CreditFloor = 1 })
	k := addKey(t, p, "a", 10)
	p.Record(k.ID, 0, 0, 9.5) // 余额 0.5 < 1

	if _, err := p.Acquire(PickRequest{Model: "paid", Paid: true}); err == nil {
		t.Fatal("余额触底不该再接收费模型")
	}
	l, err := p.Acquire(PickRequest{Model: "free", Paid: false})
	if err != nil {
		t.Fatalf("免费模型应放行: %v", err)
	}
	p.ReleaseSuccess(l)
}

func TestUnknownBudgetNotStarved(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.CreditFloor = 5
		c.TopK = 2
	})
	addKey(t, p, "known", 0)
	fresh := addKey(t, p, "fresh", 0)
	hits := 0
	for i := 0; i < 200; i++ {
		l := acquire(t, p, PickRequest{Model: "paid", Paid: true})
		if l.KeyID == fresh.ID {
			hits++
		}
		p.ReleaseSuccess(l)
	}
	if hits == 0 {
		t.Fatal("充值额未知的号被打成 0 权重，永远抽不中")
	}
}

func TestInflightLimit(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.MaxInFlight = 1 })
	addKey(t, p, "a", 0)

	l := acquire(t, p, PickRequest{Model: "m"})
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("单号在途已满，不该再派")
	}
	p.ReleaseSuccess(l)
	l2 := acquire(t, p, PickRequest{Model: "m"})
	p.ReleaseSuccess(l2)
}

func TestDoubleReleaseDoesNotLeak(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.MaxInFlight = 1 })
	addKey(t, p, "a", 0)
	l := acquire(t, p, PickRequest{Model: "m"})
	p.ReleaseSuccess(l)
	p.ReleaseSuccess(l)
	if st := p.BatchState(); st.TotalInflight != 0 {
		t.Fatalf("在途计数泄漏: %d", st.TotalInflight)
	}
	l2 := acquire(t, p, PickRequest{Model: "m"})
	p.ReleaseSuccess(l2)
}

func TestStickyTaskReturnsCoolingKey(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.ExhaustedCooldown = config.Duration(time.Minute) })
	a := addKey(t, p, "a", 0)
	addKey(t, p, "b", 0)
	a.State = StateExhausted
	a.CooldownUntil = time.Now().Add(time.Minute)

	l := acquire(t, p, PickRequest{Model: "m", StickyKeyID: a.ID})
	if l.KeyID != a.ID {
		t.Fatalf("视频查询必须回创建者 %s，实际 %s", a.ID, l.KeyID)
	}
	p.ReleaseSuccess(l)
}

func TestFallbackPicksEarliestExpiry(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.FallbackProbe = true })
	a := addKey(t, p, "a", 0)
	b := addKey(t, p, "b", 0)
	a.State, b.State = StateCooling, StateCooling
	a.CooldownUntil = time.Now().Add(30 * time.Second)
	b.CooldownUntil = time.Now().Add(10 * time.Second)

	l := acquire(t, p, PickRequest{Model: "m"})
	if l.KeyID != b.ID || !l.Fallback {
		t.Fatalf("应兜底到最早恢复的号 %s（fallback），实际 %s fallback=%v", b.ID, l.KeyID, l.Fallback)
	}
	p.ReleaseSuccess(l)
}

func TestFallbackSkipsExhaustedAndInvalid(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.FallbackProbe = true })
	a := addKey(t, p, "a", 0)
	b := addKey(t, p, "b", 0)
	a.State = StateExhausted
	a.CooldownUntil = time.Now().Add(time.Minute)
	b.State = StateInvalid
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("exhausted/invalid 号不该参与兜底")
	}
}

func TestAllKeysModelBlockedExplains(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) { c.FallbackProbe = false })
	addKey(t, p, "a", 0)
	addKey(t, p, "b", 0)
	for i := 0; i < 2; i++ {
		l := acquire(t, p, PickRequest{Model: "m"})
		p.Release(l, Outcome{Kind: fault.ModelDenied, Model: "m", Message: "MODEL_ACCESS_DENIED"})
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("全池模型封禁后应失败")
	} else if !strings.Contains(err.Error(), "封禁") {
		t.Fatalf("错误信息应说明是模型封禁，实际: %s", err.Error())
	}
}

func TestBatchAutoGrowsWithInflight(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.MaxInFlight = 10
		c.Batch.Mode = "auto"
		c.Batch.Min = 1
		c.Batch.TargetInflightPerKey = 2
	})
	for i := 0; i < 5; i++ {
		addKey(t, p, string(rune('a'+i)), 0)
	}
	var leases []*Lease
	for i := 0; i < 4; i++ {
		leases = append(leases, acquire(t, p, PickRequest{Model: "m"}))
	}
	st := p.BatchState()
	if st.Size == 0 || st.Target == 0 {
		t.Fatalf("batch 应有成员与目标: %+v", st)
	}
	if st.Target != 2 {
		t.Fatalf("4 在途 / 每号 2 → target 应为 2，实际 %+v", st)
	}
	for _, l := range leases {
		p.ReleaseSuccess(l)
	}
}

func TestSnapshotRoundTrip(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 12.5)
	k.Weight = 2
	p.SetSticky("task-1", k.ID)
	l := acquire(t, p, PickRequest{Model: "m"})
	p.Release(l, Outcome{Kind: fault.ModelDenied, Model: "m", Message: "denied"})
	p.Record(k.ID, 10, 20, 0.01)

	snap := p.Snapshot()
	q := testPool(t, nil)
	q.Restore(snap)

	views := q.Views()
	if len(views) != 1 {
		t.Fatalf("恢复后应有 1 个号，实际 %d", len(views))
	}
	v := views[0]
	if v.ID != k.ID || v.Name != "a" || v.Budget != 12.5 || v.Weight != 2 {
		t.Fatalf("恢复字段不一致: %+v", v)
	}
	if v.PromptTokens != 10 || v.CompletionTokens != 20 {
		t.Fatalf("用量没恢复: %+v", v)
	}
	if len(v.ModelBlocks) != 1 {
		t.Fatalf("模型封禁没恢复: %+v", v.ModelBlocks)
	}
	if got := q.StickyKey("task-1"); got != k.ID {
		t.Fatalf("粘性任务没恢复: %s", got)
	}
}

func TestTickRevivesCooling(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	k.State = StateCooling
	k.CooldownUntil = time.Now().Add(-time.Second)
	p.Tick()
	if k.State != StateActive {
		t.Fatalf("到期冷却应复活，实际 %s", k.State)
	}
}

func TestModelHealthAggregates(t *testing.T) {
	p := testPool(t, nil)
	a := addKey(t, p, "a", 0)
	b := addKey(t, p, "b", 0)
	for _, k := range []*Key{a, b} {
		k.ModelBlocks = map[string]ModelBlock{
			"m": {Model: "m", Until: time.Now().Add(time.Hour), Kind: "denied"},
		}
	}
	list := p.ModelHealth()
	if len(list) != 1 || list[0].Keys != 2 || list[0].Model != "m" {
		t.Fatalf("模型健康聚合不对: %+v", list)
	}
}
