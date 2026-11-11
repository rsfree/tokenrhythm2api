package pool

import (
	"strings"
	"testing"

	"trpool/internal/config"
	"trpool/internal/fault"
)

// 402 余额不足 = 账号不可用：命中即停用，没有冷却，Tick 也不会把它放回来。
//
// 这是刻意的："过一会儿自动重试"在 100 个号 99 个没充值时，只会变成
// 每轮一轮的无效请求，还白占活跃窗口名额。
func TestQuotaStopsKeyForGood(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)

	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE 账户余额不足"})

	got := p.keys[k.ID]
	if got.State != StateExhausted {
		t.Fatalf("状态 = %s, 想要 exhausted", got.State)
	}
	if got.QuotaHits != 1 {
		t.Fatalf("欠费计数 = %d, 想要 1", got.QuotaHits)
	}
	if !got.CooldownUntil.IsZero() {
		t.Fatalf("不该再有冷却（%s）：停用是终态，恢复必须靠人工", got.CooldownUntil)
	}
	if !strings.Contains(got.CooldownReason, "停用") {
		t.Fatalf("原因里应说明已停用，实际 %q", got.CooldownReason)
	}

	// 无论过多久、Tick 多少次，都不能自己回来
	for i := 0; i < 5; i++ {
		p.Tick()
	}
	if st := p.keys[k.ID].State; st != StateExhausted {
		t.Fatalf("Tick 之后状态 = %s, 停用的号不能自动复活", st)
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("余额不足的号不应再被派单")
	} else if !strings.Contains(err.Error(), "余额耗尽") {
		t.Fatalf("错误应说明是余额耗尽: %v", err)
	}
}

// 403 ACCOUNT_SUSPENDED 同样是停用终态：只有人工解封后才该回来。
func TestBannedStopsKeyForGood(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "403 ACCOUNT_SUSPENDED 账号已临时冻结"})

	got := p.keys[k.ID]
	if got.State != StateBlocked {
		t.Fatalf("状态 = %s, 想要 blocked", got.State)
	}
	if !got.CooldownUntil.IsZero() {
		t.Fatalf("冻结不该有冷却，实际 %s", got.CooldownUntil)
	}
	if !strings.Contains(got.CooldownReason, "停用") {
		t.Fatalf("原因里应说明已停用，实际 %q", got.CooldownReason)
	}
	for i := 0; i < 5; i++ {
		p.Tick()
	}
	if st := p.keys[k.ID].State; st != StateBlocked {
		t.Fatalf("Tick 之后状态 = %s, 冻结号不能自动复活", st)
	}
}

// 403 ACCOUNT_SUSPENDED 是账号冻结，不是次数配额：不能计入 429 压力，
// 否则会拿"多开号"去治一个被封的号，把弹性窗口顶起来（线上真实踩过）。
func TestBannedIsNotRatePressure(t *testing.T) {
	// 弹性关闭时统计不落窗口，这里要开着才能看到分子/分母。
	// 顺序取号，保证第一笔一定落在 a 上（加权随机每次跑可能落到 b）
	p := testPool(t, func(c *config.PoolConfig) {
		c.Batch.Elastic.Enabled = true
		c.Strategy = "sequential"
	})
	k := addKey(t, p, "a", 0)

	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "403 ACCOUNT_SUSPENDED 账号已临时冻结"})

	if got := p.keys[k.ID]; got.BanHits != 1 {
		t.Fatalf("冻结计数 = %d, 想要 1", got.BanHits)
	}
	st := p.BatchState()
	if st.RateHits != 0 {
		t.Fatalf("429 命中 = %d, 冻结不该算压力", st.RateHits)
	}
	if st.RateTotal != 1 {
		t.Fatalf("样本总数 = %d, 想要 1（算进分母，但不算分子）", st.RateTotal)
	}

	// 真 429 还是要算压力，否则弹性就废了（a 已冻结，这时候只会派 b）
	addKey(t, p, "b", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m"}), Outcome{Kind: fault.Rate, Message: "429 RATE_LIMITED"})
	if st := p.BatchState(); st.RateHits != 1 || st.RateTotal != 2 {
		t.Fatalf("真 429 应计入压力，实际 %d/%d", st.RateHits, st.RateTotal)
	}
}

// 全池不可用时的兜底探活会拿真实请求去试；停用号已经确定不可用，不能被挑中。
func TestFallbackProbeSkipsBlocked(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.FallbackProbe = true
		c.Strategy = "weighted_random"
	})
	addKey(t, p, "a", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"})

	_, err := p.Acquire(PickRequest{Model: "m"})
	if err == nil {
		t.Fatal("冻结号不该被兜底探活选中（会拿客户端请求去撞一个已知被封的号）")
	}
	if !strings.Contains(err.Error(), "账号已冻结") {
		t.Fatalf("错误里应能看出是冻结: %v", err)
	}
}

// 探活失败不该让停用号变得更难恢复，也不该累加计数（停用后没有退避这回事）。
func TestProbeFailDoesNotEscalate(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	o := Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"}
	p.MarkFail(k.ID, o)
	p.MarkFail(k.ID, o)
	got := p.keys[k.ID]
	if got.BanHits != 0 {
		t.Fatalf("探活不该累加计数，实际 %d", got.BanHits)
	}
	if got.State != StateBlocked || !got.CooldownUntil.IsZero() {
		t.Fatalf("探活失败后应保持停用且无冷却: state=%s until=%s", got.State, got.CooldownUntil)
	}
}

// 人工探活成功是停用号的恢复通道之一（充完值/解封后先探一下）。
func TestProbeOKRevivesStopped(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"})

	p.MarkOK(k.ID)
	got := p.keys[k.ID]
	if got.State != StateActive || got.BanHits != 0 || got.LastError != "" {
		t.Fatalf("探活成功应把号放回来: state=%s hits=%d err=%q", got.State, got.BanHits, got.LastError)
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err != nil {
		t.Fatalf("恢复后应能派单: %v", err)
	}
}

// 人工停用的号是"我故意摘的"，探活成功也不该把它放回来。
func TestProbeOKDoesNotTouchDisabled(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	no := false
	if err := p.Update(k.ID, nil, nil, nil, nil, &no); err != nil {
		t.Fatal(err)
	}
	p.MarkOK(k.ID)
	if st := p.keys[k.ID].State; st != StateDisabled {
		t.Fatalf("人工停用的号被探活放回来了，状态 = %s", st)
	}
}

// 补额度是欠费号的恢复通道：钱到了就不用人工再点一次。
func TestAddBudgetRevivesExhausted(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 10)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})
	if p.keys[k.ID].State != StateExhausted {
		t.Fatal("先要进入 exhausted")
	}
	p.AddBudget(k.ID, 100)
	p.ReenableIfCredits(k.ID)
	got := p.keys[k.ID]
	if got.State != StateActive || got.QuotaHits != 0 {
		t.Fatalf("补额后应恢复: state=%s hits=%d", got.State, got.QuotaHits)
	}
}

// 补额度不该顺手解开冻结号：那是上游的处罚，充钱没用。
func TestAddBudgetKeepsBlocked(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 10)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"})
	p.AddBudget(k.ID, 100)
	p.ReenableIfCredits(k.ID)
	if st := p.keys[k.ID].State; st != StateBlocked {
		t.Fatalf("补额度把冻结号解开了，状态 = %s", st)
	}
}

// 「恢复停用号」把一批欠费/冻结的号一次放回来，但不碰人工停用与 401。
func TestReviveStoppedBulk(t *testing.T) {
	p := testPool(t, nil)
	a := addKey(t, p, "a", 0)
	b := addKey(t, p, "b", 0)
	c := addKey(t, p, "c", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m", PreferKeyID: a.ID}),
		Outcome{Kind: fault.Quota, Message: "402"})
	p.Release(acquire(t, p, PickRequest{Model: "m", PreferKeyID: b.ID}),
		Outcome{Kind: fault.Banned, Message: "403"})
	p.Release(acquire(t, p, PickRequest{Model: "m", PreferKeyID: c.ID}),
		Outcome{Kind: fault.Auth, Message: "401"})

	names := p.ReviveStopped()
	if len(names) != 2 {
		t.Fatalf("恢复了 %d 个 (%v), 想要 2（a 欠费 + b 冻结）", len(names), names)
	}
	if st := p.keys[a.ID].State; st != StateActive {
		t.Fatalf("a 应恢复，实际 %s", st)
	}
	if st := p.keys[b.ID].State; st != StateActive {
		t.Fatalf("b 应恢复，实际 %s", st)
	}
	if st := p.keys[c.ID].State; st != StateInvalid {
		t.Fatalf("401 的号不该被恢复，实际 %s", st)
	}
}

// 预热/自动探活不该去打停用号：那既不会让它们复活，又白烧上游配额。
func TestEnabledKeysSkipsStopped(t *testing.T) {
	p := testPool(t, nil)
	a := addKey(t, p, "a", 0)
	addKey(t, p, "b", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m", PreferKeyID: a.ID}),
		Outcome{Kind: fault.Quota, Message: "402"})

	views := p.EnabledKeys()
	if len(views) != 1 || views[0].State != string(StateActive) {
		t.Fatalf("EnabledKeys 只应包含能派单的号，实际 %+v", views)
	}
}
