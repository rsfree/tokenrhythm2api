package pool

import (
	"strings"
	"testing"
	"time"

	"trpool/internal/config"
	"trpool/internal/fault"
)

// 402 余额不足：立刻退出窗口，按次数指数退避，而不是每 30 分钟拉回来撞一次。
func TestQuotaMarksExhaustedAndEscalates(t *testing.T) {
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
	if d := time.Until(got.CooldownUntil); d < 25*time.Minute || d > 31*time.Minute {
		t.Fatalf("首次冷却 = %s, 应约等于 exhausted_cooldown(30m)", d)
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("余额不足的号不应再被派单")
	}

	// 到期复活后再次欠费：次数要累加，冷却跟着翻倍（直接改到期时刻，不靠 sleep）
	expireNow(t, p, k.ID)
	if p.keys[k.ID].State != StateActive {
		t.Fatalf("冷却到期应回到 active，实际 %s", p.keys[k.ID].State)
	}
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})
	if hits := p.keys[k.ID].QuotaHits; hits != 2 {
		t.Fatalf("欠费计数 = %d, 想要 2", hits)
	}
	if d := time.Until(p.keys[k.ID].CooldownUntil); d < 55*time.Minute || d > 61*time.Minute {
		t.Fatalf("第二次欠费冷却 = %s, 应翻倍到 1h", d)
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

	got := p.keys[k.ID]
	if got.State != StateBlocked {
		t.Fatalf("状态 = %s, 想要 blocked（不能和普通冷却混为一谈）", got.State)
	}
	if got.BanHits != 1 {
		t.Fatalf("冻结计数 = %d, 想要 1", got.BanHits)
	}
	if !strings.Contains(got.CooldownReason, "冻结") {
		t.Fatalf("冷却原因丢了原文: %q", got.CooldownReason)
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

// 冻结号的冷却必须比 429 长一个量级：秒级重试对封号毫无意义。
func TestBannedCooldownBeatsRateCooldown(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"})
	d := time.Until(p.keys[k.ID].CooldownUntil)
	if d < 10*time.Minute {
		t.Fatalf("冻结冷却 = %s, 默认档位应远长于 429 的 %s", d, p.cfg.RateCooldown.Duration())
	}
}

// 全池不可用时的兜底探活会拿真实请求去试；冻结号已经确定不可用，不能被挑中。
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

func TestTickRevivesBlocked(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"})
	expireNow(t, p, k.ID)
	if st := p.keys[k.ID].State; st != StateActive {
		t.Fatalf("冷冻到期应自动重试，状态 = %s", st)
	}
}

// expireNow 把冷却拨到过去并跑一次 Tick，避免测试里 sleep 造成的抖动。
func expireNow(t *testing.T, p *Pool, id string) {
	t.Helper()
	p.mu.Lock()
	if k := p.keys[id]; k != nil {
		k.CooldownUntil = time.Now().Add(-time.Millisecond)
	}
	p.mu.Unlock()
	p.Tick()
}

// 探活不是业务流量：连点"探活"不能把冻结号的冷却越探越久。
func TestProbeDoesNotEscalate(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	o := Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"}
	p.MarkFail(k.ID, o)
	first := time.Until(p.keys[k.ID].CooldownUntil)
	p.MarkFail(k.ID, o)
	if hits := p.keys[k.ID].BanHits; hits != 0 {
		t.Fatalf("探活不该累加退避次数，实际 %d", hits)
	}
	if d := time.Until(p.keys[k.ID].CooldownUntil); d > first+time.Second {
		t.Fatalf("第二次探活把冷却从 %s 拉长到 %s", first, d)
	}
}

// 人工重置要能把冻结号放出来，并且清空退避计数（否则下次一欠费就长时间静默）。
func TestResetClearsTerminalCounters(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "a", 0)
	p.Release(acquire(t, p, PickRequest{Model: "m"}),
		Outcome{Kind: fault.Banned, Message: "ACCOUNT_SUSPENDED"})
	if !p.Reset(k.ID) {
		t.Fatal("reset 应成功")
	}
	got := p.keys[k.ID]
	if got.State != StateActive || got.BanHits != 0 || !got.CooldownUntil.IsZero() {
		t.Fatalf("重置后仍有残留: state=%s hits=%d until=%s", got.State, got.BanHits, got.CooldownUntil)
	}
}

func TestBackoffDur(t *testing.T) {
	cases := []struct {
		base, max time.Duration
		hits      int
		want      time.Duration
	}{
		{30 * time.Minute, 6 * time.Hour, 0, 30 * time.Minute},
		{30 * time.Minute, 6 * time.Hour, 1, 30 * time.Minute},
		{30 * time.Minute, 6 * time.Hour, 2, time.Hour},
		{30 * time.Minute, 6 * time.Hour, 4, 4 * time.Hour},
		{30 * time.Minute, 6 * time.Hour, 6, 6 * time.Hour}, // 到顶截断
		{30 * time.Minute, 6 * time.Hour, 99, 6 * time.Hour},
	}
	for _, c := range cases {
		if got := backoffDur(c.base, c.max, c.hits); got != c.want {
			t.Errorf("backoffDur(%s, %s, %d) = %s, 想要 %s", c.base, c.max, c.hits, got, c.want)
		}
	}
}
