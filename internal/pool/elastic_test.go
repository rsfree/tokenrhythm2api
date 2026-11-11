package pool

import (
	"testing"
	"time"

	"trpool/internal/config"
	"trpool/internal/fault"
)

// fixedWindow 是"100 个号只留 12 个在用"的常用配置。
func fixedWindow(size int, tune func(*config.PoolConfig)) func(*config.PoolConfig) {
	return func(c *config.PoolConfig) {
		c.Batch.Mode = "fixed"
		c.Batch.Size = size
		c.Batch.Min = 1
		c.Batch.Elastic.Enabled = false
		if tune != nil {
			tune(c)
		}
	}
}

func TestFixedWindowKeepsOnlyN(t *testing.T) {
	p := testPool(t, fixedWindow(12, nil))
	for i := 0; i < 100; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	st := p.BatchState()
	if st.Target != 12 || st.Size != 12 {
		t.Fatalf("窗口应为 12/12，实际 target=%d size=%d", st.Target, st.Size)
	}
	if st.PoolSize != 100 || st.Available != 100 {
		t.Fatalf("池子 100 个号都应可用，实际 pool=%d avail=%d", st.PoolSize, st.Available)
	}

	// 派单只应该落在窗口内的号上。
	in := map[string]bool{}
	for _, id := range st.Members {
		in[id] = true
	}
	for i := 0; i < 30; i++ {
		l := acquire(t, p, PickRequest{Model: "m"})
		if !in[l.KeyID] {
			t.Fatalf("派出窗口外的号: %s", l.Name)
		}
		p.ReleaseSuccess(l)
	}
}

// 402 让一个号变 exhausted：窗口必须当场补一个，而不是等下一笔请求。
func TestWindowRefillsWhenKeyExhausted(t *testing.T) {
	p := testPool(t, fixedWindow(3, nil))
	for i := 0; i < 6; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	if st := p.BatchState(); st.Size != 3 {
		t.Fatalf("初始窗口应为 3，实际 %d", st.Size)
	}

	l := acquire(t, p, PickRequest{Model: "m"})
	dead := l.KeyID
	p.Release(l, Outcome{Kind: fault.Quota, Message: "余额不足"})

	st := p.BatchState()
	if st.Size != 3 || st.Target != 3 {
		t.Fatalf("号欠费后窗口应自动补回 3，实际 target=%d size=%d", st.Target, st.Size)
	}
	for _, id := range st.Members {
		if id == dead {
			t.Fatal("欠费的号不应继续占窗口名额")
		}
	}
	if st.Available != 5 {
		t.Fatalf("可用号应为 5（6 个里 1 个欠费），实际 %d", st.Available)
	}
}

// 记账把余额跑空（还没等到 402）也要立刻换人。
func TestWindowRefillsWhenBudgetSpent(t *testing.T) {
	p := testPool(t, fixedWindow(2, nil))
	for i := 0; i < 4; i++ {
		addKey(t, p, "k"+itoa(i), 1)
	}
	p.Tick()
	st := p.BatchState()
	if st.Size != 2 {
		t.Fatalf("初始窗口应为 2，实际 %d", st.Size)
	}

	broke := st.Members[0]
	p.Record(broke, 10, 10, 1.0)
	st = p.BatchState()
	if st.Size != 2 {
		t.Fatalf("余额跑空后应补回 2 个，实际 %d", st.Size)
	}
	for _, id := range st.Members {
		if id == broke {
			t.Fatal("余额为 0 的号不应继续占窗口")
		}
	}
	if st.Available != 3 {
		t.Fatalf("可用号应为 3，实际 %d", st.Available)
	}
}

// 号码全死光时不应当报"目标 12"，而是把可用数如实透出来。
func TestWindowReportsShortfall(t *testing.T) {
	p := testPool(t, fixedWindow(5, nil))
	for i := 0; i < 6; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	for _, k := range p.keys {
		p.MarkFail(k.ID, Outcome{Kind: fault.Auth, Message: "密钥失效"})
	}
	st := p.BatchState()
	if st.Size != 0 || st.Available != 0 {
		t.Fatalf("全部失效后不应还有成员: size=%d avail=%d", st.Size, st.Available)
	}
	if st.Target != 5 {
		t.Fatalf("目标仍是配置的 5（缺口由 available 体现），实际 %d", st.Target)
	}
}

func elasticCfg(extra func(*config.Elastic)) func(*config.PoolConfig) {
	return func(c *config.PoolConfig) {
		c.Batch.Mode = "fixed"
		c.Batch.Size = 4
		c.Batch.Min = 1
		e := &c.Batch.Elastic
		e.Enabled = true
		e.Window = config.Duration(time.Second)
		e.TriggerRate = 0.15
		e.MinSamples = 20
		e.MinHits = 2
		e.Step = 2
		e.MaxSize = 10
		e.Hold = config.Duration(time.Minute)
		e.Stable = config.Duration(2 * time.Minute)
		e.DecayStep = 1
		if extra != nil {
			extra(e)
		}
	}
}

// 429 变多 → 窗口自动扩大，且一个统计窗口只扩一次。
func TestElasticGrowsOn429(t *testing.T) {
	p := testPool(t, elasticCfg(nil))
	for i := 0; i < 24; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	if st := p.BatchState(); st.Target != 4 || st.Extra != 0 {
		t.Fatalf("初始应 4 个号、无弹性余量: %+v", st)
	}

	burst := func(n int) {
		for i := 0; i < n; i++ {
			l := acquire(t, p, PickRequest{Model: "m"})
			p.Release(l, Outcome{Kind: fault.Rate, Message: "限流"})
		}
	}
	// 进入下一个统计窗口（把样本与扩容时间都推回去）。
	nextWindow := func() {
		p.mu.Lock()
		p.elastic.grownAt = time.Now().Add(-2 * time.Second)
		p.elastic.total.marks = nil
		p.elastic.limited.marks = nil
		p.mu.Unlock()
	}

	// 第 1 次 429：命中数不够，不扩。
	burst(1)
	if st := p.BatchState(); st.Extra != 0 {
		t.Fatalf("单次 429 不应扩容: extra=%d", st.Extra)
	}

	// 第 2 次 429：两连击就是压力信号，扩 step 个。
	burst(1)
	st := p.BatchState()
	if st.Extra != 2 || st.Target != 6 {
		t.Fatalf("两次 429 后应扩到 6（4+2），实际 extra=%d target=%d", st.Extra, st.Target)
	}
	if st.RateHits < 2 || st.RateTotal < 2 {
		t.Fatalf("窗口统计应记到 429，实际 hits=%d total=%d", st.RateHits, st.RateTotal)
	}

	// 同一窗口内继续 429，不再连续扩容（否则一波抖动会拉满）。
	burst(6)
	if st := p.BatchState(); st.Extra != 2 {
		t.Fatalf("一个窗口内只应扩一次，实际 extra=%d", st.Extra)
	}
	if st.Size < 6 {
		t.Fatalf("窗口成员应跟到 6，实际 %d", st.Size)
	}

	// 下一个窗口再撞 → 继续扩到上限 MaxSize=10，然后停住。
	nextWindow()
	burst(2)
	if st := p.BatchState(); st.Extra != 4 || st.Target != 8 {
		t.Fatalf("第二窗口应扩到 8，实际 extra=%d target=%d", st.Extra, st.Target)
	}
	nextWindow()
	burst(2)
	if st := p.BatchState(); st.Extra != 6 || st.Target != 10 {
		t.Fatalf("应顶到上限 10，实际 extra=%d target=%d", st.Extra, st.Target)
	}
	nextWindow()
	burst(2)
	if st := p.BatchState(); st.Extra != 6 || st.Target != 10 {
		t.Fatalf("到上限后不应继续扩，实际 extra=%d target=%d", st.Extra, st.Target)
	}
}

// 稳定一段时间后自动退回基准窗口。
func TestElasticShrinksWhenCalm(t *testing.T) {
	p := testPool(t, elasticCfg(nil))
	for i := 0; i < 20; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	p.mu.Lock()
	p.elastic.extra = 4
	p.elastic.grownAt = time.Now().Add(-10 * time.Minute)
	p.elastic.last429 = time.Now().Add(-10 * time.Minute)
	p.mu.Unlock()
	if st := p.BatchState(); st.Target != 8 {
		t.Fatalf("手工抬高后目标应为 8，实际 %d", st.Target)
	}

	p.Tick()
	st := p.BatchState()
	if st.Extra != 3 {
		t.Fatalf("一个 hold 只退 1 个，实际 extra=%d", st.Extra)
	}
	if st.Size != 7 {
		t.Fatalf("成员应跟着退到 7，实际 %d", st.Size)
	}

	// 退到 0 之后停住，不会低于基准。
	for i := 0; i < 6; i++ {
		p.mu.Lock()
		p.elastic.grownAt = time.Now().Add(-10 * time.Minute)
		p.mu.Unlock()
		p.Tick()
	}
	st = p.BatchState()
	if st.Extra != 0 || st.Target != 4 || st.Size != 4 {
		t.Fatalf("应回落到基准 4，实际 extra=%d target=%d size=%d", st.Extra, st.Target, st.Size)
	}
}

// 模型侧限流（UPSTREAM_RATE_LIMITED）不是"这个号次数满了"，
// 多开号加不了配额，不应触发扩容。
func TestElasticIgnoresUpstreamRate(t *testing.T) {
	p := testPool(t, elasticCfg(nil))
	for i := 0; i < 10; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	for i := 0; i < 5; i++ {
		l := acquire(t, p, PickRequest{Model: "m"})
		p.Release(l, Outcome{Kind: fault.UpstreamRate, Model: "m", Message: "上游限流"})
	}
	if st := p.BatchState(); st.Extra != 0 {
		t.Fatalf("模型侧限流不应扩窗，实际 extra=%d", st.Extra)
	}
}

// 关掉开关就固定回基准，且不再跟随 429。
func TestElasticDisabled(t *testing.T) {
	p := testPool(t, elasticCfg(func(e *config.Elastic) { e.Enabled = false }))
	for i := 0; i < 10; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	for i := 0; i < 5; i++ {
		l := acquire(t, p, PickRequest{Model: "m"})
		p.Release(l, Outcome{Kind: fault.Rate, Message: "限流"})
	}
	st := p.BatchState()
	if st.Elastic || st.Extra != 0 || st.Target != 4 {
		t.Fatalf("关闭弹性后应恒为基准窗口: %+v", st)
	}
}

// 池子比基准还小时，目标要跟着池子走，不能算出超额目标。
func TestWindowClampedByPoolSize(t *testing.T) {
	p := testPool(t, fixedWindow(12, nil))
	for i := 0; i < 3; i++ {
		addKey(t, p, "k"+itoa(i), 0)
	}
	p.Tick()
	st := p.BatchState()
	if st.Target != 3 || st.Size != 3 {
		t.Fatalf("只有 3 个号时目标应为 3，实际 target=%d size=%d", st.Target, st.Size)
	}
}

// 统计窗口为 0（配置没走 normalize，例如直接 New 出来）时，样本不能被自己裁掉。
//
// 这是 CI 上真实踩到的：Window=0 → trim(now) 把"刚记下"的样本也当过期删了，
// 429 占比永远 0/0。Windows 上两次 time.Now() 常落在同一刻度所以看不出来，
// Linux 的纳秒时钟必现。
func TestElasticZeroWindowStillCounts(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.Strategy = "sequential"
		c.Batch.Mode = "fixed"
		c.Batch.Size = 3
		c.Batch.Elastic = config.Elastic{Enabled: true} // 故意全零值
	})
	addKey(t, p, "a", 0)
	addKey(t, p, "b", 0)

	p.Release(acquire(t, p, PickRequest{Model: "m"}), Outcome{Kind: fault.Rate, Message: "429"})
	st := p.BatchState()
	if st.RateTotal != 1 {
		t.Fatalf("样本总数 = %d, 想要 1（零窗口不该把刚记的样本裁掉）", st.RateTotal)
	}
	if st.RateHits != 1 {
		t.Fatalf("429 命中 = %d, 想要 1", st.RateHits)
	}
	if st.WindowSec != 60 {
		t.Fatalf("窗口秒数 = %d, 零值应按 60s 兜底", st.WindowSec)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [8]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}
