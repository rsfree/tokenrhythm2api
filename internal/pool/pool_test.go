package pool

import (
	"fmt"
	"testing"
	"time"

	"trpool/internal/config"
)

func testCfg() config.PoolConfig {
	return config.PoolConfig{
		Strategy:          "weighted_random",
		TopK:              5,
		MaxInFlight:       4,
		BreakerThreshold:  3,
		BreakerCooldown:   config.Duration(30 * time.Second),
		BreakerMax:        config.Duration(5 * time.Minute),
		RateCooldown:      config.Duration(20 * time.Second),
		RateMax:           config.Duration(10 * time.Minute),
		ExhaustedCooldown: config.Duration(30 * time.Minute),
		CreditFloor:       0.5,
		IdleWeightPerHour: 0.5,
		IdleWeightMax:     4,
		RetryMax:          3,
		StickyTTL:         config.Duration(72 * time.Hour),
	}
}

func mustAdd(t *testing.T, p *Pool, name, secret string, budget float64) *Key {
	t.Helper()
	k, err := p.Add(name, secret, 1, budget)
	if err != nil {
		t.Fatalf("Add(%s): %v", name, err)
	}
	return k
}

func TestAddRejectsDuplicateSecret(t *testing.T) {
	p := New(testCfg())
	mustAdd(t, p, "a", "sk_tr_aaaa", 0)
	if _, err := p.Add("b", "sk_tr_aaaa", 1, 0); err == nil {
		t.Fatal("同一密钥重复入池应被拒绝")
	}
}

func TestAddRejectsBadShape(t *testing.T) {
	p := New(testCfg())
	if _, err := p.Add("x", "not-a-key", 1, 0); err == nil {
		t.Fatal("形态不对的密钥应被拒绝")
	}
}

// 429 必须让号进入冷却且不再被选中，冷却到期后 Tick 要能自动拉回。
func TestRateLimitCoolsThenRecovers(t *testing.T) {
	cfg := testCfg()
	p := New(cfg)
	k := mustAdd(t, p, "a", "sk_tr_aaaa", 0)

	lease, err := p.Acquire(PickRequest{Model: "m"})
	if err != nil {
		t.Fatalf("首次选号失败: %v", err)
	}
	kind := FailRate
	p.Release(lease, &kind, "429")

	if st := p.keys[k.ID].State; st != StateCooling {
		t.Fatalf("429 后状态 = %s，期望 cooling", st)
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("冷却中的号不应被选中")
	}

	// 把冷却时间拨到过去，Tick 应恢复
	p.mu.Lock()
	p.keys[k.ID].CooldownUntil = time.Now().Add(-time.Second)
	p.mu.Unlock()
	p.Tick()
	if st := p.keys[k.ID].State; st != StateActive {
		t.Fatalf("Tick 后状态 = %s，期望 active", st)
	}
}

// 401 是账号级死亡，必须立刻摘掉，且不会被 Tick 复活。
func TestUnauthorizedMarksInvalid(t *testing.T) {
	p := New(testCfg())
	k := mustAdd(t, p, "a", "sk_tr_aaaa", 0)

	lease, _ := p.Acquire(PickRequest{Model: "m"})
	kind := FailAuth
	p.Release(lease, &kind, "401")

	if p.keys[k.ID].State != StateInvalid {
		t.Fatalf("状态 = %s，期望 invalid", p.keys[k.ID].State)
	}
	p.Tick()
	if p.keys[k.ID].State != StateInvalid {
		t.Fatal("invalid 不应被 Tick 自动恢复")
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("失效号不应被选中")
	}
}

// 4xx 是请求本身的问题，不能记账到号上——否则一个坏请求会锁死整个池子。
func TestClientErrorDoesNotCoolKey(t *testing.T) {
	p := New(testCfg())
	k := mustAdd(t, p, "a", "sk_tr_aaaa", 0)

	lease, _ := p.Acquire(PickRequest{Model: "m"})
	kind := FailClient
	p.Release(lease, &kind, "403 灰度")

	if p.keys[k.ID].State != StateActive {
		t.Fatalf("4xx 后状态 = %s，期望仍为 active", p.keys[k.ID].State)
	}
	if p.keys[k.ID].FailStreak != 0 {
		t.Fatal("4xx 不该累计失败次数")
	}
}

// 余额保底：触底的号不接收费模型，但免费模型仍可派。
func TestCreditFloorSkipsPaidModel(t *testing.T) {
	cfg := testCfg()
	p := New(cfg)
	k := mustAdd(t, p, "poor", "sk_tr_aaaa", 1.0)
	p.Record(k.ID, 0, 0, 0.9) // 余额剩 0.1 < floor 0.5

	if _, err := p.Acquire(PickRequest{Model: "paid-model", Paid: true}); err == nil {
		t.Fatal("余额触底的号不应接收费模型")
	}
	if _, err := p.Acquire(PickRequest{Model: "free-model"}); err != nil {
		t.Fatalf("免费模型应仍可派号: %v", err)
	}
}

// 粘性命中：视频查询必须回到创建任务的号，即使它正在冷却。
func TestStickyWinsEvenWhenCooling(t *testing.T) {
	p := New(testCfg())
	a := mustAdd(t, p, "a", "sk_tr_aaaa", 0)
	b := mustAdd(t, p, "b", "sk_tr_bbbb", 0)

	p.SetSticky("task-1", a.ID)
	// 把 a 打进冷却
	lease, _ := p.Acquire(PickRequest{Model: "m", StickyKeyID: a.ID})
	kind := FailRate
	p.Release(lease, &kind, "429")

	got, err := p.Acquire(PickRequest{Model: "m", StickyKeyID: p.StickyKey("task-1")})
	if err != nil {
		t.Fatalf("粘性选号失败: %v", err)
	}
	if got.KeyID != a.ID {
		t.Fatalf("粘性指向 %s，实际选中 %s（b=%s）", a.ID, got.KeyID, b.ID)
	}
}

func TestInflightCap(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1
	p := New(cfg)
	mustAdd(t, p, "a", "sk_tr_aaaa", 0)

	l1, err := p.Acquire(PickRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("在途已满时不应再派号")
	}
	p.Release(l1, nil, "")
	if _, err := p.Acquire(PickRequest{Model: "m"}); err != nil {
		t.Fatalf("释放后应能继续派号: %v", err)
	}
}

// 加权随机应该把请求摊开，而不是永远压同一个号。
func TestWeightedRandomSpreads(t *testing.T) {
	p := New(testCfg())
	ids := []string{}
	for i := 0; i < 3; i++ {
		k := mustAdd(t, p, fmt.Sprintf("k%d", i), fmt.Sprintf("sk_tr_%04d", i), 0)
		ids = append(ids, k.ID)
	}
	counts := map[string]int{}
	for i := 0; i < 600; i++ {
		l, err := p.Acquire(PickRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		counts[l.KeyID]++
		p.Release(l, nil, "")
	}
	for _, id := range ids {
		if counts[id] < 60 {
			t.Fatalf("号 %s 只被选中 %d 次，分布过于集中: %v", id, counts[id], counts)
		}
	}
}

func TestNoKeyWhenEmpty(t *testing.T) {
	p := New(testCfg())
	if _, err := p.Acquire(PickRequest{Model: "m"}); err == nil {
		t.Fatal("空池应返回错误")
	}
}

func TestStickyGC(t *testing.T) {
	cfg := testCfg()
	cfg.StickyTTL = config.Duration(10 * time.Millisecond)
	p := New(cfg)
	k := mustAdd(t, p, "a", "sk_tr_aaaa", 0)
	p.SetSticky("t1", k.ID)
	time.Sleep(15 * time.Millisecond)
	p.Tick()
	if got := p.StickyKey("t1"); got != "" {
		t.Fatalf("过期粘性应被清理，实际仍指向 %s", got)
	}
}

func TestSnapshotRestore(t *testing.T) {
	p := New(testCfg())
	k := mustAdd(t, p, "a", "sk_tr_aaaa", 10)
	p.Record(k.ID, 100, 50, 1.25)

	snap := p.Snapshot()
	p2 := New(testCfg())
	p2.Restore(snap)

	views := p2.Views()
	if len(views) != 1 {
		t.Fatalf("恢复后应只有 1 个号，实际 %d", len(views))
	}
	v := views[0]
	if v.Spent != 1.25 || v.PromptTokens != 100 || v.CompletionTokens != 50 {
		t.Fatalf("记账未正确恢复: %+v", v)
	}
	if v.Balance != 8.75 {
		t.Fatalf("余额 = %v，期望 8.75", v.Balance)
	}
}

// ---------- 会话粘性 ----------

func stickyCfg() config.PoolConfig {
	c := testCfg()
	c.SessionSticky = config.SessionSticky{
		Enabled:        true,
		TTL:            config.Duration(time.Minute),
		MaxEntries:     100,
		PrefixAffinity: true,
	}
	return c
}

// 同一会话必须一直落在同一个号上——这是命中上游 prompt cache 的前提。
func TestSessionStickyHitsSameKey(t *testing.T) {
	p := New(stickyCfg())
	mustAdd(t, p, "a", "sk_tr_aaaa", 0)
	mustAdd(t, p, "b", "sk_tr_bbbb", 0)
	mustAdd(t, p, "c", "sk_tr_cccc", 0)

	const sess = uint64(0xdeadbeef)
	first, err := p.Acquire(PickRequest{Model: "m", SessionKey: sess})
	if err != nil {
		t.Fatal(err)
	}
	p.Release(first, nil, "")

	for i := 0; i < 12; i++ {
		l, err := p.Acquire(PickRequest{Model: "m", SessionKey: sess})
		if err != nil {
			t.Fatalf("第 %d 次: %v", i, err)
		}
		if l.KeyID != first.KeyID {
			t.Fatalf("会话被拆散了：首次 %s，第 %d 次 %s", first.KeyID, i, l.KeyID)
		}
		p.Release(l, nil, "")
	}
}

// 不同会话应该能落在不同号上，否则"粘性"就成了"只用第一个号"。
func TestSessionStickySeparatesConversations(t *testing.T) {
	p := New(stickyCfg())
	for i := 0; i < 4; i++ {
		mustAdd(t, p, fmt.Sprintf("k%d", i), fmt.Sprintf("sk_tr_%04d", i), 0)
	}
	seen := map[string]bool{}
	for s := uint64(1); s <= 24; s++ {
		l, err := p.Acquire(PickRequest{Model: "m", SessionKey: s * 7919})
		if err != nil {
			t.Fatal(err)
		}
		seen[l.KeyID] = true
		p.Release(l, nil, "")
	}
	if len(seen) < 2 {
		t.Fatalf("24 个不同会话只用了 %d 个号，粘性表把流量焊死在一个号上了", len(seen))
	}
}

// 粘住的号出事（冷却）时必须退让，不能让会话卡死。
func TestSessionStickyFallsBackWhenKeyCooling(t *testing.T) {
	p := New(stickyCfg())
	a := mustAdd(t, p, "a", "sk_tr_aaaa", 0)
	mustAdd(t, p, "b", "sk_tr_bbbb", 0)

	const sess = uint64(42)
	l1, _ := p.Acquire(PickRequest{Model: "m", SessionKey: sess})
	p.Release(l1, nil, "")

	// 把 a 打进冷却
	p.mu.Lock()
	p.keys[a.ID].State = StateCooling
	p.keys[a.ID].CooldownUntil = time.Now().Add(time.Minute)
	p.mu.Unlock()

	l2, err := p.Acquire(PickRequest{Model: "m", SessionKey: sess})
	if err != nil {
		t.Fatalf("粘性号冷却后应能换号: %v", err)
	}
	if l2.KeyID == a.ID {
		t.Fatal("冷却中的号不该被粘性选中")
	}
	p.Release(l2, nil, "")
}

// 关掉粘性后，同一会话应该会被摊开（证明开关真的起作用）。
func TestSessionStickyDisabledSpreads(t *testing.T) {
	cfg := stickyCfg()
	cfg.SessionSticky.Enabled = false
	p := New(cfg)
	for i := 0; i < 3; i++ {
		mustAdd(t, p, fmt.Sprintf("k%d", i), fmt.Sprintf("sk_tr_%04d", i), 0)
	}
	seen := map[string]bool{}
	const sess = uint64(99)
	for i := 0; i < 60; i++ {
		l, err := p.Acquire(PickRequest{Model: "m", SessionKey: sess})
		if err != nil {
			t.Fatal(err)
		}
		seen[l.KeyID] = true
		p.Release(l, nil, "")
	}
	if len(seen) < 2 {
		t.Fatal("粘性关闭后仍在粘同一个号，开关没生效")
	}
}

// ---------- batch ----------

// fixed 模式下只允许动用指定数量的号。
func TestBatchFixedLimitsActiveKeys(t *testing.T) {
	cfg := testCfg()
	cfg.Batch = config.Batch{Mode: "fixed", Size: 1, Min: 1}
	p := New(cfg)
	for i := 0; i < 4; i++ {
		mustAdd(t, p, fmt.Sprintf("k%d", i), fmt.Sprintf("sk_tr_%04d", i), 0)
	}
	used := map[string]bool{}
	for i := 0; i < 30; i++ {
		l, err := p.Acquire(PickRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		used[l.KeyID] = true
		p.Release(l, nil, "")
	}
	if len(used) != 1 {
		t.Fatalf("batch=1 应用只动用 1 个号，实际用了 %d 个", len(used))
	}
}

// batch 成员失效后要自动补齐，否则可用号会越用越少。
func TestBatchRefillsAfterMemberCooling(t *testing.T) {
	cfg := testCfg()
	cfg.Batch = config.Batch{Mode: "fixed", Size: 2, Min: 1}
	p := New(cfg)
	var keys []*Key
	for i := 0; i < 4; i++ {
		keys = append(keys, mustAdd(t, p, fmt.Sprintf("k%d", i), fmt.Sprintf("sk_tr_%04d", i), 0))
	}
	// 先让 batch 成型
	for i := 0; i < 6; i++ {
		l, err := p.Acquire(PickRequest{Model: "m"})
		if err != nil {
			t.Fatal(err)
		}
		p.Release(l, nil, "")
	}
	if st := p.BatchState(); st.Size != 2 {
		t.Fatalf("batch 大小 = %d，期望 2", st.Size)
	}

	// 把当前 batch 里的第一个号打废
	p.mu.Lock()
	victim := p.batch[0]
	p.keys[victim].State = StateInvalid
	p.mu.Unlock()

	for i := 0; i < 6; i++ {
		l, err := p.Acquire(PickRequest{Model: "m"})
		if err != nil {
			t.Fatalf("补齐失败: %v", err)
		}
		if l.KeyID == victim {
			t.Fatal("失效号仍在被派单")
		}
		p.Release(l, nil, "")
	}
	st := p.BatchState()
	if st.Size != 2 {
		t.Fatalf("失效后 batch 未补齐，大小 = %d", st.Size)
	}
	for _, id := range st.Members {
		if id == victim {
			t.Fatal("失效号仍留在 batch 里")
		}
	}
	_ = keys
}

// auto 模式下，在途越高 batch 越大（单号跑满就扩）。
func TestBatchAutoExpandsUnderInflight(t *testing.T) {
	cfg := testCfg()
	cfg.MaxInFlight = 1 // 逼出"一个号只能扛一个请求"
	cfg.Batch = config.Batch{Mode: "auto", Min: 1, Max: 3, TargetInflightPerKey: 1}
	p := New(cfg)
	for i := 0; i < 3; i++ {
		mustAdd(t, p, fmt.Sprintf("k%d", i), fmt.Sprintf("sk_tr_%04d", i), 0)
	}

	var leases []*Lease
	for i := 0; i < 3; i++ {
		l, err := p.Acquire(PickRequest{Model: "m"})
		if err != nil {
			t.Fatalf("第 %d 个并发请求选号失败: %v", i+1, err)
		}
		leases = append(leases, l)
	}
	distinct := map[string]bool{}
	for _, l := range leases {
		distinct[l.KeyID] = true
	}
	if len(distinct) != 3 {
		t.Fatalf("3 个并发应摊到 3 个号，实际 %d 个", len(distinct))
	}
	for _, l := range leases {
		p.Release(l, nil, "")
	}
}

// ---------- 在途优先 ----------

func TestLeastInflightPrefersIdleKey(t *testing.T) {
	cfg := testCfg()
	cfg.Strategy = "sequential" // 走确定性的 leastInflight 分支
	cfg.LeastInflight = true
	cfg.MaxInFlight = 10
	p := New(cfg)
	mustAdd(t, p, "a", "sk_tr_aaaa", 0)
	mustAdd(t, p, "b", "sk_tr_bbbb", 0)

	l1, err := p.Acquire(PickRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	// l1 占住一个号后，下一个必须给空闲的那个
	l2, err := p.Acquire(PickRequest{Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if l2.KeyID == l1.KeyID {
		t.Fatal("并发下仍压同一个号，在途感知没生效")
	}
	p.Release(l1, nil, "")
	p.Release(l2, nil, "")
}

func TestBatchStateReportsInflight(t *testing.T) {
	cfg := testCfg()
	cfg.Batch = config.Batch{Mode: "fixed", Size: 2, Min: 1}
	p := New(cfg)
	mustAdd(t, p, "a", "sk_tr_aaaa", 0)
	l, _ := p.Acquire(PickRequest{Model: "m"})
	st := p.BatchState()
	if st.TotalInflight != 1 {
		t.Fatalf("在途 = %d，期望 1", st.TotalInflight)
	}
	p.Release(l, nil, "")
	if p.BatchState().TotalInflight != 0 {
		t.Fatal("释放后在途应归零")
	}
}
