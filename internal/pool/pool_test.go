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

	lease, err := p.Acquire("m", false, nil, "")
	if err != nil {
		t.Fatalf("首次选号失败: %v", err)
	}
	kind := FailRate
	p.Release(lease, &kind, "429")

	if st := p.keys[k.ID].State; st != StateCooling {
		t.Fatalf("429 后状态 = %s，期望 cooling", st)
	}
	if _, err := p.Acquire("m", false, nil, ""); err == nil {
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

	lease, _ := p.Acquire("m", false, nil, "")
	kind := FailAuth
	p.Release(lease, &kind, "401")

	if p.keys[k.ID].State != StateInvalid {
		t.Fatalf("状态 = %s，期望 invalid", p.keys[k.ID].State)
	}
	p.Tick()
	if p.keys[k.ID].State != StateInvalid {
		t.Fatal("invalid 不应被 Tick 自动恢复")
	}
	if _, err := p.Acquire("m", false, nil, ""); err == nil {
		t.Fatal("失效号不应被选中")
	}
}

// 4xx 是请求本身的问题，不能记账到号上——否则一个坏请求会锁死整个池子。
func TestClientErrorDoesNotCoolKey(t *testing.T) {
	p := New(testCfg())
	k := mustAdd(t, p, "a", "sk_tr_aaaa", 0)

	lease, _ := p.Acquire("m", false, nil, "")
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

	if _, err := p.Acquire("paid-model", true, nil, ""); err == nil {
		t.Fatal("余额触底的号不应接收费模型")
	}
	if _, err := p.Acquire("free-model", false, nil, ""); err != nil {
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
	lease, _ := p.Acquire("m", false, nil, a.ID)
	kind := FailRate
	p.Release(lease, &kind, "429")

	got, err := p.Acquire("m", false, nil, p.StickyKey("task-1"))
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

	l1, err := p.Acquire("m", false, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Acquire("m", false, nil, ""); err == nil {
		t.Fatal("在途已满时不应再派号")
	}
	p.Release(l1, nil, "")
	if _, err := p.Acquire("m", false, nil, ""); err != nil {
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
		l, err := p.Acquire("m", false, nil, "")
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
	if _, err := p.Acquire("m", false, nil, ""); err == nil {
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
