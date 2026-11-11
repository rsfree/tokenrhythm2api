package pool

import (
	"testing"

	"trpool/internal/config"
	"trpool/internal/fault"
)

// 批量清理只能删"已经不再派单"的号：欠费、冻结、失效、人工停用。
// 还在干活的 active / cooling 一律不碰——那是把所有密钥一起丢掉的风险。
func TestPruneOnlyStoppedStates(t *testing.T) {
	p := testPool(t, nil)
	live := addKey(t, p, "live", 0)
	dead := addKey(t, p, "dead", 0)
	blocked := addKey(t, p, "blocked", 0)
	invalid := addKey(t, p, "invalid", 0)

	p.MarkFail(dead.ID, Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})
	p.MarkFail(blocked.ID, Outcome{Kind: fault.Banned, Message: "403 ACCOUNT_SUSPENDED"})
	p.MarkFail(invalid.ID, Outcome{Kind: fault.Auth, Message: "401 UNAUTHORIZED"})

	names, err := p.Prune([]State{StateExhausted, StateBlocked, StateDisabled}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 2 {
		t.Fatalf("应删掉欠费+冻结两个号，实际 %v", names)
	}
	if !p.Has(live.ID) {
		t.Fatal("可用号被误删了")
	}
	if !p.Has(invalid.ID) {
		t.Fatal("invalid 不在清理范围里，不该被删")
	}
	if p.Has(dead.ID) || p.Has(blocked.ID) {
		t.Fatal("停用号应已从池中移除")
	}
}

// active/cooling 出现在 states 里必须报错而不是静默跳过：
// 静默跳过会让人以为"清干净了"，实际一个都没动。
func TestPruneRejectsLiveStates(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "live", 0)
	if _, err := p.Prune([]State{StateActive}, false); err == nil {
		t.Fatal("拿 active 去批量删应该报错")
	}
	if !p.Has(k.ID) {
		t.Fatal("报错时不该动池子")
	}
}

// 人工停用的号不管底层是什么状态，面板上都算「人工停用」，清理时也是。
func TestPruneTreatsDisabledAsDisabled(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "off", 0)
	no := false
	if err := p.Update(k.ID, nil, nil, nil, nil, &no); err != nil {
		t.Fatal(err)
	}
	// 底层状态改成 exhausted 也不影响：人工摘掉的号照样按 disabled 归类。
	p.MarkFail(k.ID, Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})

	names, err := p.Prune([]State{StateDisabled}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "off" {
		t.Fatalf("应删掉人工停用的号，实际 %v", names)
	}
}

// dry_run 只报清单，不动池子——批量删除不可逆，得先让人看清要删哪些。
func TestPruneDryRunKeepsPool(t *testing.T) {
	p := testPool(t, nil)
	k := addKey(t, p, "dead", 0)
	p.MarkFail(k.ID, Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})

	names, err := p.Prune([]State{StateExhausted}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(names) != 1 || names[0] != "dead" {
		t.Fatalf("dry_run 应列出会被删的号，实际 %v", names)
	}
	if !p.Has(k.ID) {
		t.Fatal("dry_run 不该真删")
	}
}

// 删号要顺手清掉 batch 成员与粘性会话，否则会留下指向空号的脏数据。
func TestPruneCleansBatchAndSticky(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.Batch.Mode = "fixed"
		c.Batch.Size = 2
	})
	dead := addKey(t, p, "dead", 0)
	live := addKey(t, p, "live", 0)
	if got := len(p.BatchState().Members); got != 2 {
		t.Fatalf("前置条件：batch 应有 2 个成员，实际 %d", got)
	}
	p.SetSticky("task-1", dead.ID)

	if _, err := p.Prune([]State{StateExhausted, StateBlocked, StateDisabled}, false); err != nil {
		t.Fatal(err)
	}
	// 上面没把 dead 弄成停用，这里补一刀再删——确认真能删掉。
	p.MarkFail(dead.ID, Outcome{Kind: fault.Quota, Message: "402 INSUFFICIENT_BALANCE"})
	if _, err := p.Prune([]State{StateExhausted}, false); err != nil {
		t.Fatal(err)
	}
	if p.Has(dead.ID) {
		t.Fatal("停用号应已删除")
	}
	if p.StickyKey("task-1") != "" {
		t.Fatal("粘性会话应随号一起清掉")
	}
	for _, id := range p.BatchState().Members {
		if id == dead.ID {
			t.Fatal("batch 成员里不该留着已删除的号")
		}
	}
	if !p.Has(live.ID) {
		t.Fatal("可用号被误删了")
	}
}
