package pool

import (
	"strings"
	"testing"

	"trpool/internal/config"
)

func TestAddManyPartialSuccess(t *testing.T) {
	p := testPool(t, nil)
	addKey(t, p, "old", 0)

	notify := 0
	p.SetOnChange(func() { notify++ })

	res := p.AddMany([]KeyDraft{
		{Name: "a", Secret: "sk_tr_a_1"},
		{Name: "dup-in-pool", Secret: "sk_tr_old_0123456789"}, // 与池中重复
		{Name: "b", Secret: "sk_tr_b_1"},
		{Name: "dup-in-batch", Secret: "sk_tr_a_1"}, // 与本批第 1 行重复
		{Name: "bad", Secret: "not-a-key"},
		{Name: "", Secret: "sk_tr_c_1"}, // 名称缺省 → key-<n>
	})
	if len(res) != 6 {
		t.Fatalf("结果条数 = %d, 想要 6", len(res))
	}

	// 缺省名按池内现有条数续号：old/a/b 已在池里，所以这条是 key-4
	wantOK := map[int]string{0: "a", 2: "b", 5: "key-4"}
	for i, name := range wantOK {
		if res[i].Error != nil {
			t.Errorf("第 %d 行应该成功，却报错: %v", i+1, res[i].Error)
		}
		if res[i].Name != name {
			t.Errorf("第 %d 行名称 = %q, 想要 %q", i+1, res[i].Name, name)
		}
		if res[i].Key == nil || !p.Has(res[i].Key.ID) {
			t.Errorf("第 %d 行说成功了，池里却没有", i+1)
		}
	}
	for _, i := range []int{1, 3, 4} {
		if res[i].Error == nil {
			t.Errorf("第 %d 行应该失败，却成功了", i+1)
		}
	}
	if got, want := res[3].Error.Error(), "与第 1 行"; !strings.Contains(got, want) {
		t.Errorf("批内重复的提示应指向首行，得到 %q", got)
	}
	// 失败的行不能把别人的槽位顶掉
	if total, _, _, _, _, _ := p.Counts(); total != 4 {
		t.Errorf("池内共 %d 个号，想要 4 个", total)
	}
	// 6 条只通知一次，否则 100 个号要触发 100 次落盘
	if notify != 1 {
		t.Errorf("onChange 触发 %d 次，想要 1 次", notify)
	}
}

func TestAddManyAllBadNoNotify(t *testing.T) {
	p := testPool(t, nil)
	notify := 0
	p.SetOnChange(func() { notify++ })

	res := p.AddMany([]KeyDraft{
		{Secret: ""},
		{Secret: "sk"},
	})
	for i, r := range res {
		if r.Error == nil {
			t.Errorf("第 %d 行应报错", i+1)
		}
	}
	if notify != 0 {
		t.Errorf("一个号都没加进来，不该通知落盘（触发了 %d 次）", notify)
	}
	if total, _, _, _, _, _ := p.Counts(); total != 0 {
		t.Errorf("池子应为空，实有 %d 个", total)
	}
}

// 批量入池要立刻参与活跃窗口：否则导 100 个号后还得等下一笔请求才补齐。
func TestAddManyFillsBatchWindow(t *testing.T) {
	p := testPool(t, func(c *config.PoolConfig) {
		c.Batch.Mode = "fixed"
		c.Batch.Size = 3
	})
	p.AddMany([]KeyDraft{
		{Name: "a", Secret: "sk_tr_a_1"},
		{Name: "b", Secret: "sk_tr_b_1"},
		{Name: "c", Secret: "sk_tr_c_1"},
		{Name: "d", Secret: "sk_tr_d_1"},
	})
	bs := p.BatchState()
	if bs.Size != 3 {
		t.Fatalf("窗口大小 = %d, 想要 3", bs.Size)
	}
	if n := len(bs.Members); n != 3 {
		t.Fatalf("窗口成员 = %d, 想要 3", n)
	}
}
