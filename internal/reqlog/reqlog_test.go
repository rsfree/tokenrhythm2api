package reqlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRingOrderAndRestore(t *testing.T) {
	r := New(Config{RingSize: 3})
	base := time.Now()
	for i := 0; i < 5; i++ {
		r.Record(Entry{Time: base.Add(time.Duration(i) * time.Second), Status: 200, Model: "m"})
	}
	got := r.Recent(10)
	if len(got) != 3 {
		t.Fatalf("环形缓冲应只留 3 条，实际 %d", len(got))
	}
	if !got[0].Time.After(got[1].Time) {
		t.Fatal("Recent 应新的在前")
	}

	r2 := New(Config{RingSize: 10})
	r2.Restore(got)
	back := r2.Recent(3)
	for i := range got {
		if !back[i].Time.Equal(got[i].Time) {
			t.Fatalf("Restore 后顺序变了: %v vs %v", back[i].Time, got[i].Time)
		}
	}
}

func TestArchiveQueryAndFilters(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{RingSize: 10, Archive: true, ArchiveDir: dir, RetainDays: 7, MaxMB: 10})
	now := time.Now()
	r.Record(Entry{Time: now.Add(-2 * time.Hour), KeyID: "k1", KeyName: "a", Model: "glm", Status: 200, Cost: 0.1})
	r.Record(Entry{Time: now.Add(-time.Hour), KeyID: "k2", KeyName: "b", Model: "kimi", Status: 429, Error: "rate limited"})
	r.Record(Entry{Time: now, KeyID: "k1", KeyName: "a", Model: "kimi", Status: 200})
	r.Close() // flush 归档

	all, err := r.Query(Filter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("应有 3 条，实际 %d", len(all))
	}
	if all[0].Status != 200 || all[1].Status != 429 {
		t.Fatalf("应按时间倒序: %+v", all)
	}

	bad, err := r.Query(Filter{Status: "bad"})
	if err != nil {
		t.Fatal(err)
	}
	if len(bad) != 1 || bad[0].Status != 429 {
		t.Fatalf("bad 过滤错: %+v", bad)
	}

	byKey, _ := r.Query(Filter{KeyID: "k1"})
	if len(byKey) != 2 {
		t.Fatalf("按号过滤错: %d", len(byKey))
	}

	text, _ := r.Query(Filter{Text: "rate"})
	if len(text) != 1 {
		t.Fatalf("文本过滤错: %+v", text)
	}

	since, _ := r.Query(Filter{Since: now.Add(-90 * time.Minute)})
	if len(since) != 2 {
		t.Fatalf("时间过滤错: %d", len(since))
	}
}

func TestArchiveVisibleWithoutClose(t *testing.T) {
	dir := t.TempDir()
	r := New(Config{RingSize: 10, Archive: true, ArchiveDir: dir, RetainDays: 7, MaxMB: 10})
	defer r.Close()
	// 只写 1 条：写盘是 32 条批量 flush 的，Query 必须先 flush 才看得见。
	r.Record(Entry{Model: "glm", Status: 200})
	got, err := r.Query(Filter{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Model != "glm" {
		t.Fatalf("归档里应能立刻查到刚写的记录: %+v", got)
	}
}

func TestArchivePruneRetention(t *testing.T) {
	dir := t.TempDir()
	old := filepath.Join(dir, "requests-2020-01-01.jsonl")
	if err := os.WriteFile(old, []byte(`{"status":200}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	r := New(Config{RingSize: 10, Archive: true, ArchiveDir: dir, RetainDays: 3, MaxMB: 10})
	r.Prune()
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("过期归档应被删除")
	}
	if meta := r.ArchiveMeta(); meta["enabled"] != true {
		t.Fatalf("meta 不对: %+v", meta)
	}
}

func TestArchiveDisabled(t *testing.T) {
	r := New(Config{RingSize: 5})
	if _, err := r.Query(Filter{}); err == nil {
		t.Fatal("未启用归档应报错")
	}
}

// Note 是"成功了但有话要说"：客户端 Ctrl-C、usage 估算。它不能被算成失败，
// 否则面板的"仅失败/最近异常"会被用户主动取消刷满。
func TestNoteIsNotFailure(t *testing.T) {
	r := New(Config{RingSize: 5, Archive: true, ArchiveDir: t.TempDir(), RetainDays: 7, MaxMB: 10})
	defer r.Close()
	r.Record(Entry{Status: 200, Model: "m", Note: "客户端断开: context canceled"})

	ok, err := r.Query(Filter{Status: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if len(ok) != 1 || ok[0].Note == "" {
		t.Fatalf("带 Note 的成功记录应落在 ok 组: %+v", ok)
	}
	bad, _ := r.Query(Filter{Status: "bad"})
	if len(bad) != 0 {
		t.Fatalf("Note 不该被算成失败: %+v", bad)
	}
	if hit, _ := r.Query(Filter{Text: "context"}); len(hit) != 1 {
		t.Fatal("文本搜索应覆盖 Note")
	}
}

func TestRequestID(t *testing.T) {
	a, b := NewRequestID(), NewRequestID()
	if len(a) != 32 || a == b {
		t.Fatalf("请求 id 应是 32 位 hex 且唯一: %q %q", a, b)
	}
}
