package session

import (
	"testing"
	"time"
)

func TestFingerprintExplicit(t *testing.T) {
	a := Fingerprint([]byte(`{"user":"u1","messages":[{"role":"user","content":"hi"}]}`), true)
	b := Fingerprint([]byte(`{"user":"u1","messages":[{"role":"user","content":"different"}]}`), true)
	if a == 0 || a != b {
		t.Fatalf("显式 user 应决定指纹: %d vs %d", a, b)
	}
}

func TestFingerprintPrefix(t *testing.T) {
	one := []byte(`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"A"}]}`)
	two := []byte(`{"messages":[{"role":"system","content":"S"},{"role":"user","content":"B"}]}`)
	if Fingerprint(one, true) == Fingerprint(two, true) {
		t.Fatal("首条 user 不同应得到不同指纹")
	}
	if Fingerprint(one, false) != 0 {
		t.Fatal("关闭前缀亲和且无显式标识时应为 0")
	}
	if Fingerprint(nil, true) != 0 {
		t.Fatal("空 body 应为 0")
	}
}

func TestTableTTLAndLRU(t *testing.T) {
	tbl := New(50*time.Millisecond, 2)
	tbl.Bind(1, "a")
	tbl.Bind(2, "b")
	if id, ok := tbl.Lookup(1); !ok || id != "a" {
		t.Fatalf("命中失败: %s %v", id, ok)
	}
	time.Sleep(60 * time.Millisecond)
	if _, ok := tbl.Lookup(1); ok {
		t.Fatal("过期后不该命中")
	}

	tbl2 := New(time.Hour, 2)
	tbl2.Bind(1, "a")
	time.Sleep(time.Millisecond)
	tbl2.Bind(2, "b")
	time.Sleep(time.Millisecond)
	tbl2.Bind(3, "c") // 超出上限，淘汰最久未用的 1
	if _, ok := tbl2.Lookup(1); ok {
		t.Fatal("最久未用的会话应被淘汰")
	}
	if _, ok := tbl2.Lookup(2); !ok {
		t.Fatal("较新的会话应保留")
	}
	if got := tbl2.ByKey()["c"]; got != 1 {
		t.Fatalf("ByKey 统计错: %v", tbl2.ByKey())
	}
}
