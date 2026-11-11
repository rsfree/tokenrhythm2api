package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDurationJSON(t *testing.T) {
	var v struct {
		D Duration `json:"d"`
	}
	if err := json.Unmarshal([]byte(`{"d":"30s"}`), &v); err != nil {
		t.Fatal(err)
	}
	if v.D.Duration() != 30*time.Second {
		t.Fatalf("got %s", v.D.Duration())
	}
	b, err := json.Marshal(Duration(2 * time.Minute))
	if err != nil || string(b) != `"2m0s"` {
		t.Fatalf("marshal: %s %v", b, err)
	}
}

func TestDefaultsApplied(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	if err := os.WriteFile(path, []byte(`{"listen":":9999","pool":{"strategy":"nonsense","top_k":0}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	os.Setenv("TRPOOL_ADMIN_KEY", "adm")
	os.Setenv("TRPOOL_CLIENT_KEYS", "a, b ,c")
	defer os.Unsetenv("TRPOOL_ADMIN_KEY")
	defer os.Unsetenv("TRPOOL_CLIENT_KEYS")

	c, existed, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !existed {
		t.Fatal("文件存在")
	}
	if c.Listen != ":9999" {
		t.Fatalf("listen=%s", c.Listen)
	}
	if c.Pool.Strategy != "weighted_random" {
		t.Fatalf("非法策略应回落默认: %s", c.Pool.Strategy)
	}
	if c.Pool.TopK != 5 {
		t.Fatalf("top_k 应补默认: %d", c.Pool.TopK)
	}
	if len(c.ClientKeys) != 3 || c.ClientKeys[1] != "b" {
		t.Fatalf("环境变量 client keys 解析错: %v", c.ClientKeys)
	}
	if c.Pool.ModelBlockBase.Duration() != 6*time.Hour {
		t.Fatalf("模型封禁默认值错: %s", c.Pool.ModelBlockBase.Duration())
	}
}

func TestLoadMissingFile(t *testing.T) {
	dir := t.TempDir()
	os.Unsetenv("TRPOOL_ADMIN_KEY")
	os.Unsetenv("TRPOOL_CLIENT_KEYS")
	_, existed, err := Load(filepath.Join(dir, "nope.json"))
	if existed {
		t.Fatal("文件不存在")
	}
	if err == nil || !strings.Contains(err.Error(), "admin_key") {
		t.Fatalf("缺密钥应报错，实际 %v", err)
	}
}

func TestWritePoolPreservesUnknownKeysAndSecrets(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	raw := `{"listen":":8787","admin_key":"keep-me","custom":{"x":1},"pool":{"strategy":"round_robin"}}`
	if err := os.WriteFile(path, []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	pool := Default().Pool
	pool.Strategy = "sequential"
	pool.TopK = 9
	if err := WritePool(path, pool); err != nil {
		t.Fatal(err)
	}
	b, _ := os.ReadFile(path)
	out := string(b)
	if !strings.Contains(out, `"admin_key": "keep-me"`) {
		t.Fatalf("admin_key 被改写或丢失: %s", out)
	}
	if !strings.Contains(out, `"custom"`) {
		t.Fatalf("未知键被丢弃: %s", out)
	}
	if !strings.Contains(out, `"strategy": "sequential"`) || !strings.Contains(out, `"top_k": 9`) {
		t.Fatalf("pool 段没写回: %s", out)
	}
}

func TestValidate(t *testing.T) {
	c := Default()
	if err := c.Validate(); err == nil {
		t.Fatal("无 admin_key 应报错")
	}
	c.AdminKey = "a"
	if err := c.Validate(); err == nil {
		t.Fatal("无 client_keys 应报错")
	}
	c.AllowUpstreamKeyPassthrough = true
	if err := c.Validate(); err != nil {
		t.Fatalf("开启直连后应通过: %v", err)
	}
}

// TestExampleConfigMatchesStruct 守住文档与结构体的一致性：
// config.example.json 里出现结构体没有的字段（打错字、改名后忘同步）就直接失败，
// 否则 json 会静默忽略，文档就成了假的。
func TestExampleConfigMatchesStruct(t *testing.T) {
	raw, err := os.ReadFile("../../config.example.json")
	if err != nil {
		t.Fatalf("读示例配置: %v", err)
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var c Config
	if err := dec.Decode(&c); err != nil {
		t.Fatalf("config.example.json 与 Config 结构不一致: %v", err)
	}
	if c.Pool.ModelBlockBase.Duration() != 6*time.Hour || c.Logging.MaxMB != 100 {
		t.Fatalf("示例配置未被解析: %+v", c.Pool)
	}
}

func TestArchiveDefaultOn(t *testing.T) {
	c := Default()
	if !c.Logging.ArchiveOn() {
		t.Fatal("归档默认应开启")
	}
	off := false
	c.Logging.RequestArchive = &off
	if c.Logging.ArchiveOn() {
		t.Fatal("显式关闭应生效")
	}
}
