package panel

import (
	"encoding/json"
	"strings"
	"testing"
)

// 关键点：从表格/文档里整列复制出来的密钥，格式五花八门，
// 每一条都不能把密钥和名称搞反——反了就是拿名字当密钥去调上游。
func TestParseKeyLines(t *testing.T) {
	cases := []struct {
		name       string
		line       string
		wantName   string
		wantSecret string
	}{
		{"纯密钥", "sk_tr_abc", "", "sk_tr_abc"},
		{"等号", "主账号=sk_tr_abc", "主账号", "sk_tr_abc"},
		{"英文逗号", "账号B,sk_tr_abc", "账号B", "sk_tr_abc"},
		{"中文逗号", "账号C，sk_tr_abc", "账号C", "sk_tr_abc"},
		{"制表符", "账号D\tsk_tr_abc", "账号D", "sk_tr_abc"},
		{"空格", "账号E sk_tr_abc", "账号E", "sk_tr_abc"},
		{"顺序颠倒也对", "sk_tr_abc=账号F", "账号F", "sk_tr_abc"},
		{"列表符", "- sk_tr_abc", "", "sk_tr_abc"},
		{"成对引号", `"sk_tr_abc"`, "", "sk_tr_abc"},
		{"行尾逗号", "sk_tr_abc,", "", "sk_tr_abc"},
		{"前后空白", "   sk_tr_abc   ", "", "sk_tr_abc"},
		{"注释行", "# 下面开始是最新一批", "", ""},
		{"空行", "   ", "", ""},
		{"带空格的名字", "主 账 号=sk_tr_abc", "主 账 号", "sk_tr_abc"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			gotName, gotSecret := splitKeyLine(c.line)
			if gotName != c.wantName || gotSecret != c.wantSecret {
				t.Fatalf("splitKeyLine(%q) = (%q, %q), 想要 (%q, %q)",
					c.line, gotName, gotSecret, c.wantName, c.wantSecret)
			}
		})
	}
}

// 两个都不像密钥时整行当密钥，让后面的形态校验报错，别悄悄塞一个空密钥进池。
func TestParseKeyLinesKeepsGarbage(t *testing.T) {
	items := parseKeyLines("hello world\nabc=def", "key-", nil, 1, 0)
	if len(items) != 2 {
		t.Fatalf("解析出 %d 条, 想要 2", len(items))
	}
	if items[0].Draft.Secret != "hello world" {
		t.Errorf("无密钥特征的整行应原样保留，得到 %q", items[0].Draft.Secret)
	}
	if items[1].Draft.Secret != "def" {
		t.Errorf("abc=def 应把右侧当密钥，得到 %q", items[1].Draft.Secret)
	}
}

// 自动命名从 1 开始，并跳过池内已占用的名字：导 20 个号就该是 tr-1…tr-20。
func TestParseKeyLinesAutoName(t *testing.T) {
	text := "sk_tr_a\nsk_tr_b\n# 注释\n\n有名字=sk_tr_c\nsk_tr_d"
	items := parseKeyLines(text, "tr-", []string{"账号A", "tr-1", "tr-3"}, 1, 2.5)
	got := make([]string, len(items))
	for i, it := range items {
		got[i] = it.Draft.Name
	}
	want := []string{"tr-2", "tr-4", "有名字", "tr-5"}
	if strings.Join(got, "|") != strings.Join(want, "|") {
		t.Fatalf("名称 = %v, 想要 %v", got, want)
	}
	// 行号要反映原文行号（空行、注释行也占行），前端靠它指认哪一行有问题
	if items[0].Line != 1 || items[1].Line != 2 || items[2].Line != 5 || items[3].Line != 6 {
		t.Fatalf("行号 = %d/%d/%d/%d", items[0].Line, items[1].Line, items[2].Line, items[3].Line)
	}
	for _, it := range items {
		if it.Draft.Weight != 1 || it.Draft.Budget != 2.5 {
			t.Fatalf("统一权重/充值额没生效: %+v", it.Draft)
		}
	}
}

func TestPanelBatchAdd(t *testing.T) {
	h := newPanelHarness(t)
	body := `{"secrets":"sk_tr_a1\nsk_tr_a2\n账号C=sk_tr_a3\nsk_tr_a1\nabc=def\n","name_prefix":"t-","budget":5}`
	resp, out := h.call(t, "POST", "/panel/api/keys/batch", body, true)
	if resp.StatusCode != 200 {
		t.Fatalf("状态码 = %d, body=%s", resp.StatusCode, out)
	}
	var got struct {
		OK      bool `json:"ok"`
		Total   int  `json:"total"`
		Added   int  `json:"added"`
		Skipped int  `json:"skipped"`
		Results []struct {
			Line  int    `json:"line"`
			Name  string `json:"name"`
			Error string `json:"error"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(out), &got); err != nil {
		t.Fatalf("解析响应失败: %v\n%s", err, out)
	}
	// 第 4 行是第 1 行的重复密钥，第 5 行密钥形态不对
	if got.Total != 5 || got.Added != 3 || got.Skipped != 2 {
		t.Fatalf("total/added/skipped = %d/%d/%d, 想要 5/3/2（%s）", got.Total, got.Added, got.Skipped, out)
	}
	if got.Results[2].Name != "账号C" || got.Results[2].Error != "" {
		t.Errorf("「名称=密钥」应拆成 账号C / sk_tr_a3，得到 %+v", got.Results[2])
	}
	if got.Results[3].Error == "" || got.Results[4].Error == "" {
		t.Errorf("重复密钥与非法密钥都要给出原因: %+v", got.Results)
	}
	if total, _, _, _, _, _ := h.pool.Counts(); total != 3 {
		t.Errorf("池内 %d 个号，想要 3", total)
	}
	// 统一充值额落到了每个号上
	for _, v := range h.pool.Views() {
		if v.Budget != 5 {
			t.Errorf("%s 的充值额 = %v, 想要 5", v.Name, v.Budget)
		}
	}

	// 空文本要明确报错，不能返回成功 0 个
	resp, out = h.call(t, "POST", "/panel/api/keys/batch", `{"secrets":"  \n# 只有注释\n"}`, true)
	if resp.StatusCode != 400 {
		t.Fatalf("空批次应返回 400，得到 %d: %s", resp.StatusCode, out)
	}
}
