package panel

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestPanelDomIDsExist 静态对齐「JS 用到的 id」与「HTML 里定义的 id」。
//
// 前端是 go:embed 的，Go 编译看不见它：脚本顶层写一句
// $('btnX').addEventListener(...)，只要 index.html 少了 id="btnX"，
// 初始化就会抛 TypeError，整个面板白屏——而所有 Go 测试照样全绿。
// node --check 也救不了（语法没错，只是取到了 null）。
func TestPanelDomIDsExist(t *testing.T) {
	htmlRaw, err := os.ReadFile("web/index.html")
	if err != nil {
		t.Fatal(err)
	}
	jsRaw, err := os.ReadFile("web/app.js")
	if err != nil {
		t.Fatal(err)
	}
	html, js := string(htmlRaw), string(jsRaw)

	re := regexp.MustCompile(`\$\('([A-Za-z0-9_-]+)'\)`)
	seen := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(js, -1) {
		id := m[1]
		if seen[id] {
			continue
		}
		seen[id] = true
		if !strings.Contains(html, `id="`+id+`"`) && !strings.Contains(js, `id="`+id+`"`) {
			t.Errorf("app.js 引用了 #%s，但 index.html 与 app.js 里都没有这个 id（面板会在初始化时白屏）", id)
		}
	}
	if len(seen) == 0 {
		t.Fatal("没解析到任何 $('id') 引用，检查正则是否失效")
	}
}
