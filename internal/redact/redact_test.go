package redact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBodyDropsCostAndTrace(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	in := `{"id":"chatcmpl-1","model":"m","usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8},"cost_cny":"0.00007740","billing_pending":false,"trace_id":"trace_da97"}`
	out := string(s.Body([]byte(in)))

	for _, leak := range []string{"cost_cny", "billing_pending", "trace_id", "trace_da97"} {
		if strings.Contains(out, leak) {
			t.Errorf("应删掉 %s，实际 %s", leak, out)
		}
	}
	// 业务字段一个不能少，否则客户端解析不动。
	for _, keep := range []string{"chatcmpl-1", `"total_tokens":8`, `"prompt_tokens":5`, `"model":"m"`} {
		if !strings.Contains(out, keep) {
			t.Errorf("应保留 %s，实际 %s", keep, out)
		}
	}
}

func TestBodyDropsPriceList(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	in := `{"object":"list","data":[{"id":"glm-5.1","object":"model","created":0,"owned_by":"tokenrhythm",` +
		`"context_length":200000,"currency":"CNY","has_discount":false,` +
		`"input_price_per_million":"8.00000000","effective_output_price_per_million":"28.00000000",` +
		`"pricing":{"currency":"CNY","prompt":"8.00000000"},"supports_tools":true}]}`
	out := s.Body([]byte(in))

	var m struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(out, &m); err != nil {
		t.Fatalf("输出不是合法 JSON: %v → %s", err, out)
	}
	m0 := m.Data[0]
	// owned_by 不能删：openai SDK 的 Model 里它是必填字段，删了客户端解析会失败。
	if m0["owned_by"] != "system" {
		t.Errorf("owned_by 应改成 system，实际 %v", m0["owned_by"])
	}
	for _, k := range []string{"currency", "has_discount", "input_price_per_million",
		"effective_output_price_per_million", "pricing"} {
		if _, ok := m0[k]; ok {
			t.Errorf("应删掉价目字段 %s，实际 %s", k, out)
		}
	}
	// 能力元信息是客户选模型要用的，必须留着。
	if m0["context_length"] == nil || m0["supports_tools"] == nil || m0["id"] != "glm-5.1" {
		t.Errorf("能力字段不应被删，实际 %s", out)
	}
}

func TestBodyRewritesUpstreamDomainInMessage(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	in := `{"code":"INSUFFICIENT_BALANCE","message":"账户余额不足，请充值后重试。充值地址：https://tokenrhythm.studio/account/recharge","traceId":"trace_1"}`
	out := s.Body([]byte(in))

	if strings.Contains(string(out), "tokenrhythm") {
		t.Fatalf("上游域名不该出现在 body 里：%s", out)
	}
	if !strings.Contains(string(out), "[hidden]") {
		t.Fatalf("链接应被替换成 [hidden]：%s", out)
	}
	// 错误码要留着，客户端靠它分流。
	if !strings.Contains(string(out), "INSUFFICIENT_BALANCE") {
		t.Fatalf("错误码应保留：%s", out)
	}
}

func TestTextRewritesSubdomainAndPort(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	cases := []string{
		"详见 https://static.tokenrhythm.studio/a/b.png 文档",
		"裸域名 tokenrhythm.studio 指向上游",
		"带端口 tokenrhythm.studio:443 也是上游",
	}
	for _, in := range cases {
		out := s.Text(in)
		if strings.Contains(out, "tokenrhythm") {
			t.Errorf("应替换掉域名，输入 %q → %q", in, out)
		}
	}
	// 别的域名不能被误伤。
	if out := s.Text("见 https://example.com/x"); out != "见 https://example.com/x" {
		t.Errorf("不该改与上游无关的域名：%q", out)
	}
}

func TestNonJSONOnlyRewritesDomain(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	in := "<html><body>moved to https://tokenrhythm.studio/login</body></html>"
	out := string(s.Body([]byte(in)))
	if strings.Contains(out, "tokenrhythm.studio") {
		t.Fatalf("非 JSON 也要替换域名：%s", out)
	}
	if !strings.Contains(out, "<html>") {
		t.Fatalf("结构不能被破坏：%s", out)
	}
}

func TestSSELine(t *testing.T) {
	s := New("https://tokenrhythm.studio")

	// 带内部字段的尾帧
	in := []byte("data: {\"id\":\"1\",\"choices\":[],\"usage\":{\"total_tokens\":14},\"cost_cny\":\"0.00001320\",\"trace_id\":\"trace_54a5\"}\n")
	out := string(s.SSELine(in))
	if strings.Contains(out, "cost_cny") || strings.Contains(out, "trace_id") {
		t.Fatalf("SSE 帧里的内部字段应删掉：%s", out)
	}
	if !strings.HasPrefix(out, "data: ") || !strings.HasSuffix(out, "\n") {
		t.Fatalf("SSE 帧格式被破坏：%q", out)
	}
	if !strings.Contains(out, `"total_tokens":14`) {
		t.Fatalf("usage 必须保留（客户端要用它算上下文）：%s", out)
	}

	// 普通内容帧不该被改写（含中文与引号）
	plain := []byte("data: {\"choices\":[{\"delta\":{\"content\":\"他说：\\\"hi\\\"\"}}]}\n\n")
	if got := string(s.SSELine(plain)); got != string(plain) {
		t.Fatalf("普通帧不该被改写：%q → %q", plain, got)
	}

	// [DONE] 与注释/事件行原样
	for _, l := range []string{"data: [DONE]\n\n", ": keepalive\n\n", "event: ping\n"} {
		if got := string(s.SSELine([]byte(l))); got != l {
			t.Fatalf("%q 应原样返回，实际 %q", l, got)
		}
	}

	// \r\n 行尾要保留
	crlf := []byte("data: {\"cost_cny\":\"1\",\"ok\":true}\r\n")
	if got := string(s.SSELine(crlf)); !strings.HasSuffix(got, "\r\n") {
		t.Fatalf("\\r\\n 行尾应保留：%q", got)
	}
}

// json.Number 保证大整数与浮点不被 float64 改写（created 时间戳、超长 id 都靠它）。
func TestNumbersRoundTrip(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	in := `{"created":1794454472,"big":12345678901234567890,"trace_id":"x"}`
	out := string(s.Body([]byte(in)))
	if !strings.Contains(out, `"created":1794454472`) {
		t.Errorf("时间戳被改写：%s", out)
	}
	if !strings.Contains(out, `"big":12345678901234567890`) {
		t.Errorf("大整数精度丢失：%s", out)
	}
}

// 没有敏感字段时应当原样返回（连重新序列化都不做），避免无谓改动。
func TestCleanBodyUntouched(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	in := []byte(`{"id":"chatcmpl-1","choices":[{"message":{"content":"hi"}}]}`)
	out := s.Body(in)
	if string(out) != string(in) {
		t.Fatalf("干净的 body 不该被重写：%s → %s", in, out)
	}
}

// base 解析不出 host 时仍然要能删字段，只是不做域名替换。
func TestEmptyBaseStillDropsFields(t *testing.T) {
	s := New("")
	out := string(s.Body([]byte(`{"trace_id":"t","ok":1}`)))
	if strings.Contains(out, "trace_id") || !strings.Contains(out, `"ok":1`) {
		t.Fatalf("空 base 也应删字段：%s", out)
	}
}

// 推理引擎的权重版本是上游部署细节，客户端用不到。
func TestBodyDropsEngineMetadata(t *testing.T) {
	s := New("https://tokenrhythm.studio")
	in := `{"id":"chatcmpl-1","metadata":{"weight_version":"default","weight_versions":[{"start":0,"end":1,"version":"default"}]},"usage":{"total_tokens":14}}`
	out := string(s.Body([]byte(in)))
	for _, leak := range []string{"weight_version", "weight_versions", "default"} {
		if strings.Contains(out, leak) {
			t.Errorf("应删掉引擎元信息 %s：%s", leak, out)
		}
	}
	if !strings.Contains(out, `"total_tokens":14`) {
		t.Errorf("业务字段不能误删：%s", out)
	}
}
