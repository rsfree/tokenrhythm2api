// Package redact 抹掉上游响应体里"属于渠道内部"的东西。
//
// 响应头已经收成白名单了（见 server.passthroughHeaders），但 body 还是上游原样透传：
// 上游在自己 JSON 里塞了 trace_id、cost_cny（真实结算金额）、billing_pending，
// /v1/models 里还带着整套进货价目表和 owned_by=tokenrhythm；
// 402 的 message 更直接——"充值地址：https://tokenrhythm.studio/account/recharge"。
//
// 调用方拿到这些等于同时拿到了**上游是谁**和**你的进价**，加价空间一目了然。
// 这里做两件事：
//  1. 按键名删除内部字段（递归，不限层级）；
//  2. 把文本里出现的上游域名替换成 [hidden]（错误 message 里的充值/文档链接）。
//
// 只处理 JSON；非 JSON 的响应体只做域名替换，不碰结构（可能是二进制/文件流）。
package redact

import (
	"bytes"
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
)

// 这些键一旦出现在响应里，就等于告诉调用方"你背后是谁、你的成本是多少"。
var dropKeys = map[string]bool{
	// 上游链路 ID：能直接拿去上游工单系统对账，也能反查"这单从哪个渠道出的"。
	"trace_id": true, "traceId": true, "traceID": true, "trace_id_hex": true,

	// 上游结算字段：cost_cny 是这笔请求在上游的真实扣费，即进货价。
	"cost_cny": true, "billing_pending": true, "billing": true,

	// 推理引擎的权重版本（metadata.weight_version(s)）：客户端一辈子用不到，
	// 却能看出上游在跑什么引擎、同时挂了几个权重版本。
	"weight_version": true, "weight_versions": true,

	// /v1/models 里的完整价目表（上游扩展字段，OpenAI 标准里没有）。
	// 留着等于把进货价印在商品标签上：客户一眼就能算出你加了多少。
	"currency":                               true,
	"has_discount":                           true,
	"input_price_per_million":                true,
	"output_price_per_million":               true,
	"cache_read_price_per_million":           true,
	"discount_input_price_per_million":       true,
	"discount_output_price_per_million":      true,
	"discount_cache_read_price_per_million":  true,
	"effective_input_price_per_million":      true,
	"effective_output_price_per_million":     true,
	"effective_cache_read_price_per_million": true,
	"pricing":                                true,
	"effective_pricing":                      true,
	"discount_pricing":                       true,
}

// owned_by 不能删：openai SDK 的 Model 里它是必填字段，删了客户端解析
// /v1/models 会抛 ValidationError。改成中性值即可（官方也有 "system"）。
const (
	ownedByKey   = "owned_by"
	ownedByValue = "system"
)

// hidden 是域名被替换后的占位。故意留成肉眼可见的标记：
// 客户端看到"充值地址：[hidden]"就知道这里原本有个链接被拿掉了，
// 而不是以为上游漏写。
const hidden = "[hidden]"

// Scrubber 是线程安全的（内部只有只读正则），进程内建一个反复用即可。
type Scrubber struct {
	urlRe  *regexp.Regexp // 整条 URL，连路径一起吞掉
	hostRe *regexp.Regexp // 裸域名（含子域与端口）
	marks  [][]byte       // 预检用：命中才值得解析 JSON
}

// New 按上游地址构造。base 解析不出 host 时仍可删字段，只是不做域名替换。
func New(base string) *Scrubber {
	s := &Scrubber{}
	host := ""
	if u, err := url.Parse(strings.TrimSpace(base)); err == nil {
		host = u.Hostname()
	}
	if host != "" {
		q := regexp.QuoteMeta(strings.TrimPrefix(host, "www."))
		// 先整条 URL：https://tokenrhythm.studio/account/recharge → [hidden]
		s.urlRe = regexp.MustCompile(`(?i)https?://(?:[a-z0-9-]+\.)*` + q + `(?::\d+)?[^\s"'<>()（）\[\]，,。；;]*`)
		// 再裸域名：static.tokenrhythm.studio / tokenrhythm.studio:443
		s.hostRe = regexp.MustCompile(`(?i)(?:[a-z0-9-]+\.)*` + q + `(?::\d+)?`)
		s.marks = append(s.marks, []byte(host), []byte(strings.TrimPrefix(host, "www.")))
	}
	for k := range dropKeys {
		s.marks = append(s.marks, []byte(`"`+k+`"`))
	}
	s.marks = append(s.marks, []byte(`"`+ownedByKey+`"`))
	return s
}

// Body 处理一份完整的响应体。不是 JSON 就只做域名替换。
func (s *Scrubber) Body(body []byte) []byte {
	if len(body) == 0 || !s.dirty(body) {
		return body
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber() // 不用 float64：created / token 计数必须原样保留
	var v any
	if err := dec.Decode(&v); err != nil {
		return []byte(s.Text(string(body)))
	}
	out, changed := s.walk(v)
	if !changed {
		return body
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // 内容里的 < > & 不要被转成 \u003c，白增体积
	if err := enc.Encode(out); err != nil {
		return body
	}
	return bytes.TrimRight(buf.Bytes(), "\n")
}

// SSELine 处理一帧 SSE。只有 data: 帧会被解析，其余（事件名、空行、注释）原样回。
//
// 注意调用顺序：usage 提取必须在脱敏之前做——cost_cny 正好是记账要用的字段，
// 先脱敏再解析就只剩 0 了。
func (s *Scrubber) SSELine(line []byte) []byte {
	t := bytes.TrimSpace(line)
	if !bytes.HasPrefix(t, []byte("data:")) {
		return line
	}
	payload := bytes.TrimSpace(t[5:])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return line
	}
	nb := s.Body(payload)
	if bytes.Equal(nb, payload) {
		return line
	}
	// 保留原行尾：上游可能用 \r\n，改写成 \n 会让某些严格的客户端反复解析。
	eol := "\n"
	if bytes.HasSuffix(line, []byte("\r\n")) {
		eol = "\r\n"
	}
	return []byte("data: " + string(nb) + eol)
}

// Text 做纯文本级的域名替换（错误 message、非 JSON 的响应体）。
func (s *Scrubber) Text(v string) string {
	if s.hostRe == nil || v == "" {
		return v
	}
	out := v
	if s.urlRe != nil {
		out = s.urlRe.ReplaceAllString(out, hidden)
	}
	return s.hostRe.ReplaceAllString(out, hidden)
}

func (s *Scrubber) walk(v any) (any, bool) {
	switch t := v.(type) {
	case map[string]any:
		changed := false
		for k, val := range t {
			if dropKeys[k] {
				delete(t, k)
				changed = true
				continue
			}
			if k == ownedByKey {
				if str, ok := val.(string); ok && str != ownedByValue && str != "" {
					t[k] = ownedByValue
					changed = true
				}
				continue
			}
			if nv, c := s.walk(val); c {
				t[k] = nv
				changed = true
			}
		}
		return t, changed
	case []any:
		changed := false
		for i, val := range t {
			if nv, c := s.walk(val); c {
				t[i] = nv
				changed = true
			}
		}
		return t, changed
	case string:
		if nv := s.Text(t); nv != t {
			return nv, true
		}
		return t, false
	}
	return v, false
}

// dirty 是预检：绝大多数 SSE 帧只有内容增量，一次 bytes.Contains 就能跳过解析。
func (s *Scrubber) dirty(b []byte) bool {
	for _, m := range s.marks {
		if bytes.Contains(b, m) {
			return true
		}
	}
	return false
}
