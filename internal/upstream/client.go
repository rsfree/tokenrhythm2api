// Package upstream 是到 tokenrhythm 的传输层：只负责"把请求原样送过去、把响应原样
// 拿回来"，不含任何选号/重试/记账逻辑（那是 server 与 pool 的职责）。
//
// tokenrhythm 本身就是 OpenAI/Anthropic 兼容的纯 API，没有鉴权握手、没有设备码、
// 没有客户端指纹，所以这一层比 workbuddy2api 薄得多：一个 Transport 加一次 Do。
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"
)

// hop-by-hop 头不能跨代理转发，否则会破坏连接语义。
var hopHeaders = map[string]bool{
	"connection":          true,
	"keep-alive":          true,
	"proxy-authenticate":  true,
	"proxy-authorization": true,
	"te":                  true,
	"trailer":             true,
	"transfer-encoding":   true,
	"upgrade":             true,
}

type Client struct {
	base string
	hc   *http.Client
	ua   string
}

// New 构建传输层。超时由调用方用 context 控制（对话/视频预算不同），
// 所以 http.Client 自身不设 Timeout——否则视频任务会被对话超时误杀。
func New(base string) *Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          256,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// 关掉自动压缩：我们自己管，避免透传 gzip 后记账读不到明文。
		DisableCompression: true,
	}
	return &Client{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Transport: tr},
		ua:   "trpool/2 (+https://github.com/rsfree/tokenrhythm2api)",
	}
}

func (c *Client) Base() string { return c.base }

// Result 统一封装上游响应。Body 与 Stream 二选一：
// 流式且 2xx 时给 Stream（调用方负责 Close），其余情况 Body 已读完。
type Result struct {
	Status  int
	Header  http.Header
	Body    []byte
	Stream  io.ReadCloser
	TraceID string
}

func (r *Result) Close() {
	if r.Stream != nil {
		_ = r.Stream.Close()
	}
}

// Do 发出一次上游请求。stream=true 且上游返回 2xx 时不读 body，直接交回连接。
func (c *Client) Do(ctx context.Context, method, path string, body []byte, secret string, hdr http.Header, stream bool) (*Result, error) {
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return nil, err
	}
	// 复制客户端头，剔除逐跳头与鉴权头（鉴权由池子接管）。
	for k, vs := range hdr {
		lk := strings.ToLower(k)
		if hopHeaders[lk] || lk == "authorization" || lk == "host" ||
			lk == "content-length" || lk == "accept-encoding" {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("User-Agent", c.ua)
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, classifyNetErr(err)
	}

	res := &Result{
		Status:  resp.StatusCode,
		Header:  resp.Header.Clone(),
		TraceID: firstHeader(resp.Header, "x-trace-id", "x-request-id", "request-id"),
	}

	if stream && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		res.Stream = resp.Body
		return res, nil
	}
	// 限 32MB：正常响应远小于此，超了说明不是我们预期的形状。
	b, err := io.ReadAll(io.LimitReader(resp.Body, 32<<20))
	_ = resp.Body.Close()
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("读上游响应失败: %w", err)
	}
	res.Body = b
	if res.TraceID == "" {
		res.TraceID = traceFromBody(b)
	}
	return res, nil
}

// FetchModels 用某个号拉一次 /v1/models（价目表 + 探活共用）。
// 只读，不消耗生成额度。
func (c *Client) FetchModels(ctx context.Context, secret string) ([]byte, error) {
	res, err := c.Do(ctx, http.MethodGet, "/v1/models", nil, secret, http.Header{}, false)
	if err != nil {
		return nil, err
	}
	if res.Status != http.StatusOK {
		return nil, &StatusError{Status: res.Status, Body: res.Body}
	}
	return res.Body, nil
}

// ProbeChat 用最小代价跑一次真实生成：max_tokens 默认 1 的 chat 请求。
//
// 为什么不只打 /v1/models：那是免费端点，**欠费号拉模型列表也是 200**，
// 拿它当探活会把刚因 402 停用的号又放回"可用"。只有真跑一次生成，
// 才会把 402 欠费 / 403 冻结这些"账号级不可用"暴露出来。
func (c *Client) ProbeChat(ctx context.Context, secret, model string, maxTokens int) (*Result, error) {
	if maxTokens <= 0 {
		maxTokens = 1
	}
	payload, err := json.Marshal(map[string]any{
		"model":      model,
		"messages":   []map[string]string{{"role": "user", "content": "ping"}},
		"max_tokens": maxTokens,
		"stream":     false,
	})
	if err != nil {
		return nil, err
	}
	res, err := c.Do(ctx, http.MethodPost, "/v1/chat/completions", payload, secret, http.Header{}, false)
	if err != nil {
		return nil, err
	}
	if res.Status < 200 || res.Status >= 300 {
		return res, &StatusError{Status: res.Status, Body: res.Body}
	}
	return res, nil
}

// StatusError 是上游返回非 2xx 时的结构化错误。
// 调用方（预热、探活）需要按状态码决定怎么处理这个号，不能只看一句字符串。
type StatusError struct {
	Status int
	Body   []byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("上游返回 %d: %s", e.Status, snippet(e.Body, 240))
}

func firstHeader(h http.Header, names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(h.Get(n)); v != "" {
			return v
		}
	}
	return ""
}

func classifyNetErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("上游超时或被取消: %w", err)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("上游超时: %w", err)
	}
	return fmt.Errorf("连接上游失败: %w", err)
}

func snippet(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

func traceFromBody(b []byte) string {
	const k = `"traceId"`
	i := bytes.Index(b, []byte(k))
	if i < 0 {
		return ""
	}
	rest := b[i+len(k):]
	j := 0
	for j < len(rest) && (rest[j] == ':' || rest[j] == ' ' || rest[j] == '"') {
		j++
	}
	end := j
	for end < len(rest) && rest[end] != '"' && rest[end] != ',' && rest[end] != '}' {
		end++
	}
	return string(rest[j:end])
}
