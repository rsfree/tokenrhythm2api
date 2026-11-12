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

// inboundOnlyHeaders 是"只属于客户端到网关这一段"的头，一律不往上游送。
//
// 两类东西：
//   - 网络身份：X-Forwarded-For / X-Real-IP / Forwarded / CF-Connecting-IP… 记的是
//     终端用户（或我们前面那层反代）的 IP。上游本来不该知道"这个 sk_tr_ 账号的
//     请求来自哪些 IP"，而一堆号轮流携带同一批客户端 IP，恰好是"这些号被同一个人
//     攥在手里"的最直白证据。
//   - 入口身份：Origin / Referer / Cookie 里写的是**网关自己**的域名与面板会话。
//     原样转上去等于把中转站的地址主动报给上游。
var inboundOnlyHeaders = map[string]bool{
	"x-forwarded-for": true, "x-forwarded-host": true, "x-forwarded-proto": true,
	"x-forwarded-port": true, "x-forwarded-server": true, "forwarded": true,
	"x-real-ip": true, "client-ip": true, "x-client-ip": true,
	"x-originating-ip": true, "x-remote-ip": true, "x-remote-addr": true,
	"true-client-ip": true, "cf-connecting-ip": true, "cf-ipcountry": true,
	"fastly-client-ip": true, "x-cluster-client-ip": true, "x-proxyuser-ip": true,

	"via": true, "x-via": true, "x-proxy-id": true, "proxy-connection": true,

	"origin": true, "referer": true, "cookie": true,
}

const (
	// defaultUA 是"客户端没带 UA"时的兜底，例如面板探活、启动预热、价目表刷新。
	//
	// 老实现把自己的身份写死在 UA 里（trpool/2 + GitHub 仓库地址）：同一出口 IP 上
	// 20 个号全用这一个字符串，等于举着"我是号池"的牌子。用官方 SDK 的常见形态
	// 兜底，没有额外信息量——上游看到的就是一次普通的 SDK 调用。
	defaultUA = "OpenAI/Python 1.55.3"

	// uaPassthrough 是 c.ua 的空值语义：透传客户端 UA。
	uaPassthrough = ""
)

type Client struct {
	base string
	hc   *http.Client
	// ua 为空 = 透传客户端 UA（客户端也没带才用 defaultUA）；
	// 非空 = 所有出站请求强制用这个值（上游挑 UA 时才需要）。
	ua string
}

// Option 是 New 的可选项。
type Option func(*Client)

// WithUserAgent 覆盖出站 User-Agent。传空字符串 = 透传客户端 UA。
func WithUserAgent(ua string) Option {
	return func(c *Client) { c.ua = strings.TrimSpace(ua) }
}

// New 构建传输层。超时由调用方用 context 控制（对话/视频预算不同），
// 所以 http.Client 自身不设 Timeout——否则视频任务会被对话超时误杀。
func New(base string, opts ...Option) *Client {
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
	c := &Client{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Transport: tr},
		ua:   uaPassthrough,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *Client) Base() string { return c.base }

// UA 返回当前的 UA 策略（空字符串表示透传客户端 UA），供面板展示。
func (c *Client) UA() string { return c.ua }

// pickUA 决定这次出站用哪个 User-Agent（见 Client.ua 的注释）。
func (c *Client) pickUA(in http.Header) string {
	if c.ua != "" {
		return c.ua
	}
	if ua := strings.TrimSpace(in.Get("User-Agent")); ua != "" {
		return ua
	}
	return defaultUA
}

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
	// 复制客户端头，剔除逐跳头、入口专有头与鉴权头（鉴权由池子接管）。
	for k, vs := range hdr {
		lk := strings.ToLower(k)
		if hopHeaders[lk] || inboundOnlyHeaders[lk] || lk == "authorization" ||
			lk == "host" || lk == "content-length" || lk == "accept-encoding" ||
			lk == "user-agent" {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("User-Agent", c.pickUA(hdr))
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
