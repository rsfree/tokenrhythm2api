// Package upstream 是到 tokenrhythm 的传输层：只负责"把请求原样送过去、把响应原样拿回来"，
// 不含任何选号/重试逻辑（那是 proxy 的职责）。
package upstream

import (
	"bytes"
	"context"
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
	base      string
	chat      *http.Client
	video     *http.Client
	userAgent string
}

func New(base string, chatTimeout, videoTimeout time.Duration) *Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          200,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		// 关掉自动压缩：我们自己管，避免透传 gzip 后记账读不到明文。
		DisableCompression: true,
	}
	return &Client{
		base:      strings.TrimRight(base, "/"),
		chat:      &http.Client{Transport: tr, Timeout: chatTimeout},
		video:     &http.Client{Transport: tr, Timeout: videoTimeout},
		userAgent: "trpool/1.0",
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
func (c *Client) Do(ctx context.Context, method, path string, body []byte, secret string, hdr http.Header, stream, isVideo bool) (*Result, error) {
	url := c.base + path
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, err
	}
	// 复制客户端头，剔除逐跳头与鉴权头（鉴权由池子接管）。
	for k, vs := range hdr {
		lk := strings.ToLower(k)
		if hopHeaders[lk] || lk == "authorization" || lk == "host" || lk == "content-length" {
			continue
		}
		// 交给 Go 的 Transport 自己协商压缩，否则记账要解 gzip。
		if lk == "accept-encoding" {
			continue
		}
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("User-Agent", c.userAgent)
	if body != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if stream {
		req.Header.Set("Accept", "text/event-stream")
	}

	hc := c.chat
	if isVideo {
		hc = c.video
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, classifyNetErr(err)
	}

	res := &Result{
		Status:  resp.StatusCode,
		Header:  resp.Header.Clone(),
		TraceID: resp.Header.Get("x-trace-id"),
	}
	if res.TraceID == "" {
		// 有些错误把 traceId 放在 body 里，读的时候再补。
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

// StatusError 是上游返回非 2xx 时的结构化错误。
// 调用方（预热、探活）需要按状态码决定怎么处理这个号，不能只看一句字符串。
type StatusError struct {
	Status int
	Body   []byte
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("上游返回 %d: %s", e.Status, snippet(e.Body))
}

// FetchModels 用某个号拉一次价目表。
func (c *Client) FetchModels(ctx context.Context, secret string) ([]byte, error) {
	res, err := c.Do(ctx, http.MethodGet, "/v1/models", nil, secret, http.Header{}, false, false)
	if err != nil {
		return nil, err
	}
	if res.Status != http.StatusOK {
		return nil, &StatusError{Status: res.Status, Body: res.Body}
	}
	return res.Body, nil
}

func classifyNetErr(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("上游超时: %w", err)
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return fmt.Errorf("上游超时: %w", err)
	}
	return fmt.Errorf("连接上游失败: %w", err)
}

func snippet(b []byte) string {
	if len(b) > 300 {
		return string(b[:300]) + "…"
	}
	return string(b)
}

func traceFromBody(b []byte) string {
	const k = `"traceId"`
	i := bytes.Index(b, []byte(k))
	if i < 0 {
		return ""
	}
	rest := b[i+len(k):]
	// 跳过 : " 与空白
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
