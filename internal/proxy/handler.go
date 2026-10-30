// Package proxy 是网关本体：鉴权 → 选号 → 透传 → 失败换号 → 记账。
//
// 重试的边界（务必守住）：只有当上游响应头已到、但**还没向客户端写出任何字节**时
// 才允许换号。流式响应一旦开始刷字节，就只能如实透传到底，中途换号会让客户端收到
// 两个不同上游的拼接内容。
package proxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"trpool/internal/config"
	"trpool/internal/ledger"
	"trpool/internal/pool"
	"trpool/internal/upstream"
)

type Handler struct {
	cfg    config.Config
	pool   *pool.Pool
	up     *upstream.Client
	prices *ledger.Prices
	log    *ledger.Log
	daily  *ledger.Daily

	started time.Time
	stats   Stats

	// PanelHandler 由 main 注入，避免 proxy 反向依赖 panel。
	PanelHandler http.Handler
}

type Stats struct {
	Total    atomic.Int64
	OK       atomic.Int64
	Failed   atomic.Int64
	Retries  atomic.Int64
	Switched atomic.Int64
}

func (s *Stats) Snapshot() map[string]int64 {
	return map[string]int64{
		"total":    s.Total.Load(),
		"ok":       s.OK.Load(),
		"failed":   s.Failed.Load(),
		"retries":  s.Retries.Load(),
		"switched": s.Switched.Load(),
	}
}

func New(cfg config.Config, p *pool.Pool, up *upstream.Client, prices *ledger.Prices, lg *ledger.Log, daily *ledger.Daily) *Handler {
	return &Handler{cfg: cfg, pool: p, up: up, prices: prices, log: lg, daily: daily, started: time.Now()}
}

func (h *Handler) Uptime() time.Duration { return time.Since(h.started) }
func (h *Handler) Stats() *Stats         { return &h.stats }

var videoPathRe = regexp.MustCompile(`^/v1/videos/([^/?]+)$`)

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/healthz":
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"service":"trpool"}`))
		return
	case strings.HasPrefix(r.URL.Path, "/panel"):
		if h.PanelHandler != nil {
			h.PanelHandler.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
		return
	case r.URL.Path == "/":
		http.Redirect(w, r, "/panel", http.StatusFound)
		return
	case strings.HasPrefix(r.URL.Path, "/v1/"):
		h.handleProxy(w, r)
		return
	}
	http.NotFound(w, r)
}

// meta 只需要三个字段，不必完整反序列化请求体。
type reqMeta struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

func (h *Handler) handleProxy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	h.stats.Total.Add(1)

	authKey := bearer(r)
	if !h.authorized(authKey) {
		h.stats.Failed.Add(1)
		writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "网关密钥无效；请使用面板下发的 client key")
		return
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 64<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "读取请求体失败: "+err.Error())
		return
	}
	_ = r.Body.Close()

	var meta reqMeta
	if len(body) > 0 {
		_ = json.Unmarshal(body, &meta)
	}

	// 直接透传模式：客户端自带 sk_tr_ 且未启用池化
	if h.cfg.AllowUpstreamKeyPassthrough && strings.HasPrefix(authKey, "sk_tr_") {
		h.serveDirect(w, r, body, start)
		return
	}

	path := r.URL.Path
	isVideoCreate := r.Method == http.MethodPost && path == "/v1/videos/generations"
	isVideoQuery := r.Method == http.MethodGet && videoPathRe.MatchString(path)

	// 视频查询必须回到创建任务的那个号，否则上游查不到。
	stickyKey := ""
	if isVideoQuery {
		if m := videoPathRe.FindStringSubmatch(path); m != nil {
			stickyKey = h.pool.StickyKey(m[1])
		}
	}

	stream := meta.Stream
	if !stream {
		stream = strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	}
	if stream && path == "/v1/chat/completions" {
		body = injectIncludeUsage(body)
	}

	model := meta.Model
	paid := h.prices.Paid(model)
	// 会话指纹只对"有对话内容"的请求有意义；/v1/models 这类没有 body 的自然算不出来。
	sessKey := sessionKey(body, h.cfg.Pool.SessionSticky.PrefixAffinity)

	tried := map[string]bool{}
	// 模型侧限流时记下这个号，下一次尝试仍落回它：换号等于把另一个号的配额也烧掉。
	preferKey := ""
	lastKey := "" // 同号重试不算"换号"，统计口径要分开
	maxAttempts := h.cfg.Pool.RetryMax
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastBody []byte
	var lastStatus int
	var lastTrace string
	var lastErr string

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lease, err := h.pool.Acquire(pool.PickRequest{
			Model:       model,
			Paid:        paid,
			Tried:       tried,
			StickyKeyID: stickyKey,
			SessionKey:  sessKey,
			PreferKeyID: preferKey,
		})
		if err != nil {
			// 已经有上游错误时保留它：把上游的 500 改写成网关的 503，
			// 会让排障的人以为是网关容量问题，实际是上游在炸。
			if lastStatus == 0 {
				lastStatus = http.StatusServiceUnavailable
				lastBody = errJSON("NO_CAPACITY", err.Error())
				lastErr = err.Error()
			}
			break
		}
		tried[lease.KeyID] = true
		preferKey = ""
		if attempt > 1 {
			h.stats.Retries.Add(1)
			if lease.KeyID != lastKey {
				h.stats.Switched.Add(1)
			}
		}
		lastKey = lease.KeyID

		ctx, cancel := context.WithTimeout(r.Context(), h.timeoutFor(isVideoCreate || isVideoQuery))
		res, err := h.up.Do(ctx, r.Method, path, body, lease.Secret, r.Header, stream, isVideoCreate || isVideoQuery)
		if err != nil {
			cancel()
			kind := pool.FailUpstream
			h.pool.Release(lease, &kind, err.Error())
			lastStatus = http.StatusBadGateway
			lastBody = errJSON("UPSTREAM_ERROR", err.Error())
			lastErr = err.Error()
			h.charge(lease, model, path, 0, 0, 0, 0, attempt, start, true, err.Error())
			continue
		}

		if res.Status >= 400 {
			res.Close()
			cancel()
			kind, retryAfter := classify(res)
			msg := errorMessage(res.Body)
			h.pool.ReleaseWithHint(lease, &kind, msg, retryAfter)
			lastStatus, lastBody, lastTrace, lastErr = res.Status, res.Body, res.TraceID, msg
			h.charge(lease, model, path, res.Status, 0, 0, 0, attempt, start, true, msg)
			// 4xx 是请求本身的问题：换号也没用，立即返回。
			if kind == pool.FailClient {
				break
			}
			if kind == pool.FailUpstreamRate && attempt < maxAttempts {
				// 模型侧限流跟号无关：短暂退避后优先用同一个号再试一次。
				select {
				case <-time.After(h.upstreamRateBackoff(attempt, retryAfter)):
				case <-r.Context().Done():
					return
				}
				delete(tried, lease.KeyID)
				preferKey = lease.KeyID
				continue
			}
			continue
		}

		// 成功：往下走透传。注意此处不能再换号。
		h.stats.OK.Add(1)
		h.deliver(w, r, res, lease, model, path, stream, attempt, start, isVideoCreate, body)
		cancel()
		return
	}

	h.stats.Failed.Add(1)
	if lastStatus == 0 {
		lastStatus = http.StatusBadGateway
	}
	if lastBody == nil {
		lastBody = errJSON("UPSTREAM_ERROR", lastErr)
	}
	copyErrHeaders(w, lastTrace)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(lastStatus)
	_, _ = w.Write(lastBody)
}

func (h *Handler) timeoutFor(video bool) time.Duration {
	if video {
		return h.cfg.Pool.VideoTimeout.Duration()
	}
	return h.cfg.Pool.UpstreamTimeout.Duration()
}

// deliver 把已确认 2xx 的响应交给客户端，并同步记账。
func (h *Handler) deliver(w http.ResponseWriter, r *http.Request, res *upstream.Result, lease *pool.Lease,
	model, path string, stream bool, attempt int, start time.Time, isVideoCreate bool, reqBody []byte) {

	if res.Stream != nil {
		defer res.Close()
		copySuccessHeaders(w, res.Header, true)
		w.WriteHeader(res.Status)
		rc := http.NewResponseController(w)
		_ = rc.Flush()

		u := h.pipeSSE(w, res.Stream)
		cost := h.prices.Cost(model, u)
		h.pool.Record(lease.KeyID, u.Prompt, u.Completion, cost)
		var kind *pool.FailKind
		h.pool.Release(lease, kind, "")
		h.charge(lease, model, path, res.Status, u.Prompt, u.Completion, cost, attempt, start, false, "")
		return
	}

	u, _ := ledger.ParseUsage(res.Body)
	if !u.HasDirect && u.Prompt == 0 && u.Completion == 0 && looksLikeChat(path) {
		// 上游没回 usage：按字符估算，面板会标成估算值，不静默当成 0。
		u.Prompt = int64(len(reqBody)) / 3
		u.Completion = int64(len(res.Body)) / 6
		u.Estimated = true
	}
	cost := h.prices.Cost(model, u)

	if isVideoCreate {
		if id := videoID(res.Body); id != "" {
			h.pool.SetSticky(id, lease.KeyID)
		}
	}
	h.pool.Record(lease.KeyID, u.Prompt, u.Completion, cost)
	var kind *pool.FailKind
	h.pool.Release(lease, kind, "")

	copySuccessHeaders(w, res.Header, false)
	w.WriteHeader(res.Status)
	_, _ = w.Write(res.Body)
	h.charge(lease, model, path, res.Status, u.Prompt, u.Completion, cost, attempt, start, false, "")
}

// pipeSSE 逐行透传并解析 usage。逐行而不是整包：既要保实时性，也要能在内存里
// 只留最小状态。
func (h *Handler) pipeSSE(w http.ResponseWriter, src io.Reader) ledger.Usage {
	rc := http.NewResponseController(w)
	br := bufio.NewReaderSize(src, 64<<10)
	var u ledger.Usage
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			if _, werr := w.Write(line); werr != nil {
				return u // 客户端断了，别再往上游要数据
			}
			_ = rc.Flush()
			scanSSELine(line, &u)
		}
		if err != nil {
			return u
		}
	}
}

func scanSSELine(line []byte, u *ledger.Usage) {
	t := bytes.TrimSpace(line)
	if !bytes.HasPrefix(t, []byte("data:")) {
		return
	}
	payload := bytes.TrimSpace(t[5:])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return
	}
	if !bytes.Contains(payload, []byte(`"usage"`)) && !bytes.Contains(payload, []byte(`"input_tokens"`)) &&
		!bytes.Contains(payload, []byte(`"output_tokens"`)) {
		return
	}
	if parsed, ok := ledger.ParseUsage(payload); ok {
		if parsed.Prompt > 0 {
			u.Prompt = parsed.Prompt
		}
		if parsed.Completion > 0 {
			u.Completion = parsed.Completion
		}
		if parsed.Cached > 0 {
			u.Cached = parsed.Cached
		}
		if parsed.HasDirect {
			u.Direct = parsed.Direct
			u.HasDirect = true
		}
	}
}

// charge 落一条请求日志 + 日统计。
func (h *Handler) charge(lease *pool.Lease, model, path string, status int, pt, ct int64, cost float64,
	attempt int, start time.Time, isErr bool, errMsg string) {
	if lease == nil {
		return
	}
	e := ledger.Entry{
		Time: time.Now(), KeyID: lease.KeyID, KeyName: lease.Name, Model: model, Path: path,
		Status: status, LatencyMS: time.Since(start).Milliseconds(),
		Prompt: pt, Completion: ct, Cost: cost, Attempt: attempt, Error: errMsg,
	}
	h.log.Add(e)
	h.daily.Add(e.Time, pt, ct, cost, isErr || status >= 400)
}

func (h *Handler) serveDirect(w http.ResponseWriter, r *http.Request, body []byte, start time.Time) {
	stream := strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	if len(body) > 0 {
		var m reqMeta
		if json.Unmarshal(body, &m) == nil && m.Stream {
			stream = true
		}
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeoutFor(strings.Contains(r.URL.Path, "/videos")))
	defer cancel()
	res, err := h.up.Do(ctx, r.Method, r.URL.Path, body, bearer(r), r.Header, stream, strings.Contains(r.URL.Path, "/videos"))
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "UPSTREAM_ERROR", err.Error())
		return
	}
	if res.Status >= 400 {
		res.Close()
		copyErrHeaders(w, res.TraceID)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(res.Status)
		_, _ = w.Write(res.Body)
		return
	}
	if res.Stream != nil {
		defer res.Close()
		copySuccessHeaders(w, res.Header, true)
		w.WriteHeader(res.Status)
		_ = http.NewResponseController(w).Flush()
		_, _ = io.Copy(w, res.Stream)
		return
	}
	copySuccessHeaders(w, res.Header, false)
	w.WriteHeader(res.Status)
	_, _ = w.Write(res.Body)
	_ = start
}

// ---------- 辅助 ----------

func (h *Handler) authorized(k string) bool {
	if k == "" {
		return false
	}
	for _, want := range h.cfg.ClientKeys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(want)) == 1 {
			return true
		}
	}
	return false
}

func bearer(r *http.Request) string {
	if v := r.Header.Get("Authorization"); v != "" {
		if len(v) > 7 && strings.EqualFold(v[:7], "bearer ") {
			return strings.TrimSpace(v[7:])
		}
		return strings.TrimSpace(v)
	}
	if v := r.Header.Get("x-api-key"); v != "" {
		return strings.TrimSpace(v)
	}
	return ""
}

// classify 把上游响应翻译成号池的失败类型，并顺带取出"多久后恢复"的提示。
//
// 429 有两种，状态码一样但处置相反，必须看 body：
//
//	RATE_LIMITED          本平台的次数配额（实测每号 30 次/约 15 秒窗口）→ 罚号，按 retryAfter 冷却
//	UPSTREAM_RATE_LIMITED 模型供应链限流 → 换号救不了，不罚号，退避后同号重试
func classify(res *upstream.Result) (pool.FailKind, time.Duration) {
	status, body := res.Status, res.Body
	switch {
	case status == http.StatusUnauthorized:
		return pool.FailAuth, 0
	case status == http.StatusPaymentRequired:
		return pool.FailQuota, 0
	case status == http.StatusTooManyRequests:
		if bytes.Contains(body, []byte("UPSTREAM_RATE_LIMITED")) {
			return pool.FailUpstreamRate, retryAfterFrom(res)
		}
		return pool.FailRate, retryAfterFrom(res)
	case status == http.StatusForbidden:
		// 403 是同一个状态码、三种完全不同的意思，必须看 body 才能判：
		//   ACCOUNT_SUSPENDED   账号被临时冻结 → 是这个号的问题，冷却+换号
		//   MODEL_ACCESS_DENIED 模型未开通/未灰度 → 换哪个号都一样，直接回客户端
		//   其它                认不出来 → 冷却久一点，别反复撞
		switch {
		case bytes.Contains(body, []byte("ACCOUNT_SUSPENDED")):
			return pool.FailRate, retryAfterFrom(res)
		case bytes.Contains(body, []byte("MODEL_ACCESS_DENIED")),
			bytes.Contains(body, []byte("灰度")):
			return pool.FailClient, 0
		default:
			return pool.FailQuota, 0
		}
	case status >= 500:
		return pool.FailUpstream, 0
	case status >= 400:
		return pool.FailClient, 0
	}
	return pool.FailClient, 0
}

// retryAfterFrom 依次看 Retry-After 头与 body 里的 data.retryAfterSeconds，
// 都没有就返回 0，交给配置里的默认冷却。
func retryAfterFrom(res *upstream.Result) time.Duration {
	if v := strings.TrimSpace(res.Header.Get("Retry-After")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
		if t, err := http.ParseTime(v); err == nil {
			if d := time.Until(t); d > 0 {
				return d
			}
		}
	}
	var m struct {
		Data struct {
			RetryAfterSeconds float64 `json:"retryAfterSeconds"`
		} `json:"data"`
	}
	if json.Unmarshal(res.Body, &m) == nil && m.Data.RetryAfterSeconds > 0 {
		return time.Duration(m.Data.RetryAfterSeconds * float64(time.Second))
	}
	return 0
}

// upstreamRateBackoff 是模型侧限流的退避：听上游的提示，但最多 5s，
// 免得把同步请求拖到客户端自己超时。
func (h *Handler) upstreamRateBackoff(attempt int, hint time.Duration) time.Duration {
	if hint > 0 {
		if hint > 5*time.Second {
			return 5 * time.Second
		}
		return hint
	}
	d := time.Duration(attempt) * 700 * time.Millisecond
	if d > 3*time.Second {
		return 3 * time.Second
	}
	return d
}

func errorMessage(body []byte) string {
	var m struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Error   *struct {
			Message string `json:"message"`
			Type    string `json:"type"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &m) == nil {
		if m.Message != "" {
			return strings.TrimSpace(m.Code + " " + m.Message)
		}
		if m.Error != nil && m.Error.Message != "" {
			return m.Error.Message
		}
	}
	return snippet(body, 240)
}

func snippet(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "…"
}

func errJSON(code, msg string) []byte {
	b, _ := json.Marshal(map[string]any{
		"error": map[string]any{"type": code, "message": msg, "code": code},
	})
	return b
}

func writeJSONError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(errJSON(code, msg))
}

func copyErrHeaders(w http.ResponseWriter, trace string) {
	if trace != "" {
		w.Header().Set("x-trace-id", trace)
	}
	w.Header().Set("x-trpool", "error")
}

func copySuccessHeaders(w http.ResponseWriter, src http.Header, stream bool) {
	for k, vs := range src {
		lk := strings.ToLower(k)
		switch lk {
		case "content-length", "connection", "transfer-encoding", "keep-alive",
			"proxy-authenticate", "proxy-authorization", "te", "trailer", "upgrade",
			"content-encoding":
			continue
		}
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	if stream {
		w.Header().Del("Content-Length")
	}
}

func looksLikeChat(path string) bool {
	return strings.Contains(path, "/chat/completions") || strings.Contains(path, "/messages")
}

func videoID(body []byte) string {
	var m struct {
		ID string `json:"id"`
	}
	if json.Unmarshal(body, &m) == nil {
		return m.ID
	}
	return ""
}

// injectIncludeUsage 让上游在流式结束时多发一个 usage chunk，否则流式请求
// 只能靠估算记账。多出的这个 chunk 是 OpenAI 官方语义，SDK 会正常忽略。
func injectIncludeUsage(body []byte) []byte {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(body, &m); err != nil {
		return body
	}
	so := map[string]any{}
	if raw, ok := m["stream_options"]; ok {
		_ = json.Unmarshal(raw, &so)
	}
	if v, ok := so["include_usage"].(bool); ok && v {
		return body
	}
	so["include_usage"] = true
	raw, err := json.Marshal(so)
	if err != nil {
		return body
	}
	m["stream_options"] = raw
	out, err := json.Marshal(m)
	if err != nil {
		return body
	}
	return out
}

// VerifyClientKey 供面板下发密钥时自检。
func VerifyClientKey(cfg config.Config, k string) bool {
	for _, want := range cfg.ClientKeys {
		if subtle.ConstantTimeCompare([]byte(k), []byte(want)) == 1 {
			return true
		}
	}
	return false
}
