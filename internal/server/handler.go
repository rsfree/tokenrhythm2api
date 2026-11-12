package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"trpool/internal/fault"
	"trpool/internal/httpauth"
	"trpool/internal/pool"
	"trpool/internal/reqlog"
	"trpool/internal/session"
	"trpool/internal/upstream"
	"trpool/internal/usage"
)

var videoPathRe = regexp.MustCompile(`^/v1/videos/([^/?]+)$`)

type reqMeta struct {
	Model  string `json:"model"`
	Stream bool   `json:"stream"`
}

func (s *Server) handleProxy(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	reqID := strings.TrimSpace(r.Header.Get("X-Request-Id"))
	if reqID == "" {
		reqID = reqlog.NewRequestID()
	}
	w.Header().Set("X-Request-Id", reqID)
	s.stats.Total.Add(1)

	// 鉴权：客户端只认网关下发的 key；直连模式允许客户端自带 sk_tr_。
	snap := s.d.Live.Load()
	tok, _ := httpauth.Credential(r)
	direct := snap.AllowPassthrough && strings.HasPrefix(tok, "sk_tr_")
	if !direct && !httpauth.MatchAny(tok, snap.ClientKeys) {
		s.stats.Failed.Add(1)
		writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "网关密钥无效；请使用面板下发的 client key")
		return
	}

	limit := int64(s.d.Cfg.Server.BodyLimitMB) << 20
	if limit <= 0 {
		limit = 64 << 20
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		s.stats.Failed.Add(1)
		writeJSONError(w, http.StatusBadRequest, "BAD_REQUEST", "读取请求体失败: "+err.Error())
		return
	}
	_ = r.Body.Close()
	if int64(len(body)) > limit {
		s.stats.Failed.Add(1)
		writeJSONError(w, http.StatusRequestEntityTooLarge, "BODY_TOO_LARGE", "请求体超过上限")
		return
	}

	var meta reqMeta
	if len(body) > 0 {
		_ = json.Unmarshal(body, &meta)
	}

	if direct {
		s.serveDirect(w, r, body, meta, reqID, start)
		return
	}

	path := r.URL.Path
	isVideoCreate := r.Method == http.MethodPost && path == "/v1/videos/generations"
	isVideoQuery := r.Method == http.MethodGet && videoPathRe.MatchString(path)

	// 视频查询必须回到创建任务的那个号，否则上游查不到（可能返回 404）。
	stickyKey := ""
	if isVideoQuery {
		if m := videoPathRe.FindStringSubmatch(path); m != nil {
			stickyKey = s.d.Pool.StickyKey(m[1])
		}
	}

	stream := meta.Stream
	if !stream {
		stream = strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	}
	if stream && path == "/v1/chat/completions" {
		// 强制 include_usage：否则流式拿不到 usage，记账只能靠估算。
		body = injectIncludeUsage(body)
	}

	model := meta.Model
	paid := s.d.Prices.Paid(model)
	// 会话指纹只对"有对话内容"的请求有意义；/v1/models 这类没有 body 的自然算不出来。
	pcfg := s.d.Pool.Config()
	sessKey := session.Fingerprint(body, pcfg.SessionSticky.PrefixAffinity)

	tried := map[string]bool{}
	preferKey := "" // 模型侧限流时"还是这个号再试一次"
	lastKey := ""
	maxAttempts := pcfg.RetryMax
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var lastBody []byte
	var lastStatus int
	var lastErr string
	// 失败也要把 Retry-After 这类语义头带出去（429 的退避全靠它），
	// 但只带白名单里的，上游其余头一个不放。
	var lastHeader http.Header
	// 每次失败尝试的归类：用来判断"这一整轮是不是号都欠费/冻结了"。
	var failedKinds []fault.Kind

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lease, err := s.d.Pool.Acquire(pool.PickRequest{
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
				// 选不到号也要留一条日志：池空了/全都在冷却是最需要人介入的故障，
				// 不记的话面板上什么都看不到。
				s.noteFailure(r, reqID, nil, model, path, lastStatus, attempt, start, "", err.Error())
			}
			break
		}
		tried[lease.KeyID] = true
		preferKey = ""
		if attempt > 1 {
			s.stats.Retries.Add(1)
			if lease.KeyID != lastKey {
				s.stats.Switched.Add(1)
			}
		}
		lastKey = lease.KeyID

		ctx, cancel := context.WithTimeout(r.Context(), s.timeoutFor(isVideoCreate || isVideoQuery))
		res, err := s.d.Up.Do(ctx, r.Method, path, body, lease.Secret, r.Header, stream)
		if err != nil {
			cancel()
			s.d.Pool.Release(lease, pool.Outcome{
				Kind: fault.Upstream, Model: model, Message: err.Error(),
			})
			lastStatus = http.StatusBadGateway
			lastBody = errJSON("UPSTREAM_ERROR", err.Error())
			lastErr = err.Error()
			failedKinds = append(failedKinds, fault.Upstream)
			s.noteFailure(r, reqID, lease, model, path, lastStatus, attempt, start, "", err.Error())
			if attempt < maxAttempts && !sleepCtx(r.Context(), backoffFor(attempt-1)) {
				return
			}
			continue
		}

		if res.Status >= 400 {
			res.Close()
			cancel()
			v := fault.Classify(res.Status, res.Body, res.Header)
			s.d.Pool.Release(lease, pool.Outcome{
				Kind: v.Kind, Model: model, Message: v.Message, RetryAfter: v.RetryAfter,
			})
			lastStatus, lastBody, lastErr = res.Status, res.Body, v.Message
			lastHeader = res.Header
			failedKinds = append(failedKinds, v.Kind)
			s.noteFailure(r, reqID, lease, model, path, res.Status, attempt, start, res.TraceID, v.Message)

			if !v.Retryable {
				break // 4xx 里认不出来的那些：请求本身有问题，换号也没用
			}
			if v.Kind == fault.UpstreamRate && attempt < maxAttempts {
				// 模型侧限流跟号无关：退避后优先用同一个号再试，别把别的号的配额也烧掉。
				if !sleepCtx(r.Context(), rateBackoff(attempt, v.RetryAfter)) {
					return
				}
				delete(tried, lease.KeyID)
				preferKey = lease.KeyID
				continue
			}
			if v.Kind == fault.Upstream && attempt < maxAttempts {
				if !sleepCtx(r.Context(), backoffFor(attempt-1)) {
					return
				}
			}
			continue
		}

		// 成功：从这里开始不能再换号。
		s.stats.OK.Add(1)
		s.deliver(w, r, res, lease, model, path, stream, attempt, start, isVideoCreate, reqID)
		cancel()
		return
	}

	// 全部尝试失败：把上游原样回给客户端（能保留真实原因），没有上游响应才自己造。
	s.stats.Failed.Add(1)
	if lastStatus == 0 {
		lastStatus = http.StatusBadGateway
	}
	if lastBody == nil {
		lastBody = errJSON("UPSTREAM_ERROR", lastErr)
	}

	// 整轮都栽在"号欠费 / 号被冻结"上：这是号池的账，不是客户端的账。
	//
	// 原样透传上游的 402/403 会让调用方以为自己欠费、或被永久拒绝，去查自己的账号，
	// 而真正要做的是给号充值 / 等解封——还得有人在面板上数一遍才知道是哪个号。
	// 所以这里改写成网关自己的 503，并把池子现状写进 message。
	if onlyDeadKeys(failedKinds) {
		total, healthy, cooling, invalid, exhausted, blocked, disabled := s.d.Pool.Counts()
		msg := fmt.Sprintf("号池无可用账号：本次 %d 个号全部欠费或冻结（池内共 %d 个：欠费 %d、冻结 %d、失效 %d、冷却 %d、禁用 %d）",
			len(failedKinds), total, exhausted, blocked, invalid, cooling, disabled)
		if healthy > 0 {
			// 池里还有活号，只是这一轮抽到的全是死号：让客户端重试即可，别谎报"没有账号"。
			msg = fmt.Sprintf("本轮 %d 个号都已欠费或冻结，池内仍有 %d 个可用号，请重试",
				len(failedKinds), healthy)
		}
		lastStatus = http.StatusServiceUnavailable
		lastBody = errJSON("NO_CAPACITY", msg)
		lastErr = msg
	}

	// 不再回任何 X-Trpool-* 头：号名、第几次尝试、上游 trace_id 都是渠道内部信息，
	// 客户端拿到只会泄露"背后有一池号"。排障这些信息在面板日志与 reqlog 里都有，
	// 失败原因也能从 body 的 error.code（NO_CAPACITY / UPSTREAM_ERROR…）读出来。
	copyResponseHeaders(w, lastHeader, false)
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(lastStatus)
	_, _ = w.Write(lastBody)
	s.d.Daily.Add(time.Now(), 0, 0, 0, true)
}

func (s *Server) timeoutFor(video bool) time.Duration {
	// 走 pool 的热配置：面板改了超时应该立刻生效，而不是等重启。
	cfg := s.d.Pool.Config()
	if video {
		return cfg.VideoTimeout.Duration()
	}
	return cfg.UpstreamTimeout.Duration()
}

// deliver 把已确认 2xx 的响应交给客户端，并同步记账。
func (s *Server) deliver(w http.ResponseWriter, r *http.Request, res *upstream.Result, lease *pool.Lease,
	model, path string, stream bool, attempt int, start time.Time, isVideoCreate bool, reqID string) {

	entry := reqlog.Entry{
		Time: time.Now(), RequestID: reqID,
		KeyID: lease.KeyID, KeyName: lease.Name, Model: model, Path: path,
		Status: res.Status, Stream: stream, Attempt: attempt,
		Fallback: lease.Fallback, TraceID: res.TraceID,
		ClientIP: clientIP(r), UA: truncate(r.UserAgent(), 160),
	}

	if res.Stream != nil {
		defer res.Close()
		copyResponseHeaders(w, res.Header, true)
		w.WriteHeader(res.Status)

		sw := newStreamWriter(w, s.d.Cfg.Server.StreamKeepalive.Duration())
		u, ttfb, werr := s.pipeSSE(sw, res.Stream, start)
		sw.Close()

		entry.LatencyMS = time.Since(start).Milliseconds()
		entry.TTFBMS = ttfb
		entry.PromptTokens, entry.CompletionTokens = u.Prompt, u.Completion
		cost := s.d.Prices.Cost(model, u)
		entry.Cost = cost
		if werr != nil {
			entry.Note = "客户端断开: " + werr.Error()
		}
		s.d.Pool.Record(lease.KeyID, u.Prompt, u.Completion, cost)
		s.d.Pool.Release(lease, pool.Outcome{Model: model})
		s.d.Log.Record(entry)
		s.d.Daily.Add(entry.Time, u.Prompt, u.Completion, cost, false)
		s.logLine(entry)
		return
	}

	u, _ := usage.Parse(res.Body)
	if !u.HasDirect && u.Prompt == 0 && u.Completion == 0 && looksLikeChat(path) && len(res.Body) > 0 {
		// 上游没回 usage：按字节估算，面板会标估算值，不静默当成 0。
		u.Prompt = int64(len(res.Body)) / 8
		u.Completion = int64(len(res.Body)) / 8
		u.Estimated = true
	}
	cost := s.d.Prices.Cost(model, u)

	if isVideoCreate {
		if id := videoID(res.Body); id != "" {
			s.d.Pool.SetSticky(id, lease.KeyID)
		}
	}
	s.d.Pool.Record(lease.KeyID, u.Prompt, u.Completion, cost)
	s.d.Pool.Release(lease, pool.Outcome{Model: model})

	entry.LatencyMS = time.Since(start).Milliseconds()
	entry.TTFBMS = entry.LatencyMS
	entry.PromptTokens, entry.CompletionTokens = u.Prompt, u.Completion
	entry.Cost = cost
	if u.Estimated {
		entry.Note = "usage 为估算值"
	}

	copyResponseHeaders(w, res.Header, false)
	w.WriteHeader(res.Status)
	if len(res.Body) > 0 {
		_, _ = w.Write(res.Body)
	}
	s.d.Log.Record(entry)
	s.d.Daily.Add(entry.Time, u.Prompt, u.Completion, cost, false)
	s.logLine(entry)
}

// serveDirect 是"客户端自带 sk_tr_ 直连"模式：不选号、不记额度，但仍然记日志。
func (s *Server) serveDirect(w http.ResponseWriter, r *http.Request, body []byte, meta reqMeta, reqID string, start time.Time) {
	stream := meta.Stream || strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	tok, _ := httpauth.Credential(r)
	isVideo := strings.Contains(r.URL.Path, "/videos")
	ctx, cancel := context.WithTimeout(r.Context(), s.timeoutFor(isVideo))
	defer cancel()

	res, err := s.d.Up.Do(ctx, r.Method, r.URL.Path, body, tok, r.Header, stream)
	if err != nil {
		writeJSONError(w, http.StatusBadGateway, "UPSTREAM_ERROR", err.Error())
		return
	}
	if res.Status >= 400 {
		defer res.Close()
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(res.Status)
		_, _ = w.Write(res.Body)
		return
	}

	entry := reqlog.Entry{
		Time: time.Now(), RequestID: reqID,
		KeyID: "direct", KeyName: "(直连)", Model: meta.Model, Path: r.URL.Path,
		Status: res.Status, Stream: res.Stream != nil, Attempt: 1,
		TraceID: res.TraceID, ClientIP: clientIP(r), UA: truncate(r.UserAgent(), 160),
	}
	if res.Stream != nil {
		defer res.Close()
		copyResponseHeaders(w, res.Header, true)
		w.WriteHeader(res.Status)
		sw := newStreamWriter(w, s.d.Cfg.Server.StreamKeepalive.Duration())
		u, ttfb, _ := s.pipeSSE(sw, res.Stream, start)
		sw.Close()
		entry.LatencyMS = time.Since(start).Milliseconds()
		entry.TTFBMS = ttfb
		entry.PromptTokens, entry.CompletionTokens = u.Prompt, u.Completion
		entry.Cost = s.d.Prices.Cost(meta.Model, u)
	} else {
		u, _ := usage.Parse(res.Body)
		copyResponseHeaders(w, res.Header, false)
		w.WriteHeader(res.Status)
		_, _ = w.Write(res.Body)
		entry.LatencyMS = time.Since(start).Milliseconds()
		entry.TTFBMS = entry.LatencyMS
		entry.PromptTokens, entry.CompletionTokens = u.Prompt, u.Completion
		entry.Cost = s.d.Prices.Cost(meta.Model, u)
	}
	s.d.Log.Record(entry)
	s.d.Daily.Add(entry.Time, entry.PromptTokens, entry.CompletionTokens, entry.Cost, false)
	s.logLine(entry)
}

// ---------- 辅助 ----------

// onlyDeadKeys 判断这一轮失败是否全部来自"号欠费(quota) / 号被冻结(banned)"。
//
// 只认这两种：掺进一条 5xx、网络错误或 429，就说明池子不是"全死了"，
// 那种情况保留上游原文更有排障价值。
func onlyDeadKeys(kinds []fault.Kind) bool {
	if len(kinds) == 0 {
		return false
	}
	for _, k := range kinds {
		if k != fault.Quota && k != fault.Banned {
			return false
		}
	}
	return true
}

// passthroughHeaders 是唯一允许从上游透传给客户端的响应头。
//
// 老实现是黑名单：只挡 hop-by-hop，其余原样转发。结果是上游把自己的实现细节
// 整套送出去——实测 /v1/models 的响应里带着上游的
// Content-Security-Policy（里面列着它的前端域名、阿里云 OSS、飞书、验证码源）、
// X-Tr-Frontend-Commit（上游构建 commit）、X-Trace-Id、Server 等等。
// 这些对客户端毫无用处，却是实打实的渠道泄露。
//
// 改成白名单：只留客户端真正需要的那几个。漏掉一个头的代价远小于漏掉内部实现。
var passthroughHeaders = map[string]bool{
	"content-type":        true, // 必需：区分 JSON 与 text/event-stream
	"retry-after":         true, // 429/503 的重试语义，客户端退避行为依赖它
	"content-disposition": true, // 文件/附件下载的文件名
	"content-range":       true, // 分片下载
	"accept-ranges":       true,
}

func copyResponseHeaders(w http.ResponseWriter, src http.Header, stream bool) {
	for k, vs := range src {
		if !passthroughHeaders[strings.ToLower(k)] {
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

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
