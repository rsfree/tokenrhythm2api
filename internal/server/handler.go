package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"strconv"
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

// 渠道病（上游在抖）在同一账号上重试的次数与间隔。1 次足够区分"瞬时抖动"与
// "真坏了"；间隔要短——这一跳的意义是"等一个抖动过去"，不是退避。
const (
	sameKeyRetryLimit = 1
	sameKeyPause      = 150 * time.Millisecond
)

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
		// 这是**我方**的鉴权，不是上游凭据问题：文案必须点明该用哪把 key，
		// 否则调用方会拿面板令牌/上游 key 一直重试。
		s.writeFault(w, reqID, fault.Verdict{Canonical: fault.CAuthentication},
			"网关密钥无效；请使用面板下发的 client key")
		return
	}

	limit := int64(s.d.Cfg.Server.BodyLimitMB) << 20
	if limit <= 0 {
		limit = 64 << 20
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if err != nil {
		s.stats.Failed.Add(1)
		s.writeFault(w, reqID, fault.Verdict{Canonical: fault.CInvalidRequest},
			"读取请求体失败，请重试")
		return
	}
	_ = r.Body.Close()
	if int64(len(body)) > limit {
		s.stats.Failed.Add(1)
		s.writeFault(w, reqID, fault.Verdict{Canonical: fault.CRequestTooLarge}, "")
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
	maxAttempts := pcfg.RetryMax
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	// 本轮所有失败：对外只回一个结论，但运维记录里一条都不能少。
	var failures []fault.Verdict
	// 同一个号上的渠道病重试次数，换号后清零。
	sameKeyRetries := 0
	lastKey := ""

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
			// 一个号都没试过（池空 / 全在冷却）：这属于"暂时没有容量"，
			// 503 + Retry-After 让客户端稍后再来，比 502"网关坏了"准确。
			if len(failures) == 0 {
				v := fault.Verdict{
					Canonical:  fault.CServerOverloaded,
					Kind:       fault.Upstream,
					RuleID:     "pool.no_lease",
					Message:    err.Error(),
					RetryAfter: 30 * time.Second,
				}
				s.stats.Failed.Add(1)
				s.noteFailure(r, reqID, nil, model, path, v, attempt, start, "", err.Error())
				s.writeFault(w, reqID, v, "")
				s.d.Daily.Add(time.Now(), 0, 0, 0, true)
				return
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
		if lease.KeyID != lastKey {
			sameKeyRetries = 0
		}
		lastKey = lease.KeyID

		ctx, cancel := context.WithTimeout(r.Context(), s.timeoutFor(isVideoCreate || isVideoQuery))
		res, err := s.d.Up.Do(ctx, r.Method, path, body, lease.Secret, r.Header, stream)
		if err != nil {
			cancel()
			v := fault.ClassifyNet(err)
			s.d.Pool.Release(lease, pool.Outcome{
				Kind: v.Kind, Model: model, Message: err.Error(),
			})
			failures = append(failures, v)
			s.noteFailure(r, reqID, lease, model, path, v, attempt, start, "", "")
			if attempt < maxAttempts && !sleepCtx(r.Context(), backoffFor(attempt-1)) {
				return
			}
			continue
		}

		// 200 里带 error 成员也要当失败：当成功处理会记一条空补全并计费。
		broken := res.Status >= 400
		if !broken && res.Stream == nil && fault.SuccessCarriesError(res.Body) {
			broken = true
		}
		if broken {
			res.Close()
			cancel()
			v := fault.Classify(res.Status, res.Body, res.Header)
			s.d.Pool.Release(lease, pool.Outcome{
				Kind: v.Kind, Model: model, Message: v.Message, RetryAfter: v.RetryAfter,
			})
			failures = append(failures, v)
			s.noteFailure(r, reqID, lease, model, path, v, attempt, start, res.TraceID, "")

			// 请求病：换谁都一样。立刻返回，别把 N 个号都撞一遍。
			if v.RequestFault() || !v.Retryable || attempt >= maxAttempts {
				break
			}

			switch {
			case v.SameKeyRetryWorth() && sameKeyRetries < sameKeyRetryLimit:
				// 裸 5xx：先在同一账号上重试一次。瞬时抖动换号救不了，
				// 反而把新号也拖进熔断统计。
				sameKeyRetries++
				delete(tried, lease.KeyID)
				preferKey = lease.KeyID
				if !sleepCtx(r.Context(), jitterDur(sameKeyPause)) {
					return
				}
				continue
			case v.Kind == fault.UpstreamRate:
				// 模型侧限流跟号无关：退避后优先用同一个号，别烧别的号的配额。
				if !sleepCtx(r.Context(), rateBackoff(attempt, v.RetryAfter)) {
					return
				}
				delete(tried, lease.KeyID)
				preferKey = lease.KeyID
				continue
			}
			continue
		}

		// 成功：从这里开始不能再换号。
		s.stats.OK.Add(1)
		s.deliver(w, r, res, lease, model, path, stream, attempt, start, isVideoCreate, reqID)
		cancel()
		return
	}

	// 全部尝试失败：按"对调用方的行动价值"挑一个结论，不是拿最后一个凑数。
	s.stats.Failed.Add(1)
	final, ok := fault.PickFinal(failures)
	if !ok {
		final = fault.Verdict{Canonical: fault.CUnknown, Kind: fault.Upstream, RuleID: "pool.no_attempt"}
	}
	s.noteFailure(r, reqID, nil, model, path, final, maxAttempts, start, "", summarize(failures))
	s.writeFault(w, reqID, final, "")
	s.d.Daily.Add(time.Now(), 0, 0, 0, true)
}

// summarize 把本轮每个号的失败压成一行，给 reqlog 与面板用。
// 形如 `2×insufficient_quota(402 INSUFFICIENT_BALANCE) 1×rate_limit_exceeded(429)`。
func summarize(vs []fault.Verdict) string {
	if len(vs) == 0 {
		return ""
	}
	type key struct {
		c      fault.Canonical
		status int
		code   string
	}
	var order []key
	count := map[key]int{}
	for _, v := range vs {
		k := key{v.Canonical, v.Status, v.RawCode}
		if count[k] == 0 {
			order = append(order, k)
		}
		count[k]++
	}
	var b strings.Builder
	for i, k := range order {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(strconv.Itoa(count[k]))
		b.WriteString("×")
		b.WriteString(string(k.c))
		b.WriteString("(")
		if k.status > 0 {
			b.WriteString(strconv.Itoa(k.status))
		} else {
			b.WriteString("transport")
		}
		if k.code != "" {
			b.WriteString(" ")
			b.WriteString(k.code)
		}
		b.WriteString(")")
	}
	return b.String()
}

// writeFault 是 /v1 链路唯一的错误出口。
//
// 出站只有四个字段：message / type / code / request_id，message 来自模板。
// **上游返回的任何文本都不进来**——不是"过滤后透传"，是"不透传"：
// 正则拦得住邮箱和 JWT，拦不住"你本月还剩 3 次"这种纯数字句子。
//
// message 参数只允许放**我方**自己写的话（例如"网关密钥无效"），
// 任何来自上游的字符串都不许从这里经过。
func (s *Server) writeFault(w http.ResponseWriter, reqID string, v fault.Verdict, message string) {
	sp := fault.SpecOf(v.Canonical)
	if message == "" {
		message = fault.Message(v.Canonical, int(v.RetryAfter/time.Second), reqID)
	}
	if sp.RetryAfter {
		secs := int(v.RetryAfter / time.Second)
		if secs <= 0 {
			secs = sp.RetryAfterDefault
		}
		if secs > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(secs))
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(sp.HTTP)
	_, _ = w.Write(faultBody(v.Canonical, sp.Type, message, reqID))
}

func faultBody(code fault.Canonical, typ, message, reqID string) []byte {
	b, _ := json.Marshal(map[string]any{
		"error": map[string]any{
			"message":    message,
			"type":       typ,
			"code":       string(code),
			"request_id": reqID,
		},
	})
	return b
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
		u, ttfb, werr := s.pipeSSE(sw, res.Stream, reqID, true, start)
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
		// 记账已经读完（usage.Parse / videoID 都在这之前），这时才轮到脱敏：
		// 上游的 cost_cny 是本地记账要用的进货价，先洗掉就只剩 0 了。
		_, _ = w.Write(s.scrub.Body(res.Body))
	}
	s.d.Log.Record(entry)
	s.d.Daily.Add(entry.Time, u.Prompt, u.Completion, cost, false)
	s.logLine(entry)
}

// serveDirect 是"客户端自带 sk_tr_ 直连"模式：不选号、不记额度，但仍然记日志。
//
// 这条路径上凭据是调用方自己的，上游的错误体对他没有保密价值（他自己就是那个账号），
// 所以 body 原样透传；只把传输层错误归一到 canonical。
func (s *Server) serveDirect(w http.ResponseWriter, r *http.Request, body []byte, meta reqMeta, reqID string, start time.Time) {
	stream := meta.Stream || strings.Contains(r.Header.Get("Accept"), "text/event-stream")
	tok, _ := httpauth.Credential(r)
	isVideo := strings.Contains(r.URL.Path, "/videos")
	ctx, cancel := context.WithTimeout(r.Context(), s.timeoutFor(isVideo))
	defer cancel()

	res, err := s.d.Up.Do(ctx, r.Method, r.URL.Path, body, tok, r.Header, stream)
	if err != nil {
		s.writeFault(w, reqID, fault.ClassifyNet(err), "")
		return
	}
	if res.Status >= 400 {
		defer res.Close()
		// Retry-After 归一为秒再给：上游可能给 HTTP-date，客户端解析不了就会立刻重试。
		if ra := fault.RetryAfter(res.Header, res.Body); ra > 0 {
			w.Header().Set("Retry-After", strconv.Itoa(int(ra/time.Second)))
		}
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
		u, ttfb, _ := s.pipeSSE(sw, res.Stream, reqID, false, start)
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

// passthroughHeaders 是唯一允许从上游透传给客户端的响应头。
//
// 老实现是黑名单：只挡 hop-by-hop，其余原样转发。结果是上游把自己的实现细节
// 整套送出去——实测 /v1/models 的响应里带着上游的
// Content-Security-Policy（里面列着它的前端域名、阿里云 OSS、飞书、验证码源）、
// X-Tr-Frontend-Commit（上游构建 commit）、X-Trace-Id、Server 等等。
// 这些对客户端毫无用处，却是实打实的渠道泄露。
//
// 改成白名单：只留客户端真正需要的那几个。漏掉一个头的代价远小于漏掉内部实现。
//
// 注意 Retry-After **不在**这里：上游给的可能是不合规的 HTTP-date 或离谱大数，
// 由 writeFault 统一归一成秒后再设置，不能让它原样穿过去覆盖掉。
var passthroughHeaders = map[string]bool{
	"content-type":        true, // 必需：区分 JSON 与 text/event-stream
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
