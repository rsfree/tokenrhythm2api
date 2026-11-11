package server

import (
	"log"
	"net/http"
	"time"

	"trpool/internal/pool"
	"trpool/internal/reqlog"
)

// noteFailure 记一条失败尝试。请求级失败（4xx）不写进日账，
// 日账在 handleProxy 的最终出口只记一次，避免"一次请求 3 次尝试 = 3 次请求"。
//
// lease 允许为 nil：选不到号时也要留痕，否则面板对"池空了/全冷却"这类
// 最需要人介入的故障是完全静默的。
func (s *Server) noteFailure(r *http.Request, reqID string, lease *pool.Lease,
	model, path string, status, attempt int, start time.Time, trace, msg string) {

	e := reqlog.Entry{
		Time: time.Now(), RequestID: reqID,
		Model: model, Path: path,
		Status: status, LatencyMS: time.Since(start).Milliseconds(), TTFBMS: time.Since(start).Milliseconds(),
		Attempt: attempt, TraceID: trace, Error: msg,
		ClientIP: clientIP(r), UA: truncate(r.UserAgent(), 160),
	}
	if lease != nil {
		e.KeyID, e.KeyName, e.Fallback = lease.KeyID, lease.Name, lease.Fallback
	}
	s.d.Log.Record(e)
	s.logLine(e)
}

// logLine 是每请求一行的运维日志（对齐 workbuddy2api 的 logfmt 形态）。
func (s *Server) logLine(e reqlog.Entry) {
	note := ""
	if e.Error != "" {
		note = " err=" + truncate(e.Error, 120)
	}
	fallback := ""
	if e.Fallback {
		fallback = " fallback=1"
	}
	client := ""
	if e.ClientIP != "" {
		client = " ip=" + e.ClientIP
	}
	log.Printf("[req] id=%s key=%s model=%s %s %d %dms attempt=%d pt=%d ct=%d cost=%.6f%s%s%s",
		shortID(e.RequestID), e.KeyName, orDash(e.Model), e.Path, e.Status,
		e.LatencyMS, e.Attempt, e.PromptTokens, e.CompletionTokens, e.Cost, fallback, client, note)
}

func shortID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
