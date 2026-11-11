// Package server 是网关本体：路由、鉴权、选号、透传、换号、记账。
//
// 重试的边界（务必守住）：只有上游响应头已到、但还没向客户端写出任何字节时才允许
// 换号。流式响应一旦开始刷字节，就只能如实透传到底——中途换号会让客户端收到两个
// 不同上游拼接起来的内容。
package server

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"trpool/internal/config"
	"trpool/internal/httpauth"
	"trpool/internal/livecfg"
	"trpool/internal/pool"
	"trpool/internal/reqlog"
	"trpool/internal/upstream"
	"trpool/internal/usage"
)

type Deps struct {
	Cfg     *config.Config
	Live    *livecfg.Holder
	Pool    *pool.Pool
	Up      *upstream.Client
	Prices  *usage.Prices
	Log     *reqlog.Recorder
	Daily   *usage.Daily
	Version string
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

type Server struct {
	d       Deps
	panel   http.Handler
	started time.Time
	stats   Stats
}

func New(d Deps) *Server {
	return &Server{d: d, started: time.Now()}
}

// MountPanel 由 main 注入，避免 server 反向依赖 panel。
func (s *Server) MountPanel(h http.Handler) { s.panel = h }

func (s *Server) Uptime() time.Duration { return time.Since(s.started) }
func (s *Server) Stats() *Stats         { return &s.stats }

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/healthz":
		s.handleHealth(w)
	case path == "/status":
		s.handleStatus(w, r)
	case strings.HasPrefix(path, "/panel"):
		if s.panel != nil {
			s.panel.ServeHTTP(w, r)
			return
		}
		http.NotFound(w, r)
	case path == "/":
		http.Redirect(w, r, "/panel", http.StatusFound)
	case strings.HasPrefix(path, "/v1/"):
		s.handleProxy(w, r)
	default:
		http.NotFound(w, r)
	}
}

// handleHealth 给负载均衡/宿主探活用。永远 200：进程活着就是活着；
// 号池健康度放在字段里，不拿 503 让编排系统去重启一个"只是暂时没号"的进程。
func (s *Server) handleHealth(w http.ResponseWriter) {
	total, healthy, cooling, invalid, exhausted, blocked, disabled := s.d.Pool.Counts()
	st := s.d.Pool.BatchState()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":             true,
		"service":        "trpool",
		"version":        s.d.Version,
		"keys":           total,
		"healthy":        healthy,
		"cooling":        cooling,
		"invalid":        invalid,
		"exhausted":      exhausted,
		"blocked":        blocked,
		"disabled":       disabled,
		"inflight":       st.TotalInflight,
		"uptime_seconds": int64(s.Uptime().Seconds()),
	})
}

// handleStatus 是给运维脚本看的汇总（面板之外的机器可读入口）。
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	snap := s.d.Live.Load()
	if !httpauth.VerifyBearer(r, snap.AdminKey) {
		writeJSONError(w, http.StatusUnauthorized, "UNAUTHORIZED", "管理令牌无效")
		return
	}
	total, healthy, _, _, _, _, _ := s.d.Pool.Counts()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":       true,
		"service":  "trpool",
		"version":  s.d.Version,
		"upstream": s.d.Up.Base(),
		"keys":     s.d.Pool.Views(),
		"totals":   map[string]any{"keys": total, "healthy": healthy},
		"stats":    s.stats.Snapshot(),
		"batch":    s.d.Pool.BatchState(),
	})
}

// ---------- 输出辅助 ----------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
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

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.IndexByte(xff, ','); i > 0 {
			return strings.TrimSpace(xff[:i])
		}
		return strings.TrimSpace(xff)
	}
	if xr := r.Header.Get("X-Real-IP"); xr != "" {
		return strings.TrimSpace(xr)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
