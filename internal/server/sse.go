package server

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"

	"trpool/internal/usage"
)

// sseKeepaliveCheck 是心跳检查频率；触发阈值由配置的 StreamKeepalive 决定。
const sseKeepaliveCheck = 5 * time.Second

// streamWriter 在向客户端写 SSE 的同时负责心跳。
//
// 为什么需要它：上游长思考时可能几十秒不吐字节，而客户端/反代的空闲阈值通常
// 是 60~120s——先断的是客户端，而那一轮额度已经烧掉了。补一条 SSE 注释
// （`: keepalive`）不改变协议语义，SDK 会忽略。
type streamWriter struct {
	w    http.ResponseWriter
	rc   *http.ResponseController
	mu   sync.Mutex
	last time.Time
	idle time.Duration

	stop   chan struct{}
	done   chan struct{}
	closed bool
	err    error
}

func newStreamWriter(w http.ResponseWriter, idle time.Duration) *streamWriter {
	s := &streamWriter{
		w:    w,
		rc:   http.NewResponseController(w),
		last: time.Now(),
		idle: idle,
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	if idle > 0 {
		go s.keepalive()
	} else {
		close(s.done)
	}
	return s
}

func (s *streamWriter) keepalive() {
	defer close(s.done)
	t := time.NewTicker(sseKeepaliveCheck)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				return
			}
			if time.Since(s.last) >= s.idle {
				if _, err := io.WriteString(s.w, ": keepalive\n\n"); err != nil {
					s.err = err
					s.mu.Unlock()
					return
				}
				_ = s.rc.Flush()
				s.last = time.Now()
			}
			s.mu.Unlock()
		}
	}
}

func (s *streamWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, io.ErrClosedPipe
	}
	n, err := s.w.Write(p)
	if err == nil {
		err = s.rc.Flush()
	}
	if err != nil {
		s.err = err
		return n, err
	}
	s.last = time.Now()
	return n, nil
}

// Close 停心跳并等它退出——Close 之后 handler 就返回了，
// ResponseWriter 不能再被后台 goroutine 碰。
func (s *streamWriter) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	close(s.stop)
	<-s.done
}

// pipeSSE 逐行透传并解析 usage。逐行而不是整包：既要保实时性，
// 也要能在内存里只留最小状态。
func (s *Server) pipeSSE(sw *streamWriter, src io.Reader, start time.Time) (usage.Usage, int64, error) {
	br := bufio.NewReaderSize(src, 64<<10)
	var u usage.Usage
	ttfb := int64(-1)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			// 顺序不能换：scanUsageLine 要靠 cost_cny 这一帧记账，
			// 先脱敏的话那个字段已经没了，成本会全部记成 0。
			scanUsageLine(line, &u)
			if _, werr := sw.Write(s.scrub.SSELine(line)); werr != nil {
				return u, ttfb, werr // 客户端断了，别再往上游要数据
			}
			if ttfb < 0 {
				ttfb = time.Since(start).Milliseconds()
			}
		}
		if err != nil {
			if err == io.EOF {
				return u, ttfb, nil
			}
			return u, ttfb, err
		}
	}
}

func scanUsageLine(line []byte, u *usage.Usage) {
	t := bytes.TrimSpace(line)
	if !bytes.HasPrefix(t, []byte("data:")) {
		return
	}
	payload := bytes.TrimSpace(t[5:])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return
	}
	// 先做子串检查再解析：流式里绝大多数帧不带 usage，省掉无谓的 JSON 解析。
	if !bytes.Contains(payload, []byte(`"usage"`)) &&
		!bytes.Contains(payload, []byte(`"input_tokens"`)) &&
		!bytes.Contains(payload, []byte(`"output_tokens"`)) &&
		!bytes.Contains(payload, []byte(`"cost_cny"`)) {
		return
	}
	if parsed, ok := usage.ParseChunk(payload); ok {
		u.Add(parsed)
	}
}
