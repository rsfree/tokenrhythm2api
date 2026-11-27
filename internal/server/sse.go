package server

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"sync"
	"time"

	"trpool/internal/fault"
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
//
// canonicalize 为真时，流里的 error 帧会换成我们自己的 canonical 文案：
// 一旦发过 200 就改不了状态码，只能靠这一帧告诉客户端"这一轮失败了"，
// 而它同样是泄露口——上游会把 traceId、余额、充值链接写在这个帧里。
func (s *Server) pipeSSE(sw *streamWriter, src io.Reader, reqID string, canonicalize bool, start time.Time) (usage.Usage, int64, error) {
	br := bufio.NewReaderSize(src, 64<<10)
	var u usage.Usage
	ttfb := int64(-1)
	for {
		line, err := br.ReadBytes('\n')
		if len(line) > 0 {
			// 顺序不能换：scanUsageLine 要靠 cost_cny 这一帧记账，
			// 先脱敏的话那个字段已经没了，成本会全部记成 0。
			scanUsageLine(line, &u)
			out := s.scrub.SSELine(line)
			if canonicalize {
				out = s.sseErrorFrame(line, out, reqID)
			}
			if _, werr := sw.Write(out); werr != nil {
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

// sseErrorFrame 把流里的错误帧换成 canonical 文案；不是错误帧就回传 scrub 后的原样。
//
// 只换 data 行，不动 event 行与空行——上游若发 `event: error` + `data: {...}`，
// 两行拼起来才是完整的一帧，多插/少插空行都会破坏帧边界。
func (s *Server) sseErrorFrame(raw, scrubbed []byte, reqID string) []byte {
	t := bytes.TrimSpace(raw)
	if !bytes.HasPrefix(t, []byte("data:")) {
		return scrubbed
	}
	payload := bytes.TrimSpace(t[5:])
	if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
		return scrubbed
	}
	if !fault.SuccessCarriesError(payload) {
		return scrubbed
	}
	v := fault.Classify(http.StatusOK, payload, nil)
	body := faultBody(v.Canonical, v.Type(), v.CallerMessage(reqID), reqID)
	eol := "\n"
	if bytes.HasSuffix(raw, []byte("\r\n")) {
		eol = "\r\n"
	}
	return []byte("data: " + string(body) + eol)
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
