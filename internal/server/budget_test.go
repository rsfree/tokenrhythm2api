package server

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"trpool/internal/config"
)

const chatStreamBody = `{"model":"glm-5.1","messages":[{"role":"user","content":"hi"}],"stream":true}`

// 上游挂住不回任何字节，直到我们主动断开。用于验证"超时真的发生了"。
func hangUp(r *http.Request) {
	<-r.Context().Done()
}

// 非流式超时不能让重试把等待时间叠成 N×单跳。retry_reserve=0 时，
// 首跳用满就应当直接放弃：第一跳 1s，第二跳没有预算，总耗时约 1s 而不是 3s。
func TestTotalBudgetCapsTimeoutRetries(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) {
		c.UpstreamTimeout = config.Duration(time.Second)
		c.RetryReserve = 0
		c.RetryMax = 3
	})
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		hangUp(r)
	}
	start := time.Now()
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	elapsed := time.Since(start)

	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("应回 504 upstream_timeout，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"upstream_timeout"`) {
		t.Fatalf("对外 code 应为 upstream_timeout，实际 %s", body)
	}
	// 预算是 1s + 0。允许 1.6s 的调度余量；关键是远小于 3×1s。
	if elapsed > 1600*time.Millisecond {
		t.Fatalf("总耗时 %s，说明重试仍在叠加单跳预算", elapsed)
	}
	if c := h.fk.count(); c != 1 {
		t.Fatalf("预算耗尽后不该再发起新一跳，实际打了 %d 次", c)
	}
}

// retry_reserve 存在的唯一理由：首跳超时后，用一小段尾巴去捞"上游其实已经算完、
// 只是响应没回来"的结果（prompt cache 命中）。这里验证那一次重试确实发生且能成功。
func TestRetryReserveAllowsOneMoreHop(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) {
		c.UpstreamTimeout = config.Duration(400 * time.Millisecond)
		c.RetryReserve = config.Duration(2 * time.Second)
		c.RetryMax = 3
	})
	var mu sync.Mutex
	calls := 0
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		mu.Lock()
		calls++
		first := calls == 1
		mu.Unlock()
		if first {
			hangUp(r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"ok","choices":[{"message":{"content":"hi"}}],"usage":{"prompt_tokens":3,"completion_tokens":2}}`)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("第二跳应成功，实际 %d %s", resp.StatusCode, body)
	}
	if c := h.fk.count(); c != 2 {
		t.Fatalf("应为 超时 → 重试成功，实际 %d 次", c)
	}
}

// 流式的判活标准是"有没有出字"，不是"总共跑了多久"。
// 这里让上游出字 20 次共 2 秒，远超单跳超时（500ms）；老实现会在 500ms 处把
// 流截断，现在必须完整透传。
func TestStreamNotKilledByTotalBudget(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) {
		c.UpstreamTimeout = config.Duration(500 * time.Millisecond)
		c.StreamTTFB = config.Duration(2 * time.Second)
		c.StreamIdle = config.Duration(2 * time.Second)
	})
	const frames = 20
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl, ok := w.(http.Flusher)
		if !ok {
			return
		}
		for i := 0; i < frames; i++ {
			_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n")
			fl.Flush()
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
		fl.Flush()
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatStreamBody, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("应 200，实际 %d %s", resp.StatusCode, body)
	}
	if got := strings.Count(body, "data: "); got < frames {
		t.Fatalf("流被截断：只收到 %d 帧，应至少 %d 帧（body=%q）", got, frames, truncateStr(body, 200))
	}
	if !strings.Contains(body, "[DONE]") {
		t.Fatal("缺少 [DONE]：客户端会认为连接异常中断")
	}
	logs := h.log.Recent(1)
	if len(logs) != 1 || logs[0].Error != "" {
		t.Fatalf("正常流不该记错误，实际 %+v", logs)
	}
}

// 上游连响应头都不回：属于渠道病，对外 504 upstream_timeout，
// 且日志要能区分"首字节超时"与"客户端断开"。
func TestStreamHeadTimeoutClassified(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) {
		c.UpstreamTimeout = config.Duration(3 * time.Second)
		c.StreamTTFB = config.Duration(200 * time.Millisecond)
		c.StreamIdle = config.Duration(time.Second)
		c.RetryMax = 1
	})
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		hangUp(r)
	}
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatStreamBody, nil)
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("应 504，实际 %d %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"code":"upstream_timeout"`) {
		t.Fatalf("对外 code 应为 upstream_timeout，实际 %s", body)
	}
	logs := h.log.Recent(5)
	found := false
	for _, e := range logs {
		if e.RuleID == "stream.head_timeout" {
			found = true
		}
		// 绝不能记成客户端断开——那会把渠道故障藏进 note 里。
		if strings.Contains(e.Note, "客户端断开") {
			t.Fatalf("我们自己的首字节超时被记成客户端断开: %+v", e)
		}
	}
	if !found {
		t.Fatalf("日志里应留 stream.head_timeout 规则，实际 %+v", logs)
	}
}

// 出字之后上游卡死：这是截断，必须记 Error（进异常面板）而不是 Note，
// 因为客户端拿到的是没有 [DONE] 的残缺流，且 usage 没取到、这一轮成本只能记 0。
func TestStreamIdleTimeoutRecordsError(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) {
		c.UpstreamTimeout = config.Duration(5 * time.Second)
		c.StreamTTFB = config.Duration(2 * time.Second)
		c.StreamIdle = config.Duration(300 * time.Millisecond)
		c.RetryMax = 1
	})
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		_, _ = io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"半句话\"}}]}\n\n")
		if fl != nil {
			fl.Flush()
		}
		time.Sleep(2 * time.Second) // 远大于 StreamIdle
	}
	start := time.Now()
	resp, body := h.do(t, http.MethodPost, "/v1/chat/completions", chatStreamBody, nil)
	elapsed := time.Since(start)

	if resp.StatusCode != 200 {
		t.Fatalf("已经发过 200 就改不了状态码，实际 %d", resp.StatusCode)
	}
	if !strings.Contains(body, "半句话") {
		t.Fatalf("第一帧应当已经透传，实际 %q", body)
	}
	// 判活阈值 300ms + 250ms 检查粒度，远小于上游卡住的 2s。
	if elapsed > 1500*time.Millisecond {
		t.Fatalf("空闲超时没生效，实际等了 %s", elapsed)
	}
	logs := h.log.Recent(1)
	if len(logs) != 1 {
		t.Fatalf("应有 1 条记录，实际 %d", len(logs))
	}
	e := logs[0]
	if e.Error == "" {
		t.Fatalf("截断必须记 Error（Note 不进异常面板），实际 %+v", e)
	}
	if !strings.Contains(e.Error, "空闲超时") {
		t.Fatalf("应标明是空闲超时，实际 %q", e.Error)
	}
	if strings.Contains(e.Error, "客户端断开") {
		t.Fatalf("不该归因为客户端断开: %q", e.Error)
	}
}

// 客户端自己断开（按 Ctrl-C / 取消请求）仍然只算 Note：它不该被计入故障率。
func TestClientDisconnectStaysNote(t *testing.T) {
	h := newHarness(t, func(c *config.PoolConfig) {
		c.StreamTTFB = config.Duration(2 * time.Second)
		c.StreamIdle = config.Duration(2 * time.Second)
		c.RetryMax = 1
	})
	h.fk.behave = func(w http.ResponseWriter, r *http.Request, secret string, n int, body []byte) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		fl, _ := w.(http.Flusher)
		for i := 0; i < 50; i++ {
			if _, err := io.WriteString(w, "data: {\"choices\":[{\"delta\":{\"content\":\"x\"}}]}\n\n"); err != nil {
				return
			}
			if fl != nil {
				fl.Flush()
			}
			select {
			case <-r.Context().Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.gw.URL+"/v1/chat/completions", strings.NewReader(chatStreamBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+clientKy)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	br := bufio.NewReader(resp.Body)
	if _, err := br.ReadString('\n'); err != nil {
		t.Fatalf("应先收到一帧: %v", err)
	}
	cancel()
	_ = resp.Body.Close()

	// 等 handler 收尾记账。判据只能是"记录已存在"：这里整个请求在 1ms 内
	// 就结束了，LatencyMS 用 Milliseconds() 截断后是 0。老写法要求
	// LatencyMS > 0 才认记录，在 Linux（纳秒时钟、截断成 0）上必然等空
	// 2s 判失败；Windows 时钟精度 15.6ms 反而侥幸通过——所以这个 bug
	// 只在 CI 里现形。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		logs := h.log.Recent(1)
		if len(logs) == 1 {
			e := logs[0]
			if e.Error != "" {
				t.Fatalf("客户端断开不该记 Error，实际 %q", e.Error)
			}
			// 必须是"记了 Note"而不是"什么都没记"：漏记和错记一样是缺陷，
			// 只断言 Error 为空的话，归因整段被删掉也能过。
			if !strings.Contains(e.Note, "客户端断开") {
				t.Fatalf("客户端断开应记 Note，实际 %q", e.Note)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("等不到请求记录")
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
