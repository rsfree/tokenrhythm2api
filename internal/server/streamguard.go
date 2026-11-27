package server

import (
	"context"
	"sync"
	"time"
)

// streamGuardTick 是看门狗检查频率。250ms 对 60s/120s 这类预算足够，
// 也不会在长回答里制造可观测的 CPU 噪声。
const streamGuardTick = 250 * time.Millisecond

// streamGuard 给流式请求做两段判活，取代"整个请求一个总超时"。
//
// 为什么不能给流式设总超时：首字节一到，上游就在正常生成；此时按总时长掐断，
// 得到的是"状态码 200 已经发出去 + 输出被截断 + usage 永远拿不到"。实测一次
// 300s 截断，上游生成了近 300 秒的 token 并扣了费，我们记 0 —— 每发生一次
// 就是一次净亏损，客户端还拿到一个没有 [DONE] 的残缺流。
//
// 所以流式按"有没有进展"判活，而不是按总时长：
//   - head 阶段：等响应头的上限（上游排队、还没开始生成）
//   - body 阶段：每读到一帧就重置，超过 idle 没有任何数据才认为上游卡死
type streamGuard struct {
	cancel context.CancelFunc
	head   time.Duration
	idle   time.Duration

	mu       sync.Mutex
	deadline time.Time
	inBody   bool
	fired    bool

	done     chan struct{}
	stopOnce sync.Once
}

func newStreamGuard(cancel context.CancelFunc, head, idle time.Duration) *streamGuard {
	g := &streamGuard{
		cancel:   cancel,
		head:     head,
		idle:     idle,
		deadline: time.Now().Add(head),
		done:     make(chan struct{}),
	}
	go g.watch()
	return g
}

func (g *streamGuard) watch() {
	t := time.NewTicker(streamGuardTick)
	defer t.Stop()
	for {
		select {
		case <-g.done:
			return
		case <-t.C:
			g.mu.Lock()
			expired := time.Now().After(g.deadline)
			if expired {
				g.fired = true
			}
			g.mu.Unlock()
			if expired {
				// 用 cancel 而不是直接关连接：让 Do / ReadBytes 走正常的错误路径，
				// 上层才能记日志、还租约。
				g.cancel()
				return
			}
		}
	}
}

// Head 在拿到响应头时调用：从"等首字节"切换到"逐帧空闲"。
func (g *streamGuard) Head() {
	g.mu.Lock()
	g.inBody = true
	g.deadline = time.Now().Add(g.idle)
	g.mu.Unlock()
}

// Touch 每读到一帧重置空闲计时。没进 body 阶段时不动——head 阶段的预算是
// 一次性给满的，不该被"上游发了点无关字节"续命。
func (g *streamGuard) Touch() {
	g.mu.Lock()
	if g.inBody {
		g.deadline = time.Now().Add(g.idle)
	}
	g.mu.Unlock()
}

func (g *streamGuard) Stop() {
	g.stopOnce.Do(func() { close(g.done) })
}

// Fired 报告这次中断是不是**我们**掐断的。
//
// 归因要命：同样是 context.Canceled 形态的错误，"我们超时"和"客户端断开"
// 是两种完全不同的故障。老实现一律记成"客户端断开"，结果这类漏记账的
// 截断在面板上一次都没出现过。
func (g *streamGuard) Fired() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.fired
}

// TimeoutWhy 给日志用：区分"上游没回响应头"与"出字后卡住"。
func (g *streamGuard) TimeoutWhy() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.inBody {
		return "空闲超时(" + g.idle.String() + " 无数据)"
	}
	return "首字节超时(" + g.head.String() + ")"
}
