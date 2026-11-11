package server

import (
	"context"
	"math/rand/v2"
	"time"
)

// 轮转退避：换号前短暂等待，避免多个请求同相位重试把上游打成一片 429/5xx。
const (
	rotateBackoffBase = 300 * time.Millisecond
	rotateBackoffCap  = 3 * time.Second
	jitterFraction    = 0.25
)

// backoffFor 返回第 n 次轮转（0 基）前应等待的时长：base·2^n 封顶，再 ±25% 抖动。
func backoffFor(n int) time.Duration {
	d := rotateBackoffBase
	for k := 0; k < n && d < rotateBackoffCap; k++ {
		d *= 2
	}
	if d > rotateBackoffCap {
		d = rotateBackoffCap
	}
	return jitterDur(d)
}

// rateBackoff 是模型侧限流的退避：听上游提示，但最多 5s，
// 免得把同步请求拖到客户端自己超时。
func rateBackoff(attempt int, hint time.Duration) time.Duration {
	if hint > 0 {
		if hint > 5*time.Second {
			return 5 * time.Second
		}
		return jitterDur(hint)
	}
	d := time.Duration(attempt) * 700 * time.Millisecond
	if d > 3*time.Second {
		d = 3 * time.Second
	}
	return jitterDur(d)
}

func jitterDur(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	f := 1 + (rand.Float64()*2-1)*jitterFraction
	out := time.Duration(float64(d) * f)
	if out < 0 {
		return 0
	}
	return out
}

// sleepCtx 是可取消的等待：客户端断连/停机时立即返回 false。
func sleepCtx(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
