package pool

import (
	"time"

	"trpool/internal/config"
)

// sampleLimit 是窗口内保留的最大样本数。
// 打满即丢弃最旧的一半：高并发下窗口内可能真有上万个样本，
// 每次只挪一格是 O(n²)，一次挪一半是摊还 O(1)。
const sampleLimit = 8192

// sampleWindow 是滑动窗口内的请求时间戳。只存时间戳——
// 要算的是"窗口内 429 占比"，不需要请求内容。
type sampleWindow struct {
	marks []time.Time
}

func (w *sampleWindow) add(t time.Time) {
	if len(w.marks) >= sampleLimit {
		w.marks = append(w.marks[:0], w.marks[len(w.marks)/2:]...)
	}
	w.marks = append(w.marks, t)
}

func (w *sampleWindow) trim(before time.Time) {
	i := 0
	for i < len(w.marks) && w.marks[i].Before(before) {
		i++
	}
	if i > 0 {
		w.marks = w.marks[i:]
	}
}

func (w *sampleWindow) len() int { return len(w.marks) }

// elasticState 是弹性窗口的运行时状态（不落盘：重启后按当时的压力重新学）。
type elasticState struct {
	extra   int       // 相对基准窗口额外开出来的号数
	grownAt time.Time // 上次扩/缩容时间，用于 hold 与"一个窗口只动一次"
	last429 time.Time
	total   sampleWindow // 窗口内全部请求
	limited sampleWindow // 窗口内被 429 的请求
}

// observe 记一次请求结果。rate 表示这次是"按号限流"的 429。
func (p *Pool) observe(now time.Time, rate bool) {
	p.elastic.total.add(now)
	if rate {
		p.elastic.limited.add(now)
		p.elastic.last429 = now
	}
}

// ResetElastic 清空弹性状态（人工重置/热更新关闭弹性时用）。
func (p *Pool) ResetElastic() {
	p.mu.Lock()
	p.elastic = elasticState{}
	p.mu.Unlock()
	p.notify()
}

// elasticCapLocked 是弹性窗口能到的最大值。
func (p *Pool) elasticCapLocked() int {
	n := len(p.keys)
	if n == 0 {
		return 0
	}
	c := p.cfg.Batch.Elastic.MaxSize
	if c <= 0 {
		c = p.cfg.Batch.Max
	}
	if c <= 0 || c > n {
		c = n
	}
	if c < 1 {
		c = 1
	}
	return c
}

// evalElasticLocked 按 429 压力调 extra。返回是否发生变化。
// 调用方必须已持有 p.mu。
// elasticWindow 返回统计窗口长度，零值按 60s 处理。
//
// 不能直接用原值：Window<=0 时 from == now，trim 会把"刚刚记下的样本"也裁掉
// （样本时间戳必然早于调用方此刻的 now），429 占比永远是 0/0。
// Linux 的纳秒时钟一测就露；Windows 上两次 time.Now() 常落在同一刻度，
// 所以这个问题只在 Linux 上显形。配置走 normalize 时本不会为 0，
// 但 New() 被直接调用（测试、内嵌）时没有这一步，得自己兜住。
func elasticWindow(e *config.Elastic) time.Duration {
	if d := e.Window.Duration(); d > 0 {
		return d
	}
	return 60 * time.Second
}

// evalElasticLocked 按 429 压力调 extra。返回是否发生变化。
// 调用方必须已持有 p.mu。
func (p *Pool) evalElasticLocked(now time.Time) bool {
	e := &p.cfg.Batch.Elastic
	if !e.Enabled {
		if p.elastic.extra != 0 {
			p.elastic.extra = 0
			p.elastic.grownAt = time.Time{}
			return true
		}
		return false
	}

	base, ok := p.batchBase()
	if !ok {
		if p.elastic.extra != 0 {
			p.elastic.extra = 0
			return true
		}
		return false
	}

	win := elasticWindow(e)
	from := now.Add(-win)
	p.elastic.total.trim(from)
	p.elastic.limited.trim(from)

	maxExtra := p.elasticCapLocked() - base
	if maxExtra < 0 {
		maxExtra = 0
	}
	if p.elastic.extra > maxExtra {
		// 池子被删到比窗口还小时，额度要跟着降下来。
		p.elastic.extra = maxExtra
		return true
	}

	total, hits := p.elastic.total.len(), p.elastic.limited.len()
	if total == 0 {
		hits = 0
	}
	ratio := 0.0
	if total > 0 {
		ratio = float64(hits) / float64(total)
	}

	// 扩容：占比过线且样本可信；一个统计窗口最多扩一次，
	// 否则一波抖动（每个请求都撞 429）会连着几轮把池子拉到上限。
	enough := total >= e.MinSamples || hits >= e.MinHits
	if enough && hits > 0 && ratio >= e.TriggerRate && p.elastic.extra < maxExtra {
		if p.elastic.grownAt.IsZero() || now.Sub(p.elastic.grownAt) >= win {
			p.elastic.extra = minInt(p.elastic.extra+e.Step, maxExtra)
			p.elastic.grownAt = now
			return true
		}
	}

	// 缩容：连续 stable 没再撞 429，且扩容后已过 hold。
	// 一次只退 DecayStep 个，中间只要再出现 429 就立刻停手。
	if p.elastic.extra > 0 {
		calm := p.elastic.last429.IsZero() || now.Sub(p.elastic.last429) >= e.Stable.Duration()
		held := p.elastic.grownAt.IsZero() || now.Sub(p.elastic.grownAt) >= e.Hold.Duration()
		if calm && held {
			p.elastic.extra -= e.DecayStep
			if p.elastic.extra < 0 {
				p.elastic.extra = 0
			}
			p.elastic.grownAt = now
			return true
		}
	}
	return false
}

// elasticView 是给面板看的弹性快照。
type elasticView struct {
	enabled bool
	extra   int
	cap     int
	ratio   float64
	hits    int
	total   int
	window  int
	lastAgo int
}

func (p *Pool) elasticViewLocked(now time.Time) elasticView {
	e := p.cfg.Batch.Elastic
	v := elasticView{enabled: e.Enabled, extra: p.elastic.extra, cap: p.elasticCapLocked()}
	if !e.Enabled {
		return v
	}
	win := elasticWindow(&p.cfg.Batch.Elastic)
	p.elastic.total.trim(now.Add(-win))
	p.elastic.limited.trim(now.Add(-win))
	v.hits = p.elastic.limited.len()
	v.total = p.elastic.total.len()
	v.window = int(win / time.Second)
	if v.total > 0 {
		v.ratio = float64(v.hits) / float64(v.total)
	}
	if !p.elastic.last429.IsZero() {
		v.lastAgo = int(now.Sub(p.elastic.last429) / time.Second)
	}
	return v
}
