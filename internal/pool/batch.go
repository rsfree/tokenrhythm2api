package pool

import (
	"sort"
	"time"
)

// batchTarget 算出"现在该同时用几个号"。调用方必须已持有 p.mu。
func (p *Pool) batchTarget() (int, bool) {
	mode := p.cfg.Batch.Mode
	if mode == "" || mode == "off" || len(p.keys) == 0 {
		return 0, false
	}
	n := len(p.keys)
	minS, maxS := p.cfg.Batch.Min, p.cfg.Batch.Max
	if minS <= 0 {
		minS = 1
	}
	if minS > n {
		minS = n
	}
	if maxS <= 0 || maxS > n {
		maxS = n
	}
	if maxS < minS {
		maxS = minS
	}

	switch mode {
	case "fixed":
		return clampInt(p.cfg.Batch.Size, minS, maxS), true
	default: // auto
		per := p.cfg.Batch.TargetInflightPerKey
		if per <= 0 {
			per = 4
		}
		need := (p.totalInflight + per - 1) / per
		return clampInt(need, minS, maxS), true
	}
}

// syncBatchLocked 把 batch 向目标大小收敛。
// 扩容：按权重从高到低补；缩容：只淘汰空闲号，正在跑请求的等它跑完再说。
func (p *Pool) syncBatchLocked(target int, now time.Time) {
	alive := make([]string, 0, len(p.batch))
	for _, id := range p.batch {
		if k := p.keys[id]; k != nil && k.usable(now) {
			alive = append(alive, id)
		}
	}
	p.batch = alive

	if len(p.batch) < target {
		in := make(map[string]bool, len(p.batch))
		for _, id := range p.batch {
			in[id] = true
		}
		type cand struct {
			id string
			w  float64
		}
		cs := make([]cand, 0, len(p.order))
		for _, id := range p.order {
			k := p.keys[id]
			if k == nil || in[id] || !k.usable(now) {
				continue
			}
			cs = append(cs, cand{id, p.weightOf(k, now)})
		}
		sort.Slice(cs, func(i, j int) bool { return cs[i].w > cs[j].w })
		for _, c := range cs {
			if len(p.batch) >= target {
				break
			}
			p.batch = append(p.batch, c.id)
		}
		return
	}

	excess := len(p.batch) - target
	if excess <= 0 {
		return
	}
	keep := make([]string, 0, len(p.batch))
	for i := len(p.batch) - 1; i >= 0; i-- {
		id := p.batch[i]
		if excess > 0 {
			if k := p.keys[id]; k != nil && k.inflight == 0 {
				excess--
				continue
			}
		}
		keep = append(keep, id)
	}
	for i, j := 0, len(keep)-1; i < j; i, j = i+1, j-1 {
		keep[i], keep[j] = keep[j], keep[i]
	}
	p.batch = keep
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
