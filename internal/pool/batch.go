package pool

import (
	"sort"
	"time"
)

// batchBase 算出"基准窗口"大小（不含弹性余量）。调用方必须已持有 p.mu。
func (p *Pool) batchBase() (int, bool) {
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

// batchTarget 是"现在该同时用几个号"：基准 + 弹性余量，夹在弹性上限内。
func (p *Pool) batchTarget() (int, bool) {
	base, ok := p.batchBase()
	if !ok {
		return 0, false
	}
	if p.elastic.extra <= 0 {
		return base, true
	}
	if c := p.elasticCapLocked(); base+p.elastic.extra > c {
		return c, true
	}
	return base + p.elastic.extra, true
}

// refreshBatchLocked 让窗口立刻收敛到目标。
//
// 为什么要在每次请求收尾时都调一次：某个号刚 401/402 变不可用时，
// 窗口会瞬间从 12 掉到 11——要是等下一笔请求进来才补，这中间的
// 并发额度就白白少了一份，面板上也会看到"目标 12 / 实际 11"来回跳。
func (p *Pool) refreshBatchLocked(now time.Time) bool {
	t, ok := p.batchTarget()
	if !ok {
		return false
	}
	return p.syncBatchLocked(t, now)
}

// syncBatchLocked 把 batch 向目标大小收敛，返回成员是否变化。
//
// 扩容：按权重从高到低补，跳过冷却/失效/欠费的号。
// 缩容：只淘汰空闲号，正在跑请求的等它跑完；淘汰顺序是最弱的先走。
func (p *Pool) syncBatchLocked(target int, now time.Time) bool {
	floor := p.cfg.CreditFloor
	before := len(p.batch)

	alive := make([]string, 0, len(p.batch))
	seen := make(map[string]bool, len(p.batch))
	for _, id := range p.batch {
		k := p.keys[id]
		if k == nil || seen[id] || !k.usableForBatch(now, floor) {
			continue
		}
		seen[id] = true
		alive = append(alive, id)
	}
	p.batch = alive
	changed := before != len(p.batch)

	if len(p.batch) < target {
		type cand struct {
			id string
			w  float64
		}
		cs := make([]cand, 0, len(p.order))
		for _, id := range p.order {
			k := p.keys[id]
			if k == nil || seen[id] || !k.usableForBatch(now, floor) {
				continue
			}
			cs = append(cs, cand{id, p.weightOf(k, now)})
		}
		sort.SliceStable(cs, func(i, j int) bool { return cs[i].w > cs[j].w })
		for _, c := range cs {
			if len(p.batch) >= target {
				break
			}
			p.batch = append(p.batch, c.id)
			changed = true
		}
		return changed
	}

	excess := len(p.batch) - target
	if excess <= 0 {
		return changed
	}

	// 先挑"能走"的：空闲 + 权重最低。权重里含闲置增益与在途惩罚，
	// 所以蒸发掉的通常是久未使用、余额最少的那些。
	type victim struct {
		idx int
		w   float64
	}
	vs := make([]victim, 0, len(p.batch))
	for i, id := range p.batch {
		k := p.keys[id]
		if k == nil || k.inflight == 0 {
			w := 0.0
			if k != nil {
				w = p.weightOf(k, now)
			}
			vs = append(vs, victim{i, w})
		}
	}
	sort.SliceStable(vs, func(i, j int) bool { return vs[i].w < vs[j].w })
	drop := make(map[int]bool, excess)
	for _, v := range vs {
		if len(drop) >= excess {
			break
		}
		drop[v.idx] = true
	}
	if len(drop) == 0 {
		// 全都在跑请求：这一轮一个也裁不掉，等它们跑完再说。
		return changed
	}
	keep := make([]string, 0, len(p.batch)-len(drop))
	for i, id := range p.batch {
		if drop[i] {
			continue
		}
		keep = append(keep, id)
	}
	p.batch = keep
	return true
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
