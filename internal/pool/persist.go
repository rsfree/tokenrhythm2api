package pool

import "time"

// Snapshot 是落盘用的号池快照。字段名与 v1（旧 trpool）保持兼容，
// 老状态文件可以直接读进来，只是多出来的新字段为零值。
type Snapshot struct {
	Keys   []*Key                 `json:"keys"`
	Order  []string               `json:"order"`
	Sticky map[string]stickyEntry `json:"sticky"`
}

func (p *Pool) Snapshot() Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := Snapshot{Order: append([]string(nil), p.order...), Sticky: map[string]stickyEntry{}}
	for _, k := range p.order {
		key := p.keys[k]
		if key == nil {
			continue
		}
		c := *key
		// 运行态不落盘：inflight / lastPickAt / seq 重启即重建。
		c.inflight = 0
		c.lastPickAt = time.Time{}
		c.seq = 0
		if len(key.ModelBlocks) > 0 {
			c.ModelBlocks = make(map[string]ModelBlock, len(key.ModelBlocks))
			for m, b := range key.ModelBlocks {
				c.ModelBlocks[m] = b
			}
		}
		out.Keys = append(out.Keys, &c)
	}
	for tid, s := range p.sticky {
		out.Sticky[tid] = s
	}
	return out
}

// Restore 用磁盘快照恢复号池。文件损坏/字段缺失时宁可少恢复，也不要带半个池子跑。
func (p *Pool) Restore(s Snapshot) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.keys = map[string]*Key{}
	p.order = nil
	for _, k := range s.Keys {
		if k == nil || k.Secret == "" {
			continue
		}
		if k.ID == "" {
			k.ID = newID()
		}
		if k.Weight <= 0 {
			k.Weight = 1
		}
		if k.CreatedAt.IsZero() {
			k.CreatedAt = time.Now()
		}
		if k.State == "" {
			if k.Enabled {
				k.State = StateActive
			} else {
				k.State = StateDisabled
			}
		}
		// enabled 与 state 双向对齐：旧文件里可能只有一个字段。
		switch k.State {
		case StateDisabled:
			k.Enabled = false
		default:
			if !k.Enabled {
				k.State = StateDisabled
			}
		}
		if k.State == StateCooling && k.CooldownUntil.IsZero() {
			k.State = StateActive
		}
		k.inflight = 0
		k.lastPickAt = time.Time{}
		k.seq = 0
		p.keys[k.ID] = k
		p.order = append(p.order, k.ID)
	}
	if len(p.order) == 0 {
		for id := range p.keys {
			p.order = append(p.order, id)
		}
	}
	if len(s.Sticky) > 0 {
		p.sticky = s.Sticky
	}
	// 会话粘性与 batch 是运行时状态，重启即重建：会话已经断了，
	// 硬留着只会把新对话指到不该指的号上。
	p.sessions.Reset()
	p.batch = nil
	p.totalInflight = 0
	p.seq = 0
	p.rr = 0
}

// Touch 标脏（供不改变状态但需要落盘的场景，如面板改了备注）。
func (p *Pool) Touch() { p.notify() }
