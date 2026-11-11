// Package livecfg 运行期可变配置的并发安全持有者。
//
// 启动时读入的配置是普通字段（读多写零），但面板允许在线改少数几项，
// 于是这些字段需要有并发安全的读写点。这里用不可变快照 + atomic 指针：
// 读方 Load 拿到一致视图，写方 Store 整体替换，无锁无数据竞争。
package livecfg

import (
	"strings"
	"sync/atomic"
)

// Snapshot 是一次读取的不可变鉴权/日志视图。
type Snapshot struct {
	AdminKey         string
	ClientKeys       []string
	AllowPassthrough bool
	LogBodies        bool
}

// Holder 原子持有当前快照。
type Holder struct {
	p atomic.Pointer[Snapshot]
}

func New(s Snapshot) *Holder {
	h := &Holder{}
	h.Store(s)
	return h
}

// Load 返回当前快照；Holder 为空时返回零值（调用方无需判空）。
func (h *Holder) Load() Snapshot {
	if h == nil {
		return Snapshot{}
	}
	if s := h.p.Load(); s != nil {
		return *s
	}
	return Snapshot{}
}

// Store 整体替换快照（深拷贝切片，避免调用方后续修改原切片）。
func (h *Holder) Store(s Snapshot) {
	s.ClientKeys = append([]string(nil), s.ClientKeys...)
	for i := range s.ClientKeys {
		s.ClientKeys[i] = strings.TrimSpace(s.ClientKeys[i])
	}
	h.p.Store(&s)
}
