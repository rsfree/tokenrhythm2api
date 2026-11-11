// Package session 提供会话指纹与粘性表。
//
// 为什么值得做：上游的 prompt cache 按账号隔离，缓存命中价通常只有输入价的
// 1/3 ~ 1/5（如 glm-5.1 ¥8/M → ¥2/M）。同一段对话一旦被打散到不同号，
// 这段差价就永远吃不到。指纹只留在内存里，不落 prompt 内容。
package session

import (
	"encoding/json"
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

// Fingerprint 从请求体算出会话指纹；0 表示"算不出会话"，按无粘性处理。
//
// 优先级：
//  1. 显式会话标识 —— OpenAI 的 user、Anthropic 的 metadata.user_id
//  2. system/developer 消息 + 首条 user 消息的前缀（prefixAffinity）
//
// 只看内容不看模型：同一段对话换模型仍应落回同一个号（缓存通常按账号共享）。
func Fingerprint(body []byte, prefixAffinity bool) uint64 {
	if len(body) == 0 {
		return 0
	}
	var req struct {
		User         string `json:"user"`
		Conversation string `json:"conversation_id"`
		Metadata     struct {
			UserID string `json:"user_id"`
		} `json:"metadata"`
		Messages []struct {
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return 0
	}

	seed := strings.TrimSpace(req.User)
	if seed == "" {
		seed = strings.TrimSpace(req.Conversation)
	}
	if seed == "" {
		seed = strings.TrimSpace(req.Metadata.UserID)
	}

	if seed == "" && prefixAffinity {
		var b strings.Builder
		// system 先来：同一人格/角色设定往往意味着同一批连续对话
		for _, m := range req.Messages {
			if m.Role == "system" || m.Role == "developer" {
				b.Write(m.Content)
				b.WriteByte('|')
			}
		}
		// 首条 user 决定"这段对话是什么"，是区分不同会话的关键
		for _, m := range req.Messages {
			if m.Role == "user" {
				b.Write(m.Content)
				break
			}
		}
		seed = b.String()
	}

	if seed == "" {
		return 0
	}
	const maxSeed = 4096 // 超长 system prompt 只取前缀，避免每请求哈希几百 KB
	if len(seed) > maxSeed {
		seed = seed[:maxSeed]
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(seed))
	sum := h.Sum64()
	if sum == 0 { // 0 是"无会话"的哨兵值
		return 1
	}
	return sum
}

type entry struct {
	KeyID string
	At    time.Time
}

// Table 是会话指纹 → 号的粘性表，TTL 滑动续期，容量上限内做 LRU 淘汰。
type Table struct {
	mu   sync.Mutex
	m    map[uint64]entry
	ttl  time.Duration
	max  int
	hits int64
	miss int64
}

func New(ttl time.Duration, max int) *Table {
	if max <= 0 {
		max = 2000
	}
	return &Table{m: make(map[uint64]entry), ttl: ttl, max: max}
}

// Lookup 命中则续期并返回号 id。过期的条目顺手删掉。
func (t *Table) Lookup(key uint64) (string, bool) {
	if t == nil || key == 0 {
		return "", false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.m[key]
	if !ok {
		t.miss++
		return "", false
	}
	now := time.Now()
	if t.ttl > 0 && now.Sub(e.At) > t.ttl {
		delete(t.m, key)
		t.miss++
		return "", false
	}
	e.At = now
	t.m[key] = e
	t.hits++
	return e.KeyID, true
}

// Bind 记录"这段会话落在哪个号上"。超出容量时淘汰最久未用的。
func (t *Table) Bind(key uint64, keyID string) {
	if t == nil || key == 0 || keyID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.m[key]; !exists && len(t.m) >= t.max {
		var oldestKey uint64
		var oldestAt time.Time
		for k, v := range t.m {
			if oldestAt.IsZero() || v.At.Before(oldestAt) {
				oldestKey, oldestAt = k, v.At
			}
		}
		delete(t.m, oldestKey)
	}
	t.m[key] = entry{KeyID: keyID, At: time.Now()}
}

// Drop 解除一段会话的绑定（命中的号失败时调用，下次重新粘）。
func (t *Table) Drop(key uint64) {
	if t == nil || key == 0 {
		return
	}
	t.mu.Lock()
	delete(t.m, key)
	t.mu.Unlock()
}

// Expire 清掉过期条目；后台 tick 调用。
func (t *Table) Expire(now time.Time) {
	if t == nil || t.ttl <= 0 {
		return
	}
	t.mu.Lock()
	for k, e := range t.m {
		if now.Sub(e.At) > t.ttl {
			delete(t.m, k)
		}
	}
	t.mu.Unlock()
}

// Reset 重启恢复时清空：会话已经断了，硬留着只会把新对话指到不该指的号上。
func (t *Table) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.m = make(map[uint64]entry)
	t.mu.Unlock()
}

func (t *Table) Len() int {
	if t == nil {
		return 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.m)
}

// ByKey 返回每个号上粘着的会话数，面板展示用。
func (t *Table) ByKey() map[string]int {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make(map[string]int, len(t.m))
	for _, e := range t.m {
		out[e.KeyID]++
	}
	return out
}

// Stats 是命中率，容量页展示"粘性到底省没省钱"。
func (t *Table) Stats() (hits, misses int64) {
	if t == nil {
		return 0, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.hits, t.miss
}
