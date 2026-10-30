package proxy

import (
	"encoding/json"
	"hash/fnv"
	"strings"
)

// sessionKey 从请求体里算出会话指纹，用于把同一段对话粘在同一个号上。
//
// 为什么值得做：多数模型服务商的 prompt cache 按账号隔离，缓存命中价通常只有
// 输入价的 1/3 ~ 1/5（如 glm-5.1 是 ¥8 → ¥2）。会话一散，这段钱就永远吃不到。
//
// 指纹优先级：
//  1. 显式会话标识 —— OpenAI 的 user、Anthropic 的 metadata.user_id
//  2. system/developer 消息 + 首条 user 消息的前缀（prefixAffinity）
//
// 返回 0 表示"算不出会话"，调用方按无粘性处理。
func sessionKey(body []byte, prefixAffinity bool) uint64 {
	if len(body) == 0 {
		return 0
	}
	var req struct {
		User     string `json:"user"`
		Metadata struct {
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
	// 超长 system prompt 只取前缀，避免每请求哈希几百 KB
	const maxSeed = 4096
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
