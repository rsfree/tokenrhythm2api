// Package httpauth 网关与面板共用的 Bearer 鉴权原语。
//
// 单独成包的原因：/v1/*（server）与 /panel/api/*（panel）两处鉴权必须完全同口径。
// 比较用 SHA-256 摘要 + subtle.ConstantTimeCompare：定长比较，不会因为前缀长度
// 不同提前返回；即使有侧信道也拿不到密钥原文。
package httpauth

import (
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

const bearerPrefix = "Bearer "

// VerifyBearer 校验请求是否携带正确密钥。key 为空表示"不鉴权"，恒放行。
//
// 两处取密钥：`Authorization: Bearer <key>`，或 Anthropic 客户端惯用的
// `x-api-key: <key>`。后者不是可选兼容项——Claude Code 默认只发 x-api-key。
func VerifyBearer(r *http.Request, key string) bool {
	if key == "" {
		return true
	}
	tok, ok := Credential(r)
	if !ok {
		subtle.ConstantTimeCompare(digest(""), digest(key))
		return false
	}
	return VerifyToken(tok, key)
}

// VerifyToken 常量时间比较一个明文 token 与期望密钥（面板 ?token= 用）。
func VerifyToken(token, key string) bool {
	if key == "" {
		return true
	}
	return subtle.ConstantTimeCompare(digest(token), digest(key)) == 1
}

// Credential 按优先级取请求携带的密钥：Bearer 优先，其次 x-api-key。
// Authorization 存在但方案不是 Bearer 时不回落到 x-api-key——两个头矛盾时以显式
// Authorization 为准，避免"上层代理塞了个 Basic 头、下层用 x-api-key 绕过"。
func Credential(r *http.Request) (string, bool) {
	authz := r.Header.Get("Authorization")
	if len(authz) >= len(bearerPrefix) && strings.EqualFold(authz[:len(bearerPrefix)], bearerPrefix) {
		return strings.TrimSpace(authz[len(bearerPrefix):]), true
	}
	if authz != "" {
		return "", false
	}
	if v := strings.TrimSpace(r.Header.Get("x-api-key")); v != "" {
		return v, true
	}
	return "", false
}

// MatchAny 常量时间比较 token 是否等于列表中的任意一个。
// 列表为空返回 false（调用方自行决定是否放行）。
func MatchAny(token string, list []string) bool {
	if token == "" {
		return false
	}
	ok := 0
	for _, want := range list {
		// 不提前 break：遍历完整个列表，耗时只与列表长度有关。
		ok |= subtle.ConstantTimeCompare(digest(token), digest(want))
	}
	return ok == 1
}

func digest(s string) []byte {
	sum := sha256.Sum256([]byte(s))
	return sum[:]
}
