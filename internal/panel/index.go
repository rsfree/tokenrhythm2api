package panel

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"net/http"
	"strings"
)

// 资源经 go:embed 打进二进制：随服务部署，无外部构建步骤、无 CDN 依赖。
//
//go:embed web/index.html web/app.js web/style.css
var webFS embed.FS

// setSecurityHeaders 只留"不透露实现"的几条硬性防护。
//
// 去掉了 Content-Security-Policy：它会把面板的脚本/连接源清单写进响应头，
// 属于对外暴露自身实现细节（上游 API 的 CSP 更夸张，直接列了它的域名与 OSS）。
// 面板的抗 XSS 现在靠两点：模板里所有上游文本都过 esc()，以及 admin 令牌鉴权。
// nosniff / X-Frame-Options 这类不泄露信息的头保留——它们只约束浏览器行为。
func setSecurityHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("Cross-Origin-Opener-Policy", "same-origin")
	w.Header().Set("Cross-Origin-Resource-Policy", "same-origin")
}

func (p *Panel) handleIndex(w http.ResponseWriter, r *http.Request) {
	setSecurityHeaders(w)
	rel := strings.TrimPrefix(r.URL.Path, "/panel")
	switch rel {
	case "", "/":
		serveAsset(w, r, "web/index.html", "text/html; charset=utf-8")
	case "/app.js":
		serveAsset(w, r, "web/app.js", "application/javascript; charset=utf-8")
	case "/style.css":
		serveAsset(w, r, "web/style.css", "text/css; charset=utf-8")
	default:
		http.NotFound(w, r)
	}
}

// serveAsset 带 ETag + Cache-Control: no-cache。
//
// 资源名是固定的（app.js 没有内容哈希后缀），若一个缓存头都不发，浏览器会按
// 启发式规则长期缓存，升级后仍跑旧 app.js（新后端 + 旧前端）。no-cache 的语义是
// "可缓存，但每次使用前必须校验"，配合 ETag 命中即 304，代价只是一次条件请求。
func serveAsset(w http.ResponseWriter, r *http.Request, name, contentType string) {
	body, err := webFS.ReadFile(name)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	sum := sha256.Sum256(body)
	etag := `"` + hex.EncodeToString(sum[:8]) + `"`
	h := w.Header()
	h.Set("Content-Type", contentType)
	h.Set("Cache-Control", "no-cache")
	h.Set("ETag", etag)
	if inm := r.Header.Get("If-None-Match"); inm != "" && strings.Contains(inm, etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
