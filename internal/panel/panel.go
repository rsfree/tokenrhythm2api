// Package panel 提供号池的管理接口与内嵌前端。
// 鉴权：Bearer <admin_key>，也接受 ?token=<admin_key>（方便浏览器直开）。
package panel

import (
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"trpool/internal/config"
	"trpool/internal/ledger"
	"trpool/internal/pool"
	"trpool/internal/upstream"
)

//go:embed web/*
var webFS embed.FS

type Deps struct {
	Cfg     *config.Config
	Pool    *pool.Pool
	Prices  *ledger.Prices
	Log     *ledger.Log
	Daily   *ledger.Daily
	Up      *upstream.Client
	Stats   func() map[string]int64
	Uptime  func() time.Duration
	Persist func() // 把当前配置/状态写盘
}

type Panel struct {
	d   Deps
	mux *http.ServeMux
}

func New(d Deps) *Panel {
	p := &Panel{d: d, mux: http.NewServeMux()}
	sub, _ := fs.Sub(webFS, "web")
	p.mux.Handle("/panel/", http.StripPrefix("/panel", http.FileServer(http.FS(sub))))
	p.mux.HandleFunc("/panel/api/state", p.guard(p.handleState))
	p.mux.HandleFunc("/panel/api/keys", p.guard(p.handleKeys))
	p.mux.HandleFunc("/panel/api/keys/", p.guard(p.handleKeyItem))
	p.mux.HandleFunc("/panel/api/pool", p.guard(p.handlePool))
	p.mux.HandleFunc("/panel/api/keys/probe", p.guard(p.handleProbeAll))
	p.mux.HandleFunc("/panel/api/prices", p.guard(p.handlePrices))
	p.mux.HandleFunc("/panel/api/logs", p.guard(p.handleLogs))
	return p
}

func (p *Panel) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.mux.ServeHTTP(w, r) }

func (p *Panel) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := ""
		if v := r.Header.Get("Authorization"); strings.HasPrefix(strings.ToLower(v), "bearer ") {
			tok = strings.TrimSpace(v[7:])
		}
		if tok == "" {
			tok = r.URL.Query().Get("token")
		}
		if tok != p.d.Cfg.AdminKey {
			writeErr(w, http.StatusUnauthorized, "管理令牌无效")
			return
		}
		next(w, r)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]any{"ok": false, "error": msg})
}

func (p *Panel) handleState(w http.ResponseWriter, r *http.Request) {
	views := p.d.Pool.Views()
	var (
		totalReq    int64
		totalErr    int64
		totalSpent  float64
		totalBudget float64
		healthy     int
	)
	for _, v := range views {
		totalReq += v.Requests
		totalErr += v.Errors
		totalSpent += v.Spent
		totalBudget += v.Budget
		if v.Enabled && (v.State == "active" || v.State == "cooling") {
			healthy++
		}
	}
	cfg := p.d.Pool.Config()
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":             true,
		"upstream":       p.d.Up.Base(),
		"uptime_seconds": int64(p.d.Uptime().Seconds()),
		"stats":          p.d.Stats(),
		"keys":           views,
		"pool":           cfg,
		"client_keys":    p.d.Cfg.ClientKeys,
		"prices": map[string]any{
			"count":      p.d.Prices.Count(),
			"updated_at": p.d.Prices.UpdatedAt().Format(time.RFC3339),
		},
		"totals": map[string]any{
			"requests": totalReq, "errors": totalErr,
			"spent": totalSpent, "budget": totalBudget, "healthy": healthy,
		},
		"days": p.d.Daily.Last(14),
		"logs": p.d.Log.Recent(80),
	})
}

func (p *Panel) handleLogs(w http.ResponseWriter, r *http.Request) {
	n, _ := strconv.Atoi(r.URL.Query().Get("n"))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "logs": p.d.Log.Recent(n)})
}

type keyReq struct {
	Name    string   `json:"name"`
	Secret  string   `json:"secret"`
	Weight  *float64 `json:"weight"`
	Budget  *float64 `json:"budget"`
	Enabled *bool    `json:"enabled"`
}

func (p *Panel) handleKeys(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "keys": p.d.Pool.Views()})
	case http.MethodPost:
		var req keyReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
			return
		}
		w1, b1 := 1.0, 0.0
		if req.Weight != nil {
			w1 = *req.Weight
		}
		if req.Budget != nil {
			b1 = *req.Budget
		}
		k, err := p.d.Pool.Add(req.Name, req.Secret, w1, b1)
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if p.d.Persist != nil {
			p.d.Persist()
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": k.ID})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
	}
}

// handleKeyItem 路由 /panel/api/keys/{id}[/action]
func (p *Panel) handleKeyItem(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/panel/api/keys/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusBadRequest, "缺少号 id")
		return
	}
	id := parts[0]
	action := ""
	if len(parts) > 1 {
		action = parts[1]
	}

	switch {
	case action == "reset" && r.Method == http.MethodPost:
		if !p.d.Pool.Reset(id) {
			writeErr(w, http.StatusNotFound, "号不存在")
			return
		}
		if p.d.Persist != nil {
			p.d.Persist()
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case action == "probe" && r.Method == http.MethodPost:
		secret, ok := p.d.Pool.SecretOf(id)
		if !ok {
			writeErr(w, http.StatusNotFound, "号不存在")
			return
		}
		start := time.Now()
		_, err := p.d.Up.FetchModels(r.Context(), secret)
		ms := time.Since(start).Milliseconds()
		if err != nil {
			writeJSON(w, http.StatusOK, map[string]any{"ok": false, "latency_ms": ms, "error": err.Error()})
			return
		}
		p.d.Pool.Reset(id)
		if p.d.Persist != nil {
			p.d.Persist()
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "latency_ms": ms})

	case r.Method == http.MethodPatch || r.Method == http.MethodPut:
		var req keyReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
			return
		}
		var namep, secretp *string
		if req.Name != "" {
			namep = &req.Name
		}
		if req.Secret != "" {
			secretp = &req.Secret
		}
		if err := p.d.Pool.Update(id, namep, secretp, req.Weight, req.Budget, req.Enabled); err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		if p.d.Persist != nil {
			p.d.Persist()
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case r.Method == http.MethodDelete:
		if !p.d.Pool.Remove(id) {
			writeErr(w, http.StatusNotFound, "号不存在")
			return
		}
		if p.d.Persist != nil {
			p.d.Persist()
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	default:
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
	}
}

// handlePool 热更新调度参数，并写回配置文件。
func (p *Panel) handlePool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	var in config.PoolConfig
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	switch in.Strategy {
	case "weighted_random", "round_robin", "sequential":
	default:
		writeErr(w, http.StatusBadRequest, "strategy 只能是 weighted_random / round_robin / sequential")
		return
	}
	if in.TopK <= 0 {
		in.TopK = 5
	}
	if in.RetryMax <= 0 {
		in.RetryMax = 1
	}
	if in.UpstreamTimeout.Duration() <= 0 {
		in.UpstreamTimeout = p.d.Cfg.Pool.UpstreamTimeout
	}
	if in.VideoTimeout.Duration() <= 0 {
		in.VideoTimeout = p.d.Cfg.Pool.VideoTimeout
	}
	if in.StickyTTL.Duration() <= 0 {
		in.StickyTTL = p.d.Cfg.Pool.StickyTTL
	}
	p.d.Pool.SetConfig(in)
	p.d.Cfg.Pool = in
	if p.d.Persist != nil {
		p.d.Persist()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pool": in})
}

func (p *Panel) handlePrices(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	secret := ""
	if v := p.d.Pool.Views(); len(v) > 0 {
		for _, k := range v {
			if k.Enabled && k.State == "active" {
				if s, ok := p.d.Pool.SecretOf(k.ID); ok {
					secret = s
					break
				}
			}
		}
	}
	if secret == "" {
		writeErr(w, http.StatusBadRequest, "池内没有可用号，无法拉取价目表")
		return
	}
	body, err := p.d.Up.FetchModels(r.Context(), secret)
	if err != nil {
		writeErr(w, http.StatusBadGateway, err.Error())
		return
	}
	if err := p.d.Prices.Load(body); err != nil {
		writeErr(w, http.StatusBadGateway, "解析价目表失败: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": p.d.Prices.Count()})
}

func (p *Panel) handleProbeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	type res struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		OK      bool   `json:"ok"`
		Latency int64  `json:"latency_ms"`
		Error   string `json:"error,omitempty"`
	}
	var out []res
	for _, k := range p.d.Pool.Views() {
		secret, ok := p.d.Pool.SecretOf(k.ID)
		if !ok {
			continue
		}
		start := time.Now()
		_, err := p.d.Up.FetchModels(r.Context(), secret)
		item := res{ID: k.ID, Name: k.Name, OK: err == nil, Latency: time.Since(start).Milliseconds()}
		if err != nil {
			item.Error = err.Error()
		} else {
			p.d.Pool.Reset(k.ID)
		}
		out = append(out, item)
	}
	if p.d.Persist != nil {
		p.d.Persist()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "results": out})
}
