// Package panel 提供号池的管理 API 与内嵌前端。
//
// 鉴权：Authorization: Bearer <admin_key>，也接受 ?token=<admin_key>（浏览器直开）。
// 前端用 go:embed 打进二进制，零外部依赖、零构建步骤。
package panel

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"trpool/internal/capacity"
	"trpool/internal/config"
	"trpool/internal/fault"
	"trpool/internal/httpauth"
	"trpool/internal/livecfg"
	"trpool/internal/pool"
	"trpool/internal/reqlog"
	"trpool/internal/upstream"
	"trpool/internal/usage"
)

type Deps struct {
	Cfg        *config.Config
	Live       *livecfg.Holder
	Pool       *pool.Pool
	Prices     *usage.Prices
	Log        *reqlog.Recorder
	Daily      *usage.Daily
	Up         *upstream.Client
	Version    string
	ConfigPath string
	Stats      func() map[string]int64
	Uptime     func() time.Duration
	Persist    func() // 标脏落盘（状态文件）
}

type Panel struct {
	d   Deps
	mux *http.ServeMux
}

func New(d Deps) *Panel {
	p := &Panel{d: d, mux: http.NewServeMux()}
	p.mux.HandleFunc("/panel/", p.handleIndex)
	p.mux.HandleFunc("/panel/api/state", p.guard(p.handleState))
	p.mux.HandleFunc("/panel/api/logs", p.guard(p.handleLogs))
	p.mux.HandleFunc("/panel/api/capacity", p.guard(p.handleCapacity))
	p.mux.HandleFunc("/panel/api/models", p.guard(p.handleModels))
	p.mux.HandleFunc("/panel/api/keys", p.guard(p.handleKeys))
	// 比 "/panel/api/keys/" 这个子树更具体，ServeMux 会优先匹配到它
	p.mux.HandleFunc("/panel/api/keys/batch", p.guard(p.handleKeysBatch))
	p.mux.HandleFunc("/panel/api/keys/probe", p.guard(p.handleProbeAll))
	p.mux.HandleFunc("/panel/api/keys/revive", p.guard(p.handleReviveStopped))
	p.mux.HandleFunc("/panel/api/keys/", p.guard(p.handleKeyItem))
	p.mux.HandleFunc("/panel/api/pool", p.guard(p.handlePool))
	p.mux.HandleFunc("/panel/api/prices", p.guard(p.handlePrices))
	p.mux.HandleFunc("/panel/api/config", p.guard(p.handleConfig))
	return p
}

func (p *Panel) ServeHTTP(w http.ResponseWriter, r *http.Request) { p.mux.ServeHTTP(w, r) }

func (p *Panel) guard(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := ""
		if v, ok := httpauth.Credential(r); ok {
			tok = v
		}
		if tok == "" {
			tok = r.URL.Query().Get("token")
		}
		if !httpauth.VerifyToken(tok, p.d.Live.Load().AdminKey) {
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

// ---------- 总览 ----------

func (p *Panel) handleState(w http.ResponseWriter, r *http.Request) {
	views := p.d.Pool.Views()
	pcfg := p.d.Pool.Config()
	var totalSpent, totalBudget float64
	healthy := 0
	for _, v := range views {
		totalSpent += v.Spent
		totalBudget += v.Budget
		// 冷却中且没到期的不算"可用"：面板上的可用数要能直接对上"派单能落到几个号"。
		if v.Enabled && (v.State == "active" || (v.State == "cooling" && v.CooldownLeft <= 0)) {
			healthy++
		}
	}
	reqs, errs, _, _, dailyCost := p.d.Daily.Total()
	logs := p.d.Log.Recent(80)
	capSnap := capacity.Compute(views, p.d.Log.Recent(400), pcfg.MaxInFlight, pcfg.KeyQPS, p.d.Uptime())
	hits, misses := p.d.Pool.Sessions().Stats()

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":             true,
		"version":        p.d.Version,
		"upstream":       p.d.Up.Base(),
		"listen":         p.d.Cfg.Listen,
		"data_dir":       p.d.Cfg.DataDir,
		"auto_probe":     p.d.Cfg.AutoProbe,
		"logging":        p.d.Cfg.Logging,
		"archive":        p.d.Log.ArchiveMeta(),
		"uptime_seconds": int64(p.d.Uptime().Seconds()),
		"stats":          p.d.Stats(),
		"keys":           views,
		"pool":           pcfg,
		"batch":          p.d.Pool.BatchState(),
		"client_keys":    p.d.Live.Load().ClientKeys,
		"prices":         priceBlock(p.d.Prices),
		"totals": map[string]any{
			"requests": reqs, "errors": errs, "spent": totalSpent, "budget": totalBudget,
			"healthy": healthy, "cost": dailyCost,
		},
		"days":         p.d.Daily.Last(14),
		"logs":         logs,
		"capacity":     capSnap,
		"model_health": p.d.Pool.ModelHealth(),
		"sessions": map[string]any{
			"active": p.d.Pool.Sessions().Len(), "hits": hits, "misses": misses,
		},
	})
}

func priceBlock(prices *usage.Prices) map[string]any {
	meta := prices.Meta()
	meta["list"] = prices.Snapshot()
	return meta
}

// ---------- 日志 ----------

func (p *Panel) handleLogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	n, _ := strconv.Atoi(q.Get("n"))
	if n <= 0 {
		n = 400
	}
	if q.Get("history") != "1" {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "source": "ring", "logs": p.d.Log.Recent(n)})
		return
	}
	f := reqlog.Filter{
		Status: q.Get("status"),
		KeyID:  q.Get("key"),
		Model:  q.Get("model"),
		Text:   q.Get("text"),
		Limit:  n,
	}
	if v := q.Get("since"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.Since = t
		}
	}
	if v := q.Get("until"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			f.Until = t
		}
	}
	logs, err := p.d.Log.Query(f)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "source": "archive", "logs": logs})
}

// ---------- 容量 ----------

func (p *Panel) handleCapacity(w http.ResponseWriter, r *http.Request) {
	pcfg := p.d.Pool.Config()
	snap := capacity.Compute(p.d.Pool.Views(), p.d.Log.Recent(400), pcfg.MaxInFlight, pcfg.KeyQPS, p.d.Uptime())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "capacity": snap})
}

// ---------- 模型 ----------

func (p *Panel) handleModels(w http.ResponseWriter, r *http.Request) {
	logs := p.d.Log.Recent(400)
	type agg struct {
		Model    string  `json:"model"`
		Requests int     `json:"requests"`
		Errors   int     `json:"errors"`
		Tokens   int64   `json:"tokens"`
		Cost     float64 `json:"cost"`
		Latency  int64   `json:"p95_ms"`
	}
	byModel := map[string]*agg{}
	lat := map[string][]int64{}
	for _, e := range logs {
		if e.Model == "" {
			continue
		}
		a := byModel[e.Model]
		if a == nil {
			a = &agg{Model: e.Model}
			byModel[e.Model] = a
		}
		a.Requests++
		if e.Error != "" || e.Status >= 400 {
			a.Errors++
		}
		a.Tokens += e.PromptTokens + e.CompletionTokens
		a.Cost += e.Cost
		if e.LatencyMS > 0 && e.Status < 400 {
			lat[e.Model] = append(lat[e.Model], e.LatencyMS)
		}
	}
	out := make([]*agg, 0, len(byModel))
	for m, a := range byModel {
		a.Latency = p95(lat[m])
		out = append(out, a)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Cost > out[j].Cost })
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":           true,
		"models":       out,
		"prices":       priceBlock(p.d.Prices),
		"model_health": p.d.Pool.ModelHealth(),
		"window":       len(logs),
	})
}

func p95(xs []int64) int64 {
	if len(xs) == 0 {
		return 0
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	i := int(float64(len(xs)) * 0.95)
	if i >= len(xs) {
		i = len(xs) - 1
	}
	return xs[i]
}

// ---------- 号 ----------

type keyReq struct {
	Name    string   `json:"name"`
	Secret  string   `json:"secret"`
	Weight  *float64 `json:"weight"`
	Budget  *float64 `json:"budget"`
	Enabled *bool    `json:"enabled"`
	Model   string   `json:"model"`
	Delta   *float64 `json:"delta"`
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
		p.persist()
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
		p.persist()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case action == "probe" && r.Method == http.MethodPost:
		secret, ok := p.d.Pool.SecretOf(id)
		if !ok {
			writeErr(w, http.StatusNotFound, "号不存在")
			return
		}
		res := p.probeOne(r.Context(), id, secret)
		p.persist()
		writeJSON(w, http.StatusOK, res)

	case action == "models" && r.Method == http.MethodDelete:
		model := r.URL.Query().Get("model")
		if model == "" {
			writeErr(w, http.StatusBadRequest, "缺少 model 参数")
			return
		}
		if !p.d.Pool.ResetModel(id, model) {
			writeErr(w, http.StatusNotFound, "号不存在")
			return
		}
		p.persist()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case action == "budget" && r.Method == http.MethodPost:
		var req keyReq
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Delta == nil {
			writeErr(w, http.StatusBadRequest, "需要 delta")
			return
		}
		if !p.d.Pool.AddBudget(id, *req.Delta) {
			writeErr(w, http.StatusNotFound, "号不存在")
			return
		}
		p.d.Pool.ReenableIfCredits(id)
		p.persist()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

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
		p.d.Pool.ReenableIfCredits(id)
		p.persist()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	case r.Method == http.MethodDelete:
		if !p.d.Pool.Remove(id) {
			writeErr(w, http.StatusNotFound, "号不存在")
			return
		}
		p.persist()
		writeJSON(w, http.StatusOK, map[string]any{"ok": true})

	default:
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
	}
}

type probeResult struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	OK      bool   `json:"ok"`
	Latency int64  `json:"latency_ms"`
	Error   string `json:"error,omitempty"`
}

// probeOne 打一次 /v1/models：只读，不消耗生成额度。
// 成功只清 invalid（models 通了不代表 chat 通了），失败按类别落号状态。
func (p *Panel) probeOne(ctx context.Context, id, secret string) probeResult {
	name := id
	for _, v := range p.d.Pool.Views() {
		if v.ID == id {
			name = v.Name
			break
		}
	}
	start := time.Now()
	c, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	_, err := p.d.Up.FetchModels(c, secret)
	res := probeResult{ID: id, Name: name, OK: err == nil, Latency: time.Since(start).Milliseconds()}
	if err == nil {
		p.d.Pool.MarkOK(id)
		return res
	}
	res.Error = err.Error()
	var se *upstream.StatusError
	if errors.As(err, &se) {
		v := fault.Classify(se.Status, se.Body, nil)
		p.d.Pool.MarkFail(id, pool.Outcome{Kind: v.Kind, Message: v.Message, RetryAfter: v.RetryAfter})
	}
	return res
}

func (p *Panel) handleProbeAll(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	out := make([]probeResult, 0)
	for _, k := range p.d.Pool.Views() {
		// 人工停用的号不探：那是"我故意摘掉的"，探通了也不该放回来。
		// 欠费/冻结（enabled 仍为 true）照探——这是它们的唯一恢复通道。
		if !k.Enabled {
			continue
		}
		secret, ok := p.d.Pool.SecretOf(k.ID)
		if !ok {
			continue
		}
		out = append(out, p.probeOne(r.Context(), k.ID, secret))
	}
	p.persist()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "results": out})
}

// handleReviveStopped 批量恢复因欠费/冻结停用的号。
// 典型用法：给 20 个号充完值，点一次「恢复停用号」，不用一个个点「重置」。
func (p *Panel) handleReviveStopped(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	names := p.d.Pool.ReviveStopped()
	if len(names) > 0 {
		p.persist()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revived": len(names), "names": names})
}

// ---------- 调度参数 ----------

// handlePool 热更新调度参数，并写回配置文件的 pool 段（密钥不落盘）。
func (p *Panel) handlePool(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	// 以当前配置打底再覆盖：请求体里没出现的字段保持原值。
	//
	// 不要"解到零值再逐项兜底"——漏兜的字段（比如 breaker_threshold、
	// idle_weight_per_hour）会被静默清零，而服务看起来还活着：熔断再也不触发、
	// 闲置补偿悄无声息地关掉。手写 curl 只改一两个字段的场景太常见了。
	cur := p.d.Pool.Config()
	in := cur
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
	switch in.Batch.Mode {
	case "off", "auto", "fixed":
	case "":
		in.Batch.Mode = cur.Batch.Mode
	default:
		writeErr(w, http.StatusBadRequest, "batch.mode 只能是 off / auto / fixed")
		return
	}
	in.Batch.Normalize()

	p.d.Pool.SetConfig(in)
	if err := config.WritePool(p.d.ConfigPath, in); err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pool": p.d.Pool.Config(), "warning": "写回配置文件失败: " + err.Error()})
		return
	}
	p.persist()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "pool": p.d.Pool.Config()})
}

// ---------- 价目表 ----------

func (p *Panel) handlePrices(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "prices": priceBlock(p.d.Prices)})
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	// 逐个号试：某个号被冻结/限流不该让整张价目表刷不出来。
	var lastErr error
	tried := 0
	for _, k := range p.d.Pool.Views() {
		if !k.Enabled || k.State == "invalid" || k.State == "disabled" {
			continue
		}
		secret, ok := p.d.Pool.SecretOf(k.ID)
		if !ok {
			continue
		}
		tried++
		c, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		body, err := p.d.Up.FetchModels(c, secret)
		cancel()
		if err != nil {
			lastErr = err
			continue
		}
		if err := p.d.Prices.Load(body, k.Name); err != nil {
			lastErr = err
			continue
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "count": p.d.Prices.Count(), "via": k.Name})
		return
	}
	if tried == 0 {
		writeErr(w, http.StatusBadRequest, "池内没有可用号，无法拉取价目表")
		return
	}
	msg := "所有号都拉不到价目表"
	if lastErr != nil {
		msg += ": " + lastErr.Error()
	}
	writeErr(w, http.StatusBadGateway, msg)
}

// ---------- 配置 ----------

func (p *Panel) handleConfig(w http.ResponseWriter, r *http.Request) {
	cfg := *p.d.Cfg
	cfg.AdminKey = ""
	cfg.ClientKeys = nil
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"config":  cfg,
		"pool":    p.d.Pool.Config(),
		"version": p.d.Version,
	})
}

func (p *Panel) persist() {
	if p.d.Persist != nil {
		p.d.Persist()
	}
}
