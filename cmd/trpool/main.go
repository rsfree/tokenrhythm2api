// Command trpool 是一个面向 tokenrhythm 的多账号号池网关。
//
// 一句话：客户端只认一个 key，池子替你选号、换号、记账。
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"trpool/internal/config"
	"trpool/internal/ledger"
	"trpool/internal/panel"
	"trpool/internal/pool"
	"trpool/internal/proxy"
	"trpool/internal/upstream"
)

// version 由发布流水线通过 -ldflags "-X main.version=vX.Y.Z" 注入。
var version = "dev"

type stateFile struct {
	Pool  pool.Snapshot `json:"pool"`
	Daily *ledger.Daily `json:"daily"`
	Saved string        `json:"saved_at"`
}

func main() {
	cfgPath := flag.String("config", "config.json", "配置文件路径")
	listen := flag.String("listen", "", "覆盖监听地址，如 :8787")
	dataDir := flag.String("data", "", "覆盖数据目录")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("配置错误: %v", err)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		log.Fatalf("创建数据目录失败: %v", err)
	}

	p := pool.New(cfg.Pool)
	prices := ledger.NewPrices()
	lg := ledger.NewLog(cfg.Logging.RingSize)
	daily := ledger.NewDaily()
	up := upstream.New(cfg.Upstream, cfg.Pool.UpstreamTimeout.Duration(), cfg.Pool.VideoTimeout.Duration())

	statePath := filepath.Join(cfg.DataDir, "state.json")
	loadState(statePath, p, daily)
	seedFromEnv(p)

	h := proxy.New(cfg, p, up, prices, lg, daily)

	// 配置写回：只替换 config.json 里的 pool 段，绝不把 admin_key/client_keys 落盘。
	var persistDirty atomic.Bool
	persist := func() {
		persistDirty.Store(true)
	}
	p.SetOnChange(persist)
	writeConfigPool := func() {
		if *cfgPath == "" {
			return
		}
		raw, err := os.ReadFile(*cfgPath)
		if err != nil {
			return // 文件不存在就不新建，避免把密钥写进配置
		}
		var top map[string]json.RawMessage
		if json.Unmarshal(raw, &top) != nil {
			return
		}
		blk, err := json.MarshalIndent(cfg.Pool, "", "  ")
		if err != nil {
			return
		}
		top["pool"] = blk
		out, err := json.MarshalIndent(top, "", "  ")
		if err != nil {
			return
		}
		tmp := *cfgPath + ".tmp"
		if os.WriteFile(tmp, append(out, '\n'), 0o600) == nil {
			_ = os.Rename(tmp, *cfgPath)
		}
	}

	pan := panel.New(panel.Deps{
		Cfg: &cfg, Pool: p, Prices: prices, Log: lg, Daily: daily, Up: up,
		Stats:  h.Stats().Snapshot,
		Uptime: h.Uptime,
		Persist: func() {
			persistDirty.Store(true)
			writeConfigPool()
		},
	})
	h.PanelHandler = pan

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           h,
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.Pool.Warmup.Enabled && cfg.Pool.Warmup.OnStart {
		go warmup(ctx, p, up, cfg.Pool.Warmup.Concurrency)
	}
	go pinger(ctx, p, prices, up, cfg)
	go saver(ctx, p, daily, statePath, &persistDirty)

	go func() {
		log.Printf("trpool %s 启动：listen=%s upstream=%s 号池=%d 个 client_key=%d 个",
			version, cfg.Listen, cfg.Upstream, len(p.Views()), len(cfg.ClientKeys))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务失败: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("收到退出信号，正在保存状态…")
	shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutCtx)
	saveState(statePath, p, daily)
	log.Println("已退出")
}

// loadState 从磁盘恢复号池与日账。文件损坏时宁可空池启动，也不要带着半个池子跑。
func loadState(path string, p *pool.Pool, daily *ledger.Daily) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("WARN: 读状态文件失败: %v", err)
		}
		return
	}
	var st stateFile
	if err := json.Unmarshal(raw, &st); err != nil {
		log.Printf("WARN: 状态文件损坏，忽略: %v", err)
		return
	}
	p.Restore(st.Pool)
	if st.Daily != nil && st.Daily.Days != nil {
		daily.Days = st.Daily.Days
	}
	log.Printf("状态已恢复：%d 个号，%d 天账本", len(st.Pool.Keys), len(daily.Days))
}

func saveState(path string, p *pool.Pool, daily *ledger.Daily) {
	st := stateFile{Pool: p.Snapshot(), Daily: daily, Saved: time.Now().Format(time.RFC3339)}
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		log.Printf("WARN: 序列化状态失败: %v", err)
		return
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("WARN: 写状态失败: %v", err)
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.Printf("WARN: 替换状态文件失败: %v", err)
	}
}

func saver(ctx context.Context, p *pool.Pool, daily *ledger.Daily, path string, dirty *atomic.Bool) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if dirty.Swap(false) {
				saveState(path, p, daily)
			}
		}
	}
}

// pinger 干三件事：到期的冷却号复活、价目表定时刷新、失联号自动探活。
func pinger(ctx context.Context, p *pool.Pool, prices *ledger.Prices, up *upstream.Client, cfg config.Config) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()
	priceTick := time.NewTicker(10 * time.Minute)
	defer priceTick.Stop()

	probeEvery := cfg.AutoProbe.Interval.Duration()
	if probeEvery <= 0 {
		probeEvery = 2 * time.Minute
	}
	probeTick := time.NewTicker(probeEvery)
	defer probeTick.Stop()

	var keepTick *time.Ticker
	if iv := cfg.Pool.Warmup.Interval.Duration(); cfg.Pool.Warmup.Enabled && iv > 0 {
		keepTick = time.NewTicker(iv)
		defer keepTick.Stop()
	}
	var keepC <-chan time.Time
	if keepTick != nil {
		keepC = keepTick.C
	}

	refreshPrices := func() {
		var lastErr error
		for _, v := range p.Views() {
			if !v.Enabled || v.State == "invalid" || v.State == "disabled" {
				continue
			}
			secret, ok := p.SecretOf(v.ID)
			if !ok {
				continue
			}
			c, cancel := context.WithTimeout(ctx, 20*time.Second)
			body, err := up.FetchModels(c, secret)
			cancel()
			if err != nil {
				// 单个号挂着（冻结/限流）不该拖垮整张价目表，换下一个号继续试。
				lastErr = err
				continue
			}
			if err := prices.Load(body); err != nil {
				lastErr = err
				continue
			}
			log.Printf("价目表已更新：%d 个模型（经 %s）", prices.Count(), v.Name)
			return
		}
		if lastErr != nil {
			log.Printf("WARN: 价目表刷新失败，所有号都不可用: %v", lastErr)
		}
	}

	// 启动即拉一次价目表：否则头几分钟记出来的成本全是 0，账就不可信了。
	refreshPrices()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			p.Tick()
		case <-priceTick.C:
			if prices.Count() == 0 {
				refreshPrices()
			}
		case <-keepC:
			// 周期保活：保持连接热 + 尽早发现整体性故障
			warmup(ctx, p, up, cfg.Pool.Warmup.Concurrency)
		case <-probeTick.C:
			if !cfg.AutoProbe.Enabled {
				continue
			}
			if prices.Count() == 0 {
				refreshPrices()
			}
			probeBroken(ctx, p, up)
		}
	}
}

// warmup 启动时把所有启用的号并发探一遍。
//
// 这不是"模型冷启动"——上游是 API 网关，没有加载权重那回事。价值在两点：
//  1. 启动后立刻知道每个号真实可用，不让第一波业务请求去试错；
//  2. 顺带把到上游的 TLS 连接建起来，首个真实请求省掉握手。
//
// 只打 /v1/models，不消耗生成额度。
func warmup(ctx context.Context, p *pool.Pool, up *upstream.Client, conc int) {
	views := p.Views()
	if len(views) == 0 {
		return
	}
	if conc <= 0 {
		conc = 4
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	start := time.Now()
	for _, v := range views {
		if !v.Enabled {
			continue
		}
		secret, ok := p.SecretOf(v.ID)
		if !ok {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id, name, secret string) {
			defer wg.Done()
			defer func() { <-sem }()
			c, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			if _, err := up.FetchModels(c, secret); err != nil {
				// 预热的全部价值就在这一步：探出坏号并直接记进状态，
				// 而不是只打一行日志、让第一波业务请求再试错一遍。
				markByProbeError(p, id, err)
				log.Printf("预热失败 %s: %v", name, err)
				return
			}
			// 注意：不在这里 Reset。models 探活成功只说明"这个号能取模型列表"，
			// 不代表 chat 可用（模型级灰度、账号级风控都可能只拦生成接口）。
			// 探活成功就把 cooling 拉回 active，等于让业务请求替我们重试。
			// 冷却交给 Tick 按时间自然恢复，那是真实流量给出的判断。
		}(v.ID, v.Name, secret)
	}
	wg.Wait()
	log.Printf("预热完成：%d 个号，耗时 %dms", len(views), time.Since(start).Milliseconds())
}

// markByProbeError 把一次探活失败翻译成号的状态转移。
// 只处理"确定是这个号的问题"的几类；网络抖动之类不动状态，免得误伤。
func markByProbeError(p *pool.Pool, id string, err error) {
	var se *upstream.StatusError
	if !errors.As(err, &se) {
		return
	}
	switch {
	case se.Status == http.StatusUnauthorized:
		p.MarkFail(id, pool.FailAuth, se.Error())
	case se.Status == http.StatusTooManyRequests:
		p.MarkFail(id, pool.FailRate, se.Error())
	case se.Status == http.StatusForbidden &&
		bytes.Contains(se.Body, []byte("ACCOUNT_SUSPENDED")):
		// 账号被临时冻结：按额度类冷却（ExhaustedCooldown），比 429 退避更久
		p.MarkFail(id, pool.FailQuota, se.Error())
	}
}

// probeBroken 只探"应该能用但现在不能用"的号，避免无谓的上游调用。
func probeBroken(ctx context.Context, p *pool.Pool, up *upstream.Client) {
	for _, v := range p.Views() {
		// 只探 invalid：那是 401（key 失效），探通了说明又能用了。
		// cooling/exhausted 是按时间恢复的，不该被一次 models 探活提前放行。
		if !v.Enabled || v.State != "invalid" {
			continue
		}
		secret, ok := p.SecretOf(v.ID)
		if !ok {
			continue
		}
		c, cancel := context.WithTimeout(ctx, 20*time.Second)
		_, err := up.FetchModels(c, secret)
		cancel()
		if err == nil {
			p.Reset(v.ID)
			log.Printf("探活成功，号 %s 已恢复", v.Name)
		} else {
			markByProbeError(p, v.ID, err)
		}
	}
}

// seedFromEnv 支持首启批量导入：TRPOOL_KEYS="名称=sk_tr_xxx=100,名称2=sk_tr_yyy=50"
// 只在号池为空时生效，避免每次重启重复导入。
func seedFromEnv(p *pool.Pool) {
	if len(p.Views()) > 0 {
		return
	}
	raw := strings.TrimSpace(os.Getenv("TRPOOL_KEYS"))
	if raw == "" {
		return
	}
	n := 0
	for _, item := range strings.FieldsFunc(raw, func(r rune) bool { return r == ',' || r == ';' || r == '\n' }) {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		var name, secret string
		var budget float64
		parts := strings.Split(item, "=")
		switch len(parts) {
		case 2:
			name, secret = parts[0], parts[1]
		case 3:
			name, secret = parts[0], parts[1]
			if v, err := parseFloat(parts[2]); err == nil {
				budget = v
			}
		default:
			log.Printf("WARN: 忽略无法解析的 TRPOOL_KEYS 项: %q", item)
			continue
		}
		if _, err := p.Add(strings.TrimSpace(name), strings.TrimSpace(secret), 1, budget); err != nil {
			log.Printf("WARN: 导入 %s 失败: %v", name, err)
			continue
		}
		n++
	}
	if n > 0 {
		log.Printf("已从 TRPOOL_KEYS 导入 %d 个号", n)
	}
}

func parseFloat(s string) (float64, error) {
	var f float64
	err := json.Unmarshal([]byte(strings.TrimSpace(s)), &f)
	return f, err
}
