// Package scheduler 是后台任务：到期冷却复活、探活、价目表刷新、状态落盘、归档清理。
//
// 所有任务都是"幂等且可重入"的：多跑一次最多多打一个 /v1/models，
// 少跑一次也不会把状态写坏——这是后台任务的基本要求。
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"trpool/internal/config"
	"trpool/internal/fault"
	"trpool/internal/livecfg"
	"trpool/internal/pool"
	"trpool/internal/reqlog"
	"trpool/internal/upstream"
	"trpool/internal/usage"
)

// persistLogs 是随状态文件一起落盘的日志条数上限。
// 取 200：够覆盖"最近一小时的排查窗口"，又不至于让 5 秒一次的重写变笨重。
const persistLogs = 200

// stateVersion 让未来的格式迁移有话可说；v1（旧 trpool）没有这个字段，按 1 处理。
const stateVersion = 2

type Deps struct {
	Cfg       *config.Config
	Pool      *pool.Pool
	Up        *upstream.Client
	Prices    *usage.Prices
	Log       *reqlog.Recorder
	Daily     *usage.Daily
	StatePath string
	// Live 提供在线可改的探活参数（探活模型 / max_tokens）。
	Live *livecfg.Holder
}

type Scheduler struct {
	d     Deps
	dirty atomic.Bool
	mu    sync.Mutex // 保护写盘，避免 tick 与停机保存撞在一起
}

func New(d Deps) *Scheduler { return &Scheduler{d: d} }

func (s *Scheduler) MarkDirty() { s.dirty.Store(true) }

type stateFile struct {
	Version int            `json:"version"`
	Pool    pool.Snapshot  `json:"pool"`
	Daily   *usage.Daily   `json:"daily"`
	Logs    []reqlog.Entry `json:"logs,omitempty"`
	SavedAt string         `json:"saved_at"`
}

// Load 从磁盘恢复号池与账本。文件损坏时宁可空池启动，也不要带着半个池子跑。
func (s *Scheduler) Load() error {
	raw, err := os.ReadFile(s.d.StatePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("读状态文件: %w", err)
	}
	var st stateFile
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("状态文件损坏，已忽略: %w", err)
	}
	s.d.Pool.Restore(st.Pool)
	if st.Daily != nil && st.Daily.Days != nil {
		s.d.Daily.Restore(st.Daily.Days)
	}
	if len(st.Logs) > 0 {
		s.d.Log.Restore(st.Logs)
	}
	log.Printf("状态已恢复：%d 个号，%d 天账本，%d 条历史日志（格式 v%d）",
		len(st.Pool.Keys), len(s.d.Daily.Days), len(st.Logs), maxInt(st.Version, 1))
	return nil
}

// Save 原子写状态文件（临时文件 + rename，权限 600）。
func (s *Scheduler) Save() {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := stateFile{
		Version: stateVersion,
		Pool:    s.d.Pool.Snapshot(),
		Daily:   s.d.Daily,
		Logs:    s.d.Log.Recent(persistLogs),
		SavedAt: time.Now().Format(time.RFC3339),
	}
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		log.Printf("WARN: 序列化状态失败: %v", err)
		return
	}
	tmp := s.d.StatePath + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		log.Printf("WARN: 写状态失败: %v", err)
		return
	}
	if err := os.Rename(tmp, s.d.StatePath); err != nil {
		log.Printf("WARN: 替换状态文件失败: %v", err)
	}
}

// Run 启动所有后台循环，阻塞到 ctx 取消。
func (s *Scheduler) Run(ctx context.Context) {
	tick := time.NewTicker(5 * time.Second)
	defer tick.Stop()

	probeEvery := s.d.Cfg.AutoProbe.Interval.Duration()
	if probeEvery <= 0 {
		probeEvery = 2 * time.Minute
	}
	probeTick := time.NewTicker(probeEvery)
	defer probeTick.Stop()

	priceEvery := s.d.Cfg.Prices.RefreshInterval.Duration()
	if priceEvery <= 0 {
		priceEvery = 10 * time.Minute
	}
	priceTick := time.NewTicker(priceEvery)
	defer priceTick.Stop()

	hourly := time.NewTicker(time.Hour)
	defer hourly.Stop()

	var keepC <-chan time.Time
	if iv := s.d.Cfg.Pool.Warmup.Interval.Duration(); s.d.Cfg.Pool.Warmup.Enabled && iv > 0 {
		keepTick := time.NewTicker(iv)
		defer keepTick.Stop()
		keepC = keepTick.C
	}

	// 启动即拉一次价目表：否则头几分钟记出来的成本全是 0，账就不可信了。
	s.RefreshPrices(ctx)
	if s.d.Cfg.Pool.Warmup.Enabled && s.d.Cfg.Pool.Warmup.OnStart {
		go s.Warmup(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			s.d.Pool.Tick()
			s.d.Pool.Sessions().Expire(time.Now())
			if s.dirty.Swap(false) {
				s.Save()
			}
		case <-priceTick.C:
			s.RefreshPrices(ctx)
		case <-keepC:
			// 周期保活：保持连接热 + 尽早发现整体性故障
			go s.Warmup(ctx)
		case <-probeTick.C:
			if s.d.Cfg.AutoProbe.Enabled {
				s.ProbeInvalid(ctx)
			}
		case <-hourly.C:
			s.d.Log.Prune()
			s.d.Daily.Trim(400)
			s.Save()
		}
	}
}

// Warmup 把所有启用的号并发探一遍。
//
// 这不是"模型冷启动"——上游是 API 网关，没有加载权重那回事。价值在两点：
//  1. 启动后立刻知道每个号真实可用，不让第一波业务请求去试错；
//  2. 顺带把到上游的 TLS 连接建起来，首个真实请求省掉握手。
func (s *Scheduler) Warmup(ctx context.Context) {
	views := s.d.Pool.EnabledKeys()
	if len(views) == 0 {
		return
	}
	conc := s.d.Cfg.Pool.Warmup.Concurrency
	if conc <= 0 {
		conc = 4
	}
	timeout := s.d.Cfg.AutoProbe.Timeout.Duration()
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	start := time.Now()
	for _, v := range views {
		secret, ok := s.d.Pool.SecretOf(v.ID)
		if !ok {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(id, name, secret string) {
			defer wg.Done()
			defer func() { <-sem }()
			c, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			if _, err := s.d.Up.FetchModels(c, secret); err != nil {
				// 预热的全部价值就在这一步：探出坏号并直接记进状态，
				// 而不是只打一行日志、让第一波业务请求再试错一遍。
				s.markByError(id, err)
				log.Printf("预热失败 %s: %v", name, err)
				return
			}
		}(v.ID, v.Name, secret)
	}
	wg.Wait()
	log.Printf("预热完成：%d 个号，耗时 %dms", len(views), time.Since(start).Milliseconds())
}

// ProbeInvalid 只探 invalid（401 判死）的号：探通了说明密钥又能用了。
// cooling/exhausted/blocked 不在自动探活范围内——欠费/冻结是终态，
// 只允许人工「探活」「恢复」把它们放回来，不能让定时任务自动复活。
func (s *Scheduler) ProbeInvalid(ctx context.Context) {
	if s.d.Prices.Count() == 0 {
		s.RefreshPrices(ctx)
	}
	timeout := s.d.Cfg.AutoProbe.Timeout.Duration()
	if timeout <= 0 {
		timeout = 20 * time.Second
	}
	// 探活模型可在线配置：配了就真跑一次生成（能验出欠费/冻结），没配只打 /v1/models。
	model, maxTokens := "", 1
	if s.d.Live != nil {
		model, maxTokens = s.d.Live.Load().Probe()
	}
	for _, v := range s.d.Pool.Views() {
		if !v.Enabled || v.State != "invalid" {
			continue
		}
		secret, ok := s.d.Pool.SecretOf(v.ID)
		if !ok {
			continue
		}
		c, cancel := context.WithTimeout(ctx, timeout)
		err := s.probe(c, secret, model, maxTokens)
		cancel()
		if err == nil {
			if model == "" {
				// 只证明密钥有效：不能顺手复活欠费/冻结的号。
				s.d.Pool.MarkAuthOK(v.ID)
			} else {
				s.d.Pool.MarkOK(v.ID)
			}
			log.Printf("探活成功，号 %s 已恢复", v.Name)
			continue
		}
		s.markByError(v.ID, err)
	}
}

// probe 按配置打一次探活请求：配了模型就真跑生成，没配只拉 /v1/models。
func (s *Scheduler) probe(ctx context.Context, secret, model string, maxTokens int) error {
	if model == "" {
		_, err := s.d.Up.FetchModels(ctx, secret)
		return err
	}
	_, err := s.d.Up.ProbeChat(ctx, secret, model, maxTokens)
	return err
}

// RefreshPrices 用第一个能用的号拉 /v1/models，更新价目表。
func (s *Scheduler) RefreshPrices(ctx context.Context) (via string, count int, err error) {
	var lastErr error
	tried := 0
	for _, v := range s.d.Pool.EnabledKeys() {
		secret, ok := s.d.Pool.SecretOf(v.ID)
		if !ok {
			continue
		}
		tried++
		c, cancel := context.WithTimeout(ctx, 20*time.Second)
		body, ferr := s.d.Up.FetchModels(c, secret)
		cancel()
		if ferr != nil {
			lastErr = ferr
			continue
		}
		if lerr := s.d.Prices.Load(body, v.Name); lerr != nil {
			lastErr = lerr
			continue
		}
		s.MarkDirty()
		log.Printf("价目表已更新：%d 个模型（经 %s）", s.d.Prices.Count(), v.Name)
		return v.Name, s.d.Prices.Count(), nil
	}
	if tried == 0 {
		err = errors.New("池内没有可用号")
	} else if lastErr != nil {
		err = lastErr
	} else {
		err = errors.New("没有号能返回模型列表")
	}
	s.d.Prices.NoteError(err.Error())
	log.Printf("WARN: 价目表刷新失败: %v", err)
	return "", s.d.Prices.Count(), err
}

// markByError 把一次探活失败翻译成号的状态转移。
// 只处理"确定是这个号的问题"的几类；网络抖动之类不动状态，免得误伤。
func (s *Scheduler) markByError(id string, err error) {
	var se *upstream.StatusError
	if !errors.As(err, &se) {
		return
	}
	v := fault.Classify(se.Status, se.Body, nil)
	switch v.Kind {
	case fault.Auth, fault.Quota, fault.Rate, fault.Banned:
		s.d.Pool.MarkFail(id, pool.Outcome{Kind: v.Kind, Message: v.Message, RetryAfter: v.RetryAfter})
	}
}

// SeedFromEnv 支持首启批量导入：TRPOOL_KEYS="名称=sk_tr_xxx=100,名称2=sk_tr_yyy=50"
// 只在号池为空时生效，避免每次重启重复导入。
func SeedFromEnv(p *pool.Pool) {
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
		parts := strings.Split(item, "=")
		if len(parts) < 2 {
			log.Printf("WARN: 忽略无法解析的 TRPOOL_KEYS 项: %q", item)
			continue
		}
		name, secret := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		var budget float64
		if len(parts) >= 3 {
			if v, err := parseFloat(parts[2]); err == nil {
				budget = v
			}
		}
		if _, err := p.Add(name, secret, 1, budget); err != nil {
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

// EnsureDir 建好数据目录（权限 700：里面是密钥与账本）。
func EnsureDir(dir string) error {
	if dir == "" {
		return errors.New("数据目录为空")
	}
	return os.MkdirAll(dir, 0o700)
}

// DefaultStatePath 是 <data_dir>/state.json。
func DefaultStatePath(dataDir string) string { return filepath.Join(dataDir, "state.json") }

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
