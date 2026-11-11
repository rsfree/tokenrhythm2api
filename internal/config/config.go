// Package config 负责加载、校验与回写网关配置。
//
// 设计取向沿用 workbuddy2api：配置文件只放"能公开的东西"（策略、阈值、路径），
// 密钥（admin_key / client_keys）优先从环境变量读，避免随镜像或仓库泄露；
// 面板改的调度参数只回写 pool 段，绝不把密钥写进文件。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Duration 让 JSON 里可以写 "30m" / "6h" 这种人话，而不是纳秒整数。
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*d = 0
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("时长格式错误 %q（用 30s / 5m / 2h）: %w", s, err)
	}
	*d = Duration(v)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return []byte(`"` + time.Duration(d).String() + `"`), nil
}

// SessionSticky 让同一段对话尽量落在同一个号上。
//
// 目的不是"均衡"，而是命中上游 prompt cache：多数模型服务商的缓存按账号隔离，
// 会话一散就永远命中不了，而缓存价通常只有输入价的 1/3 ~ 1/5。
type SessionSticky struct {
	Enabled bool     `json:"enabled"`
	TTL     Duration `json:"ttl"`
	// 表大小上限，防止长跑后内存无界增长；超出后淘汰最久未用的。
	MaxEntries int `json:"max_entries"`
	// 用 system + 首条 user 做指纹时，同一人格的连续对话更容易命中；
	// 关掉则只看显式会话标识（OpenAI 的 user / Anthropic 的 metadata.user_id）。
	PrefixAffinity bool `json:"prefix_affinity"`
}

// Batch 控制"同一时刻只用几个号"，也就是活跃窗口。
//
// off=全池参与；fixed=固定 N 个（死一个补一个）；auto=按在途并发自动伸缩。
type Batch struct {
	Mode string `json:"mode"`
	// Size 是 fixed 的窗口大小 / auto 的下限。池子有 100 个号、Size=12，
	// 就是"只有 12 个号在派单，其余待命"。
	Size int `json:"size"`
	Min  int `json:"min"`
	Max  int `json:"max"` // 0 = 不限（等于池大小）
	// auto 的换算基准：每多少在途请求扩一个号
	TargetInflightPerKey int `json:"target_inflight_per_key"`

	// Elastic 是 429 驱动的弹性扩容：压力上来多开几个号，稳定后回落到 Size。
	Elastic Elastic `json:"elastic"`
}

// Elastic 让活跃窗口在 [Size, MaxSize] 之间自动浮动。
//
// 只统计"按号限流"的 429（RATE_LIMITED）——那是这个号的次数窗口满了，
// 多开号确实能加总配额。模型侧限流（UPSTREAM_RATE_LIMITED）是全池共享的
// 上游瓶颈，开更多号不加吞吐，不计入。
type Elastic struct {
	Enabled bool `json:"enabled"`
	// Window 是统计窗口；也是两次扩容之间的最小间隔（一个窗口最多扩一次）。
	Window Duration `json:"window"`
	// TriggerRate 是窗口内 429 占比的触发线（0.15 = 15%）。
	TriggerRate float64 `json:"trigger_rate"`
	// MinSamples 是占比生效所需的最小样本数；样本不足时只要窗口内
	// 出现 MinHits 次 429 也算压力（低流量池子照样能扩）。
	MinSamples int `json:"min_samples"`
	MinHits    int `json:"min_hits"`
	Step       int `json:"step"`
	// MaxSize 是弹性上限（0 = 用 batch.max，再不行就用池大小）。
	MaxSize int `json:"max_size"`
	// Hold 是扩容后至少保持多久才开始考虑缩容。
	Hold Duration `json:"hold"`
	// Stable 是连续多久没有 429 才缩容。
	Stable    Duration `json:"stable"`
	DecayStep int      `json:"decay_step"`
}

// Normalize 补齐 Batch 的零值。热更新接口也会调用它：
// 前端少传一项不应该把窗口大小变成 0。
func (b *Batch) Normalize() {
	if b.Min <= 0 {
		b.Min = 1
	}
	if b.Size <= 0 {
		b.Size = 3
	}
	if b.TargetInflightPerKey <= 0 {
		b.TargetInflightPerKey = 4
	}
	e := &b.Elastic
	if e.Window.Duration() <= 0 {
		e.Window = Duration(60 * time.Second)
	}
	if e.TriggerRate <= 0 || e.TriggerRate > 1 {
		e.TriggerRate = 0.15
	}
	if e.MinSamples <= 0 {
		e.MinSamples = 20
	}
	if e.MinHits <= 0 {
		e.MinHits = 2
	}
	if e.Step <= 0 {
		e.Step = 2
	}
	if e.Hold.Duration() <= 0 {
		e.Hold = Duration(2 * time.Minute)
	}
	if e.Stable.Duration() <= 0 {
		e.Stable = Duration(5 * time.Minute)
	}
	if e.DecayStep <= 0 {
		e.DecayStep = 1
	}
	if e.MaxSize < 0 {
		e.MaxSize = 0
	}
}

// Warmup 启动预热与周期保活。
type Warmup struct {
	Enabled     bool     `json:"enabled"`
	OnStart     bool     `json:"on_start"`
	Interval    Duration `json:"interval"` // 0 = 不做周期保活
	Concurrency int      `json:"concurrency"`
}

// PoolConfig 是选号与容错的全部旋钮。
type PoolConfig struct {
	// Strategy: weighted_random | round_robin | sequential
	Strategy string `json:"strategy"`

	// TopK 是加权随机前的短名单宽度。取 1 退化为"永远最强"，取大更分散。
	TopK int `json:"top_k"`

	MaxInFlight       int `json:"max_in_flight"`
	MaxInFlightGlobal int `json:"max_in_flight_global"`

	// 连续失败多少次进入熔断（与"一次性错误冷却"解耦）
	BreakerThreshold int      `json:"breaker_threshold"`
	BreakerCooldown  Duration `json:"breaker_cooldown"`
	BreakerMax       Duration `json:"breaker_cooldown_max"`

	// 429 的软冷却起点与上限（有上游 retryAfter 时按它来，不做指数放大）
	RateCooldown Duration `json:"rate_cooldown"`
	RateMax      Duration `json:"rate_cooldown_max"`

	// 402 余额不足与 403 账号冻结没有冷却参数：两者都是账号级不可用，
	// 一命中就停用（exhausted / blocked），只能人工恢复
	// （补额度 / 「恢复」 / 「探活」）。不留"过一会儿自己回来"的通道，
	// 否则 100 个号 99 个没充值时，每轮自动重试都只是白撞一遍。

	// 模型级封禁（403 MODEL_ACCESS_DENIED）：只封"这个号 + 这个模型"，
	// 指数退避，成功一次即解除。换号通常能救，所以不罚整个号。
	ModelBlockBase Duration `json:"model_block_base"`
	ModelBlockMax  Duration `json:"model_block_max"`

	// 模型供应链限流（429 UPSTREAM_RATE_LIMITED）：换号救不了，
	// 只对 (号, 模型) 做短暂冷却，退避后优先同号重试。
	ModelRateCooldown Duration `json:"model_rate_cooldown"`

	// 余额低于该值（CNY）且模型收费时不再派单。0 表示不启用保底。
	CreditFloor float64 `json:"credit_floor"`

	// 闲置补偿：每小时未使用带来的权重增益，以及增益上限
	IdleWeightPerHour float64 `json:"idle_weight_per_hour"`
	IdleWeightMax     float64 `json:"idle_weight_max"`

	// 单次请求最多换多少个号
	RetryMax int `json:"retry_max"`

	// 上游单请求超时（视频接口另算）
	UpstreamTimeout Duration `json:"upstream_timeout"`
	VideoTimeout    Duration `json:"video_timeout"`

	// 视频任务 ID → key 的粘性保留时长
	StickyTTL Duration `json:"sticky_ttl"`

	// 在途感知：把在途数折进权重，避免并发高峰全压一个号。
	LeastInflight   bool    `json:"least_inflight"`
	InflightPenalty float64 `json:"inflight_penalty"`

	// 同一账号在该窗口内不重复被选中（防并发惊群）；全部刚用过时按 LRU 兜底。
	MinPickGap Duration `json:"min_pick_gap"`

	// 全池冷却时允许挑"最早恢复"的号试一次。默认开：
	// 冷却是按时间恢复的，等它自然到期不如让真实请求去验证。
	FallbackProbe bool `json:"fallback_probe"`

	// 每号上游配额（次/秒）。仅用于容量页估算，不参与选号。
	KeyQPS float64 `json:"key_qps"`

	SessionSticky SessionSticky `json:"session_sticky"`
	Batch         Batch         `json:"batch"`
	Warmup        Warmup        `json:"warmup"`
}

// ServerConfig 是入站 HTTP 的行为参数。
type ServerConfig struct {
	// 入站请求读取（含 body 上传）总时长上限；大上下文经反代转发超时会 400。
	ReadTimeout Duration `json:"read_timeout"`
	// 请求体上限（MiB）。多模态请求可能带大图，默认 64。
	BodyLimitMB int `json:"body_limit_mb"`
	// 流式静默多久补一次心跳；0 = 关闭。
	StreamKeepalive Duration `json:"stream_keepalive"`
	// 优雅停机预算。
	ShutdownTimeout Duration `json:"shutdown_timeout"`
}

// LoggingConfig 是请求日志与归档。
type LoggingConfig struct {
	RingSize  int  `json:"ring_size"`
	LogBodies bool `json:"log_bodies"`

	// RequestArchive 把请求元数据写成 JSONL（不写提示词/响应正文/凭证）。
	RequestArchive *bool `json:"request_archive"`
	// 空 = <data_dir>/requests
	ArchiveDir    string `json:"archive_dir"`
	RetentionDays int    `json:"retention_days"`
	MaxMB         int    `json:"request_archive_max_mb"`
}

// ArchiveOn 报告是否启用归档（缺省开）。
func (l LoggingConfig) ArchiveOn() bool {
	if l.RequestArchive == nil {
		return true
	}
	return *l.RequestArchive
}

// ProbeConfig 是自动探活。
type ProbeConfig struct {
	Enabled     bool     `json:"enabled"`
	Interval    Duration `json:"interval"`
	Timeout     Duration `json:"timeout"`
	Concurrency int      `json:"concurrency"`
}

// PriceConfig 是价目表刷新。
type PriceConfig struct {
	RefreshInterval Duration `json:"refresh_interval"`
}

// Config 是根配置。
type Config struct {
	Listen   string `json:"listen"`
	Upstream string `json:"upstream"`
	DataDir  string `json:"data_dir"`

	AdminKey   string   `json:"admin_key"`
	ClientKeys []string `json:"client_keys"`

	// 是否允许客户端自带的 sk_tr_ 直接透传（不安全，默认关）
	AllowUpstreamKeyPassthrough bool `json:"allow_upstream_key_passthrough"`

	Server    ServerConfig  `json:"server"`
	Pool      PoolConfig    `json:"pool"`
	Logging   LoggingConfig `json:"logging"`
	AutoProbe ProbeConfig   `json:"auto_probe"`
	Prices    PriceConfig   `json:"prices"`
}

func Default() Config {
	c := Config{
		Listen:   ":8787",
		Upstream: "https://tokenrhythm.studio",
		DataDir:  "./data",
		Server: ServerConfig{
			ReadTimeout:     Duration(300 * time.Second),
			BodyLimitMB:     64,
			StreamKeepalive: Duration(15 * time.Second),
			ShutdownTimeout: Duration(10 * time.Second),
		},
		Pool: PoolConfig{
			Strategy:          "weighted_random",
			TopK:              5,
			MaxInFlight:       3,
			MaxInFlightGlobal: 0,
			BreakerThreshold:  3,
			BreakerCooldown:   Duration(30 * time.Second),
			BreakerMax:        Duration(5 * time.Minute),
			RateCooldown:      Duration(20 * time.Second),
			RateMax:           Duration(10 * time.Minute),
			ModelBlockBase:    Duration(6 * time.Hour),
			ModelBlockMax:     Duration(24 * time.Hour),
			ModelRateCooldown: Duration(30 * time.Second),
			CreditFloor:       0.5,
			IdleWeightPerHour: 0.5,
			IdleWeightMax:     4,
			RetryMax:          3,
			UpstreamTimeout:   Duration(300 * time.Second),
			VideoTimeout:      Duration(600 * time.Second),
			StickyTTL:         Duration(72 * time.Hour),
			LeastInflight:     true,
			InflightPenalty:   3,
			MinPickGap:        Duration(100 * time.Millisecond),
			FallbackProbe:     true,
			KeyQPS:            2,
			SessionSticky: SessionSticky{
				Enabled:        true,
				TTL:            Duration(30 * time.Minute),
				MaxEntries:     2000,
				PrefixAffinity: true,
			},
			Batch: Batch{
				Mode:                 "auto",
				Size:                 3,
				Min:                  1,
				Max:                  0,
				TargetInflightPerKey: 4,
			},
			Warmup: Warmup{
				Enabled:     true,
				OnStart:     true,
				Concurrency: 4,
			},
		},
		Logging: LoggingConfig{
			RingSize:      400,
			RetentionDays: 7,
			MaxMB:         100,
		},
		AutoProbe: ProbeConfig{
			Enabled:     true,
			Interval:    Duration(2 * time.Minute),
			Timeout:     Duration(15 * time.Second),
			Concurrency: 4,
		},
		Prices: PriceConfig{RefreshInterval: Duration(10 * time.Minute)},
	}
	return c
}

// Load 读配置文件，缺省项用 Default 补齐；随后叠加环境变量。
// 文件不存在不报错（首启可用环境变量直接跑），但会返回 false 供调用方提示。
func Load(path string) (Config, bool, error) {
	c := Default()
	existed := false
	if path != "" {
		b, err := os.ReadFile(path)
		switch {
		case err == nil:
			existed = true
			if err := json.Unmarshal(b, &c); err != nil {
				return c, true, fmt.Errorf("解析配置 %s: %w", path, err)
			}
		case !os.IsNotExist(err):
			return c, false, fmt.Errorf("读配置 %s: %w", path, err)
		}
	}
	applyEnv(&c)
	c.normalize()
	return c, existed, c.Validate()
}

func applyEnv(c *Config) {
	if v := os.Getenv("TRPOOL_ADMIN_KEY"); v != "" {
		c.AdminKey = v
	}
	if v := os.Getenv("TRPOOL_CLIENT_KEYS"); v != "" {
		c.ClientKeys = splitCSV(v)
	}
	if v := os.Getenv("TRPOOL_LISTEN"); v != "" {
		c.Listen = v
	}
	if v := os.Getenv("TRPOOL_UPSTREAM"); v != "" {
		c.Upstream = v
	}
	if v := os.Getenv("TRPOOL_DATA_DIR"); v != "" {
		c.DataDir = v
	}
	if v := os.Getenv("TRPOOL_BODY_LIMIT_MB"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			c.Server.BodyLimitMB = n
		}
	}
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (c *Config) normalize() {
	c.Upstream = strings.TrimRight(strings.TrimSpace(c.Upstream), "/")
	if c.Upstream == "" {
		c.Upstream = "https://tokenrhythm.studio"
	}
	if c.DataDir == "" {
		c.DataDir = "./data"
	}
	if c.Listen == "" {
		c.Listen = ":8787"
	}
	if c.Server.BodyLimitMB <= 0 {
		c.Server.BodyLimitMB = 64
	}
	if c.Server.ReadTimeout.Duration() <= 0 {
		c.Server.ReadTimeout = Duration(300 * time.Second)
	}
	if c.Server.ShutdownTimeout.Duration() <= 0 {
		c.Server.ShutdownTimeout = Duration(10 * time.Second)
	}
	if c.Server.StreamKeepalive.Duration() < 0 {
		c.Server.StreamKeepalive = 0
	}

	p := &c.Pool
	switch p.Strategy {
	case "weighted_random", "round_robin", "sequential":
	default:
		p.Strategy = "weighted_random"
	}
	if p.TopK <= 0 {
		p.TopK = 5
	}
	if p.RetryMax <= 0 {
		p.RetryMax = 3
	}
	if p.BreakerThreshold <= 0 {
		p.BreakerThreshold = 3
	}
	if p.BreakerCooldown.Duration() <= 0 {
		p.BreakerCooldown = Duration(30 * time.Second)
	}
	if p.RateCooldown.Duration() <= 0 {
		p.RateCooldown = Duration(20 * time.Second)
	}
	if p.ModelBlockBase.Duration() <= 0 {
		p.ModelBlockBase = Duration(6 * time.Hour)
	}
	if p.ModelBlockMax.Duration() <= 0 {
		p.ModelBlockMax = Duration(24 * time.Hour)
	}
	if p.ModelRateCooldown.Duration() <= 0 {
		p.ModelRateCooldown = Duration(30 * time.Second)
	}
	if p.UpstreamTimeout.Duration() <= 0 {
		p.UpstreamTimeout = Duration(300 * time.Second)
	}
	if p.VideoTimeout.Duration() <= 0 {
		p.VideoTimeout = Duration(600 * time.Second)
	}
	if p.StickyTTL.Duration() <= 0 {
		p.StickyTTL = Duration(72 * time.Hour)
	}
	if p.MinPickGap.Duration() < 0 {
		p.MinPickGap = 0
	}
	if p.KeyQPS <= 0 {
		p.KeyQPS = 2
	}
	switch p.Batch.Mode {
	case "off", "auto", "fixed":
	default:
		p.Batch.Mode = "auto"
	}
	p.Batch.Normalize()
	if p.InflightPenalty <= 0 {
		p.InflightPenalty = 3
	}
	if p.SessionSticky.TTL.Duration() <= 0 {
		p.SessionSticky.TTL = Duration(30 * time.Minute)
	}
	if p.SessionSticky.MaxEntries <= 0 {
		p.SessionSticky.MaxEntries = 2000
	}
	if p.Warmup.Concurrency <= 0 {
		p.Warmup.Concurrency = 4
	}

	if c.Logging.RingSize <= 0 {
		c.Logging.RingSize = 400
	}
	if c.Logging.RetentionDays <= 0 {
		c.Logging.RetentionDays = 7
	}
	if c.Logging.MaxMB <= 0 {
		c.Logging.MaxMB = 100
	}
	if c.AutoProbe.Interval.Duration() <= 0 {
		c.AutoProbe.Interval = Duration(2 * time.Minute)
	}
	if c.AutoProbe.Timeout.Duration() <= 0 {
		c.AutoProbe.Timeout = Duration(15 * time.Second)
	}
	if c.AutoProbe.Concurrency <= 0 {
		c.AutoProbe.Concurrency = 4
	}
	if c.Prices.RefreshInterval.Duration() <= 0 {
		c.Prices.RefreshInterval = Duration(10 * time.Minute)
	}
}

// Validate 只拦"启动就会炸"的问题。
func (c Config) Validate() error {
	if c.AdminKey == "" {
		return fmt.Errorf("缺少 admin_key（面板与管理接口鉴权），可用环境变量 TRPOOL_ADMIN_KEY 提供")
	}
	if len(c.ClientKeys) == 0 && !c.AllowUpstreamKeyPassthrough {
		return fmt.Errorf("缺少 client_keys：客户端要用它访问网关；也可设 allow_upstream_key_passthrough=true")
	}
	return nil
}

// WritePool 把调度参数写回 config.json 的 pool 段，保留其它未知键与密钥。
// 文件不存在时不做任何事（不新建，避免把内存里的密钥落盘）。
func WritePool(path string, pool PoolConfig) error {
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(raw, &top); err != nil {
		return fmt.Errorf("配置文件不是合法 JSON: %w", err)
	}
	blk, err := json.MarshalIndent(pool, "", "  ")
	if err != nil {
		return err
	}
	top["pool"] = blk
	out, err := json.MarshalIndent(top, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(out, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// WriteDefault 在首启且文件缺失时落一份推荐配置（含随机 API key），
// 省掉"先抄 example 再改"的一步。
func WriteDefault(path string, c Config) error {
	if path == "" {
		return nil
	}
	out, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(out, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
