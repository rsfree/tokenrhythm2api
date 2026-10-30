// Package config 负责加载与校验号池网关的配置。
//
// 设计取向：配置文件只放"能公开的东西"（策略、阈值、路径）。
// 密钥类（admin_key、client_keys）优先从环境变量读，避免随镜像/仓库泄露。
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// Duration 让 JSON 里可以写 "30m" / "6h" 这种人话，而不是纳秒整数。
type Duration time.Duration

func (d Duration) Duration() time.Duration { return time.Duration(d) }

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

// PoolConfig 是选号与容错的全部旋钮。
type PoolConfig struct {
	// Strategy: weighted_random | round_robin | sequential
	Strategy string `json:"strategy"`

	// TopK 是加权随机前的短名单宽度。取 1 退化为"永远最强"，取大更分散。
	TopK int `json:"top_k"`

	MaxInFlight       int `json:"max_in_flight"`
	MaxInFlightGlobal int `json:"max_in_flight_global"`

	// 连续失败多少次进入冷却（熔断）
	BreakerThreshold int      `json:"breaker_threshold"`
	BreakerCooldown  Duration `json:"breaker_cooldown"`
	BreakerMax       Duration `json:"breaker_cooldown_max"`

	// 429 的软冷却起点与上限（指数退避）
	RateCooldown Duration `json:"rate_cooldown"`
	RateMax      Duration `json:"rate_cooldown_max"`

	// 402/403 判为额度耗尽后的冷却
	ExhaustedCooldown Duration `json:"exhausted_cooldown"`

	// 余额低于该值（CNY）且模型收费时，不再派单。0 表示不启用保底。
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
}

// Config 是根配置。
type Config struct {
	Listen     string   `json:"listen"`
	Upstream   string   `json:"upstream"`
	DataDir    string   `json:"data_dir"`
	AdminKey   string   `json:"admin_key"`
	ClientKeys []string `json:"client_keys"`

	// 是否允许客户端自带的 sk_tr_ 直接透传（不安全，默认关）
	AllowUpstreamKeyPassthrough bool `json:"allow_upstream_key_passthrough"`

	Pool    PoolConfig `json:"pool"`
	Logging struct {
		RingSize  int  `json:"ring_size"`
		LogBodies bool `json:"log_bodies"`
	} `json:"logging"`

	// AutoProbe 定期用 /v1/models 探活，把 cooling/invalid 的号拉回候选
	AutoProbe struct {
		Enabled  bool     `json:"enabled"`
		Interval Duration `json:"interval"`
	} `json:"auto_probe"`
}

func Default() Config {
	c := Config{
		Listen:   ":8787",
		Upstream: "https://tokenrhythm.studio",
		DataDir:  "./data",
		Pool: PoolConfig{
			Strategy:          "weighted_random",
			TopK:              5,
			MaxInFlight:       4,
			MaxInFlightGlobal: 0,
			BreakerThreshold:  3,
			BreakerCooldown:   Duration(30 * time.Second),
			BreakerMax:        Duration(5 * time.Minute),
			RateCooldown:      Duration(20 * time.Second),
			RateMax:           Duration(10 * time.Minute),
			ExhaustedCooldown: Duration(30 * time.Minute),
			CreditFloor:       0.5,
			IdleWeightPerHour: 0.5,
			IdleWeightMax:     4,
			RetryMax:          3,
			UpstreamTimeout:   Duration(300 * time.Second),
			VideoTimeout:      Duration(600 * time.Second),
			StickyTTL:         Duration(72 * time.Hour),
		},
	}
	c.Logging.RingSize = 400
	c.AutoProbe.Enabled = true
	c.AutoProbe.Interval = Duration(2 * time.Minute)
	return c
}

// Load 读配置文件，缺省项用 Default 补齐；随后叠加环境变量。
func Load(path string) (Config, error) {
	c := Default()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return c, fmt.Errorf("读配置 %s: %w", path, err)
			}
		} else if err := json.Unmarshal(b, &c); err != nil {
			return c, fmt.Errorf("解析配置 %s: %w", path, err)
		}
	}
	// 环境变量优先：密文不进文件。
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
	c.normalize()
	return c, c.Validate()
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
	switch c.Pool.Strategy {
	case "weighted_random", "round_robin", "sequential":
	default:
		c.Pool.Strategy = "weighted_random"
	}
	if c.Pool.TopK <= 0 {
		c.Pool.TopK = 5
	}
	if c.Pool.RetryMax <= 0 {
		c.Pool.RetryMax = 3
	}
	if c.Logging.RingSize <= 0 {
		c.Logging.RingSize = 400
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
