// Package pool 是号池的调度核心：状态机、选号、冷却、熔断、在途租约、粘性。
//
// 五条不变量（破坏了就会出脏账或幽灵请求）：
//  1. 只有"可用且对当前模型没被封"的号才会被选中；
//  2. 状态转移只发生在 Release/MarkFail/Tick 里，选号过程不改状态；
//  3. 粘性任务（视频查询）永远回到创建它的号，哪怕那个号正在冷却；
//  4. 每个 Lease 只能 Release 一次，否则在途计数会永久泄漏；
//  5. 模型级封禁只影响"这个号 + 这个模型"，不牵连同号的其他模型。
package pool

import (
	"fmt"
	"strings"
	"time"
)

type State string

const (
	StateActive  State = "active"
	StateCooling State = "cooling"
	StateInvalid State = "invalid" // 401，需人工/探活重置
	// StateExhausted 是 402 INSUFFICIENT_BALANCE：余额不足。
	// 命中即停用，没有冷却也不会自己回来——充值后由人工恢复
	// （补额度 / 「恢复」 / 「探活」）。
	StateExhausted State = "exhausted"
	// StateBlocked 是 403 ACCOUNT_SUSPENDED：账号被平台冻结/封禁。
	// 同样是停用终态，只能等上游解封后人工恢复。
	// 和 exhausted 分开是因为处置不同（充钱 vs 等解封），而且它绝不能被算成
	// "429 压力"去触发弹性扩容——多开号救不了封号。
	StateBlocked  State = "blocked"
	StateDisabled State = "disabled" // 人工摘号
)

// ModelBlock 是 (号, 模型) 维度的封禁/冷却。
//
// 为什么需要它：403 MODEL_ACCESS_DENIED 说的是"这个号没开通这个模型"，
// 换号大概率能救，罚整个号等于把所有模型一起关掉；反过来，如果直接返回客户端，
// 一个未开通的模型就能让整个池子对合法请求失效。
type ModelBlock struct {
	Model  string    `json:"model"`
	Until  time.Time `json:"until"`
	Reason string    `json:"reason,omitempty"`
	// Kind: denied（模型未开通，长退避）| limited（模型侧限流，短冷却）
	Kind string `json:"kind"`
	Hits int    `json:"hits,omitempty"`
}

func (b ModelBlock) Left(now time.Time) int {
	if d := int(b.Until.Sub(now).Seconds()); d > 0 {
		return d
	}
	return 0
}

// Key 是一个上游凭证的完整运行时状态。
type Key struct {
	ID     string `json:"id"`
	Name   string `json:"name"`
	Secret string `json:"secret"`

	Weight  float64 `json:"weight"`  // 手动权重倍率
	Enabled bool    `json:"enabled"` // 人工启停
	Budget  float64 `json:"budget"`  // 充值额（CNY），0 = 未知

	State          State     `json:"state"`
	CooldownUntil  time.Time `json:"cooldown_until"`
	CooldownReason string    `json:"cooldown_reason,omitempty"`
	FailStreak     int       `json:"fail_streak"`
	// RateStreak 是连续软限流次数（独立于熔断计数：429 是"配额用完"，
	// 不该被 5xx 的熔断计数污染，也不该在熔断触发时被清零）。
	RateStreak int `json:"rate_streak,omitempty"`
	// QuotaHits / BanHits 是"欠费"与"被冻结"的连续命中次数，只用来放大退避。
	// 不复用 RateStreak：同一个号又欠费又被限流时，两套节律互不污染。
	QuotaHits int `json:"quota_hits,omitempty"`
	BanHits   int `json:"ban_hits,omitempty"`
	// 熔断与冷却解耦：冷却按错误类别给固定时长，熔断只对"反复失败"逐次加长。
	BreakerUntil time.Time `json:"breaker_until,omitempty"`
	BreakerCount int       `json:"breaker_count,omitempty"`
	LastError    string    `json:"last_error"`
	LastErrorAt  time.Time `json:"last_error_at"`

	SpentCNY         float64 `json:"spent_cny"`
	Requests         int64   `json:"requests"`
	Errors           int64   `json:"errors"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`

	LastUsedAt time.Time `json:"last_used_at"`
	LastOKAt   time.Time `json:"last_ok_at"`
	CreatedAt  time.Time `json:"created_at"`

	// ModelBlocks 是 (号, 模型) 级封禁表；空 = 没有模型被封。
	ModelBlocks map[string]ModelBlock `json:"model_blocks,omitempty"`

	// 运行态，不落盘。
	inflight   int
	lastPickAt time.Time
	seq        uint64
}

// Balance 返回本地记账口径的剩余额度；Budget<=0 表示充值额未知。
func (k *Key) Balance() float64 {
	if k.Budget <= 0 {
		return -1
	}
	b := k.Budget - k.SpentCNY
	if b < 0 {
		return 0
	}
	return b
}

func (k *Key) Masked() string {
	s := k.Secret
	if len(s) <= 14 {
		return "****"
	}
	return s[:10] + "..." + s[len(s)-4:]
}

// usable 是账号级健康判定：不看模型。
func (k *Key) usable(now time.Time) bool {
	if !k.Enabled || k.State == StateDisabled || k.State == StateInvalid ||
		k.State == StateExhausted || k.State == StateBlocked {
		return false
	}
	if !k.BreakerUntil.IsZero() && now.Before(k.BreakerUntil) {
		return false
	}
	if k.State == StateCooling && now.Before(k.CooldownUntil) {
		return false
	}
	return true
}

// modelBlocked 返回该号在该模型上的有效封禁（顺手清过期项）。
func (k *Key) modelBlocked(now time.Time, model string) (ModelBlock, bool) {
	if model == "" || len(k.ModelBlocks) == 0 {
		return ModelBlock{}, false
	}
	b, ok := k.ModelBlocks[model]
	if !ok {
		return ModelBlock{}, false
	}
	if !now.Before(b.Until) {
		delete(k.ModelBlocks, model)
		return ModelBlock{}, false
	}
	return b, true
}

// usableFor 是选号口径：账号级健康 + 该模型未被封。
func (k *Key) usableFor(now time.Time, model string) bool {
	if !k.usable(now) {
		return false
	}
	if _, blocked := k.modelBlocked(now, model); blocked {
		return false
	}
	return true
}

// usableForBatch 是"够不够格占一个活跃窗口名额"。
//
// 比 usable 多看一眼余额：钱跑空的号留在窗口里只是占位——
// 面板显示 12 个在用，实际只有 11 个能派单。让它把名额让给下一个，
// 这就是"1 个没钱了自动补 1 个"的判定依据。
// Budget<=0 表示未填充值额（未知），不参与余额判断。
func (k *Key) usableForBatch(now time.Time, floor float64) bool {
	if !k.usable(now) {
		return false
	}
	if k.Budget > 0 && k.Balance() <= floor {
		return false
	}
	return true
}

// expiry 返回该号最早恢复可用的时刻（冷却/熔断），无视模型级封禁。
// 零值表示"没有时间型等待"。
func (k *Key) expiry(now time.Time) time.Time {
	var out time.Time
	for _, t := range []time.Time{k.CooldownUntil, k.BreakerUntil} {
		if t.After(now) && (out.IsZero() || t.Before(out)) {
			out = t
		}
	}
	return out
}

func (k *Key) stateLabel() State {
	if !k.Enabled {
		return StateDisabled
	}
	return k.State
}

// View 是面板看到的号快照。
type View struct {
	ID               string  `json:"id"`
	Name             string  `json:"name"`
	Masked           string  `json:"masked"`
	Enabled          bool    `json:"enabled"`
	State            string  `json:"state"`
	Weight           float64 `json:"weight"`
	Budget           float64 `json:"budget"`
	Spent            float64 `json:"spent"`
	Balance          float64 `json:"balance"` // -1 = 未知
	Requests         int64   `json:"requests"`
	Errors           int64   `json:"errors"`
	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	FailStreak       int     `json:"fail_streak"`
	// 连续欠费 / 冻结次数：决定退避已经放大到第几档，面板上解释"为什么冷却变长了"。
	QuotaHits      int          `json:"quota_hits"`
	BanHits        int          `json:"ban_hits"`
	CooldownLeft   int          `json:"cooldown_left"`
	CooldownReason string       `json:"cooldown_reason,omitempty"`
	BreakerLeft    int          `json:"breaker_left"`
	LastError      string       `json:"last_error"`
	LastUsedAt     string       `json:"last_used_at"`
	LastOKAt       string       `json:"last_ok_at"`
	Inflight       int          `json:"inflight"`
	StickyTasks    int          `json:"sticky_tasks"`
	Share          float64      `json:"share"` // 消耗占比 0..1
	WeightNow      float64      `json:"weight_now"`
	InBatch        bool         `json:"in_batch"`
	Sessions       int          `json:"sessions"`
	ModelBlocks    []ModelBlock `json:"model_blocks"`
}

// Lease 是一次选号的结果。Release 必须调用一次，否则在途计数会永久泄漏。
type Lease struct {
	KeyID  string
	Secret string
	Name   string
	// Fallback 表示这是"全池冷却时挑最早恢复的号试一次"，日志里要标出来。
	Fallback       bool
	FallbackReason string
	released       bool
}

func (l *Lease) String() string {
	if l == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s(%s)", l.Name, l.KeyID)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return strings.TrimSpace(s[:n]) + "…"
}
