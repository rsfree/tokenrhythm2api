// Package capacity 把"最近请求的延迟分布 + 号池规模"换算成容量结论。
//
// 这页只回答一个问题：要不要加号。上游实测的配额是"每号 30 次 / 约 15 秒滑动窗口"
// （≈2 QPS），不是并发数——31 并发时正好 30 过 1 拒。所以安全在飞数 ≈ QPS × 响应耗时，
// 而不是"号数 × 某个并发上限"。
package capacity

import (
	"math"
	"sort"
	"time"

	"trpool/internal/pool"
	"trpool/internal/reqlog"
)

type KeyStat struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	State    string `json:"state"`
	Inflight int    `json:"inflight"`
	Requests int64  `json:"requests"`
	Errors   int64  `json:"errors"`
	Sessions int    `json:"sessions"`
	Cooling  int    `json:"cooling_left"`
	P50      int64  `json:"p50_ms"`
	Samples  int    `json:"samples"`
	LastOKAt string `json:"last_ok_at"`
}

type Snapshot struct {
	Keys        int       `json:"keys"`
	Healthy     int       `json:"healthy"`
	MaxPerKey   int       `json:"max_per_key"`
	Inflight    int       `json:"inflight"`
	P50         int64     `json:"p50_ms"`
	P95         int64     `json:"p95_ms"`
	Samples     int       `json:"samples"`
	KeyQPS      float64   `json:"key_qps"`
	SafePerKey  int       `json:"safe_per_key"`
	SafeTotal   int       `json:"safe_total"`
	RPM         int       `json:"rpm"`
	ObservedRPM int       `json:"observed_rpm"`
	SuccessRate float64   `json:"success_rate"`
	PerKey      []KeyStat `json:"per_key"`
}

// Compute 生成容量快照。logs 是最近请求环（新→旧），uptime 用来算实观测 RPM。
func Compute(views []pool.View, logs []reqlog.Entry, maxPerKey int, keyQPS float64, uptime time.Duration) Snapshot {
	if keyQPS <= 0 {
		keyQPS = 2
	}
	s := Snapshot{Keys: len(views), MaxPerKey: maxPerKey, KeyQPS: keyQPS}

	// 全池延迟分布：只统计成功请求（失败请求的耗时没有容量含义）。
	var all []int64
	okCount, total := 0, 0
	byKey := map[string][]int64{}
	for _, e := range logs {
		total++
		if e.Status >= 200 && e.Status < 400 && e.LatencyMS > 0 {
			okCount++
			all = append(all, e.LatencyMS)
			byKey[e.KeyID] = append(byKey[e.KeyID], e.LatencyMS)
		}
	}
	s.P50 = percentile(all, 0.50)
	s.P95 = percentile(all, 0.95)
	s.Samples = len(all)
	if total > 0 {
		s.SuccessRate = float64(okCount) / float64(total)
	}

	for _, v := range views {
		if v.Enabled && (v.State == "active" || v.State == "cooling") {
			s.Healthy++
		}
		s.Inflight += v.Inflight
		s.PerKey = append(s.PerKey, KeyStat{
			ID: v.ID, Name: v.Name, State: v.State, Inflight: v.Inflight,
			Requests: v.Requests, Errors: v.Errors, Sessions: v.Sessions,
			Cooling: v.CooldownLeft, P50: percentile(byKey[v.ID], 0.5),
			Samples: len(byKey[v.ID]), LastOKAt: v.LastOKAt,
		})
	}

	// 安全在飞数：QPS × 延迟。延迟样本不足时给 1（至少能挡住"上限设成 0 无限并发"）。
	if s.P95 > 0 {
		s.SafePerKey = int(math.Ceil(keyQPS * float64(s.P95) / 1000))
	}
	if s.SafePerKey < 1 {
		s.SafePerKey = 1
	}
	if s.Healthy > 0 {
		s.SafeTotal = s.SafePerKey * s.Healthy
	}
	s.RPM = int(math.Round(keyQPS * 60 * float64(maxInt(s.Healthy, 0))))
	if uptime > 0 {
		minutes := uptime.Minutes()
		if minutes >= 0.5 {
			s.ObservedRPM = int(math.Round(float64(okCount) / minutes))
		}
	}
	return s
}

func percentile(xs []int64, q float64) int64 {
	if len(xs) == 0 {
		return 0
	}
	cp := append([]int64(nil), xs...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	idx := int(math.Floor(q * float64(len(cp))))
	if idx >= len(cp) {
		idx = len(cp) - 1
	}
	return cp[idx]
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
