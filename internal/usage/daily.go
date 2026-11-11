package usage

import (
	"sort"
	"sync"
	"time"
)

// Day 是一天的账。
type Day struct {
	Date       string  `json:"date"`
	Requests   int64   `json:"requests"`
	Errors     int64   `json:"errors"`
	Prompt     int64   `json:"prompt_tokens"`
	Completion int64   `json:"completion_tokens"`
	Cost       float64 `json:"cost"`
}

// Daily 是按本地日期切的账本。落盘、跨重启保留；面板的"今日/近 14 日"都取自这里。
type Daily struct {
	mu   sync.Mutex
	Days map[string]*Day `json:"days"`
}

func NewDaily() *Daily { return &Daily{Days: map[string]*Day{}} }

// Add 记一笔。now 用调用方时间，按本地时区切日（容器里 TZ=Asia/Shanghai）。
func (d *Daily) Add(now time.Time, prompt, completion int64, cost float64, isErr bool) {
	key := now.Format("2006-01-02")
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Days == nil {
		d.Days = map[string]*Day{}
	}
	day := d.Days[key]
	if day == nil {
		day = &Day{Date: key}
		d.Days[key] = day
	}
	day.Requests++
	if isErr {
		day.Errors++
	}
	day.Prompt += prompt
	day.Completion += completion
	day.Cost += cost
}

// Last 返回最近 n 天，按日期倒序（新的在前）。
func (d *Daily) Last(n int) []Day {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([]Day, 0, len(d.Days))
	for _, v := range d.Days {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Date > out[j].Date })
	if n > 0 && len(out) > n {
		out = out[:n]
	}
	return out
}

// Trim 只保留最近 n 天，防止账本无限增长（默认 400 天足够回溯）。
func (d *Daily) Trim(n int) {
	if n <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.Days) <= n {
		return
	}
	dates := make([]string, 0, len(d.Days))
	for k := range d.Days {
		dates = append(dates, k)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(dates)))
	for _, k := range dates[n:] {
		delete(d.Days, k)
	}
}

// Restore 用状态文件里的账本替换当前内容（损坏时保持空表）。
func (d *Daily) Restore(in map[string]*Day) {
	if in == nil {
		return
	}
	d.mu.Lock()
	d.Days = in
	d.mu.Unlock()
}

// Total 汇总所有天，面板总览用。
func (d *Daily) Total() (requests, errors, prompt, completion int64, cost float64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, v := range d.Days {
		requests += v.Requests
		errors += v.Errors
		prompt += v.Prompt
		completion += v.Completion
		cost += v.Cost
	}
	return
}
