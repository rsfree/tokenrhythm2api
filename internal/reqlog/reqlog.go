// Package reqlog 是请求日志：内存环给面板实时看，JSONL 归档给"跨重启、跨天"排查。
//
// 归档只写请求元数据（时间、号、模型、状态、耗时、token、成本、trace_id），
// 不写提示词、响应正文和凭证——排障需要的是"谁在什么时候把哪个请求打成了 429"，
// 不是用户聊了什么。
package reqlog

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Entry 是一条请求记录。
type Entry struct {
	Time      time.Time `json:"time"`
	RequestID string    `json:"request_id,omitempty"`

	KeyID   string `json:"key_id"`
	KeyName string `json:"key_name"`
	Model   string `json:"model"`
	Path    string `json:"path"`

	Status    int   `json:"status"`
	LatencyMS int64 `json:"latency_ms"`
	TTFBMS    int64 `json:"ttfb_ms,omitempty"`

	PromptTokens     int64   `json:"prompt_tokens"`
	CompletionTokens int64   `json:"completion_tokens"`
	Cost             float64 `json:"cost"`

	Stream   bool   `json:"stream"`
	Attempt  int    `json:"attempt"`
	Fallback bool   `json:"fallback,omitempty"`
	TraceID  string `json:"trace_id,omitempty"`
	Error    string `json:"error,omitempty"`

	ClientIP string `json:"client_ip,omitempty"`
	UA       string `json:"ua,omitempty"`
}

// Filter 是归档查询条件，字段与面板筛选一一对应。
type Filter struct {
	Status string // all | ok | bad
	KeyID  string
	Model  string
	Text   string
	Since  time.Time
	Until  time.Time
	Limit  int
}

func (f Filter) match(e Entry) bool {
	bad := e.Error != "" || e.Status >= 400
	switch f.Status {
	case "bad":
		if !bad {
			return false
		}
	case "ok":
		if bad {
			return false
		}
	}
	if f.KeyID != "" && e.KeyID != f.KeyID {
		return false
	}
	if f.Model != "" && e.Model != f.Model {
		return false
	}
	if !f.Since.IsZero() && e.Time.Before(f.Since) {
		return false
	}
	if !f.Until.IsZero() && e.Time.After(f.Until) {
		return false
	}
	if f.Text != "" {
		t := strings.ToLower(f.Text)
		hay := strings.ToLower(e.RequestID + " " + e.TraceID + " " + e.Error + " " + e.Model + " " + e.KeyName + " " + e.Path)
		if !strings.Contains(hay, t) {
			return false
		}
	}
	return true
}

// Config 是日志组件的开关。
type Config struct {
	RingSize   int
	ArchiveDir string
	Archive    bool
	RetainDays int
	MaxMB      int
}

type Recorder struct {
	mu   sync.RWMutex
	buf  []Entry
	size int
	pos  int
	full bool

	archive *archive
	// OnRecord 在每条日志写入后调用（环内写完后、锁外）。
	OnRecord func()
}

func New(cfg Config) *Recorder {
	size := cfg.RingSize
	if size <= 0 {
		size = 400
	}
	r := &Recorder{buf: make([]Entry, size), size: size}
	if cfg.Archive && cfg.ArchiveDir != "" {
		r.archive = newArchive(cfg.ArchiveDir, cfg.RetainDays, cfg.MaxMB)
	}
	return r
}

// Record 写一条日志：先环，再尽力归档（归档失败不影响主流程）。
func (r *Recorder) Record(e Entry) {
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	r.mu.Lock()
	r.buf[r.pos] = e
	r.pos = (r.pos + 1) % r.size
	if r.pos == 0 {
		r.full = true
	}
	cb := r.OnRecord
	r.mu.Unlock()

	if r.archive != nil {
		r.archive.write(e)
	}
	if cb != nil {
		cb()
	}
}

// Restore 用历史日志回填环。olds 按时间倒序（Recent 的输出格式），
// 这里反着灌，保证灌完再 Recent 出来顺序不变。
func (r *Recorder) Restore(olds []Entry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(olds) - 1; i >= 0; i-- {
		r.buf[r.pos] = olds[i]
		r.pos = (r.pos + 1) % r.size
		if r.pos == 0 {
			r.full = true
		}
	}
}

// Recent 返回最近 n 条，按时间倒序（新的在前）。
func (r *Recorder) Recent(n int) []Entry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	total := r.pos
	if r.full {
		total = r.size
	}
	if n <= 0 || n > total {
		n = total
	}
	out := make([]Entry, 0, n)
	for i := 0; i < n; i++ {
		idx := (r.pos - 1 - i + r.size) % r.size
		out = append(out, r.buf[idx])
	}
	return out
}

// Query 从归档里按条件取记录（新的在前）。limit<=0 时默认 500，上限 5000。
func (r *Recorder) Query(f Filter) ([]Entry, error) {
	if r.archive == nil {
		return nil, errors.New("请求归档未启用")
	}
	// 先把缓冲刷到磁盘：写盘是按 32 条批量 flush 的，不刷的话面板切到
	// "历史归档"会看不到刚发生的请求（尤其是低频使用的小池子）。
	r.archive.flush()
	limit := f.Limit
	if limit <= 0 {
		limit = 500
	}
	if limit > 5000 {
		limit = 5000
	}
	return r.archive.query(f, limit)
}

func (r *Recorder) ArchiveMeta() map[string]any {
	if r.archive == nil {
		return map[string]any{"enabled": false}
	}
	return r.archive.meta()
}

// Prune 手动触发归档清理（启动与每小时各一次）。
func (r *Recorder) Prune() {
	if r.archive != nil {
		r.archive.prune()
	}
}

func (r *Recorder) Close() {
	if r.archive != nil {
		r.archive.close()
	}
}

// NewRequestID 生成 32 位 hex 请求 id（与账号无关，纯随机）。
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// ---------- 归档 ----------

type archive struct {
	mu       sync.Mutex
	dir      string
	retain   int
	maxBytes int64
	day      string
	f        *os.File
	w        *bufio.Writer
	written  int64
	lastErr  string
	files    int
}

func newArchive(dir string, retainDays, maxMB int) *archive {
	a := &archive{dir: dir, retain: retainDays, maxBytes: int64(maxMB) << 20}
	if a.retain <= 0 {
		a.retain = 7
	}
	if a.maxBytes <= 0 {
		a.maxBytes = 100 << 20
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		a.lastErr = err.Error()
		return a
	}
	a.prune()
	return a
}

func (a *archive) pathFor(day string) string {
	return filepath.Join(a.dir, "requests-"+day+".jsonl")
}

func (a *archive) write(e Entry) {
	day := e.Time.Format("2006-01-02")
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.f == nil || a.day != day {
		a.rotateLocked(day)
	}
	if a.f == nil {
		return
	}
	b, err := json.Marshal(e)
	if err != nil {
		a.lastErr = err.Error()
		return
	}
	if _, err := a.w.Write(append(b, '\n')); err != nil {
		a.lastErr = err.Error()
		return
	}
	a.written++
	// 每 32 条刷一次：既不至于每请求一次 fsync，也不会在崩溃时丢太多。
	if a.written%32 == 0 {
		if err := a.w.Flush(); err != nil {
			a.lastErr = err.Error()
		}
	}
}

func (a *archive) rotateLocked(day string) {
	if a.w != nil {
		a.flushLocked()
	}
	if a.f != nil {
		_ = a.f.Close()
		a.f = nil
	}
	f, err := os.OpenFile(a.pathFor(day), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		a.lastErr = err.Error()
		return
	}
	a.f = f
	a.w = bufio.NewWriterSize(f, 32<<10)
	a.day = day
}

func (a *archive) close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
	if a.f != nil {
		_ = a.f.Close()
		a.f = nil
	}
}

// flush 把缓冲写出去，供"读之前"和"停机前"调用。
func (a *archive) flush() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.flushLocked()
}

func (a *archive) flushLocked() {
	if a.w != nil {
		if err := a.w.Flush(); err != nil {
			a.lastErr = err.Error()
		}
	}
}

// prune 删过期文件；再按总量上限删最旧，直到不超。
func (a *archive) prune() {
	entries, err := os.ReadDir(a.dir)
	if err != nil {
		a.mu.Lock()
		a.lastErr = err.Error()
		a.mu.Unlock()
		return
	}
	type fileInfo struct {
		path string
		day  string
		size int64
	}
	var files []fileInfo
	var total int64
	cutoff := time.Now().AddDate(0, 0, -a.retain).Format("2006-01-02")
	for _, de := range entries {
		name := de.Name()
		if de.IsDir() || !strings.HasPrefix(name, "requests-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		day := strings.TrimSuffix(strings.TrimPrefix(name, "requests-"), ".jsonl")
		if _, err := time.Parse("2006-01-02", day); err != nil {
			continue
		}
		info, err := de.Info()
		if err != nil {
			continue
		}
		path := filepath.Join(a.dir, name)
		if day < cutoff {
			_ = os.Remove(path)
			continue
		}
		files = append(files, fileInfo{path: path, day: day, size: info.Size()})
		total += info.Size()
	}
	sort.Slice(files, func(i, j int) bool { return files[i].day < files[j].day })
	for _, fi := range files {
		if total <= a.maxBytes {
			break
		}
		if err := os.Remove(fi.path); err == nil {
			total -= fi.size
			files = files[1:]
		}
	}
	a.mu.Lock()
	a.files = len(files)
	if a.lastErr == "" && err != nil {
		a.lastErr = err.Error()
	}
	a.mu.Unlock()
}

func (a *archive) query(f Filter, limit int) ([]Entry, error) {
	entries, err := os.ReadDir(a.dir)
	if err != nil {
		return nil, err
	}
	days := make([]string, 0, len(entries))
	for _, de := range entries {
		name := de.Name()
		if de.IsDir() || !strings.HasPrefix(name, "requests-") || !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		day := strings.TrimSuffix(strings.TrimPrefix(name, "requests-"), ".jsonl")
		if _, err := time.Parse("2006-01-02", day); err != nil {
			continue
		}
		if !f.Since.IsZero() && day < f.Since.Format("2006-01-02") {
			continue
		}
		if !f.Until.IsZero() && day > f.Until.Format("2006-01-02") {
			continue
		}
		days = append(days, day)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(days)))

	out := make([]Entry, 0, limit)
	for _, day := range days {
		batch, err := readFile(a.pathFor(day), f)
		if err != nil {
			continue
		}
		// 同一天内也是新的在前：文件是追加写，倒着取。
		for i := len(batch) - 1; i >= 0; i-- {
			out = append(out, batch[i])
			if len(out) >= limit {
				return out, nil
			}
		}
	}
	return out, nil
}

func readFile(path string, f Filter) ([]Entry, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	sc := bufio.NewScanner(fh)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	var out []Entry
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var e Entry
		if err := json.Unmarshal(line, &e); err != nil {
			continue
		}
		if f.match(e) {
			out = append(out, e)
		}
	}
	return out, sc.Err()
}

func (a *archive) meta() map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	m := map[string]any{
		"enabled":      true,
		"dir":          a.dir,
		"retain_days":  a.retain,
		"max_mb":       a.maxBytes >> 20,
		"current_file": "",
	}
	if a.day != "" {
		m["current_file"] = a.pathFor(a.day)
	}
	if a.lastErr != "" {
		m["last_error"] = a.lastErr
	}
	return m
}
