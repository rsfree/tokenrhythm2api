package panel

// 批量加号：一次粘贴一整列 sk_tr_ 密钥。
//
// 前端把 textarea 原文丢过来，解析放在服务端做——这样 curl / 脚本也能用同一个接口，
// 不用在前端和后端各写一份"一行怎么切"的规则。

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"unicode/utf8"

	"trpool/internal/pool"
)

// maxBatchKeys 单次导入上限。真有一千个号要导，分几次也没什么损失，
// 但一次吃掉整个请求体的内存就得防。
const maxBatchKeys = 500

type batchKeyReq struct {
	Secrets string   `json:"secrets"`     // 每行一个；支持「名称=密钥」
	Prefix  string   `json:"name_prefix"` // 没给名称时的前缀，默认 key-
	Budget  *float64 `json:"budget"`      // 统一充值额
	Weight  *float64 `json:"weight"`      // 统一权重
}

type batchItem struct {
	Line  int           `json:"line"`
	Draft pool.KeyDraft `json:"-"`
}

type batchItemResult struct {
	Line  int    `json:"line"`
	Name  string `json:"name,omitempty"`
	ID    string `json:"id,omitempty"`
	Error string `json:"error,omitempty"`
}

func (p *Panel) handleKeysBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	var req batchKeyReq
	// 500 行 × 每行百来字节，1 MiB 的余量足够，也挡住了拿大 body 打内存的路子。
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}

	budget, weight := 0.0, 1.0
	if req.Budget != nil {
		budget = *req.Budget
	}
	if req.Weight != nil && *req.Weight > 0 {
		weight = *req.Weight
	}
	items := parseKeyLines(req.Secrets, req.Prefix, p.d.Pool.Names(), weight, budget)
	if len(items) == 0 {
		writeErr(w, http.StatusBadRequest, "没有解析到任何密钥：每行一个 sk_tr_ 开头的密钥")
		return
	}
	if len(items) > maxBatchKeys {
		writeErr(w, http.StatusBadRequest, fmt.Sprintf("一次最多 %d 个号，当前 %d 个，请分批导入", maxBatchKeys, len(items)))
		return
	}

	drafts := make([]pool.KeyDraft, len(items))
	for i, it := range items {
		drafts[i] = it.Draft
	}
	res := p.d.Pool.AddMany(drafts)

	added := 0
	out := make([]batchItemResult, len(res))
	for i, r := range res {
		item := batchItemResult{Line: items[i].Line, Name: r.Name}
		if r.Error != nil {
			item.Error = r.Error.Error()
		} else {
			item.ID = r.Key.ID
			added++
		}
		out[i] = item
	}
	if added > 0 {
		p.persist()
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"total":   len(out),
		"added":   added,
		"skipped": len(out) - added,
		"results": out,
	})
}

// parseKeyLines 把粘进来的文本切成待入池的号。
//
// 认得的写法（前后空白、列表符号「- 」、成对引号、行尾逗号/分号都会被清掉）：
//
//	sk_tr_xxx                     整行就是密钥
//	主账号=sk_tr_xxx              「=」分隔
//	主账号,sk_tr_xxx              「,」或「，」分隔（从表格里整列复制常见）
//	主账号<TAB>sk_tr_xxx          制表符分隔
//	主账号 sk_tr_xxx              空格分隔
//
// 谁在左谁在右不靠猜：带 sk- / sk_ 前缀的那半永远被当密钥，另一半当名称。
// 只给了密钥的行按「前缀 + 序号」自动命名：序号从 1 开始，跳过池内已占用的名称
// （导 20 个号就该看到 tr-1…tr-20，而不是接着池里条数从 tr-4 起）。
func parseKeyLines(text, prefix string, existing []string, weight, budget float64) []batchItem {
	if strings.TrimSpace(prefix) == "" {
		prefix = "key-"
	}
	taken := make(map[string]bool, len(existing))
	for _, n := range existing {
		taken[n] = true
	}
	seq := 1
	var out []batchItem
	for i, raw := range strings.Split(text, "\n") {
		name, secret := splitKeyLine(raw)
		// 全空的占位行不算错误，直接跳过（粘贴时常带空行）
		if name == "" && secret == "" {
			continue
		}
		if name == "" {
			for {
				name = fmt.Sprintf("%s%d", prefix, seq)
				seq++
				if !taken[name] {
					break
				}
			}
			taken[name] = true
		}
		out = append(out, batchItem{
			Line:  i + 1,
			Draft: pool.KeyDraft{Name: name, Secret: secret, Weight: weight, Budget: budget},
		})
	}
	return out
}

// poolCounts 只为拿总数，抽个小接口免得解析函数依赖整个 Pool。
type poolCounts struct{ Total int }

func splitKeyLine(raw string) (name, secret string) {
	line := strings.TrimSpace(raw)
	line = strings.TrimPrefix(line, "- ")
	line = strings.TrimSpace(line)
	line = strings.Trim(line, `"'`)
	line = strings.TrimSpace(line)
	// 行尾的逗号/分号多半是"从别处复制时带下来的"，不是密钥的一部分
	line = strings.TrimRight(line, ",;，；")
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", ""
	}

	if i := strings.IndexAny(line, "=\t,，"); i >= 0 {
		// 分隔符可能是全角逗号（3 字节），不能简单地 i+1
		_, size := utf8.DecodeRuneInString(line[i:])
		a := strings.TrimSpace(line[:i])
		b := strings.TrimSpace(line[i+size:])
		a = strings.Trim(a, `"'`)
		b = strings.Trim(b, `"'`)
		switch {
		case looksLikeSecret(b) && !looksLikeSecret(a):
			return a, b // 名称=密钥
		case looksLikeSecret(a) && !looksLikeSecret(b):
			return b, a // 密钥=名称（有人把表格列的顺序放反了）
		default:
			return a, b // 都不像密钥：按「名称=密钥」记，交给形态校验报错
		}
	}

	// 空格分隔：只有右侧确实像密钥时才拆，否则整行当密钥（让它按"形态不对"报错，
	// 比悄悄截断成一个谁也不认识的名称好排查）。
	if f := strings.Fields(line); len(f) == 2 && looksLikeSecret(f[1]) && !looksLikeSecret(f[0]) {
		return f[0], f[1]
	}
	return "", line
}

func looksLikeSecret(s string) bool {
	return strings.HasPrefix(s, "sk-") || strings.HasPrefix(s, "sk_")
}
