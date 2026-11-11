package panel

// 批量清理停用号。
//
// 号池里最脏的场景是"充完值回来，发现 20 个号全欠费"——一个个点删除要 20 次确认。
// 但批量删是不可逆的：密钥只在 state.json 里存一份，删掉就只能翻备份。
// 所以这个接口只认「已经不再派单」的终态，并且支持 dry_run 先看清单。

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"trpool/internal/pool"
)

// prunableByDefault 是"停用号"的口径：欠费、冻结、人工停用。
//
// 不含 invalid（401 密钥失效）：那类号还可能因为上游抽风被探活救回来，
// 想删得显式勾上，免得一次手滑把能救的号清掉。
var prunableByDefault = []pool.State{pool.StateExhausted, pool.StateBlocked, pool.StateDisabled}

type pruneReq struct {
	States []string `json:"states"`  // 留空 = 欠费 + 冻结 + 人工停用
	DryRun bool     `json:"dry_run"` // 只报清单，不真删
}

type pruneResp struct {
	OK      bool     `json:"ok"`
	DryRun  bool     `json:"dry_run"`
	Matched int      `json:"matched"`
	Removed int      `json:"removed"`
	States  []string `json:"states"`
	Names   []string `json:"names"`
}

func (p *Panel) handlePrune(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "方法不支持")
		return
	}
	var req pruneReq
	// 请求体是几个状态名，16 KiB 的余量足够，也挡住拿大 body 打内存的路子。
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16))
	if err := dec.Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}

	states := prunableByDefault
	if len(req.States) > 0 {
		states = make([]pool.State, 0, len(req.States))
		for _, s := range req.States {
			states = append(states, pool.State(s))
		}
	}
	names, err := p.d.Pool.Prune(states, req.DryRun)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(names) > 0 && !req.DryRun {
		p.persist()
	}
	out := pruneResp{OK: true, DryRun: req.DryRun, Matched: len(names), Names: names}
	if !req.DryRun {
		out.Removed = len(names)
	}
	out.States = make([]string, len(states))
	for i, s := range states {
		out.States[i] = string(s)
	}
	if out.Names == nil {
		out.Names = []string{}
	}
	writeJSON(w, http.StatusOK, out)
}
