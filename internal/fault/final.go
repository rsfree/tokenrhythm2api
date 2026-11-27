package fault

// 一轮请求会在池子里连续失败多次，"最后一个失败"不应该直接当成对外结论：
// 号池里可能同时有欠费的号、被冻结的号、正在抖的上游。对外只给一个结论，
// 选谁取决于**对调用方的行动价值**，不是取决于谁排在最后。

// finalRank 是全池失败后的对外优先级（越小越优先）。
//
// 依据：
//   - 请求病排最前——它明确告诉调用方"改请求"，比让他重试一百次都有用；
//   - insufficient_quota 排第二——它明确告诉调用方"等也没用"，比给个 429 诚实；
//   - 渠道病在限流之前——上游真的坏了比"你太快"更该被优先告知；
//   - 全部限流时仍返回 rate_limit_exceeded（确实会自愈，别伪造成别的）。
var finalRank = map[Canonical]int{
	CInvalidRequest:    0,
	CContextLength:     0,
	CContentPolicy:     0,
	CRequestTooLarge:   0,
	CModelNotFound:     1,
	CInsufficientQuota: 2,
	CAuthentication:    3,
	CPermissionDenied:  4,
	CServerOverloaded:  5,
	CApiError:          5,
	CUpstreamTimeout:   5,
	CRateLimit:         6,
	CUnknown:           7,
}

// PickFinal 从本轮所有失败里挑出对外要返回的那一个。
// 第二个返回值表示是否挑到了（一个都没试过时是 false）。
func PickFinal(vs []Verdict) (Verdict, bool) {
	if len(vs) == 0 {
		return Verdict{}, false
	}
	best := vs[0]
	bestRank := rankOf(best.Canonical)
	for _, v := range vs[1:] {
		r := rankOf(v.Canonical)
		if r < bestRank {
			best, bestRank = v, r
		}
	}
	return best, true
}

func rankOf(c Canonical) int {
	if r, ok := finalRank[c]; ok {
		return r
	}
	return 7
}

// RequestFault 报告这个结论是否是"请求本身有病"（换号重试毫无意义）。
func (v Verdict) RequestFault() bool { return rankOf(v.Canonical) == 0 }

// ChannelFault 报告这个结论是否是"整条上游在抖"（该计入熔断）。
func (v Verdict) ChannelFault() bool { return rankOf(v.Canonical) == 5 }

// SameKeyRetryWorth 判断"渠道病是否值得在同一个账号上立刻重试一次"。
//
// 两种情况不值得：
//   - 上游明确给了 Retry-After：它自己知道要等多久，150ms 后重试是白撞；
//   - 超时：这一跳已经吃掉了整个 timeout 预算，再等一轮等于把客户端挂更久，
//     换号更快也更可能成功。
func (v Verdict) SameKeyRetryWorth() bool {
	if !v.ChannelFault() || v.RetryAfterHint {
		return false
	}
	return v.Canonical != CUpstreamTimeout
}
