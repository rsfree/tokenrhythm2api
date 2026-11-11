package usage

import (
	"testing"
	"time"
)

func TestParseOpenAI(t *testing.T) {
	u, ok := Parse([]byte(`{"usage":{"prompt_tokens":65,"completion_tokens":16,"prompt_tokens_details":{"cached_tokens":32}}}`))
	if !ok || u.Prompt != 65 || u.Completion != 16 || u.Cached != 32 {
		t.Fatalf("got %+v ok=%v", u, ok)
	}
}

func TestParseAnthropic(t *testing.T) {
	u, ok := Parse([]byte(`{"usage":{"input_tokens":68,"output_tokens":32,"cache_read_input_tokens":10}}`))
	if !ok || u.Prompt != 68 || u.Completion != 32 || u.Cached != 10 {
		t.Fatalf("got %+v ok=%v", u, ok)
	}
}

func TestParseDirectCost(t *testing.T) {
	u, ok := Parse([]byte(`{"cost_cny":"0.0000476"}`))
	if !ok || !u.HasDirect || u.Direct != 0.0000476 {
		t.Fatalf("got %+v ok=%v", u, ok)
	}
	u, ok = Parse([]byte(`{"billing":{"total_cost_cny":0.5}}`))
	if !ok || !u.HasDirect || u.Direct != 0.5 {
		t.Fatalf("got %+v ok=%v", u, ok)
	}
}

func TestParseChunkMerge(t *testing.T) {
	var u Usage
	c1, _ := ParseChunk([]byte(`{"usage":{"prompt_tokens":10}}`))
	u.Add(c1)
	c2, _ := ParseChunk([]byte(`{"usage":{"completion_tokens":20}}`))
	u.Add(c2)
	if u.Prompt != 10 || u.Completion != 20 {
		t.Fatalf("合并丢字段: %+v", u)
	}
}

func TestPricesLoadAndCost(t *testing.T) {
	p := NewPrices()
	body := []byte(`{"data":[
	  {"id":"glm-5.1","pricing":{"prompt":"8","completion":"10","cache_read":"2","currency":"CNY"}},
	  {"id":"kimi-k2.6","pricing":{"prompt":"6.5","completion":"8"},"effective_pricing":{"prompt":"3.25","completion":"4"}}
	]}`)
	if err := p.Load(body, "k"); err != nil {
		t.Fatal(err)
	}
	if p.Count() != 2 {
		t.Fatalf("count=%d", p.Count())
	}
	glm, ok := p.Get("glm-5.1")
	if !ok || glm.Input != 8 || glm.CacheRead != 2 {
		t.Fatalf("glm 价格解析错: %+v", glm)
	}
	// effective 优先
	kimi, _ := p.Get("kimi-k2.6")
	if kimi.Input != 3.25 {
		t.Fatalf("应优先 effective_pricing: %+v", kimi)
	}
	// 前缀回落
	if _, ok := p.Get("openai/glm-5.1"); !ok {
		t.Fatal("带前缀的模型名应能回退匹配")
	}

	cost := p.Cost("glm-5.1", Usage{Prompt: 1_000_000, Completion: 1_000_000, Cached: 500_000})
	// 500k*8/M + 500k*2/M + 1M*10/M = 4+1+10 = 15
	if cost != 15 {
		t.Fatalf("成本计算错: %v", cost)
	}
	// 上游直接给了结算额就以它为准
	if c := p.Cost("glm-5.1", Usage{Direct: 1.23, HasDirect: true}); c != 1.23 {
		t.Fatalf("应以 direct 为准: %v", c)
	}
	if !p.Paid("unknown-model") {
		t.Fatal("未知模型应按收费处理")
	}
}

func TestDaily(t *testing.T) {
	d := NewDaily()
	now := time.Date(2026, 11, 11, 10, 0, 0, 0, time.Local)
	d.Add(now, 10, 20, 0.5, false)
	d.Add(now.AddDate(0, 0, -1), 1, 2, 0.1, true)
	days := d.Last(14)
	if len(days) != 2 || days[0].Date != "2026-11-11" {
		t.Fatalf("应按日期倒序: %+v", days)
	}
	reqs, errs, pt, ct, cost := d.Total()
	if reqs != 2 || errs != 1 || pt != 11 || ct != 22 || cost != 0.6 {
		t.Fatalf("汇总错: %d %d %d %d %v", reqs, errs, pt, ct, cost)
	}
	d.Trim(1)
	if len(d.Last(0)) != 1 {
		t.Fatal("Trim 应只留最近 1 天")
	}
}
