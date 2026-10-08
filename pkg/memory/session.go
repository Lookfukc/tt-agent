package memory

import (
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// sessionLog 单会话消息日志：消息本体 + 对齐的 token 估算缓存 + 入栈时间
//
// token 在追加时一次算好缓存，装填预算只做查表求和，
// 不再对全量历史逐条重数（旧实现每次 Recent 都 O(全部字符) 地重算）
type sessionLog struct {
	msgs []core.Message
	toks []int64     // 与 msgs 对齐：单条消息的 token 估算
	tss  []time.Time // 与 msgs 对齐：入栈时间，落盘与重写时携带
}

// add 追加消息并补齐缓存
//
// 每条单独过计数器再求和，与旧实现"按组整体计数"相比：
// 单条下限（估算值不小于消息数）逐条生效，总量只会偏大不会偏小，
// 预算截断宁少勿超的方向不变
// c: token 估算器
// now: 本批消息的入栈时间
// msgs: 待追加消息
func (l *sessionLog) add(c core.TokenCounter, now time.Time, msgs ...core.Message) {
	for _, m := range msgs {
		l.msgs = append(l.msgs, m)
		l.toks = append(l.toks, c.Count([]core.Message{m}))
		l.tss = append(l.tss, now)
	}
}

// split 取回不超预算的最近历史
//
// 语义与旧 pickWithinBudget 完全一致：系统消息始终保留在前且不参与
// 丢弃，其余按原子组从新到旧装填。assistant(tool_calls) 与其后续
// tool 消息同进同退——逐条装填会在"tool 结果装得下、父消息装不下"时
// 产出孤儿 tool 消息，Anthropic/OpenAI 对无主 tool 消息一律 400，
// 会话就此永久损坏
// returns: 保留的按时间序消息、被截断丢弃的按时间序消息
func (l *sessionLog) split(budget int64) (kept, dropped []core.Message) {
	var system []core.Message
	var sysToks int64
	var restIdx []int
	for i, m := range l.msgs {
		if m.Role == core.RoleSystem {
			system = append(system, m)
			sysToks += l.toks[i]
		} else {
			restIdx = append(restIdx, i)
		}
	}

	// 切原子组（下标粒度）：tool 消息归入其前最近的 assistant(tool_calls)
	type msgGroup struct{ idx []int }
	var groups []msgGroup
	for i := 0; i < len(restIdx); {
		g := msgGroup{idx: []int{restIdx[i]}}
		i++
		if len(l.msgs[g.idx[0]].ToolCalls) > 0 {
			for i < len(restIdx) && l.msgs[restIdx[i]].Role == core.RoleTool {
				g.idx = append(g.idx, restIdx[i])
				i++
			}
		}
		groups = append(groups, g)
	}

	used := sysToks
	keepFrom := len(groups)
	for i := len(groups) - 1; i >= 0; i-- {
		var cost int64
		for _, idx := range groups[i].idx {
			cost += l.toks[idx]
		}
		if used+cost > budget {
			break
		}
		used += cost
		keepFrom = i
	}

	for _, g := range groups[keepFrom:] {
		for _, idx := range g.idx {
			kept = append(kept, l.msgs[idx])
		}
	}
	for _, g := range groups[:keepFrom] {
		for _, idx := range g.idx {
			dropped = append(dropped, l.msgs[idx])
		}
	}
	if len(kept) == 0 && len(groups) > 0 {
		// 单组都装不下的极端预算：宁可超预算也保留最新一组，
		// 空历史比超窗更不可恢复
		for _, idx := range groups[len(groups)-1].idx {
			kept = append(kept, l.msgs[idx])
		}
		dropped = dropped[:len(dropped)-len(kept)]
	}
	return append(system, kept...), dropped
}

// trim 物理丢弃最旧的 n 条非系统消息
//
// 系统消息不参与计数也绝不删除，与 split 的丢弃侧语义严格对齐：
// split 丢弃的恰好是最旧的若干条非系统消息（且按原子组整组丢弃，
// 不会切破 assistant(tool_calls)+tool 配对），先 split 后 trim 安全
func (l *sessionLog) trim(n int) {
	if n <= 0 {
		return
	}
	msgs := make([]core.Message, 0, len(l.msgs))
	toks := make([]int64, 0, len(l.toks))
	tss := make([]time.Time, 0, len(l.tss))
	for i, m := range l.msgs {
		if n > 0 && m.Role != core.RoleSystem {
			n--
			continue
		}
		msgs = append(msgs, m)
		toks = append(toks, l.toks[i])
		tss = append(tss, l.tss[i])
	}
	l.msgs, l.toks, l.tss = msgs, toks, tss
}

// count 非系统消息条数，即 trim 可作用的范围
// returns: 非系统消息数
func (l *sessionLog) count() int {
	n := 0
	for _, m := range l.msgs {
		if m.Role != core.RoleSystem {
			n++
		}
	}
	return n
}
