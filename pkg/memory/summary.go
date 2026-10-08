package memory

import (
	"context"
	"fmt"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Splitter 一次调用拿到预算内外的消息
//
// Summary 需要被截断消息的内容来压缩，旧路径只能以超大预算
// 全量取回再本地切分；实现方支持本接口可免掉这次全量物化
type Splitter interface {
	// Split 取回不超预算的最近消息与被截断丢弃的旧消息
	Split(ctx context.Context, sessionID string, budget int64) (kept, dropped []core.Message, err error)
}

// Trimmer 物理丢弃最旧的 n 条非系统消息
//
// n 按非系统消息计数且绝不删除系统消息，与 Splitter 丢弃侧语义
// 严格对齐：Split 丢弃的恰是最旧的若干条非系统消息（按原子组整组），
// 先 Split 后 Trim 不会切破 assistant(tool_calls)+tool 配对
type Trimmer interface {
	// Trim 丢弃最旧的 n 条非系统消息
	Trim(ctx context.Context, sessionID string, n int) error
}

// summaryCacheEntry 会话级摘要缓存
type summaryCacheEntry struct {
	// dropped 已折入摘要的消息条数；压缩态（已物理 Trim）恒为 0，
	// 非压缩态等于当前截断点。命中/回退/前进都由它与实际
	// dropped 条数的比较驱动
	dropped int
	// text 压缩产物
	text string
}

// Summary 摘要压缩记忆
//
// 包装内层 Memory：预算截断会丢弃的旧消息不再直接扔掉，
// 而是用 LLM 压缩成一段摘要作为上下文前缀。摘要由"已折入条数"
// 驱动缓存：同一截断点重复询问不重复压缩，截断点前进只压缩
// 增量并与旧摘要滚动合并
type Summary struct {
	inner   core.Memory
	llm     core.LLM
	counter core.TokenCounter
	// compact 压缩态：摘要成功后对支持 Trimmer 的内层物理删除
	// 已折入的消息，长会话的内存与磁盘占用有界
	compact bool
	// injected 是否注入了自定义估算器：注入口径必须由本层执行
	// 预算装填（Split 快路径用的是内层口径，会绕过注入）
	injected bool
	mu       sync.Mutex
	cache    map[string]*summaryCacheEntry
	locks    map[string]*sync.Mutex
}

// NewSummary 构造摘要记忆
//
// 预算估算默认沿用内置粗估而非内层 Memory 的精确计数器：
// core.Memory 接口不暴露计数器，摘要层拿不到内层实例的口径；
// 粗估偏保守（宁少勿超），截断点只会更早不会超窗。
// 内层消息不删除，完整保留（审计友好，占用无界——需要有界的
// 用 NewCompactingSummary）
// inner: 实际存储，Buffer 或 Persistent
// llm: 用于压缩的模型，建议用廉价小模型
// returns: 可用的记忆实例
func NewSummary(inner core.Memory, llm core.LLM) *Summary {
	return newSummary(inner, llm, nil, false)
}

// NewSummaryWithCounter 构造注入 token 估算器的摘要记忆
//
// 内外层估算口径必须一致时使用（如内层 Persistent 配了精确
// 分词计数器），预算装填才会与内层截断对齐
// inner: 实际存储，Buffer 或 Persistent
// llm: 用于压缩的模型，建议用廉价小模型
// c: token 估算器，nil 退化为内置粗估
// returns: 可用的记忆实例
func NewSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary {
	return newSummary(inner, llm, c, false)
}

// NewCompactingSummary 构造物理压缩态的摘要记忆
//
// 与 NewSummary 的差异：被摘要吞掉的旧消息随后从内层物理删除
// （内层需实现 Trimmer——Buffer/Persistent 均实现，TTL 装饰
// 不影响寻址），长会话的存储占用随摘要滚动收敛。
// 摘要本身只在内存缓存里，重启后丢失——压缩态下旧消息已删，
// 重启即真正失去这部分上下文，用 Persistent 内层时自行权衡
// inner: 实际存储
// llm: 用于压缩的模型，建议用廉价小模型
// returns: 可用的记忆实例
func NewCompactingSummary(inner core.Memory, llm core.LLM) *Summary {
	return newSummary(inner, llm, nil, true)
}

// NewCompactingSummaryWithCounter 注入估算器的物理压缩态摘要记忆
// inner: 实际存储
// llm: 用于压缩的模型
// c: token 估算器，nil 退化为内置粗估
// returns: 可用的记忆实例
func NewCompactingSummaryWithCounter(inner core.Memory, llm core.LLM, c core.TokenCounter) *Summary {
	return newSummary(inner, llm, c, true)
}

// newSummary 共用构造
// c: token 估算器，nil 退化为内置粗估
// compact: 是否物理压缩
// returns: 就绪实例
func newSummary(inner core.Memory, llm core.LLM, c core.TokenCounter, compact bool) *Summary {
	injected := c != nil
	if c == nil {
		c = roughCounter{}
	}
	return &Summary{
		inner:    inner,
		llm:      llm,
		counter:  c,
		compact:  compact,
		injected: injected,
		cache:    make(map[string]*summaryCacheEntry),
		locks:    make(map[string]*sync.Mutex),
	}
}

// Add 透传内层存储
func (s *Summary) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	return s.inner.Add(ctx, sessionID, msgs...)
}

// Clear 清空会话并作废摘要缓存
func (s *Summary) Clear(ctx context.Context, sessionID string) error {
	unlock := s.lockSession(sessionID)
	defer unlock()
	s.mu.Lock()
	delete(s.cache, sessionID)
	delete(s.locks, sessionID)
	s.mu.Unlock()
	return s.inner.Clear(ctx, sessionID)
}

// Recent 取回预算内消息，超出部分以摘要替代
func (s *Summary) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	// 整个"拆分 → 压缩 → Trim"按会话串行：并发 Recent 各自 Split
	// 到相同 dropped 后各自 Trim，第二次 Trim 删的就是尚未摘要的
	// 消息——数据丢失，不是浪费那么简单
	unlock := s.lockSession(sessionID)
	defer unlock()

	kept, dropped, err := s.splitInner(ctx, sessionID, budget)
	if err != nil {
		return nil, err
	}

	text := s.resolveSummary(ctx, sessionID, dropped)
	if text == "" {
		return kept, nil
	}
	// 摘要插在系统消息之后、保留历史之前
	out := make([]core.Message, 0, len(kept)+1)
	inserted := false
	for _, m := range kept {
		if !inserted && m.Role != core.RoleSystem {
			out = append(out, core.Message{
				Role:    core.RoleSystem,
				Content: "此前对话摘要：\n" + text,
			})
			inserted = true
		}
		out = append(out, m)
	}
	if !inserted {
		out = append(out, core.Message{Role: core.RoleSystem, Content: "此前对话摘要：\n" + text})
	}
	return out, nil
}

// splitInner 取预算内外的消息，优先走内层的 Split 快路径
//
// 注入了自定义估算器时不用快路径——Split 按内层口径装填预算，
// 注入口径会被绕过（L-M1 语义）。解装饰链寻址 Split 实现时途经
// TTL 必须触碰：Split 旁路了 TTL.Recent，不触碰的话活跃会话
// 会被 janitor 误判空闲而逐出
// returns: 保留消息、被截断消息
func (s *Summary) splitInner(ctx context.Context, sessionID string, budget int64) ([]core.Message, []core.Message, error) {
	if !s.injected {
		m := s.unwrapTouched(sessionID)
		if sp, ok := m.(Splitter); ok {
			return sp.Split(ctx, sessionID, budget)
		}
	}
	// 内层不支持 Split：全量取回本地切分。
	// 走 s.inner 而非解包结果，保持 TTL 触碰等装饰语义
	all, err := s.inner.Recent(ctx, sessionID, 1<<62)
	if err != nil {
		return nil, nil, err
	}
	var log sessionLog
	log.add(s.counter, time.Now(), all...)
	kept, dropped := log.split(budget)
	return kept, dropped, nil
}

// resolveSummary 计算当前截断点对应的摘要文本
//
// 命中或预算回退（缓存已折入条数 >= 当前 dropped 条数）直接复用
// 旧摘要——回退场景旧摘要覆盖范围是超集，不丢信息；前进时只
// 压缩增量并与旧摘要滚动合并。压缩态在压缩成功后立即 Trim，
// 失败则退化为纯截断且不写缓存，下轮重试
// returns: 摘要文本，无需摘要或压缩失败时为空串
func (s *Summary) resolveSummary(ctx context.Context, sessionID string, dropped []core.Message) string {
	s.mu.Lock()
	entry, ok := s.cache[sessionID]
	s.mu.Unlock()

	var prior string
	covered := 0
	if ok {
		prior, covered = entry.text, entry.dropped
	}
	if covered >= len(dropped) {
		return prior
	}

	text := s.compress(ctx, prior, dropped[covered:])
	if text == "" {
		return ""
	}

	cacheDropped := len(dropped)
	if s.compact {
		if tr, ok := unwrapMemory(s.inner).(Trimmer); ok {
			if err := tr.Trim(ctx, sessionID, len(dropped)); err == nil {
				// 已物理删除，下轮 dropped 从 0 重新计
				cacheDropped = 0
			}
			// Trim 失败：消息仍在内层，缓存记当前截断点，下轮命中不重压
		}
	}
	s.mu.Lock()
	s.cache[sessionID] = &summaryCacheEntry{dropped: cacheDropped, text: text}
	s.mu.Unlock()
	return text
}

// unwrapMemory 解开装饰链取最内层存储
func unwrapMemory(m core.Memory) core.Memory {
	for {
		u, ok := m.(interface{ Unwrap() core.Memory })
		if !ok {
			return m
		}
		m = u.Unwrap()
	}
}

// unwrapTouched 解开装饰链取最内层存储，途经的装饰层（如 TTL）
// 先触碰活跃时间
// returns: 最内层存储
func (s *Summary) unwrapTouched(sessionID string) core.Memory {
	m := s.inner
	for {
		if t, ok := m.(interface{ touch(string) }); ok {
			t.touch(sessionID)
		}
		u, ok := m.(interface{ Unwrap() core.Memory })
		if !ok {
			return m
		}
		m = u.Unwrap()
	}
}

// lockSession 按会话加互斥锁
//
// 全局锁会把 LLM 压缩的秒级延迟扩散到所有会话，按会话粒度
// 串行既保证 Trim 安全又不跨会话干扰；锁对象随 Clear 移除，
// 移除瞬间在途的持有者与新建锁可能短暂并行，Clear 语义上
// 本就终止会话，可接受
// returns: 解锁函数
func (s *Summary) lockSession(sessionID string) func() {
	s.mu.Lock()
	l, ok := s.locks[sessionID]
	if !ok {
		l = &sync.Mutex{}
		s.locks[sessionID] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// compress 调用 LLM 生成摘要，旧摘要非空时滚动合并
// returns: 摘要文本，失败时空串
func (s *Summary) compress(ctx context.Context, prior string, msgs []core.Message) string {
	// 单条消息截到 2KB，控制压缩请求本身的成本
	var body string
	for i, m := range msgs {
		content := m.Content
		if len(content) > 2048 {
			// 字节截断可能切碎 UTF-8 尾字符，回退到 rune 边界
			cut := 2048
			for cut > 0 && !utf8.RuneStart(content[cut]) {
				cut--
			}
			content = content[:cut] + "..."
		}
		body += fmt.Sprintf("[%s] %s\n", m.Role, content)
		if i >= 200 {
			body += "（更早消息省略）\n"
			break
		}
	}

	sysPrompt := "把对话压缩成不超过 200 字的要点摘要，保留结论、决定与未完成事项"
	user := body
	if prior != "" {
		sysPrompt = "把此前摘要与新增对话合并为不超过 200 字的要点摘要，保留结论、决定与未完成事项，输出合并后的完整摘要"
		user = "此前摘要：\n" + prior + "\n\n新增对话：\n" + body
	}

	resp, err := s.llm.Chat(ctx, core.ChatRequest{
		Model: "summarizer",
		Messages: []core.Message{
			{Role: core.RoleSystem, Content: sysPrompt},
			{Role: core.RoleUser, Content: user},
		},
	})
	if err != nil || resp == nil || resp.Content == "" {
		return ""
	}
	return resp.Content
}
