package memory

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// Persistent JSONL 落盘的会话记忆
//
// 每会话一个 <sessionID>.jsonl 追加文件，写穿透：内存缓存提供读，
// 每条消息追加落盘。重启后从目录恢复，进程崩溃最多丢最后一条
type Persistent struct {
	dir      string
	mu       sync.RWMutex
	sessions map[string][]core.Message
	counter  core.TokenCounter
}

// NewPersistent 打开或恢复持久化记忆
// dir: 会话文件目录，不存在则创建
// counter: token 估算器，nil 用内置粗估
// returns: 就绪实例；目录不可写时返回错误
func NewPersistent(dir string, counter core.TokenCounter) (*Persistent, error) {
	if counter == nil {
		counter = roughCounter{}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create memory dir: %w", err)
	}
	p := &Persistent{
		dir:      dir,
		sessions: make(map[string][]core.Message),
		counter:  counter,
	}
	if err := p.loadAll(); err != nil {
		return nil, err
	}
	return p, nil
}

// maxSessionLine 单行上限，超长行按坏行跳过而非中断恢复
const maxSessionLine = 4 << 20

// validSessionID 校验会话标识
//
// sessionID 直接拼进文件路径，含分隔符或 .. 即可穿越目录任意读写，
// 入口处拦截是唯一防线
// returns: true 表示合法
func validSessionID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	if strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.Contains(id, "..") {
		return false
	}
	return true
}

// Add 追加消息并落盘
//
// 落盘在锁内：并发 Add 的磁盘顺序必须与内存顺序一致，
// 否则重启恢复出的会话乱序
func (p *Persistent) Add(_ context.Context, sessionID string, msgs ...core.Message) error {
	if !validSessionID(sessionID) {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions[sessionID] = append(p.sessions[sessionID], msgs...)

	// 落盘失败不让进程崩溃，但调用方必须能感知
	return p.appendDisk(sessionID, msgs)
}

// Recent 取回不超预算的最近消息，语义与 Buffer 一致
func (p *Persistent) Recent(_ context.Context, sessionID string, budget int64) ([]core.Message, error) {
	p.mu.RLock()
	msgs := p.sessions[sessionID]
	p.mu.RUnlock()
	kept, _ := pickWithinBudget(msgs, budget, p.counter)
	return kept, nil
}

// Clear 清空会话并删除落盘文件
//
// 删文件与进行中的追加同锁串行，否则清空后被追加重建
func (p *Persistent) Clear(_ context.Context, sessionID string) error {
	if !validSessionID(sessionID) {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, sessionID)
	err := os.Remove(p.sessionPath(sessionID))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// sessionPath 会话文件路径
// returns: 目录内 <sessionID>.jsonl
func (p *Persistent) sessionPath(sessionID string) string {
	return filepath.Join(p.dir, sessionID+".jsonl")
}

// appendDisk 追加写会话文件
func (p *Persistent) appendDisk(sessionID string, msgs []core.Message) error {
	f, err := os.OpenFile(p.sessionPath(sessionID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open session file: %w", err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, m := range msgs {
		line, err := json.Marshal(m)
		if err != nil {
			return fmt.Errorf("encode message: %w", err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("write session file: %w", err)
		}
	}
	return w.Flush()
}

// loadAll 启动时恢复全部会话
// returns: 目录读取失败时返回错误
func (p *Persistent) loadAll() error {
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return fmt.Errorf("read memory dir: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".jsonl" {
			continue
		}
		sessionID := e.Name()[:len(e.Name())-len(".jsonl")]
		msgs, err := loadSession(p.sessionPath(sessionID))
		if err != nil {
			// 单个会话损坏不阻断启动，跳过该会话
			continue
		}
		p.sessions[sessionID] = msgs
	}
	return nil
}

// loadSession 读取单个会话文件，坏行与超长行跳过
//
// Scanner 遇 ErrTooLong 直接终止，毒行之后的消息全部丢失且永不自愈；
// 改用 Reader 按行读取，超长行丢弃后继续，恢复其余内容
// returns: 恢复的消息列表
func loadSession(path string) ([]core.Message, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64*1024)
	var msgs []core.Message
	for {
		line, readErr := readLineLimited(r, maxSessionLine)
		if len(line) > 0 {
			var m core.Message
			if json.Unmarshal(line, &m) == nil {
				msgs = append(msgs, m)
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return msgs, nil
			}
			return msgs, readErr
		}
	}
}

// readLineLimited 读一行并施加长度上限
//
// 一旦累计长度（含换行符）严格超过 limit 即进入丢弃模式：整行作废，
// 持续消费到换行才返回 nil，保证后续行仍可继续读取；恰好等于 limit
// 的行视为合法整行保留。旧实现重置后继续累积，会把超长行的尾部
// 当成独立行吐出去，毒行内容混进恢复结果
// returns: 行内容（不含换行，超长行恒为 nil）、读取错误（io.EOF 表示文件结束）
func readLineLimited(r *bufio.Reader, limit int) ([]byte, error) {
	var buf []byte
	overflow := false
	for {
		chunk, err := r.ReadSlice('\n')
		if !overflow && len(buf)+len(chunk) > limit {
			// 刚超限：已累计内容全部作废，切到丢弃模式消费到行尾
			overflow = true
			buf = nil
		}
		if !overflow {
			buf = append(buf, chunk...)
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		// err 为 nil（含换行）或 io.EOF：本行结束
		if overflow {
			return nil, err
		}
		return trimNewline(buf), err
	}
}

// trimNewline 去掉行尾换行符
// returns: 处理后的行
func trimNewline(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
		if n = len(b); n > 0 && b[n-1] == '\r' {
			b = b[:n-1]
		}
	}
	return b
}

// pickWithinBudget 从消息中挑选不超预算的最近历史，系统消息始终保留
//
// 按原子组打包：assistant(tool_calls) 与其后续 tool 消息同进同退。
// 逐条装填会在"tool 结果装得下、父消息装不下"时产出孤儿 tool 消息，
// Anthropic/OpenAI 对无主 tool 消息一律 400，会话就此永久损坏
// returns: 保留的按时间序消息、被截断丢弃的按时间序消息
func pickWithinBudget(msgs []core.Message, budget int64, counter core.TokenCounter) (kept, dropped []core.Message) {
	var system []core.Message
	var rest []core.Message
	for _, m := range msgs {
		if m.Role == core.RoleSystem {
			system = append(system, m)
		} else {
			rest = append(rest, m)
		}
	}

	// 切原子组：tool 消息归入其前最近的 assistant(tool_calls)
	type msgGroup struct{ msgs []core.Message }
	var groups []msgGroup
	for i := 0; i < len(rest); {
		g := msgGroup{msgs: []core.Message{rest[i]}}
		i++
		if len(rest[i-1].ToolCalls) > 0 {
			for i < len(rest) && rest[i].Role == core.RoleTool {
				g.msgs = append(g.msgs, rest[i])
				i++
			}
		}
		groups = append(groups, g)
	}

	used := counter.Count(system)
	keepFrom := len(groups)
	for i := len(groups) - 1; i >= 0; i-- {
		cost := counter.Count(groups[i].msgs)
		if used+cost > budget {
			break
		}
		used += cost
		keepFrom = i
	}

	for _, g := range groups[keepFrom:] {
		kept = append(kept, g.msgs...)
	}
	for _, g := range groups[:keepFrom] {
		dropped = append(dropped, g.msgs...)
	}
	if len(kept) == 0 && len(groups) > 0 {
		// 单组都装不下的极端预算：宁可超预算也保留最新一组，
		// 空历史比超窗更不可恢复
		kept = groups[len(groups)-1].msgs
		dropped = dropped[:len(dropped)-len(kept)]
	}
	return append(system, kept...), dropped
}
