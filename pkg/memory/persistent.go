package memory

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Persistent JSONL 落盘的会话记忆
//
// 每会话一个 <sessionID>.jsonl 追加文件，写穿透：内存缓存提供读，
// 每条消息追加落盘。进程崩溃最多丢最后一条。
// 会话按需惰性加载：首访问才读文件，长驻内存的会话数受
// maxLoaded 约束，超限按 LRU 卸载（只卸内存不删盘，数据源是磁盘）
type Persistent struct {
	dir       string
	mu        sync.RWMutex
	sessions  map[string]*pEntry
	counter   core.TokenCounter
	maxLoaded int // 内存驻留会话上限，0 不限
}

// pEntry 单会话的内存驻留态
type pEntry struct {
	log     sessionLog
	lastUse time.Time // LRU 依据，Add/Recent/Trim 均触碰
	// dirty 落盘失败过的会话：LRU 永不逐出（逐出即静默丢数据），
	// 直到某次全量重写成功才清除
	dirty bool
}

// diskRecord 单行落盘记录：消息 + 入栈时间
//
// 旧格式是裸 core.Message（无 ts/msg 包装）。读取先按本结构解，
// msg 角色为空再按裸消息重试，旧文件零迁移直接可用
type diskRecord struct {
	TS  time.Time    `json:"ts,omitzero"`
	Msg core.Message `json:"msg"`
}

// NewPersistent 打开或恢复持久化记忆
//
// 会话数无上限（全靠磁盘，内存按访问惰性驻留）；会话数很大且
// 都活跃时用 NewPersistentWithLRU 限制驻留上限
// dir: 会话文件目录，不存在则创建
// counter: token 估算器，nil 用内置粗估
// returns: 就绪实例；目录不可写时返回错误
func NewPersistent(dir string, counter core.TokenCounter) (*Persistent, error) {
	return NewPersistentWithLRU(dir, counter, 0)
}

// NewPersistentWithLRU 打开持久化记忆并限制内存驻留会话数
//
// 旧实现在构造时全量加载目录下所有会话，万级会话会把进程压爆；
// 惰性加载 + LRU 卸载后，内存占用只与"同时活跃的会话数"相关
// dir: 会话文件目录
// counter: token 估算器，nil 用内置粗估
// maxLoaded: 内存驻留会话上限，超限卸载最久未访问的（盘上数据仍在）
// returns: 就绪实例；目录不可写时返回错误
func NewPersistentWithLRU(dir string, counter core.TokenCounter, maxLoaded int) (*Persistent, error) {
	if counter == nil {
		counter = roughCounter{}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create memory dir: %w", err)
	}
	return &Persistent{
		dir:       dir,
		sessions:  make(map[string]*pEntry),
		counter:   counter,
		maxLoaded: maxLoaded,
	}, nil
}

// maxSessionLine 单行上限，超长行按坏行跳过而非中断恢复
const maxSessionLine = 4 << 20

// validSessionID 校验会话标识
//
// sessionID 直接拼进文件路径，含分隔符或 .. 即可穿越目录任意读写，
// 入口处拦截是唯一防线。惰性加载后读路径也会触发文件访问，
// Recent/Split/Trim 与 Add/Clear 一样必须校验
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
	e := p.loadLocked(sessionID)
	now := time.Now()
	e.log.add(p.counter, now, msgs...)

	// 落盘失败不让进程崩溃，但调用方必须能感知；
	// 标记脏防止 LRU 把"只在内存里"的数据卸载丢失
	if err := p.appendDisk(sessionID, msgs, now); err != nil {
		e.dirty = true
		return err
	}
	return nil
}

// Recent 取回不超预算的最近消息，语义与 Buffer 一致
func (p *Persistent) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	kept, _, err := p.Split(ctx, sessionID, budget)
	return kept, err
}

// Split 取回预算内外的消息，首访问惰性加载该会话
func (p *Persistent) Split(_ context.Context, sessionID string, budget int64) ([]core.Message, []core.Message, error) {
	if !validSessionID(sessionID) {
		return nil, nil, fmt.Errorf("invalid session id %q", sessionID)
	}
	// 快路径：已驻留则只加读锁
	p.mu.RLock()
	if e, ok := p.sessions[sessionID]; ok {
		kept, dropped := e.log.split(budget)
		p.mu.RUnlock()
		return kept, dropped, nil
	}
	p.mu.RUnlock()

	// 慢路径：首次访问，读文件需写锁（可能写 map）
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.loadLocked(sessionID)
	kept, dropped := e.log.split(budget)
	return kept, dropped, nil
}

// Trim 物理丢弃最旧的 n 条非系统消息并原子重写会话文件
//
// temp 文件全量重写后 rename 覆盖：进程在重写中途崩溃要么是旧文件
// 要么是新文件，不会出现半截状态
func (p *Persistent) Trim(_ context.Context, sessionID string, n int) error {
	if !validSessionID(sessionID) {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.loadLocked(sessionID)
	before := e.log.count()
	e.log.trim(n)
	if e.log.count() == before {
		return nil // 无可删内容，不动盘
	}
	return p.rewriteLocked(sessionID, e)
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

// loadLocked 取会话驻留态，未驻留则从盘读入
//
// 调用方必须持有写锁（可能写 map 与触发 LRU 卸载）
// returns: 该会话的驻留态，文件不存在或不可读时为空会话
func (p *Persistent) loadLocked(sessionID string) *pEntry {
	if e, ok := p.sessions[sessionID]; ok {
		e.lastUse = time.Now()
		return e
	}
	e := &pEntry{lastUse: time.Now()}
	msgs, tss := loadSessionRecords(p.sessionPath(sessionID))
	for i := range msgs {
		e.log.add(p.counter, tss[i], msgs[i])
	}
	p.sessions[sessionID] = e
	p.evictLocked(sessionID)
	return e
}

// evictLocked 驻留数超上限时按 LRU 卸载最久未访问的会话
//
// 只删内存条目不删盘（盘是数据源，随时可重读）；脏会话与当前
// 会话不逐出——超限是性能问题，丢数据是正确性问题
// exclude: 本次正在服务的会话
func (p *Persistent) evictLocked(exclude string) {
	if p.maxLoaded <= 0 {
		return
	}
	for len(p.sessions) > p.maxLoaded {
		victim := ""
		var oldest time.Time
		for id, e := range p.sessions {
			if id == exclude || e.dirty {
				continue
			}
			if victim == "" || e.lastUse.Before(oldest) {
				victim, oldest = id, e.lastUse
			}
		}
		if victim == "" {
			return // 全是脏会话/当前会话，宁可超限
		}
		delete(p.sessions, victim)
	}
}

// appendDisk 追加写会话文件
func (p *Persistent) appendDisk(sessionID string, msgs []core.Message, now time.Time) error {
	f, err := os.OpenFile(p.sessionPath(sessionID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open session file: %w", err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, m := range msgs {
		line, err := json.Marshal(diskRecord{TS: now, Msg: m})
		if err != nil {
			return fmt.Errorf("encode message: %w", err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("write session file: %w", err)
		}
	}
	return w.Flush()
}

// rewriteLocked 全量重写会话文件（temp + rename 原子替换）
//
// Trim 物理压缩后调用；重写成功同时清除脏标记——盘上已含全部
// 内存内容，LRU 恢复逐出资格
func (p *Persistent) rewriteLocked(sessionID string, e *pEntry) error {
	path := p.sessionPath(sessionID)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open rewrite temp: %w", err)
	}
	w := bufio.NewWriter(f)
	for i, m := range e.log.msgs {
		line, err := json.Marshal(diskRecord{TS: e.log.tss[i], Msg: m})
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("encode message: %w", err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("write rewrite temp: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("flush rewrite temp: %w", err)
	}
	// Windows 上 rename 前必须关句柄，否则目标文件被占用
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close rewrite temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("swap session file: %w", err)
	}
	e.dirty = false
	return nil
}

// loadSessionRecords 读取单个会话文件，坏行与超长行跳过
//
// 每行先按 diskRecord 解（新格式），msg 角色为空再按裸 core.Message
// 解（旧格式）；两层都失败按坏行丢弃
// returns: 恢复的消息列表与对齐的入栈时间
func loadSessionRecords(path string) (msgs []core.Message, tss []time.Time) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil // 文件不存在 = 空会话；其他读错误与旧 loadAll 一致按空处理
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64*1024)
	for {
		line, readErr := readLineLimited(r, maxSessionLine)
		if len(line) > 0 {
			var rec diskRecord
			if json.Unmarshal(line, &rec) == nil && rec.Msg.Role != "" {
				msgs = append(msgs, rec.Msg)
				tss = append(tss, rec.TS)
			} else {
				var m core.Message
				if json.Unmarshal(line, &m) == nil && m.Role != "" {
					msgs = append(msgs, m)
					tss = append(tss, time.Time{})
				}
			}
		}
		if readErr != nil {
			return msgs, tss
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
