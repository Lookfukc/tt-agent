package memory

import (
	"context"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// 默认逐出参数：半小时空闲逐出，五分钟扫一轮
const (
	defaultIdle  = 30 * time.Minute
	defaultSweep = 5 * time.Minute
)

// TTL 空闲逐出装饰器
//
// 包装任意 core.Memory：Add/Recent 触碰会话活跃时间，超过 idle
// 未活跃的会话由单个后台 janitor 周期清除。对 Persistent 而言
// Clear 会同步删盘——这是内存与磁盘无限增长的统一出口
//
// 旧实现的两个反例在此规避：
//   - janitor 只随构造启动一次（不是每个会话一个 goroutine）
//   - 判过期用最后活跃时间而非创建时间（长会话不会中途被杀）
type TTL struct {
	inner core.Memory
	idle  time.Duration
	sweep time.Duration

	mu     sync.Mutex
	last   map[string]time.Time
	cancel context.CancelFunc
	done   chan struct{}
}

// NewTTL 构造空闲逐出记忆
//
// janitor 随构造启动、ctx 取消即停；实际逐出时刻落在
// [idle, idle+sweep) 内，需要精确控制就调小 sweep
// ctx: janitor 生命周期，cancel 后不再逐出（已存活数据不动）
// inner: 实际存储，Buffer/Persistent/Summary 均可
// idle: 空闲多久逐出，<=0 用默认 30 分钟
// sweep: 检查周期，<=0 用默认 5 分钟，应小于 idle
// returns: 就绪实例
func NewTTL(ctx context.Context, inner core.Memory, idle, sweep time.Duration) *TTL {
	if idle <= 0 {
		idle = defaultIdle
	}
	if sweep <= 0 {
		sweep = defaultSweep
	}
	ctx, cancel := context.WithCancel(ctx)
	t := &TTL{
		inner:  inner,
		idle:   idle,
		sweep:  sweep,
		last:   make(map[string]time.Time),
		cancel: cancel,
		done:   make(chan struct{}),
	}
	go t.janitor(ctx)
	return t
}

// Add 追加消息并触碰活跃时间
func (t *TTL) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	t.touch(sessionID)
	return t.inner.Add(ctx, sessionID, msgs...)
}

// Recent 取回消息并触碰活跃时间
func (t *TTL) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	t.touch(sessionID)
	return t.inner.Recent(ctx, sessionID, budget)
}

// Clear 清空会话并移出追踪表
func (t *TTL) Clear(ctx context.Context, sessionID string) error {
	t.mu.Lock()
	delete(t.last, sessionID)
	t.mu.Unlock()
	return t.inner.Clear(ctx, sessionID)
}

// Unwrap 透出内层存储
//
// Summary 等同包装饰层借此穿过 TTL 寻址 Split/Trim 实现；
// 旁路访问不经过 touch 的路径必须自行触碰（见 Summary.splitInner）
func (t *TTL) Unwrap() core.Memory { return t.inner }

// touch 记录会话活跃时间，先于内层操作执行：
// janitor 持锁扫描看到的活跃时间一定包含本次访问
func (t *TTL) touch(sessionID string) {
	t.mu.Lock()
	t.last[sessionID] = time.Now()
	t.mu.Unlock()
}

// janitor 周期清除空闲会话，ctx 取退即退出
func (t *TTL) janitor(ctx context.Context) {
	defer close(t.done)
	tk := time.NewTicker(t.sweep)
	defer tk.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tk.C:
			t.sweepExpired()
		}
	}
}

// sweepExpired 清除超过空闲阈值的会话
//
// 持锁期间完成 inner.Clear：Clear 摘除与清除不原子的话，
// "扫描判过期 → 释放锁 → Add 触碰重建 → Clear 把新数据删掉"
// 的窗口真实存在。Clear 是删 map 项 + 删文件，持锁代价可忽略
func (t *TTL) sweepExpired() {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, at := range t.last {
		if now.Sub(at) <= t.idle {
			continue
		}
		// 逐出用的 Clear 不走 janitor 的 ctx：那是生命周期信号，
		// 不是本次清除的取消信号
		_ = t.inner.Clear(context.Background(), id)
		delete(t.last, id)
	}
}
