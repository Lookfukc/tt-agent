package test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
)

// growthLLM 记录请求体的摘要 mock，每次调用返回可区分的文本
type growthLLM struct {
	mu     sync.Mutex
	bodies []string
}

// Chat 记录 user 消息体并返回 S<n>
func (g *growthLLM) Chat(_ context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.bodies = append(g.bodies, req.Messages[len(req.Messages)-1].Content)
	return &core.ChatResponse{Content: fmt.Sprintf("S%d", len(g.bodies))}, nil
}

// ChatStream 未使用
func (g *growthLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, fmt.Errorf("not implemented")
}

// calls 返回已记录的调用数
func (g *growthLLM) calls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.bodies)
}

// waitFor 轮询等待条件成立，超时失败
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within deadline: %s", desc)
}

// TestTTLExpiresIdleSessionsAndDeletesDisk 空闲会话被逐出且落盘文件同步删除
//
// 内存与磁盘的无限增长以 TTL 为统一出口：Persistent 的 Clear 本就删文件，
// janitor 触发后磁盘不再残留死会话
func TestTTLExpiresIdleSessionsAndDeletesDisk(t *testing.T) {
	dir := t.TempDir()
	inner, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mem := memory.NewTTL(ctx, inner, 60*time.Millisecond, 20*time.Millisecond)

	if err := mem.Add(ctx, "s1", core.Message{Role: core.RoleUser, Content: "hi"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "s1.jsonl")); err != nil {
		t.Fatalf("session file should exist after Add: %v", err)
	}

	waitFor(t, "session file deleted after idle", func() bool {
		_, err := os.Stat(filepath.Join(dir, "s1.jsonl"))
		return os.IsNotExist(err)
	})
	got, _ := mem.Recent(ctx, "s1", 1<<62)
	if len(got) != 0 {
		t.Errorf("evicted session should read empty, got %+v", got)
	}
}

// TestTTLActiveSessionSurvivesViaSummarySplit 活跃会话不被逐出——含 Split 旁路路径
//
// Summary 的 Split 快路径不走 TTL.Recent，若不触碰 TTL，
// 活跃会话会被 janitor 误判空闲逐出；对照的空闲会话必须被逐出
func TestTTLActiveSessionSurvivesViaSummarySplit(t *testing.T) {
	buf := memory.NewBuffer(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ttl := memory.NewTTL(ctx, buf, 150*time.Millisecond, 20*time.Millisecond)
	sum := memory.NewSummary(ttl, &growthLLM{})

	_ = sum.Add(ctx, "idle", core.Message{Role: core.RoleUser, Content: "x"})
	_ = sum.Add(ctx, "live",
		core.Message{Role: core.RoleSystem, Content: "sys"},
		core.Message{Role: core.RoleUser, Content: "q1"},
		core.Message{Role: core.RoleAssistant, Content: "a1"},
	)

	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, _ = sum.Recent(ctx, "live", 1<<62) // 预算给足 → 走 Split，无压缩
		time.Sleep(40 * time.Millisecond)
	}

	live, _ := buf.Recent(ctx, "live", 1<<62)
	if len(live) != 3 {
		t.Fatalf("active session evicted via Split bypass, got %d msgs", len(live))
	}
	idle, _ := buf.Recent(ctx, "idle", 1<<62)
	if len(idle) != 0 {
		t.Errorf("idle session not evicted, got %d msgs", len(idle))
	}
}

// TestPersistentLRUEvictionKeepsDataOnDisk LRU 卸载只卸内存不丢数据
//
// 驻留超限后最久未访问的会话被卸载，再次访问应从盘上完整恢复
func TestPersistentLRUEvictionKeepsDataOnDisk(t *testing.T) {
	dir := t.TempDir()
	p, err := memory.NewPersistentWithLRU(dir, nil, 2)
	if err != nil {
		t.Fatalf("NewPersistentWithLRU: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := p.Add(ctx, fmt.Sprintf("s%d", i), core.Message{
			Role: core.RoleUser, Content: fmt.Sprintf("data-%d", i),
		}); err != nil {
			t.Fatalf("Add s%d: %v", i, err)
		}
	}
	// s2 驻入时 s0 已被 LRU 卸载；三个会话的数据必须都能找回
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("s%d", i)
		got, err := p.Recent(ctx, id, 1<<62)
		if err != nil {
			t.Fatalf("Recent %s: %v", id, err)
		}
		if len(got) != 1 || got[0].Content != fmt.Sprintf("data-%d", i) {
			t.Errorf("%s = %+v, want single data-%d", id, got, i)
		}
	}
}

// TestPersistentTrimRewritesDisk Trim 后重开实例只余未删消息
//
// 物理压缩必须真实落到磁盘：temp+rename 原子重写，系统消息保留，
// 不残留 .tmp 文件
func TestPersistentTrimRewritesDisk(t *testing.T) {
	dir := t.TempDir()
	p, _ := memory.NewPersistent(dir, nil)
	ctx := context.Background()
	_ = p.Add(ctx, "s",
		core.Message{Role: core.RoleSystem, Content: "sys"},
		core.Message{Role: core.RoleUser, Content: "m1"},
		core.Message{Role: core.RoleUser, Content: "m2"},
		core.Message{Role: core.RoleUser, Content: "m3"},
		core.Message{Role: core.RoleUser, Content: "m4"},
	)
	if err := p.Trim(ctx, "s", 2); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	reopened, _ := memory.NewPersistent(dir, nil)
	got, _ := reopened.Recent(ctx, "s", 1<<62)
	if len(got) != 3 || got[0].Role != core.RoleSystem ||
		got[1].Content != "m3" || got[2].Content != "m4" {
		t.Fatalf("after trim reopen = %+v, want [sys m3 m4]", got)
	}
	tmp, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(tmp) != 0 {
		t.Errorf("temp files leaked: %v", tmp)
	}
}

// TestBufferTrimKeepsSystemMessages Buffer 的 Trim 不删系统消息
func TestBufferTrimKeepsSystemMessages(t *testing.T) {
	ctx := context.Background()
	buf := memory.NewBuffer(nil)
	_ = buf.Add(ctx, "s",
		core.Message{Role: core.RoleSystem, Content: "sys"},
		core.Message{Role: core.RoleUser, Content: "m1"},
		core.Message{Role: core.RoleUser, Content: "m2"},
		core.Message{Role: core.RoleUser, Content: "m3"},
	)
	_ = buf.Trim(ctx, "s", 2)
	got, _ := buf.Recent(ctx, "s", 1<<62)
	if len(got) != 2 || got[0].Role != core.RoleSystem || got[1].Content != "m3" {
		t.Fatalf("after trim = %+v, want [sys m3]", got)
	}
}

// TestCompactingSummaryShrinksDiskOnDisk 压缩态摘要物理收缩会话文件
//
// NewSummary 不动内层（审计保留）；NewCompactingSummary 摘要成功的
// 消息从内层删除，重开实例只剩保留部分，且同一截断点不重复压缩
func TestCompactingSummaryShrinksDiskOnDisk(t *testing.T) {
	dir := t.TempDir()
	inner, _ := memory.NewPersistent(dir, nil)
	ctx := context.Background()
	llm := &growthLLM{}
	mem := memory.NewCompactingSummary(inner, llm)

	msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
	for i := 0; i < 8; i++ {
		// 每条约 60 字符 → 粗估 30 token，预算 100 稳定保留最近 3 条
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: fmt.Sprintf("long-msg-%02d-%s", i, strings.Repeat("x", 48)),
		})
	}
	_ = mem.Add(ctx, "s", msgs...)

	got, err := mem.Recent(ctx, "s", 100)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	hasSummary := false
	for _, m := range got {
		if m.Role == core.RoleSystem && strings.HasPrefix(m.Content, "此前对话摘要：") {
			hasSummary = true
		}
	}
	if !hasSummary {
		t.Fatalf("no summary message: %+v", got)
	}

	remains, _ := inner.Recent(ctx, "s", 1<<62)
	if len(remains) != 4 { // sys + 保留的 3 条
		t.Fatalf("inner after compact = %d msgs, want 4", len(remains))
	}
	// 同截断点重复取，命中缓存不重复压缩
	_, _ = mem.Recent(ctx, "s", 100)
	if llm.calls() != 1 {
		t.Fatalf("compress calls = %d after repeated Recent, want 1", llm.calls())
	}
	// 重开实例验证磁盘物理收缩
	reopened, _ := memory.NewPersistent(dir, nil)
	disk, _ := reopened.Recent(ctx, "s", 1<<62)
	if len(disk) != len(remains) {
		t.Fatalf("disk = %d msgs, memory = %d, trim 未落盘", len(disk), len(remains))
	}
}

// TestSummaryRollingMerge 截断点前进只压缩增量并与旧摘要合并
//
// 第二次压缩的输入应包含旧摘要文本与新增消息，不重复罗列已折入的消息
func TestSummaryRollingMerge(t *testing.T) {
	ctx := context.Background()
	llm := &growthLLM{}
	mem := memory.NewSummary(memory.NewBuffer(nil), llm)

	var msgs []core.Message
	for i := 0; i < 8; i++ {
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: fmt.Sprintf("MSG-%d-%s", i, strings.Repeat("y", 24)), // 30 字符 → 15 token
		})
	}
	_ = mem.Add(ctx, "s", msgs...)

	// 预算 100：保留 6 条（90），丢弃 2 → 第一次压缩
	_, _ = mem.Recent(ctx, "s", 100)
	if llm.calls() != 1 {
		t.Fatalf("calls = %d, want 1", llm.calls())
	}
	// 预算 60：保留 4 条，丢弃 4 → 增量 2 条，与 S1 合并
	_, _ = mem.Recent(ctx, "s", 60)
	if llm.calls() != 2 {
		t.Fatalf("calls = %d, want 2", llm.calls())
	}
	second := llm.bodies[1]
	if !strings.Contains(second, "此前摘要：\nS1") {
		t.Errorf("second compress lacks prior summary: %q", second)
	}
	if !strings.Contains(second, "MSG-2") || !strings.Contains(second, "MSG-3") {
		t.Errorf("second compress lacks incremental msgs: %q", second)
	}
	if strings.Contains(second, "MSG-0") || strings.Contains(second, "MSG-1") {
		t.Errorf("second compress re-lists already-covered msgs: %q", second)
	}
}

// TestCompactingSummaryConcurrentRecentNoDoubleTrim 并发 Recent 不会双重 Trim
//
// 两个并发 Recent 各自 Split 到相同 dropped 再各自 Trim，第二次删的是
// 尚未摘要的消息——按会话串行后恰好一次压缩、一次 Trim
func TestCompactingSummaryConcurrentRecentNoDoubleTrim(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewBuffer(nil)
	llm := &growthLLM{}
	mem := memory.NewCompactingSummary(inner, llm)

	var msgs []core.Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: fmt.Sprintf("m%d-%s", i, strings.Repeat("z", 26)), // 30 字符 → 15 token
		})
	}
	_ = mem.Add(ctx, "s", msgs...)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = mem.Recent(ctx, "s", 60) // 保留 4，丢弃 6
		}()
	}
	wg.Wait()

	if llm.calls() != 1 {
		t.Fatalf("compress calls = %d, want 1", llm.calls())
	}
	remains, _ := inner.Recent(ctx, "s", 1<<62)
	if len(remains) != 4 {
		t.Fatalf("after concurrent compact = %d msgs, want 4 (double trim suspected)", len(remains))
	}
}

// TestPersistentNewFormatWithLegacyLines 新旧落盘格式混读
//
// 新格式带 ts/msg 包装，旧格式是裸消息；同文件混排都能恢复且保序
func TestPersistentNewFormatWithLegacyLines(t *testing.T) {
	dir := t.TempDir()
	lines := `{"role":"user","content":"legacy-first"}` + "\n" +
		`{"ts":"2026-10-07T12:00:00Z","msg":{"role":"assistant","content":"wrapped-second"}}` + "\n" +
		`{"role":"user","content":"legacy-third"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "s1.jsonl"), []byte(lines), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	p, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	got, _ := p.Recent(context.Background(), "s1", 1<<62)
	if len(got) != 3 || got[0].Content != "legacy-first" ||
		got[1].Content != "wrapped-second" || got[2].Content != "legacy-third" {
		t.Fatalf("mixed format restore = %+v", got)
	}

	// 新写入必须是包装格式
	_ = p.Add(context.Background(), "s2", core.Message{Role: core.RoleUser, Content: "fresh"})
	raw, _ := os.ReadFile(filepath.Join(dir, "s2.jsonl"))
	if !strings.Contains(string(raw), `"msg"`) || !strings.Contains(string(raw), `"ts"`) {
		t.Errorf("new writes should use envelope format: %s", raw)
	}
}
