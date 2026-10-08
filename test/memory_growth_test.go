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
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
)

// growthLLM is a summarization mock that records request bodies; each call returns distinguishable text.
type growthLLM struct {
	mu     sync.Mutex
	bodies []string
}

// Chat records the user message body and returns S<n>.
func (g *growthLLM) Chat(_ context.Context, req core.ChatRequest) (*core.ChatResponse, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.bodies = append(g.bodies, req.Messages[len(req.Messages)-1].Content)
	return &core.ChatResponse{Content: fmt.Sprintf("S%d", len(g.bodies))}, nil
}

// ChatStream is unused.
func (g *growthLLM) ChatStream(_ context.Context, _ core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, fmt.Errorf("not implemented")
}

// calls returns the number of recorded calls.
func (g *growthLLM) calls() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.bodies)
}

// waitFor polls until the condition holds, failing on timeout.
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

// TestTTLExpiresIdleSessionsAndDeletesDisk verifies that idle sessions are evicted and their on-disk files deleted.
//
// TTL is the single choke point for unbounded growth in memory and on disk: Persistent's
// Clear already deletes files, so once the janitor fires no dead sessions remain on disk.
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

// TestTTLActiveSessionSurvivesViaSummarySplit verifies that active sessions are not evicted, including the Split bypass path.
//
// Summary's fast Split path does not go through TTL.Recent; if the TTL were never touched,
// the janitor would mistake an active session for idle and evict it — while the control idle session must be evicted.
func TestTTLActiveSessionSurvivesViaSummarySplit(t *testing.T) {
	buf := memorytest.NewBuffer(nil)
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
		_, _ = sum.Recent(ctx, "live", 1<<62) // generous budget → takes the Split path, no compaction
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

// TestPersistentLRUEvictionKeepsDataOnDisk verifies that LRU eviction unloads memory without losing data.
//
// Once residency exceeds the limit, the least recently accessed session is unloaded;
// accessing it again must fully restore it from disk.
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
	// When s2 is loaded, s0 has already been unloaded by LRU; all three sessions' data must be recoverable.
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

// TestPersistentTrimRewritesDisk verifies that after Trim, reopening an instance leaves only non-deleted messages.
//
// Physical compaction must actually reach the disk: an atomic temp+rename rewrite,
// system messages preserved, and no leftover .tmp files.
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

// TestBufferTrimKeepsSystemMessages verifies that Buffer's Trim does not delete system messages.
func TestBufferTrimKeepsSystemMessages(t *testing.T) {
	ctx := context.Background()
	buf := memorytest.NewBuffer(nil)
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

// TestCompactingSummaryShrinksDiskOnDisk verifies that compacting summary physically shrinks the session file.
//
// NewSummary leaves the inner layer untouched (audit retention); with NewCompactingSummary,
// summarized messages are removed from the inner layer, a reopened instance holds only the
// retained part, and the same truncation point is not compacted twice.
func TestCompactingSummaryShrinksDiskOnDisk(t *testing.T) {
	dir := t.TempDir()
	inner, _ := memory.NewPersistent(dir, nil)
	ctx := context.Background()
	llm := &growthLLM{}
	mem := memory.NewCompactingSummary(inner, llm)

	msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
	for i := 0; i < 8; i++ {
		// Each message is ~60 chars → roughly 30 tokens; a budget of 100 stably keeps the last 3.
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
	if len(remains) != 4 { // sys + 3 retained messages
		t.Fatalf("inner after compact = %d msgs, want 4", len(remains))
	}
	// Repeated Recent at the same truncation point hits the cache; no re-compaction.
	_, _ = mem.Recent(ctx, "s", 100)
	if llm.calls() != 1 {
		t.Fatalf("compress calls = %d after repeated Recent, want 1", llm.calls())
	}
	// Reopen the instance to verify the physical shrink on disk.
	reopened, _ := memory.NewPersistent(dir, nil)
	disk, _ := reopened.Recent(ctx, "s", 1<<62)
	if len(disk) != len(remains) {
		t.Fatalf("disk = %d msgs, memory = %d, trim 未落盘", len(disk), len(remains))
	}
}

// TestSummaryRollingMerge verifies that an advancing truncation point only compacts the increment and merges it into the old summary.
//
// The second compaction's input must contain the old summary text plus the new messages,
// without re-listing messages already folded in.
func TestSummaryRollingMerge(t *testing.T) {
	ctx := context.Background()
	llm := &growthLLM{}
	mem := memory.NewSummary(memorytest.NewBuffer(nil), llm)

	var msgs []core.Message
	for i := 0; i < 8; i++ {
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: fmt.Sprintf("MSG-%d-%s", i, strings.Repeat("y", 24)), // 30 chars → 15 tokens
		})
	}
	_ = mem.Add(ctx, "s", msgs...)

	// Budget 100: keep 6 messages (90), drop 2 → first compaction.
	_, _ = mem.Recent(ctx, "s", 100)
	if llm.calls() != 1 {
		t.Fatalf("calls = %d, want 1", llm.calls())
	}
	// Budget 60: keep 4 messages, drop 4 → an increment of 2 messages, merged with S1.
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

// TestCompactingSummaryConcurrentRecentNoDoubleTrim verifies that concurrent Recent calls do not double-Trim.
//
// Two concurrent Recent calls each Split to the same dropped set and each Trim; the second
// deletion would hit not-yet-summarized messages — with per-session serialization there is
// exactly one compaction and one Trim.
func TestCompactingSummaryConcurrentRecentNoDoubleTrim(t *testing.T) {
	ctx := context.Background()
	inner := memorytest.NewBuffer(nil)
	llm := &growthLLM{}
	mem := memory.NewCompactingSummary(inner, llm)

	var msgs []core.Message
	for i := 0; i < 10; i++ {
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: fmt.Sprintf("m%d-%s", i, strings.Repeat("z", 26)), // 30 chars → 15 tokens
		})
	}
	_ = mem.Add(ctx, "s", msgs...)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = mem.Recent(ctx, "s", 60) // keep 4, drop 6
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

// TestPersistentNewFormatWithLegacyLines verifies reading a mix of old and new on-disk formats.
//
// The new format uses a ts/msg envelope, the old format is a bare message; mixed lines in
// the same file must all be restored in order.
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

	// New writes must use the envelope format.
	_ = p.Add(context.Background(), "s2", core.Message{Role: core.RoleUser, Content: "fresh"})
	raw, _ := os.ReadFile(filepath.Join(dir, "s2.jsonl"))
	if !strings.Contains(string(raw), `"msg"`) || !strings.Contains(string(raw), `"ts"`) {
		t.Errorf("new writes should use envelope format: %s", raw)
	}
}

// TestCompactingSummarySurvivesRestart verifies that a compacting summary persists with the session and recovers after restart.
//
// The summary text is the only copy of the old context under compacting mode (old messages
// are physically deleted). After reopening: the summary prefix is still injected, and no
// duplicate compaction is triggered (the cache is restored from disk).
func TestCompactingSummarySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	inner, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	ctx := context.Background()
	llm := &growthLLM{}
	mem := memory.NewCompactingSummary(inner, llm)

	msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
	for i := 0; i < 8; i++ {
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: fmt.Sprintf("long-msg-%02d-%s", i, strings.Repeat("x", 48)),
		})
	}
	if err := mem.Add(ctx, "s", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := mem.Recent(ctx, "s", 100)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if !hasSummaryText(got, "S1") {
		t.Fatalf("no summary before restart: %+v", got)
	}

	// Simulate a process restart: fresh Persistent + fresh Summary, in-memory cache emptied.
	reopenedInner, _ := memory.NewPersistent(dir, nil)
	reopened := memory.NewCompactingSummary(reopenedInner, llm)
	got2, err := reopened.Recent(ctx, "s", 100)
	if err != nil {
		t.Fatalf("Recent after restart: %v", err)
	}
	if !hasSummaryText(got2, "S1") {
		t.Fatalf("summary lost after restart: %+v", got2)
	}
	if llm.calls() != 1 {
		t.Fatalf("compress calls = %d after restart, want 1 (cache should restore from disk)", llm.calls())
	}

	// Clear also deletes the summary file; after reopening it is no longer injected.
	if err := reopened.Clear(ctx, "s"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "s.summary")); !os.IsNotExist(err) {
		t.Fatalf("summary file should be removed by Clear")
	}
	third, _ := memory.NewPersistent(dir, nil)
	got3, _ := third.Recent(ctx, "s", 1<<62)
	if hasSummaryPrefix(got3) {
		t.Fatalf("summary injected after Clear: %+v", got3)
	}
}

// hasSummaryText reports whether any injected summary message contains the given text.
func hasSummaryText(msgs []core.Message, text string) bool {
	for _, m := range msgs {
		if m.Role == core.RoleSystem && strings.Contains(m.Content, "此前对话摘要：") &&
			strings.Contains(m.Content, text) {
			return true
		}
	}
	return false
}

// hasSummaryPrefix reports whether any injected summary message exists.
func hasSummaryPrefix(msgs []core.Message) bool {
	for _, m := range msgs {
		if m.Role == core.RoleSystem && strings.Contains(m.Content, "此前对话摘要：") {
			return true
		}
	}
	return false
}

// TestPersistentSummaryStoreRoundTrip covers the SummaryStore interface's save/load and error handling.
func TestPersistentSummaryStoreRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	ctx := context.Background()

	// An unsaved session returns empty.
	text, covered, err := p.LoadSummary(ctx, "none")
	if err != nil || text != "" || covered != 0 {
		t.Fatalf("LoadSummary empty = (%q, %d, %v), want empty", text, covered, err)
	}

	// After saving it reads back, including covered.
	if err := p.SaveSummary(ctx, "s1", 42, "hello world"); err != nil {
		t.Fatalf("SaveSummary: %v", err)
	}
	text, covered, err = p.LoadSummary(ctx, "s1")
	if err != nil || text != "hello world" || covered != 42 {
		t.Fatalf("LoadSummary = (%q, %d, %v)", text, covered, err)
	}

	// A reopened instance still reads it back (truly on disk, not in memory).
	p2, _ := memory.NewPersistent(dir, nil)
	text, covered, err = p2.LoadSummary(ctx, "s1")
	if err != nil || text != "hello world" || covered != 42 {
		t.Fatalf("LoadSummary after reopen = (%q, %d, %v)", text, covered, err)
	}

	// Invalid sessionIDs are rejected without touching disk.
	if err := p2.SaveSummary(ctx, "../evil", 1, "x"); err == nil {
		t.Fatalf("SaveSummary should reject invalid session id")
	}

	// A corrupted summary file is treated as no summary, without error.
	if err := os.WriteFile(filepath.Join(dir, "bad.summary"), []byte("{not json"), 0o644); err != nil {
		t.Fatalf("write bad file: %v", err)
	}
	if text, _, err = p2.LoadSummary(ctx, "bad"); err != nil || text != "" {
		t.Fatalf("LoadSummary corrupted = (%q, %v), want empty", text, err)
	}
}
