// Package memorystore_test exercises the driver contract.
//
// The point of these tests is parity: the same scenarios run against
// every backend, so a driver cannot quietly diverge in truncation,
// system-message retention or tool-call pairing — the three places
// where a subtle difference produces provider 400s in production.
package memorystore_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// memoryHarness builds a fresh, migrated driver for a test.
type memoryHarness struct {
	name string
	// newStore returns a store plus a cleanup function.
	newStore func(t *testing.T) (*memorystore.Store, func())
}

// runContract executes the shared scenario suite against a harness.
//
// Every backend must pass this unchanged; that is what makes "works on
// Redis" meaningful for Postgres and SQLite users too.
func runContract(t *testing.T, h memoryHarness) {
	t.Helper()

	t.Run("add_and_read_back", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		if err := store.Add(ctx, "s1", core.Message{Role: core.RoleUser, Content: "hello"}); err != nil {
			t.Fatalf("Add: %v", err)
		}
		got, err := store.Recent(ctx, "s1", 1<<20)
		if err != nil {
			t.Fatalf("Recent: %v", err)
		}
		if len(got) != 1 || got[0].Content != "hello" {
			t.Fatalf("Recent = %+v", got)
		}
	})

	t.Run("sessions_are_isolated", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		_ = store.Add(ctx, "a", core.Message{Role: core.RoleUser, Content: "from-a"})
		_ = store.Add(ctx, "b", core.Message{Role: core.RoleUser, Content: "from-b"})

		gotA, _ := store.Recent(ctx, "a", 1<<20)
		gotB, _ := store.Recent(ctx, "b", 1<<20)
		if len(gotA) != 1 || gotA[0].Content != "from-a" {
			t.Fatalf("session a = %+v", gotA)
		}
		if len(gotB) != 1 || gotB[0].Content != "from-b" {
			t.Fatalf("session b = %+v", gotB)
		}
	})

	t.Run("system_message_always_kept", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
		for i := 0; i < 10; i++ {
			msgs = append(msgs, core.Message{
				Role: core.RoleUser, Content: fmt.Sprintf("msg-%02d-%s", i, strings.Repeat("x", 60)),
			})
		}
		if err := store.Add(ctx, "s", msgs...); err != nil {
			t.Fatalf("Add: %v", err)
		}

		got, err := store.Recent(ctx, "s", 100) // 极小预算
		if err != nil {
			t.Fatalf("Recent: %v", err)
		}
		if len(got) == 0 || got[0].Role != core.RoleSystem || got[0].Content != "sys" {
			t.Fatalf("system message lost: %+v", got)
		}
		// 系统消息必须留在最前
		for i, m := range got {
			if i > 0 && m.Role == core.RoleSystem {
				t.Fatalf("system message appeared at %d: %+v", i, got)
			}
		}
	})

	t.Run("budget_truncation_keeps_newest", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		var msgs []core.Message
		for i := 0; i < 20; i++ {
			msgs = append(msgs, core.Message{
				Role: core.RoleUser, Content: fmt.Sprintf("m%02d-%s", i, strings.Repeat("y", 40)),
			})
		}
		_ = store.Add(ctx, "s", msgs...)

		got, _ := store.Recent(ctx, "s", 200)
		if len(got) == 0 {
			t.Fatal("nothing kept")
		}
		last := got[len(got)-1].Content
		if !strings.HasPrefix(last, "m19-") {
			t.Fatalf("newest message not kept, last = %q", last)
		}
		first := got[0].Content
		if strings.HasPrefix(first, "m00-") {
			t.Fatalf("oldest message should have been dropped, first = %q", first)
		}
	})

	t.Run("tool_call_pairing_is_atomic", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		// assistant(tool_calls) + 其 tool 结果体积很大，预算只够装下
		// tool 结果时，两者必须同进同退，否则产生孤儿 tool 消息
		pair := []core.Message{
			{Role: core.RoleSystem, Content: "sys"},
			{Role: core.RoleAssistant, ToolCalls: []core.ToolCall{
				{ID: "c1", Name: "big", Arguments: `{"x":"` + strings.Repeat("z", 200) + `"}`},
			}},
			{Role: core.RoleTool, ToolCallID: "c1", Content: strings.Repeat("r", 200)},
			{Role: core.RoleUser, Content: "tail"},
		}
		if err := store.Add(ctx, "s", pair...); err != nil {
			t.Fatalf("Add: %v", err)
		}

		got, _ := store.Recent(ctx, "s", 20) // 只装得下 tail
		for _, m := range got {
			if m.Role == core.RoleTool {
				hasParent := false
				for _, other := range got {
					if other.Role == core.RoleAssistant {
						for _, tc := range other.ToolCalls {
							if tc.ID == m.ToolCallID {
								hasParent = true
							}
						}
					}
				}
				if !hasParent {
					t.Fatalf("orphan tool message returned: %+v", got)
				}
			}
		}
	})

	t.Run("split_reports_dropped", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		var msgs []core.Message
		for i := 0; i < 12; i++ {
			msgs = append(msgs, core.Message{
				Role: core.RoleUser, Content: fmt.Sprintf("m%02d-%s", i, strings.Repeat("q", 40)),
			})
		}
		_ = store.Add(ctx, "s", msgs...)

		kept, dropped, err := store.Split(ctx, "s", 120)
		if err != nil {
			t.Fatalf("Split: %v", err)
		}
		if len(kept)+len(dropped) != 12 {
			t.Fatalf("split lost messages: kept=%d dropped=%d", len(kept), len(dropped))
		}
		if len(dropped) == 0 {
			t.Fatal("expected some messages to be dropped")
		}
		// dropped 必须是最旧的那批，且保持原序
		if !strings.HasPrefix(dropped[0].Content, "m00-") {
			t.Fatalf("dropped does not start at oldest: %q", dropped[0].Content)
		}
	})

	t.Run("trim_removes_oldest_non_system", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
		for i := 0; i < 5; i++ {
			msgs = append(msgs, core.Message{Role: core.RoleUser, Content: fmt.Sprintf("m%d", i)})
		}
		_ = store.Add(ctx, "s", msgs...)

		if err := store.Trim(ctx, "s", 2); err != nil {
			t.Fatalf("Trim: %v", err)
		}
		got, _ := store.Recent(ctx, "s", 1<<20)
		// 1 条 system + 剩下 3 条 user
		if len(got) != 4 {
			t.Fatalf("after trim = %d msgs, want 4: %+v", len(got), got)
		}
		if got[0].Role != core.RoleSystem {
			t.Fatalf("system message was trimmed: %+v", got)
		}
		if got[1].Content != "m2" {
			t.Fatalf("wrong survivor frontier: %+v", got)
		}
	})

	t.Run("clear_removes_everything", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		_ = store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"})
		_ = store.SaveSummary(ctx, "s", 3, "summary text")
		if err := store.Clear(ctx, "s"); err != nil {
			t.Fatalf("Clear: %v", err)
		}
		got, _ := store.Recent(ctx, "s", 1<<20)
		if len(got) != 0 {
			t.Fatalf("messages survived Clear: %+v", got)
		}
		text, covered, err := store.LoadSummary(ctx, "s")
		if err != nil {
			t.Fatalf("LoadSummary after Clear: %v", err)
		}
		if text != "" || covered != 0 {
			t.Fatalf("summary survived Clear: %q %d", text, covered)
		}
	})

	t.Run("summary_round_trip", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		text, covered, err := store.LoadSummary(ctx, "s")
		if err != nil || text != "" || covered != 0 {
			t.Fatalf("empty summary = (%q, %d, %v)", text, covered, err)
		}
		if err := store.SaveSummary(ctx, "s", 7, "the summary"); err != nil {
			t.Fatalf("SaveSummary: %v", err)
		}
		text, covered, err = store.LoadSummary(ctx, "s")
		if err != nil || text != "the summary" || covered != 7 {
			t.Fatalf("LoadSummary = (%q, %d, %v)", text, covered, err)
		}
		// 覆盖写
		if err := store.SaveSummary(ctx, "s", 9, "updated"); err != nil {
			t.Fatalf("SaveSummary overwrite: %v", err)
		}
		text, covered, _ = store.LoadSummary(ctx, "s")
		if text != "updated" || covered != 9 {
			t.Fatalf("overwrite failed: (%q, %d)", text, covered)
		}
	})

	t.Run("invalid_session_id_rejected", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		for _, id := range []string{"", "../evil", "a/b", "a\\b", strings.Repeat("x", 200)} {
			if err := store.Add(ctx, id, core.Message{Role: core.RoleUser, Content: "x"}); !errors.Is(err, memorystore.ErrInvalidSessionID) {
				t.Fatalf("Add(%q) err = %v, want ErrInvalidSessionID", id, err)
			}
			if _, err := store.Recent(ctx, id, 100); !errors.Is(err, memorystore.ErrInvalidSessionID) {
				t.Fatalf("Recent(%q) err = %v, want ErrInvalidSessionID", id, err)
			}
		}
	})

	t.Run("concurrent_append_and_read", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				for j := 0; j < 5; j++ {
					_ = store.Add(ctx, "shared", core.Message{
						Role: core.RoleUser, Content: fmt.Sprintf("g%d-%d", i, j),
					})
					_, _ = store.Recent(ctx, "shared", 1<<20)
				}
			}(i)
		}
		wg.Wait()

		got, err := store.Recent(ctx, "shared", 1<<20)
		if err != nil {
			t.Fatalf("Recent: %v", err)
		}
		if len(got) != 40 {
			t.Fatalf("concurrent appends = %d messages, want 40", len(got))
		}
	})

	t.Run("works_with_summary_decorator", func(t *testing.T) {
		store, cleanup := h.newStore(t)
		defer cleanup()
		ctx := context.Background()

		// summary 装饰器要求内层实现 Splitter/Trimmer/SummaryStore；
		// 这里只验证装饰链可装配且 Add/Recent 语义仍然正确。
		llm := &stubLLM{}
		mem := memory.NewSummary(store, llm)
		var msgs []core.Message
		for i := 0; i < 10; i++ {
			msgs = append(msgs, core.Message{
				Role: core.RoleUser, Content: fmt.Sprintf("m%02d-%s", i, strings.Repeat("w", 50)),
			})
		}
		if err := mem.Add(ctx, "s", msgs...); err != nil {
			t.Fatalf("Add: %v", err)
		}
		got, err := mem.Recent(ctx, "s", 150)
		if err != nil {
			t.Fatalf("Recent: %v", err)
		}
		if len(got) == 0 {
			t.Fatal("summary-decorated store returned nothing")
		}
		hasSummary := false
		for _, m := range got {
			if m.Role == core.RoleSystem && strings.Contains(m.Content, "Summary of earlier") {
				hasSummary = true
			}
		}
		if !hasSummary {
			t.Fatalf("summary prefix not injected: %+v", got)
		}
	})
}

// stubLLM returns a fixed summary so the decorator test needs no model.
type stubLLM struct{}

// Chat implements core.LLM.
func (stubLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return &core.ChatResponse{Content: "Summary of earlier conversation."}, nil
}

// ChatStream implements core.LLM.
func (stubLLM) ChatStream(context.Context, core.ChatRequest) (<-chan core.StreamEvent, error) {
	return nil, errors.New("not implemented")
}

// waitFor polls until cond is true or the deadline passes.
func waitFor(t *testing.T, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", desc)
}

// coreMessageWithEverything builds a message that exercises every field
// a codec must preserve.
func coreMessageWithEverything() core.Message {
	return core.Message{
		Role:      core.RoleAssistant,
		Content:   "text content",
		Reasoning: "chain of thought",
		ToolCalls: []core.ToolCall{
			{ID: "call-1", Name: "search", Arguments: `{"q":"go"}`},
			{ID: "call-2", Name: "fetch", Arguments: `{"url":"https://example.com"}`},
		},
		ContentParts: []core.ContentPart{
			{Type: "text", Text: "look"},
			{Type: "image", ImageURL: "https://example.com/a.png"},
		},
		ToolCallID:   "parent-call",
		FinishReason: core.FinishToolCalls,
	}
}
