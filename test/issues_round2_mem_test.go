package test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorytest"
)

// maxSessionLineMirror mirrors pkg/memory/persistent.go's unexported constant
// maxSessionLine (4<<20): black-box tests cannot access the symbol, so the
// threshold is derived from the value
const maxSessionLineMirror = 4 << 20

// readBufferMirror mirrors loadSession's 64KB reader buffer:
// the old implementation's "reset then re-accumulate" emitted a tail after the
// 65th chunk, and the offset is derived from that
const readBufferMirror = 64 * 1024

// TestN4_ReadLineCapHoldsForOverlongLine: the tail of an overlong line must not be recovered as an independent line
//
// The old implementation reset buf to nil past the limit and kept accumulating,
// so the last segment of the line (< limit) was returned as a normal line:
// constructing a line longer than limit+64KB whose tail happens to be valid JSON
// made the old implementation recover "TAIL-POISON" as a message
func TestN4_ReadLineCapHoldsForOverlongLine(t *testing.T) {
	dir := t.TempDir()

	// The old implementation discarded one [limit, limit+64KB) chunk and
	// re-accumulated from offset limit+64KB; starting the valid JSON at that
	// offset lands it exactly in the "tail line" the old implementation emitted
	pad := strings.Repeat("x", maxSessionLineMirror+readBufferMirror)
	tail := `{"role":"user","content":"TAIL-POISON"}`
	content := `{"role":"user","content":"first"}` + "\n" + pad + tail + "\n"
	if err := writeFile(filepath.Join(dir, "s1.jsonl"), content); err != nil {
		t.Fatalf("write: %v", err)
	}

	p, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	got, err := p.Recent(context.Background(), "s1", 1<<30)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 1 || got[0].Content != "first" {
		t.Fatalf("N4: over-long line tail leaked into recovery, got %d msgs: %+v", len(got), got)
	}
}

// TestN4_ExactLimitLineLoaded: a whole line (including newline) exactly at the limit must be kept
//
// The over-limit check uses strictly greater-than: a valid large message
// exactly at limit must not be wrongly dropped
func TestN4_ExactLimitLineLoaded(t *testing.T) {
	dir := t.TempDir()

	prefix := `{"role":"user","content":"`
	suffix := `"}`
	padLen := maxSessionLineMirror - 1 - len(prefix) - len(suffix) // 1 byte reserved for the newline
	line := prefix + strings.Repeat("y", padLen) + suffix
	if len(line)+1 != maxSessionLineMirror {
		t.Fatalf("setup: raw line = %d bytes, want %d", len(line)+1, maxSessionLineMirror)
	}
	if err := writeFile(filepath.Join(dir, "s2.jsonl"), line+"\n"); err != nil {
		t.Fatalf("write: %v", err)
	}

	p, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	// Budget is generous (4MB of content is roughly 2M tokens by rough estimate),
	// to keep budget truncation from interfering with the assertions
	got, err := p.Recent(context.Background(), "s2", 1<<30)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("N4: exact-limit line dropped, got %d msgs, want 1", len(got))
	}
	if got[0].Role != core.RoleUser || len(got[0].Content) != padLen {
		t.Fatalf("N4: exact-limit line corrupted, role=%s content len=%d, want %d", got[0].Role, len(got[0].Content), padLen)
	}
}

// perMessageCounter is an estimator that always counts 1 token per message
type perMessageCounter struct{}

// Count counts by number of messages
// returns: the message count
func (perMessageCounter) Count(msgs []core.Message) int64 { return int64(len(msgs)) }

// TestL_M1SummaryUsesInjectedCounter: with an injected estimator, budget packing follows the injected metric
//
// Same data, same budget: the injected metric (1 token per message) keeps the
// most recent 3 messages, while the built-in rough estimate (~29 tokens per
// message) fits none and falls back to keeping only the newest group;
// the differing truncation points prove the accounting really switched counters
func TestL_M1SummaryUsesInjectedCounter(t *testing.T) {
	ctx := context.Background()
	msgs := make([]core.Message, 0, 5)
	for i := 0; i < 5; i++ {
		// 19 three-byte CJK characters + an index digit: roughly 29 tokens/message
		// by rough estimate, 1 token/message under the injected metric
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: strings.Repeat("字", 19) + string(rune('0'+i)),
		})
	}
	const budget = int64(3)
	countUsers := func(got []core.Message) []string {
		var users []string
		for _, m := range got {
			if m.Role == core.RoleUser {
				users = append(users, m.Content)
			}
		}
		return users
	}

	injected := memory.NewSummaryWithCounter(memorytest.NewBuffer(nil), &summaryLLM{}, perMessageCounter{})
	if err := injected.Add(ctx, "s-inj", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}
	gotInj, err := injected.Recent(ctx, "s-inj", budget)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	users := countUsers(gotInj)
	if len(users) != 3 || !strings.HasSuffix(users[0], "2") ||
		!strings.HasSuffix(users[1], "3") || !strings.HasSuffix(users[2], "4") {
		t.Fatalf("L-M1: injected counter truncation point wrong, users = %v", users)
	}

	// Control group: the default constructor keeps the built-in rough-estimate
	// metric and truncates earlier under the same budget
	def := memory.NewSummary(memorytest.NewBuffer(nil), &summaryLLM{})
	if err := def.Add(ctx, "s-def", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}
	gotDef, err := def.Recent(ctx, "s-def", budget)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if users := countUsers(gotDef); len(users) != 1 || !strings.HasSuffix(users[0], "4") {
		t.Fatalf("L-M1: default rough counter baseline changed, users = %v", users)
	}
}
