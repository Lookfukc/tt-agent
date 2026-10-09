package snapshot_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// benchFixture builds a snapshot fixture with n pre-filled messages of
// realistic chat size.
func benchFixture(b *testing.B, n int) fixture {
	b.Helper()
	f := newFixture(b)
	msgs := make([]core.Message, 0, n)
	for i := 0; i < n; i++ {
		msgs = append(msgs, core.Message{
			Role:    core.RoleUser,
			Content: fmt.Sprintf("snapshot bench %06d aaaaaaaaaa bbbbbbbbbb cccccccccc", i),
		})
	}
	if err := f.mem.Add(context.Background(), "s", msgs...); err != nil {
		b.Fatalf("Add: %v", err)
	}
	return f
}

// BenchmarkCapture100 measures snapshotting a 100-message session:
// read + encode + append of the whole state.
func BenchmarkCapture100(b *testing.B) {
	f := benchFixture(b, 100)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.snaps.Capture(ctx, "s", "bench"); err != nil {
			b.Fatalf("Capture: %v", err)
		}
	}
}

// BenchmarkCapture1000 shows how capture scales with session length —
// snapshots are full copies, so this is the cost shape to know.
func BenchmarkCapture1000(b *testing.B) {
	f := benchFixture(b, 1000)
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.snaps.Capture(ctx, "s", "bench"); err != nil {
			b.Fatalf("Capture: %v", err)
		}
	}
}
