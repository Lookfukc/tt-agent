package test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
)

func TestPersistentSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	p1, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	ctx := context.Background()
	msgs := []core.Message{
		{Role: core.RoleSystem, Content: "sys"},
		{Role: core.RoleUser, Content: "问题"},
		{Role: core.RoleAssistant, Content: "回答"},
	}
	if err := p1.Add(ctx, "s1", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// 不关文件直接模拟崩溃，验证每条 Add 已落盘

	p2, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, err := p2.Recent(ctx, "s1", 1_000_000)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 3 || got[0].Role != core.RoleSystem || got[2].Content != "回答" {
		t.Errorf("restored = %+v", got)
	}

	// Clear 后再恢复应为空
	if err := p2.Clear(ctx, "s1"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	p3, _ := memory.NewPersistent(dir, nil)
	got, _ = p3.Recent(ctx, "s1", 1_000_000)
	if len(got) != 0 {
		t.Errorf("after clear = %+v", got)
	}
}

func TestPersistentSkipsCorruptLines(t *testing.T) {
	dir := t.TempDir()
	// 手工写入一行坏数据 + 一行好数据
	line := `{"role":"user","content":"ok"}` + "\n" + `{not-json}\n`
	if err := os.WriteFile(filepath.Join(dir, "s1.jsonl"), []byte(line), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	p, err := memory.NewPersistent(dir, nil)
	if err != nil {
		t.Fatalf("NewPersistent: %v", err)
	}
	got, _ := p.Recent(context.Background(), "s1", 1_000_000)
	if len(got) != 1 {
		t.Fatalf("restored = %d msgs, want 1 (bad line skipped)", len(got))
	}
}

func TestBufferAndPersistentSameTruncation(t *testing.T) {
	ctx := context.Background()
	mk := []core.Message{
		{Role: core.RoleSystem, Content: "s"},
		{Role: core.RoleUser, Content: "1"},
		{Role: core.RoleAssistant, Content: "2"},
		{Role: core.RoleUser, Content: "3"},
	}
	buf := memory.NewBuffer(nil)
	_ = buf.Add(ctx, "s", mk...)
	per, _ := memory.NewPersistent(t.TempDir(), nil)
	_ = per.Add(ctx, "s", mk...)

	b1, _ := buf.Recent(ctx, "s", 2) // 预算只够系统消息+1条
	b2, _ := per.Recent(ctx, "s", 2)
	if len(b1) != len(b2) {
		t.Fatalf("buffer=%d persistent=%d", len(b1), len(b2))
	}
	if len(b1) != 2 || b1[0].Role != core.RoleSystem || b1[1].Content != "3" {
		t.Errorf("truncated = %+v, want [sys, \"3\"]", b1)
	}
}
