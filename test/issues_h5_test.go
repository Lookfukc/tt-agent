package test

import (
	"context"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
)

// assertNoOrphanTool 校验历史中每条 tool 消息前都有携带对应 tool_call 的父 assistant
func assertNoOrphanTool(t *testing.T, msgs []core.Message) {
	t.Helper()
	callerIDs := map[string]bool{}
	for _, m := range msgs {
		for _, tc := range m.ToolCalls {
			callerIDs[tc.ID] = true
		}
		if m.Role == core.RoleTool {
			if !callerIDs[m.ToolCallID] {
				t.Fatalf("H5: orphan tool message %q — parent assistant truncated away, API would 400", m.ToolCallID)
			}
		}
	}
}

// TestH5NoOrphanToolMessages 预算刚好装下 tool 结果但装不下父消息时，
// 原逐条装填产出孤儿 tool 消息；原子组打包必须同进同退
func TestH5NoOrphanToolMessages(t *testing.T) {
	ctx := context.Background()
	msgs := []core.Message{
		{Role: core.RoleSystem, Content: "sys"},
		{Role: core.RoleUser, Content: "第一问"},
		{
			Role: core.RoleAssistant, Content: "",
			ToolCalls: []core.ToolCall{
				{ID: "t1", Name: "a", Arguments: `{}`},
				{ID: "t2", Name: "b", Arguments: `{}`},
			},
		},
		{Role: core.RoleTool, ToolCallID: "t1", Content: "结果一"},
		{Role: core.RoleTool, ToolCallID: "t2", Content: "结果二"},
		{Role: core.RoleUser, Content: "第二问"},
		{Role: core.RoleAssistant, Content: "最终回答"},
	}

	for _, budget := range []int64{4, 6, 8, 10, 12, 20, 100} {
		buf := memory.NewBuffer(nil)
		if err := buf.Add(ctx, "s", msgs...); err != nil {
			t.Fatalf("Add: %v", err)
		}
		got, err := buf.Recent(ctx, "s", budget)
		if err != nil {
			t.Fatalf("Recent(budget=%d): %v", budget, err)
		}
		assertNoOrphanTool(t, got)
	}
}

// TestH5GroupNeverEmpty 极小预算下也必须保留至少一组完整历史
func TestH5GroupNeverEmpty(t *testing.T) {
	ctx := context.Background()
	buf := memory.NewBuffer(nil)
	_ = buf.Add(ctx, "s",
		core.Message{Role: core.RoleAssistant, Content: "早", ToolCalls: []core.ToolCall{{ID: "x", Name: "n", Arguments: `{}`}}},
		core.Message{Role: core.RoleTool, ToolCallID: "x", Content: "晚"},
	)
	got, err := buf.Recent(ctx, "s", 1)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("H5: tiny budget produced empty history")
	}
	assertNoOrphanTool(t, got)
}

// TestH5SummaryDropsGroups 摘要记忆同样不得产出孤儿 tool 消息
func TestH5SummaryDropsGroups(t *testing.T) {
	ctx := context.Background()
	inner := memory.NewBuffer(nil)
	mem := memory.NewSummary(inner, &summaryLLM{})
	msgs := []core.Message{
		{Role: core.RoleSystem, Content: "sys"},
		{Role: core.RoleUser, Content: "旧问题"},
		{
			Role:      core.RoleAssistant,
			ToolCalls: []core.ToolCall{{ID: "t1", Name: "a", Arguments: `{}`}},
		},
		{Role: core.RoleTool, ToolCallID: "t1", Content: "旧结果"},
		{Role: core.RoleUser, Content: "新问题"},
	}
	_ = mem.Add(ctx, "s2", msgs...)

	got, err := mem.Recent(ctx, "s2", 8)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	assertNoOrphanTool(t, got)
}
