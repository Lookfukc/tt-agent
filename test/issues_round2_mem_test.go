package test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/core"
	"github.com/Lookfukc/send-agent/pkg/memory"
)

// maxSessionLineMirror 镜像 pkg/memory/persistent.go 的未导出常量
// maxSessionLine（4<<20）：黑盒测试拿不到符号，只能按值推导阈值
const maxSessionLineMirror = 4 << 20

// readBufferMirror 镜像 loadSession 的 Reader 缓冲 64KB：
// 旧实现的"重置后重新累积"从第 65 块之后吐出尾部，偏移由此推出
const readBufferMirror = 64 * 1024

// TestN4_ReadLineCapHoldsForOverlongLine 超长行的尾部不得被当成独立行恢复
//
// 旧实现超限后把 buf 重置为 nil 继续累积，行尾最后一段（< limit）
// 会被当正常行返回：构造行长 > limit+64KB 且尾部恰好是合法 JSON，
// 旧实现会把 "TAIL-POISON" 恢复成消息
func TestN4_ReadLineCapHoldsForOverlongLine(t *testing.T) {
	dir := t.TempDir()

	// 旧实现丢弃 [limit, limit+64KB) 一块后从 limit+64KB 偏移重新累积，
	// 让合法 JSON 从该偏移起开始，正好落入旧实现吐出的"尾部行"
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

// TestN4_ExactLimitLineLoaded 恰好等于上限的整行（含换行）必须保留
//
// 判超限用严格大于：等于 limit 的合法大消息不能被误杀
func TestN4_ExactLimitLineLoaded(t *testing.T) {
	dir := t.TempDir()

	prefix := `{"role":"user","content":"`
	suffix := `"}`
	padLen := maxSessionLineMirror - 1 - len(prefix) - len(suffix) // 1 字节留给换行
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
	// 预算给足（4MB 内容粗估约 2M token），避免预算截断干扰断言
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

// perMessageCounter 每条消息恒计 1 token 的估算器
type perMessageCounter struct{}

// Count 按消息条数计数
// returns: 消息条数
func (perMessageCounter) Count(msgs []core.Message) int64 { return int64(len(msgs)) }

// TestL_M1SummaryUsesInjectedCounter 注入估算器后预算装填按注入口径执行
//
// 同一数据同一预算：注入口径（每条 1 token）保留最近 3 条，
// 内置粗估（每条约 29 token）一条都装不下、走兜底只留最新一组，
// 截断点不同证明记账确实换了计数器
func TestL_M1SummaryUsesInjectedCounter(t *testing.T) {
	ctx := context.Background()
	msgs := make([]core.Message, 0, 5)
	for i := 0; i < 5; i++ {
		// 19 个三字节汉字 + 序号：粗估约 29 token/条，注入口径 1 token/条
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

	injected := memory.NewSummaryWithCounter(memory.NewBuffer(nil), &summaryLLM{}, perMessageCounter{})
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

	// 对照组：默认构造维持内置粗估口径，同一预算下截得更早
	def := memory.NewSummary(memory.NewBuffer(nil), &summaryLLM{})
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
