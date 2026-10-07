package provider

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// quirkEntry 命名 quirks 条目：补丁函数 + 适用协议
type quirkEntry struct {
	patch     protocol.PatchFunc
	protocols []string // 空表示不限协议
}

// quirksLibrary 命名 quirks 库
//
// 行为修正是代码概念，配置文件按名引用；
// 新增厂商行为偏差优先评估能否进库复用，而非各写各的
var quirksLibrary = map[string]quirkEntry{
	// GLM 用顶层 thinking 字段控制思考模式，标准 OpenAI 协议没有这个字段
	"glm-thinking": {patch: glmThinkingPatch, protocols: []string{"openai"}},
	// reasoner 系模型收到采样参数会直接 400，必须删除
	"deepseek-reasoner": {patch: deepSeekReasonerPatch, protocols: []string{"openai"}},
}

// QuirkNames 列出全部可用 quirks 名
// returns: 排序后的名字列表
func QuirkNames() []string {
	names := make([]string, 0, len(quirksLibrary))
	for n := range quirksLibrary {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ComposeQuirks 按名组合 quirks
// names: 配置声明的名字列表，空列表返回零值
// proto: 提供商协议，用于校验补丁适用性
// returns: 组合结果，多个补丁按声明顺序依次执行；名字未知或协议不匹配时报错并列出可用名
func ComposeQuirks(names []string, proto string) (protocol.Quirks, error) {
	var q protocol.Quirks
	var patches []protocol.PatchFunc
	for _, n := range names {
		entry, ok := quirksLibrary[n]
		if !ok {
			return q, fmt.Errorf("unknown quirk %q (available: %s)", n, strings.Join(QuirkNames(), ", "))
		}
		if len(entry.protocols) > 0 && !contains(entry.protocols, proto) {
			return q, fmt.Errorf("quirk %q applies to protocol %s, provider uses %q", n, strings.Join(entry.protocols, "/"), proto)
		}
		if entry.patch != nil {
			patches = append(patches, entry.patch)
		}
	}
	if len(patches) > 0 {
		q.PatchRequest = func(body map[string]any, req core.ChatRequest) {
			for _, p := range patches {
				p(body, req)
			}
		}
	}
	return q, nil
}

// contains 切片包含判断
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// glmThinkingPatch 将统一 Thinking 配置转译为 GLM 私有字段
func glmThinkingPatch(body map[string]any, req core.ChatRequest) {
	if req.Thinking == nil {
		return
	}
	if req.Thinking.Enabled {
		body["thinking"] = map[string]any{"type": "enabled"}
	} else {
		body["thinking"] = map[string]any{"type": "disabled"}
	}
}

// deepSeekReasonerPatch 清除 reasoner 模型不接受的采样参数
func deepSeekReasonerPatch(body map[string]any, req core.ChatRequest) {
	if req.Model != "deepseek-reasoner" {
		return
	}
	delete(body, "temperature")
	delete(body, "top_p")
}
