// Package tools 提供工具注册与查找能力
package tools

import (
	"fmt"
	"sort"
	"sync"

	"github.com/Lookfukc/send-agent/pkg/core"
)

// Registry 工具注册表，并发安全
type Registry struct {
	mu    sync.RWMutex
	tools map[string]core.Tool
}

// NewRegistry 构造空注册表
// returns: 可用的注册表实例
func NewRegistry() *Registry {
	return &Registry{tools: make(map[string]core.Tool)}
}

// Register 注册工具，重名覆盖
func (r *Registry) Register(t core.Tool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tools[t.Name()] = t
}

// Get 按名查找工具
// returns: 工具实例；ok 为 false 表示未注册
func (r *Registry) Get(name string) (core.Tool, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	t, ok := r.tools[name]
	return t, ok
}

// Specs 输出全部工具的定义，用于组装 ChatRequest
// returns: 工具定义列表，按名稳定排序
func (r *Registry) Specs() []core.ToolSpec {
	r.mu.RLock()
	defer r.mu.RUnlock()
	specs := make([]core.ToolSpec, 0, len(r.tools))
	for _, t := range r.tools {
		specs = append(specs, core.ToolSpec{
			Name: t.Name(), Description: t.Description(), Parameters: t.Parameters(),
		})
	}
	// map 迭代序随机：不排序则每次请求的工具定义顺序都不同，
	// 提供商侧按前缀缓存 prompt 的机制全部失效
	sort.Slice(specs, func(i, j int) bool { return specs[i].Name < specs[j].Name })
	return specs
}

// MustGet 按名查找工具，未注册时返回错误而非 panic，避免模型幻觉工具名打断整个循环
// returns: 工具实例
func (r *Registry) MustGet(name string) (core.Tool, error) {
	t, ok := r.Get(name)
	if !ok {
		return nil, fmt.Errorf("tool not registered: %s", name)
	}
	return t, nil
}
