package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// RunStore 运行状态存储抽象，checkpoint 续跑依赖它跨进程存活
type RunStore interface {
	// Save 保存运行状态，同 ID 覆盖
	Save(run *RunState) error
	// Get 读取运行状态
	// returns: 运行状态；ok 为 false 表示不存在
	Get(id string) (*RunState, bool)
}

// FileRunStore 每个运行一个 JSON 文件的存储
//
// 写入走临时文件加原子改名，进程崩溃不会留下半截状态
type FileRunStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileRunStore 打开或创建运行状态目录
// dir: 状态文件目录
// returns: 就绪的存储实例
func NewFileRunStore(dir string) (*FileRunStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create run store dir: %w", err)
	}
	return &FileRunStore{dir: dir}, nil
}

// Save 原子写入运行状态
//
// fsync 在 rename 前落盘：断电场景下 checkpoint 目录项可能先于
// 数据生效，续跑会读到半截状态
func (s *FileRunStore) Save(run *RunState) error {
	if !validRunID(run.ID) {
		return fmt.Errorf("invalid run id %q", run.ID)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	raw, err := json.Marshal(run)
	if err != nil {
		return fmt.Errorf("encode run: %w", err)
	}
	final := filepath.Join(s.dir, run.ID+".json")
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("write run: %w", err)
	}
	if _, err := f.Write(raw); err != nil {
		f.Close()
		return fmt.Errorf("write run: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("sync run: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close run: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("commit run: %w", err)
	}
	return nil
}

// validRunID 校验运行标识，runID 拼进文件路径须防穿越
// returns: true 表示合法
func validRunID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	if strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.Contains(id, "..") {
		return false
	}
	return true
}

// Get 读取运行状态
// returns: 运行状态；文件缺失或损坏时 ok 为 false
func (s *FileRunStore) Get(id string) (*RunState, bool) {
	if !validRunID(id) {
		return nil, false
	}
	raw, err := os.ReadFile(filepath.Join(s.dir, id+".json"))
	if err != nil {
		return nil, false
	}
	var run RunState
	if err := json.Unmarshal(raw, &run); err != nil {
		return nil, false
	}
	return &run, true
}
