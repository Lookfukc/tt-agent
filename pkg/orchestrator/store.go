package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// RunStore is the run state storage abstraction; checkpoint resumption
// depends on it outliving processes.
type RunStore interface {
	// Save persists the run state; the same ID overwrites.
	Save(run *RunState) error
	// Get reads the run state.
	// returns: the run state; ok is false if it does not exist
	Get(id string) (*RunState, bool)
}

// FileRunStore is a store using one JSON file per run.
//
// Writes go through a temp file plus an atomic rename, so a process crash
// never leaves a half-written state behind.
type FileRunStore struct {
	dir string
	mu  sync.Mutex
}

// NewFileRunStore opens or creates the run state directory.
// dir: the state file directory
// returns: the ready store instance
func NewFileRunStore(dir string) (*FileRunStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create run store dir: %w", err)
	}
	return &FileRunStore{dir: dir}, nil
}

// Save atomically writes the run state.
//
// fsync flushes before rename: under power loss, the checkpoint
// directory entry could take effect before the data, and resumption
// would read a half-written state.
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

// validRunID validates the run identifier; runIDs are joined into file
// paths and must guard against traversal.
// returns: true if valid
func validRunID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	if strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.Contains(id, "..") {
		return false
	}
	return true
}

// Get reads the run state.
// returns: the run state; ok is false when the file is missing or corrupt
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
