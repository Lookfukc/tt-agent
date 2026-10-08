package memory

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/internal/sessionlog"
)

// Persistent is a session memory persisted to JSONL files on disk.
//
// Each session is one <sessionID>.jsonl append-only file with
// write-through: the in-memory cache serves reads, and every message
// is appended to disk. A process crash loses at most the last entry.
// Sessions are loaded lazily on demand: the file is read only on first
// access, the number of sessions resident in memory is bounded by
// maxLoaded, and the least recently used ones beyond that limit are
// unloaded (memory only — the disk is never deleted, it is the source
// of truth).
type Persistent struct {
	dir       string
	mu        sync.RWMutex
	sessions  map[string]*pEntry
	counter   core.TokenCounter
	maxLoaded int // cap on sessions resident in memory; 0 means unlimited
}

// pEntry holds the in-memory resident state of a single session.
type pEntry struct {
	log     sessionlog.Log
	lastUse time.Time // LRU basis; touched by Add/Recent/Trim alike
	// dirty marks sessions whose disk write has failed: they are never
	// evicted by LRU (eviction would silently lose data), until a full
	// rewrite eventually succeeds and clears the flag.
	dirty bool
}

// diskRecord is the single-line on-disk record: message + push time.
//
// The old format was a bare core.Message (no ts/msg wrapper). Reading
// first decodes with this struct; if the msg role is empty it retries
// as a bare message, so legacy files work with zero migration.
type diskRecord struct {
	TS  time.Time    `json:"ts,omitzero"`
	Msg core.Message `json:"msg"`
}

// NewPersistent opens or restores a persistent memory.
//
// There is no cap on the number of sessions (everything lives on disk;
// memory residency is lazy and access-driven); when the session count
// is huge and all sessions are active, use NewPersistentWithLRU to
// bound the residency limit.
// dir: directory for session files, created if it does not exist.
// counter: token estimator; nil uses the built-in rough estimate.
// returns: a ready instance; an error if the directory is not writable.
func NewPersistent(dir string, counter core.TokenCounter) (*Persistent, error) {
	return NewPersistentWithLRU(dir, counter, 0)
}

// NewPersistentWithLRU opens a persistent memory and bounds the number
// of sessions resident in memory.
//
// The old implementation eagerly loaded every session in the directory
// at construction; tens of thousands of sessions would crush the
// process. With lazy loading plus LRU unloading, memory usage depends
// only on the number of simultaneously active sessions.
// dir: directory for session files.
// counter: token estimator; nil uses the built-in rough estimate.
// maxLoaded: cap on sessions resident in memory; beyond the limit the
// least recently accessed are unloaded (data stays on disk).
// returns: a ready instance; an error if the directory is not writable.
func NewPersistentWithLRU(dir string, counter core.TokenCounter, maxLoaded int) (*Persistent, error) {
	if counter == nil {
		counter = sessionlog.Rough{}
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create memory dir: %w", err)
	}
	return &Persistent{
		dir:       dir,
		sessions:  make(map[string]*pEntry),
		counter:   counter,
		maxLoaded: maxLoaded,
	}, nil
}

// maxSessionLine is the per-line limit; oversized lines are skipped as
// corrupt lines rather than aborting recovery.
const maxSessionLine = 4 << 20

// validSessionID validates a session identifier.
//
// The sessionID is concatenated directly into file paths; one
// containing separators or ".." could traverse directories and read or
// write anywhere, so intercepting at the entry point is the only line
// of defense. After lazy loading, read paths also trigger file access,
// so Recent/Split/Trim must validate just like Add/Clear.
// returns: true if the id is valid.
func validSessionID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	if strings.Contains(id, "/") || strings.Contains(id, "\\") || strings.Contains(id, "..") {
		return false
	}
	return true
}

// Add appends messages and writes them to disk.
//
// The disk write happens under the lock: the on-disk ordering of
// concurrent Adds must match the in-memory ordering, otherwise the
// session restored after a restart would be out of order.
func (p *Persistent) Add(_ context.Context, sessionID string, msgs ...core.Message) error {
	if !validSessionID(sessionID) {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.loadLocked(sessionID)
	now := time.Now()
	e.log.Add(p.counter, now, msgs...)

	// A failed disk write must not crash the process, but the caller
	// must be able to observe it; marking dirty prevents LRU from
	// unloading data that now exists "only in memory" and losing it.
	if err := p.appendDisk(sessionID, msgs, now); err != nil {
		e.dirty = true
		return err
	}
	return nil
}

// Recent returns the most recent messages within budget, with the same
// semantics as memorytest.Buffer.
func (p *Persistent) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	kept, _, err := p.Split(ctx, sessionID, budget)
	return kept, err
}

// Split returns messages inside and outside the budget, lazily loading
// the session on first access.
func (p *Persistent) Split(_ context.Context, sessionID string, budget int64) ([]core.Message, []core.Message, error) {
	if !validSessionID(sessionID) {
		return nil, nil, fmt.Errorf("invalid session id %q", sessionID)
	}
	// Fast path: if already resident, only a read lock is needed.
	p.mu.RLock()
	if e, ok := p.sessions[sessionID]; ok {
		kept, dropped := e.log.Split(budget)
		p.mu.RUnlock()
		return kept, dropped, nil
	}
	p.mu.RUnlock()

	// Slow path: first access; reading the file requires the write
	// lock (it may write to the map).
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.loadLocked(sessionID)
	kept, dropped := e.log.Split(budget)
	return kept, dropped, nil
}

// Trim physically discards the oldest n non-system messages and
// atomically rewrites the session file.
//
// A temp file is fully rewritten then renamed over the original: if
// the process crashes mid-rewrite the file is either the old one or
// the new one — a half-written state is impossible.
func (p *Persistent) Trim(_ context.Context, sessionID string, n int) error {
	if !validSessionID(sessionID) {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.loadLocked(sessionID)
	before := e.log.Count()
	e.log.Trim(n)
	if e.log.Count() == before {
		return nil // nothing to delete; leave the disk untouched
	}
	return p.rewriteLocked(sessionID, e)
}

// Clear empties the session and deletes its on-disk files (including
// the summary file).
//
// Deleting files is serialized under the same lock as in-flight
// appends; otherwise a concurrent append after the clear would
// recreate the session.
func (p *Persistent) Clear(_ context.Context, sessionID string) error {
	if !validSessionID(sessionID) {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, sessionID)
	for _, path := range []string{p.sessionPath(sessionID), p.summaryPath(sessionID)} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// sessionPath returns the session file path.
// returns: <sessionID>.jsonl inside the directory.
func (p *Persistent) sessionPath(sessionID string) string {
	return filepath.Join(p.dir, sessionID+".jsonl")
}

// summaryPath returns the session summary file path.
// returns: <sessionID>.summary inside the directory.
func (p *Persistent) summaryPath(sessionID string) string {
	return filepath.Join(p.dir, sessionID+".summary")
}

// summaryRecord is the on-disk summary record: summary text + the
// number of folded-in messages.
type summaryRecord struct {
	Covered int    `json:"covered"`
	Text    string `json:"text"`
}

// SaveSummary persists the session summary, atomically replacing the
// file via temp + rename.
//
// The summary is the only copy of the old context in compact mode, so
// persisting it must be atomic: if the process crashes mid-write the
// file is either the old summary or the new one — a half-written
// state is impossible.
func (p *Persistent) SaveSummary(_ context.Context, sessionID string, covered int, text string) error {
	if !validSessionID(sessionID) {
		return fmt.Errorf("invalid session id %q", sessionID)
	}
	line, err := json.Marshal(summaryRecord{Covered: covered, Text: text})
	if err != nil {
		return fmt.Errorf("encode summary: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	path := p.summaryPath(sessionID)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(line, '\n'), 0o644); err != nil {
		return fmt.Errorf("write summary temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("swap summary file: %w", err)
	}
	return nil
}

// LoadSummary reads back the session summary; a missing or corrupt
// file is treated as "no summary".
//
// It returns ("", 0, nil): the Summary layer uses that to take the
// "no cache" path and re-compress, so the worst degradation from a
// corrupt summary is one redundant LLM call — the session is not
// interrupted.
func (p *Persistent) LoadSummary(_ context.Context, sessionID string) (string, int, error) {
	if !validSessionID(sessionID) {
		return "", 0, fmt.Errorf("invalid session id %q", sessionID)
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	data, err := os.ReadFile(p.summaryPath(sessionID))
	if err != nil {
		return "", 0, nil // missing/unreadable are both treated as no summary
	}
	var rec summaryRecord
	if json.Unmarshal(data, &rec) != nil || rec.Text == "" {
		return "", 0, nil
	}
	return rec.Text, rec.Covered, nil
}

// loadLocked returns the session's resident state, reading it from
// disk if not yet resident.
//
// The caller must hold the write lock (it may write to the map and
// trigger LRU unloading).
// returns: the session's resident state; an empty session if the file
// does not exist or is unreadable.
func (p *Persistent) loadLocked(sessionID string) *pEntry {
	if e, ok := p.sessions[sessionID]; ok {
		e.lastUse = time.Now()
		return e
	}
	e := &pEntry{lastUse: time.Now()}
	msgs, tss := loadSessionRecords(p.sessionPath(sessionID))
	for i := range msgs {
		e.log.Add(p.counter, tss[i], msgs[i])
	}
	p.sessions[sessionID] = e
	p.evictLocked(sessionID)
	return e
}

// evictLocked unloads the least recently accessed sessions per LRU
// when the resident count exceeds the cap.
//
// It only drops in-memory entries, never disk files (disk is the
// source of truth and can be re-read at any time); dirty sessions and
// the current session are never evicted — exceeding the cap is a
// performance issue, losing data is a correctness issue.
// exclude: the session currently being served.
func (p *Persistent) evictLocked(exclude string) {
	if p.maxLoaded <= 0 {
		return
	}
	for len(p.sessions) > p.maxLoaded {
		victim := ""
		var oldest time.Time
		for id, e := range p.sessions {
			if id == exclude || e.dirty {
				continue
			}
			if victim == "" || e.lastUse.Before(oldest) {
				victim, oldest = id, e.lastUse
			}
		}
		if victim == "" {
			return // all dirty/current sessions; prefer exceeding the cap
		}
		delete(p.sessions, victim)
	}
}

// appendDisk appends to the session file.
func (p *Persistent) appendDisk(sessionID string, msgs []core.Message, now time.Time) error {
	f, err := os.OpenFile(p.sessionPath(sessionID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open session file: %w", err)
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for _, m := range msgs {
		line, err := json.Marshal(diskRecord{TS: now, Msg: m})
		if err != nil {
			return fmt.Errorf("encode message: %w", err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			return fmt.Errorf("write session file: %w", err)
		}
	}
	return w.Flush()
}

// rewriteLocked fully rewrites the session file (atomic replacement
// via temp + rename).
//
// Called after Trim physically compacts the log; a successful rewrite
// also clears the dirty flag — the disk now contains all in-memory
// content, and the session regains LRU eviction eligibility.
func (p *Persistent) rewriteLocked(sessionID string, e *pEntry) error {
	path := p.sessionPath(sessionID)
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("open rewrite temp: %w", err)
	}
	w := bufio.NewWriter(f)
	for i, m := range e.log.Msgs {
		line, err := json.Marshal(diskRecord{TS: e.log.Tss[i], Msg: m})
		if err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("encode message: %w", err)
		}
		if _, err := w.Write(append(line, '\n')); err != nil {
			f.Close()
			os.Remove(tmp)
			return fmt.Errorf("write rewrite temp: %w", err)
		}
	}
	if err := w.Flush(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("flush rewrite temp: %w", err)
	}
	// On Windows the handle must be closed before rename, otherwise
	// the target file is locked.
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close rewrite temp: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("swap session file: %w", err)
	}
	e.dirty = false
	return nil
}

// loadSessionRecords reads a single session file, skipping corrupt
// and oversized lines.
//
// Each line is first decoded as diskRecord (new format); if the msg
// role is empty it is retried as a bare core.Message (old format);
// failing both layers the line is discarded as corrupt.
// returns: the recovered message list and aligned push times.
func loadSessionRecords(path string) (msgs []core.Message, tss []time.Time) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil // file missing = empty session; other read errors are treated as empty, matching the old loadAll
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 64*1024)
	for {
		line, readErr := readLineLimited(r, maxSessionLine)
		if len(line) > 0 {
			var rec diskRecord
			if json.Unmarshal(line, &rec) == nil && rec.Msg.Role != "" {
				msgs = append(msgs, rec.Msg)
				tss = append(tss, rec.TS)
			} else {
				var m core.Message
				if json.Unmarshal(line, &m) == nil && m.Role != "" {
					msgs = append(msgs, m)
					tss = append(tss, time.Time{})
				}
			}
		}
		if readErr != nil {
			return msgs, tss
		}
	}
}

// readLineLimited reads one line under a length limit.
//
// As soon as the accumulated length (including the newline) strictly
// exceeds the limit it enters discard mode: the whole line is voided
// and input is consumed until the newline before returning nil,
// guaranteeing subsequent lines remain readable; a line exactly equal
// to the limit is kept as a valid whole line. The old implementation
// kept accumulating after a reset, emitting the tail of an oversized
// line as if it were an independent line, letting poison-line content
// leak into the recovered result.
// returns: the line content (without newline; always nil for
// oversized lines) and the read error (io.EOF means end of file).
func readLineLimited(r *bufio.Reader, limit int) ([]byte, error) {
	var buf []byte
	overflow := false
	for {
		chunk, err := r.ReadSlice('\n')
		if !overflow && len(buf)+len(chunk) > limit {
			// Just exceeded the limit: void everything accumulated
			// so far and switch to discard mode until end of line.
			overflow = true
			buf = nil
		}
		if !overflow {
			buf = append(buf, chunk...)
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		// err is nil (newline included) or io.EOF: this line is done.
		if overflow {
			return nil, err
		}
		return trimNewline(buf), err
	}
}

// trimNewline strips the trailing newline from a line.
// returns: the processed line.
func trimNewline(b []byte) []byte {
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
		if n = len(b); n > 0 && b[n-1] == '\r' {
			b = b[:n-1]
		}
	}
	return b
}
