// Package snapshot adds point-in-time snapshots and rollback to any
// session memory backend.
//
// It exists because external memory drivers store an ordered log: you
// can append and truncate, but you cannot ask "what did this session
// look like three turns ago" or undo a bad turn. Snapshots answer both
// without changing the core.Memory contract, so backends stay simple
// and every decorator built on them keeps working.
//
// Snapshots are stored through the same Driver interface the session
// data uses, under a reserved namespace key, so a snapshot inherits
// the backend's durability, encryption and multi-process visibility.
package snapshot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// ErrNoSnapshot reports a rollback target that does not exist.
var ErrNoSnapshot = errors.New("snapshot: no such snapshot")

// Entry describes one captured point in time.
type Entry struct {
	// ID identifies the snapshot within its session.
	ID string `json:"id"`

	// Label is an optional human-readable name ("before-tool-call").
	Label string `json:"label,omitempty"`

	// MessageCount is how many messages the session held when captured.
	MessageCount int `json:"message_count"`

	// CreatedAt is when the snapshot was taken.
	CreatedAt time.Time `json:"created_at"`
}

// record is the on-disk payload of one snapshot.
//
// The messages are stored the same way the session log stores them —
// one MessageRecord per message, encoded through the Codec — so an
// encrypting codec protects snapshots exactly as it protects live
// messages. Marshaling the whole record as plain JSON would silently
// bypass encryption and leak every conversation into the store.
type record struct {
	Entry Entry `json:"entry"`
	// Messages holds the codec-encoded messages, in order.
	Messages [][]byte `json:"messages"`
	// Plain carries the decoded messages for callers reading them back.
	// It is not serialized.
	Plain []core.Message `json:"-"`
}

// Store captures and restores session states.
//
// A Store wraps a memorystore.Driver directly (not a Store), so it can
// read and rewrite the raw log. Wrap the same driver with
// memorystore.New to get the core.Memory view of the same data.
type Store struct {
	driver memorystore.Driver
	codec  memorystore.Codec
	prefix string

	// maxPerSession bounds retained snapshots per session; the oldest
	// are dropped first. 0 means the default.
	maxPerSession int

	mu sync.Mutex
}

// Options configures the snapshot store.
type Options struct {
	// Codec encodes snapshot payloads. Defaults to plaintext JSON;
	// pass the same codec the driver's data uses to keep one key.
	Codec memorystore.Codec

	// MaxPerSession caps snapshots retained per session.
	MaxPerSession int

	// Prefix namespaces snapshot keys. Empty means the default.
	Prefix string
}

// defaultMaxSnapshots bounds history per session.
//
// Snapshots are full message copies, so an unbounded history would
// multiply storage by the turn count. Keeping the most recent handful
// covers the real use cases (undo the last turn, inspect the state
// before a tool call).
const defaultMaxSnapshots = 20

// defaultPrefix namespaces snapshot keys apart from message keys.
const defaultPrefix = "snapshot:"

// New builds a snapshot store over a driver.
func New(driver memorystore.Driver, opts Options) *Store {
	codec := opts.Codec
	if codec == nil {
		codec = memorystore.JSONCodec{}
	}
	limit := opts.MaxPerSession
	if limit <= 0 {
		limit = defaultMaxSnapshots
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Store{driver: driver, codec: codec, prefix: prefix, maxPerSession: limit}
}

// Capture stores the current state of a session.
//
// label may be empty. The returned Entry is what Rollback and the
// listing APIs accept.
func (s *Store) Capture(ctx context.Context, sessionID, label string) (Entry, error) {
	if !memorystore.ValidSessionID(sessionID) {
		return Entry{}, memorystore.ErrInvalidSessionID
	}
	msgs, err := s.readSession(ctx, sessionID)
	if err != nil {
		return Entry{}, err
	}
	now := time.Now()
	entry := Entry{
		ID:           fmt.Sprintf("%d", now.UnixNano()),
		Label:        label,
		MessageCount: len(msgs),
		CreatedAt:    now,
	}
	// Encode each message through the Codec so snapshots inherit
	// whatever protection the codec provides (notably encryption).
	encoded := make([][]byte, 0, len(msgs))
	for _, m := range msgs {
		data, err := s.codec.EncodeMessage(memorystore.MessageRecord{TS: now, Msg: m})
		if err != nil {
			return Entry{}, fmt.Errorf("snapshot: encode message: %w", err)
		}
		encoded = append(encoded, data)
	}
	payload, err := json.Marshal(record{Entry: entry, Messages: encoded})
	if err != nil {
		return Entry{}, fmt.Errorf("snapshot: encode: %w", err)
	}
	// Every snapshot of a session is appended to one log key, so List
	// and delete are simple ordered scans of a single session ID.
	// Giving each snapshot its own key would make enumeration
	// impossible through the Driver contract, which deliberately has
	// no key listing.
	key := s.key(sessionID)
	rec := memorystore.EncodedRecord{Data: payload}
	if err := s.driver.AppendRecords(ctx, key, []memorystore.EncodedRecord{rec}); err != nil {
		return Entry{}, err
	}
	if err := s.prune(ctx, sessionID); err != nil {
		// A failed prune must not fail the capture: the snapshot is
		// already durable, and retention is best-effort.
		return entry, nil
	}
	return entry, nil
}

// List returns a session's snapshots, newest first.
func (s *Store) List(ctx context.Context, sessionID string) ([]Entry, error) {
	if !memorystore.ValidSessionID(sessionID) {
		return nil, memorystore.ErrInvalidSessionID
	}
	var out []Entry
	err := s.driver.ScanRecords(ctx, s.key(sessionID), func(rec memorystore.EncodedRecord) error {
		var r record
		if err := json.Unmarshal(rec.Data, &r); err != nil {
			return nil // skip a corrupt snapshot rather than failing the list
		}
		out = append(out, r.Entry)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return out, nil
}

// Rollback restores a session to a snapshot.
//
// The session's message log is replaced by the snapshot's contents. It
// is deliberately destructive: the point of rollback is to discard
// what came after, and keeping both states would need conflict rules
// nobody wants to reason about. Capture a snapshot first if the
// current state should remain recoverable.
func (s *Store) Rollback(ctx context.Context, sessionID, snapshotID string) (Entry, error) {
	if !memorystore.ValidSessionID(sessionID) {
		return Entry{}, memorystore.ErrInvalidSessionID
	}
	target, err := s.load(ctx, sessionID, snapshotID)
	if err != nil {
		return Entry{}, err
	}

	// Restore the recorded payloads verbatim: they are already in the
	// codec's format, so re-encoding would be redundant and would
	// re-encrypt with a fresh nonce for no benefit.
	if err := s.driver.DeleteSession(ctx, sessionID); err != nil {
		return Entry{}, err
	}
	recs := make([]memorystore.EncodedRecord, 0, len(target.Messages))
	for i, data := range target.Messages {
		system := false
		if i < len(target.Plain) {
			system = target.Plain[i].Role == core.RoleSystem
		}
		recs = append(recs, memorystore.EncodedRecord{Data: data, System: system})
	}
	if len(recs) > 0 {
		if err := s.driver.AppendRecords(ctx, sessionID, recs); err != nil {
			return Entry{}, err
		}
	}
	return target.Entry, nil
}

// Delete removes one snapshot.
func (s *Store) Delete(ctx context.Context, sessionID, snapshotID string) error {
	if !memorystore.ValidSessionID(sessionID) {
		return memorystore.ErrInvalidSessionID
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteOne(ctx, sessionID, snapshotID)
}

// readSession materializes the current messages of a session.
func (s *Store) readSession(ctx context.Context, sessionID string) ([]core.Message, error) {
	var out []core.Message
	err := s.driver.ScanRecords(ctx, sessionID, func(rec memorystore.EncodedRecord) error {
		decoded, err := s.codec.DecodeMessage(rec.Data)
		if err != nil || decoded.Msg.Role == "" {
			return nil // skip corrupt record
		}
		out = append(out, decoded.Msg)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// load reads one snapshot payload.
func (s *Store) load(ctx context.Context, sessionID, snapshotID string) (record, error) {
	var found record
	var ok bool
	err := s.driver.ScanRecords(ctx, s.key(sessionID), func(rec memorystore.EncodedRecord) error {
		var r record
		if err := json.Unmarshal(rec.Data, &r); err != nil {
			return nil
		}
		if r.Entry.ID == snapshotID {
			// Decode the messages too: Rollback needs to know which of
			// them are system messages, and that fact lives in the
			// plaintext, not in the stored payload.
			r.Plain = s.decodeAll(r.Messages)
			found, ok = r, true
			return memorystore.ErrStopScan
		}
		return nil
	})
	if err != nil && err != memorystore.ErrStopScan {
		return record{}, err
	}
	if !ok {
		return record{}, ErrNoSnapshot
	}
	return found, nil
}

// decodeAll decodes stored message payloads, dropping corrupt entries.
func (s *Store) decodeAll(payloads [][]byte) []core.Message {
	out := make([]core.Message, 0, len(payloads))
	for _, data := range payloads {
		decoded, err := s.codec.DecodeMessage(data)
		if err != nil || decoded.Msg.Role == "" {
			continue
		}
		out = append(out, decoded.Msg)
	}
	return out
}

// prune enforces the per-session snapshot cap.
func (s *Store) prune(ctx context.Context, sessionID string) error {
	entries, err := s.List(ctx, sessionID)
	if err != nil {
		return err
	}
	if len(entries) <= s.maxPerSession {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries[s.maxPerSession:] {
		if err := s.deleteOne(ctx, sessionID, e.ID); err != nil {
			return err
		}
	}
	return nil
}

// deleteOne removes a single snapshot.
//
// The driver contract has no "delete one record", so the surviving
// snapshots are rewritten. Snapshot counts are small and bounded, and
// reusing AppendRecords/DeleteSession keeps every backend working
// without a driver-specific method.
func (s *Store) deleteOne(ctx context.Context, sessionID, snapshotID string) error {
	key := s.key(sessionID)
	var kept [][]byte
	err := s.driver.ScanRecords(ctx, key, func(rec memorystore.EncodedRecord) error {
		var r record
		if err := json.Unmarshal(rec.Data, &r); err != nil {
			return nil
		}
		if r.Entry.ID == snapshotID {
			return nil
		}
		kept = append(kept, rec.Data)
		return nil
	})
	if err != nil {
		return err
	}
	if err := s.driver.DeleteSession(ctx, key); err != nil {
		return err
	}
	if len(kept) == 0 {
		return nil
	}
	recs := make([]memorystore.EncodedRecord, 0, len(kept))
	for _, data := range kept {
		recs = append(recs, memorystore.EncodedRecord{Data: data})
	}
	return s.driver.AppendRecords(ctx, key, recs)
}

// key builds the snapshot log key for a session.
//
// Snapshots live under a derived session ID so they ride the same
// driver without colliding with the conversation's own messages.
func (s *Store) key(sessionID string) string {
	return s.prefix + sessionID
}
