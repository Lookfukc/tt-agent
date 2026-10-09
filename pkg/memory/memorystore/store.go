package memorystore

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/internal/sessionlog"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// Driver is the storage-specific half of a memory backend.
//
// A driver deals only in opaque encoded records: it never sees
// plaintext core.Message values, so an encrypting Codec protects data
// at rest without any driver needing to know about encryption. All
// budget accounting, truncation and tool-call grouping live in Store,
// so Redis, Postgres and SQLite behave identically by construction.
type Driver interface {
	// AppendRecords stores encoded records for a session, in call order.
	AppendRecords(ctx context.Context, sessionID string, recs []EncodedRecord) error

	// ScanRecords streams the session's records oldest-first.
	//
	// Implementations must stop and return the sink's error when it
	// fails, and should honor ctx cancellation between records.
	ScanRecords(ctx context.Context, sessionID string, sink RecordSink) error

	// CountMessages counts stored message records (summary excluded).
	CountMessages(ctx context.Context, sessionID string) (int, error)

	// TrimMessages removes the oldest n non-system messages.
	//
	// n counts non-system messages only, matching the drop side of
	// sessionlog.Split, so Split followed by Trim never cuts an
	// assistant(tool_calls)+tool pair in half. The driver learns
	// which records are non-system from EncodedRecord.System, so it
	// needs no plaintext access.
	TrimMessages(ctx context.Context, sessionID string, n int) error

	// DeleteSession removes every record (messages + summary).
	DeleteSession(ctx context.Context, sessionID string) error

	// SaveSummary persists the encoded summary record.
	SaveSummary(ctx context.Context, sessionID string, rec EncodedSummary) error

	// LoadSummary reads the encoded summary; missing returns ok=false.
	LoadSummary(ctx context.Context, sessionID string) (EncodedSummary, bool, error)

	// Close releases driver resources.
	Close() error
}

// EncodedRecord is one message as a driver sees it.
type EncodedRecord struct {
	// Data is the codec output — for an encrypting codec, ciphertext.
	Data []byte

	// System records whether this message has the system role.
	//
	// It is carried in clear so Trim can select the oldest non-system
	// records without decrypting the whole session. System messages
	// are never trimmed, and their role is not sensitive.
	System bool
}

// EncodedSummary is the encoded session summary.
type EncodedSummary struct {
	// Data is the codec output for the summary record.
	Data []byte
}

// RecordSink receives encoded records in stored order.
//
// Returning an error aborts the scan and propagates out of
// ScanRecords, so callers can stop a scan early (e.g. once enough
// recent messages have been collected).
type RecordSink func(rec EncodedRecord) error

// ErrStopScan is a convenience sentinel a sink may return to end a
// scan without signalling a real failure.
var ErrStopScan = jsonError("memorystore: stop scan")

// Unwrapper lets decorators expose their inner Driver, mirroring the
// Unwrap convention used by the memory package decorators.
type Unwrapper interface {
	Unwrap() Driver
}

// Unwrap walks a decorator chain and returns the innermost driver.
func Unwrap(d Driver) Driver {
	for {
		u, ok := d.(Unwrapper)
		if !ok {
			return d
		}
		d = u.Unwrap()
	}
}

// Store implements core.Memory plus the Splitter, Trimmer and
// SummaryStore capability interfaces on top of a Driver.
//
// Nothing is cached locally: every read hits the driver, so multiple
// processes sharing one backend observe each other's writes
// immediately. That is the whole point of an external driver — the
// in-process caches that memory.Persistent uses are safe only because
// it owns its files exclusively.
type Store struct {
	driver   Driver
	codec    Codec
	counter  core.TokenCounter
	observer observer.MemoryObserver
	backend  string

	// maxScan caps how many records a single read pulls from the
	// driver, so a pathologically long session cannot exhaust memory
	// while assembling a request. 0 means the default.
	maxScan int

	// cappedReads counts reads that stopped at the maxScan window.
	cappedReads atomic.Uint64
}

// defaultMaxScan bounds one read. Sessions far larger than this keep
// working (older records simply stay out of the window), matching the
// budget-truncation behaviour of the file-backed implementation.
const defaultMaxScan = 2000

// Options configures a Store.
type Options struct {
	// Codec encodes and decodes records. nil means plaintext JSON.
	// Supply an encrypting codec to protect data at rest.
	Codec Codec

	// Counter estimates tokens for budget accounting. nil means the
	// built-in conservative character estimate.
	Counter core.TokenCounter

	// MaxScan caps records fetched per read. <=0 means the default.
	MaxScan int

	// Observer receives lifecycle events (appended / trimmed /
	// cleared / summary). Emission is fire-and-forget; a slow
	// observer never slows a write. Drivers fill Backend when unset.
	Observer observer.MemoryObserver

	// Backend names the backend in emitted events. Each driver's
	// Memory method sets it automatically ("redis", "sqlite", …);
	// set it explicitly only for custom drivers.
	Backend string
}

// New builds a Store over a driver.
func New(driver Driver, opts Options) *Store {
	codec := opts.Codec
	if codec == nil {
		codec = JSONCodec{}
	}
	counter := opts.Counter
	if counter == nil {
		counter = sessionlog.Rough{}
	}
	maxScan := opts.MaxScan
	if maxScan <= 0 {
		maxScan = defaultMaxScan
	}
	return &Store{
		driver:   driver,
		codec:    codec,
		counter:  counter,
		observer: opts.Observer,
		backend:  opts.Backend,
		maxScan:  maxScan,
	}
}

// CappedReads reports how many reads stopped at the maxScan window.
//
// A climbing counter means sessions have grown past their read window
// and their oldest messages are silently out of view — raise
// Options.MaxScan (or trim/compact sessions) when that matters.
func (s *Store) CappedReads() uint64 { return s.cappedReads.Load() }

// Driver exposes the underlying driver.
func (s *Store) Driver() Driver { return s.driver }

// Close releases the underlying driver.
func (s *Store) Close() error { return s.driver.Close() }

// Add appends messages to a session.
func (s *Store) Add(ctx context.Context, sessionID string, msgs ...core.Message) error {
	if !ValidSessionID(sessionID) {
		return ErrInvalidSessionID
	}
	if len(msgs) == 0 {
		return nil
	}
	now := time.Now()
	recs := make([]EncodedRecord, 0, len(msgs))
	for _, m := range msgs {
		data, err := s.codec.EncodeMessage(MessageRecord{TS: now, Msg: m})
		if err != nil {
			return err
		}
		recs = append(recs, EncodedRecord{Data: data, System: m.Role == core.RoleSystem})
	}
	if err := s.driver.AppendRecords(ctx, sessionID, recs); err != nil {
		return err
	}
	s.emit(observer.EventMessagesAppended, sessionID, map[string]any{"count": len(msgs)})
	return nil
}

// Recent returns the most recent messages within the budget.
func (s *Store) Recent(ctx context.Context, sessionID string, budget int64) ([]core.Message, error) {
	kept, _, err := s.Split(ctx, sessionID, budget)
	return kept, err
}

// Split returns in-budget and dropped messages in one pass.
//
// The whole session window is materialized, then split with the shared
// sessionlog logic — the same code the file-backed implementation
// uses, which is what keeps truncation behaviour identical across
// backends.
func (s *Store) Split(ctx context.Context, sessionID string, budget int64) (kept, dropped []core.Message, err error) {
	if !ValidSessionID(sessionID) {
		return nil, nil, ErrInvalidSessionID
	}
	var log sessionlog.Log
	now := time.Now()
	err = s.scan(ctx, sessionID, func(rec MessageRecord) {
		log.Add(s.counter, now, rec.Msg)
	})
	if err != nil {
		return nil, nil, err
	}
	if len(log.Msgs) == 0 {
		return nil, nil, nil
	}
	kept, dropped = log.Split(budget)
	return kept, dropped, nil
}

// Trim physically discards the oldest n non-system messages.
func (s *Store) Trim(ctx context.Context, sessionID string, n int) error {
	if !ValidSessionID(sessionID) {
		return ErrInvalidSessionID
	}
	if n <= 0 {
		return nil
	}
	if err := s.driver.TrimMessages(ctx, sessionID, n); err != nil {
		return err
	}
	s.emit(observer.EventSessionTrimmed, sessionID, map[string]any{"count": n})
	return nil
}

// Clear removes the session entirely.
func (s *Store) Clear(ctx context.Context, sessionID string) error {
	if !ValidSessionID(sessionID) {
		return ErrInvalidSessionID
	}
	if err := s.driver.DeleteSession(ctx, sessionID); err != nil {
		return err
	}
	s.emit(observer.EventSessionCleared, sessionID, nil)
	return nil
}

// DefaultBackend fills the event label for a driver unless the caller
// already set one. Each driver passes its own name ("redis", "sqlite",
// "postgres"), keeping events self-describing without callers doing
// anything.
func DefaultBackend(driverName, explicit string) string {
	if explicit != "" {
		return explicit
	}
	return driverName
}

// emit forwards a lifecycle event to the observer, if any.
//
// Deliberately after the write succeeded: events describe durable
// state changes, and the helper being nil-capable keeps call sites
// unconditional.
func (s *Store) emit(kind observer.MemoryEventKind, sessionID string, detail map[string]any) {
	if s.observer == nil {
		return
	}
	s.observer.OnMemoryEvent(observer.MemoryEvent{
		Kind:    kind,
		Backend: s.backend,
		Session: sessionID,
		At:      time.Now(),
		Detail:  detail,
	})
}

// SaveSummary persists the summary, satisfying memory.SummaryStore.
func (s *Store) SaveSummary(ctx context.Context, sessionID string, covered int, text string) error {
	if !ValidSessionID(sessionID) {
		return ErrInvalidSessionID
	}
	data, err := s.codec.EncodeSummary(SummaryRecord{Covered: covered, Text: text})
	if err != nil {
		return err
	}
	if err := s.driver.SaveSummary(ctx, sessionID, EncodedSummary{Data: data}); err != nil {
		return err
	}
	s.emit(observer.EventSummarySaved, sessionID, map[string]any{"covered": covered})
	return nil
}

// LoadSummary reads the summary, satisfying memory.SummaryStore.
func (s *Store) LoadSummary(ctx context.Context, sessionID string) (string, int, error) {
	if !ValidSessionID(sessionID) {
		return "", 0, ErrInvalidSessionID
	}
	enc, ok, err := s.driver.LoadSummary(ctx, sessionID)
	if err != nil || !ok {
		return "", 0, err
	}
	rec, err := s.codec.DecodeSummary(enc.Data)
	if err != nil {
		// A summary that cannot be decoded is treated as absent:
		// the summary layer then recomputes it instead of failing
		// the whole request.
		return "", 0, nil
	}
	return rec.Text, rec.Covered, nil
}

// CountMessages reports how many messages a session holds.
func (s *Store) CountMessages(ctx context.Context, sessionID string) (int, error) {
	if !ValidSessionID(sessionID) {
		return 0, ErrInvalidSessionID
	}
	return s.driver.CountMessages(ctx, sessionID)
}

// scan decodes every stored message oldest-first.
//
// Records the codec rejects are skipped rather than failing the read:
// a single corrupt row must not take a session down, matching the
// corrupt-line tolerance of the file-backed implementation.
//
// Reads that stop at the maxScan window increment CappedReads, so
// operators can tell when sessions have outgrown their window instead
// of silently losing pre-window context.
func (s *Store) scan(ctx context.Context, sessionID string, fn func(MessageRecord)) error {
	count := 0
	err := s.driver.ScanRecords(ctx, sessionID, func(rec EncodedRecord) error {
		if cerr := ContextErr(ctx); cerr != nil {
			return cerr
		}
		if count >= s.maxScan {
			s.cappedReads.Add(1)
			return ErrStopScan
		}
		decoded, err := s.codec.DecodeMessage(rec.Data)
		if err != nil || decoded.Msg.Role == "" {
			return nil // skip corrupt record
		}
		count++
		fn(decoded)
		return nil
	})
	if err != nil && err != ErrStopScan {
		return err
	}
	return nil
}
