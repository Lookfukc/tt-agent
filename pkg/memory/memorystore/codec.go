// Package memorystore holds the shared implementation used by external
// storage drivers (Redis, Postgres, SQLite).
//
// Every driver stores the same logical model: an ordered append-only
// list of messages per session, plus a single summary record. The
// truncation and pairing semantics live in internal/sessionlog, so all
// drivers agree by construction rather than by convention.
package memorystore

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
)

// Codec converts records to and from their on-wire representation.
//
// Drivers keep the encoding pluggable so an encrypted codec can be
// layered on without touching driver code: the driver only ever sees
// opaque bytes.
type Codec interface {
	// EncodeMessage serializes one stored message record.
	EncodeMessage(rec MessageRecord) ([]byte, error)
	// DecodeMessage parses one stored message record.
	DecodeMessage(data []byte) (MessageRecord, error)
	// EncodeSummary serializes the session summary record.
	EncodeSummary(rec SummaryRecord) ([]byte, error)
	// DecodeSummary parses the session summary record.
	DecodeSummary(data []byte) (SummaryRecord, error)
}

// MessageRecord is one stored message plus its append time.
//
// It mirrors the JSONL envelope used by memory.Persistent so the same
// data can round-trip between the file-backed implementation and any
// external driver.
type MessageRecord struct {
	TS  time.Time    `json:"ts,omitzero"`
	Msg core.Message `json:"msg"`
}

// SummaryRecord is the persisted summary plus the number of messages
// folded into it.
type SummaryRecord struct {
	Covered int    `json:"covered"`
	Text    string `json:"text"`
}

// JSONCodec is the default plaintext codec.
//
// It is wire-compatible with memory.Persistent's JSONL envelope: a
// session written by the file backend decodes here unchanged, and vice
// versa.
//
// Limitation: JSON strings must be valid UTF-8, and Go's encoding/json
// silently replaces invalid bytes with U+FFFD. Chat content is text
// and this is rarely observable, but callers pushing arbitrary binary
// payloads through Message.Content should know the normalization
// happens — switching the codec is the escape hatch.
type JSONCodec struct{}

// EncodeMessage implements Codec.
func (JSONCodec) EncodeMessage(rec MessageRecord) ([]byte, error) {
	return json.Marshal(rec)
}

// DecodeMessage implements Codec.
//
// Messages written by the legacy bare-message format (no ts/msg
// envelope) are accepted too, so old data keeps working with zero
// migration. Records failing both shapes return an error; drivers
// treat that as a corrupt entry and skip it.
func (JSONCodec) DecodeMessage(data []byte) (MessageRecord, error) {
	var rec MessageRecord
	if err := json.Unmarshal(data, &rec); err == nil && rec.Msg.Role != "" {
		return rec, nil
	}
	var msg core.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return MessageRecord{}, err
	}
	if msg.Role == "" {
		return MessageRecord{}, errEmptyRecord
	}
	return MessageRecord{Msg: msg}, nil
}

// EncodeSummary implements Codec.
func (JSONCodec) EncodeSummary(rec SummaryRecord) ([]byte, error) {
	return json.Marshal(rec)
}

// DecodeSummary implements Codec.
func (JSONCodec) DecodeSummary(data []byte) (SummaryRecord, error) {
	var rec SummaryRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return SummaryRecord{}, err
	}
	return rec, nil
}

// errEmptyRecord marks a record whose role is missing.
var errEmptyRecord = jsonError("memorystore: record has no role")

// jsonError is a minimal error type so this file needs no fmt import.
type jsonError string

// Error implements error.
func (e jsonError) Error() string { return string(e) }

// MessageSink receives decoded messages in stored order.
//
// Drivers call it while scanning their storage; returning an error
// aborts the scan.
type MessageSink func(rec MessageRecord) error

// ValidSessionID reports whether a session identifier is safe to use.
//
// Drivers that key paths or rows by sessionID must reject separators
// and traversal sequences; this centralizes the rule so every driver
// applies the same one.
func ValidSessionID(id string) bool {
	if id == "" || len(id) > 128 {
		return false
	}
	for _, r := range id {
		switch r {
		case '/', '\\':
			return false
		}
	}
	return !containsDotDot(id)
}

// containsDotDot reports whether id contains a ".." sequence.
func containsDotDot(id string) bool {
	for i := 0; i+1 < len(id); i++ {
		if id[i] == '.' && id[i+1] == '.' {
			return true
		}
	}
	return false
}

// ErrInvalidSessionID is returned by drivers for unusable session IDs.
var ErrInvalidSessionID = jsonError("memorystore: invalid session id")

// ContextErr returns ctx.Err() when the context is done, else nil.
//
// Drivers call it at scan boundaries so a cancelled request stops
// streaming rows instead of reading the whole session first.
func ContextErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
