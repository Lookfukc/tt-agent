// Package ltm implements long-term memory: facts that outlive a single
// conversation.
//
// Session memory (package memory) answers "what did we just say";
// long-term memory answers "what do I know about this user". The two
// are deliberately separate: session history is an ordered log read
// chronologically, whereas long-term memory is an unordered set of
// facts read by relevance.
//
// The pipeline mirrors what production memory services do:
//
//	conversation  ->  Extractor  ->  facts  ->  Store (+ Embedder)  ->  vectors
//	user query    ->  Embedder   ->  vector ->  Store.Search        ->  relevant facts
package ltm

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// Fact is one remembered statement about a subject.
type Fact struct {
	// ID uniquely identifies the fact within its namespace.
	ID string

	// Namespace isolates facts by owner — typically a user ID, tenant
	// ID, or agent ID. Recalls never cross namespaces.
	Namespace string

	// Text is the fact itself, in natural language.
	Text string

	// Metadata carries caller-defined attributes (source session,
	// category, confidence, ...). It is stored verbatim.
	Metadata map[string]string

	// CreatedAt and UpdatedAt are set by the Store.
	CreatedAt time.Time
	UpdatedAt time.Time

	// Score is the relevance of this fact to the query that produced
	// it. Zero for facts that were not returned from a search.
	Score float64
}

// Store persists and recalls facts.
//
// Implementations must be safe for concurrent use and must scope every
// operation to the namespace they are given.
type Store interface {
	// Upsert stores a fact, replacing any existing fact with the same
	// namespace and ID. Returns the stored fact with timestamps set.
	Upsert(ctx context.Context, fact Fact, vector []float32) (Fact, error)

	// Search returns the most relevant facts in a namespace.
	//
	// When vector is non-empty, implementations that support vector
	// search rank by similarity; others fall back to text matching.
	// query is always available so keyword-only stores stay usable.
	// limit <= 0 means the implementation's default.
	Search(ctx context.Context, namespace, query string, vector []float32, limit int) ([]Fact, error)

	// List returns facts in a namespace, newest first.
	List(ctx context.Context, namespace string, limit int) ([]Fact, error)

	// Delete removes one fact.
	Delete(ctx context.Context, namespace, id string) error

	// Clear removes every fact in a namespace.
	Clear(ctx context.Context, namespace string) error
}

// Embedder turns text into a vector.
//
// The framework does not ship one: embeddings are provider-specific,
// and forcing a dependency (or a default model) on every user would be
// worse than asking for a small adapter. See EmbedderFunc for the
// one-line case.
type Embedder interface {
	// Embed returns the vector for a piece of text.
	Embed(ctx context.Context, text string) ([]float32, error)
}

// EmbedderFunc adapts a function to Embedder.
type EmbedderFunc func(ctx context.Context, text string) ([]float32, error)

// Embed implements Embedder.
func (f EmbedderFunc) Embed(ctx context.Context, text string) ([]float32, error) {
	return f(ctx, text)
}

// Extractor distills a conversation into durable facts.
//
// It is normally backed by a cheap LLM call; the framework keeps it an
// interface so tests need no model and users can plug in their own
// prompting strategy.
type Extractor interface {
	// Extract returns the facts worth remembering from a conversation.
	// Returning an empty slice is a valid outcome (nothing memorable).
	Extract(ctx context.Context, msgs []Message) ([]string, error)
}

// ExtractorFunc adapts a function to Extractor.
type ExtractorFunc func(ctx context.Context, msgs []Message) ([]string, error)

// Extract implements Extractor.
func (f ExtractorFunc) Extract(ctx context.Context, msgs []Message) ([]string, error) {
	return f(ctx, msgs)
}

// Message is the minimal conversation shape an Extractor needs.
//
// It is intentionally not core.Message: extraction only cares about
// who said what, and decoupling keeps this package dependency-free.
type Message struct {
	// Role is one of "system", "user", "assistant", "tool".
	Role string
	// Content is the message text.
	Content string
}

// Memory is the facade that ties extraction, embedding and storage
// together for callers.
type Memory struct {
	store     Store
	embedder  Embedder
	extractor Extractor
	observer  observer.MemoryObserver
	backend   string

	// defaultLimit caps recall size when the caller does not specify one.
	defaultLimit int
}

// Options configures the facade.
type Options struct {
	// Embedder enables vector search. nil keeps the memory
	// keyword-only, which still works for small namespaces.
	Embedder Embedder

	// Extractor enables Learn. nil disables it: Remember still stores
	// facts explicitly, but conversations are never auto-mined.
	Extractor Extractor

	// DefaultLimit is the recall size when a caller passes limit <= 0.
	DefaultLimit int

	// Observer receives fact lifecycle events (remembered /
	// forgotten). Emission is fire-and-forget.
	Observer observer.MemoryObserver

	// Backend labels emitted events (e.g. "memory" for the in-process
	// store, "pgvector" for Postgres).
	Backend string
}

// defaultRecallLimit bounds a recall when the caller does not say.
const defaultRecallLimit = 5

// New builds the facade over a store.
func New(store Store, opts Options) *Memory {
	limit := opts.DefaultLimit
	if limit <= 0 {
		limit = defaultRecallLimit
	}
	return &Memory{
		store:        store,
		embedder:     opts.Embedder,
		extractor:    opts.Extractor,
		observer:     opts.Observer,
		backend:      opts.Backend,
		defaultLimit: limit,
	}
}

// ErrNoStore reports a facade used without a store.
var ErrNoStore = errors.New("ltm: store is required")

// ErrEmptyNamespace reports an operation without an owning namespace.
//
// Facts are always scoped to an owner; an empty namespace would make
// one user's facts visible to every other user, so it is rejected
// rather than silently treated as a shared bucket.
var ErrEmptyNamespace = errors.New("ltm: namespace is required")

// Remember stores one explicit fact, embedding it when possible.
//
// An embedding failure is not fatal: the fact is stored without a
// vector and stays reachable through keyword search, because losing a
// user's stated preference is worse than losing its ranking quality.
func (m *Memory) Remember(ctx context.Context, namespace, text string, metadata map[string]string) (Fact, error) {
	if m.store == nil {
		return Fact{}, ErrNoStore
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return Fact{}, errors.New("ltm: fact text must not be empty")
	}
	fact := Fact{
		ID:        factID(namespace, text),
		Namespace: namespace,
		Text:      text,
		Metadata:  metadata,
	}
	vector, err := m.embed(ctx, text)
	if err != nil {
		vector = nil
	}
	stored, err := m.store.Upsert(ctx, fact, vector)
	if err == nil {
		m.emit(observer.EventFactRemembered, namespace, map[string]any{"fact_id": stored.ID})
	}
	return stored, err
}

// emit forwards a fact lifecycle event, if an observer is set.
func (m *Memory) emit(kind observer.MemoryEventKind, namespace string, detail map[string]any) {
	if m.observer == nil {
		return
	}
	m.observer.OnMemoryEvent(observer.MemoryEvent{
		Kind:    kind,
		Backend: m.backend,
		Session: namespace,
		At:      time.Now(),
		Detail:  detail,
	})
}

// Recall returns the facts most relevant to a query.
func (m *Memory) Recall(ctx context.Context, namespace, query string, limit int) ([]Fact, error) {
	if m.store == nil {
		return nil, ErrNoStore
	}
	if limit <= 0 {
		limit = m.defaultLimit
	}
	vector, err := m.embed(ctx, query)
	if err != nil {
		vector = nil // degrade to keyword search
	}
	return m.store.Search(ctx, namespace, query, vector, limit)
}

// Learn mines a conversation for facts and stores them.
//
// It returns the stored facts so callers can log or display what was
// remembered. A nil Extractor makes this a no-op, which is how
// applications that only want explicit Remember() behave.
func (m *Memory) Learn(ctx context.Context, namespace string, msgs []Message) ([]Fact, error) {
	if m.store == nil {
		return nil, ErrNoStore
	}
	if m.extractor == nil || len(msgs) == 0 {
		return nil, nil
	}
	texts, err := m.extractor.Extract(ctx, msgs)
	if err != nil {
		return nil, err
	}
	out := make([]Fact, 0, len(texts))
	for _, text := range texts {
		text = strings.TrimSpace(text)
		if text == "" {
			continue
		}
		fact, err := m.Remember(ctx, namespace, text, nil)
		if err != nil {
			return out, err
		}
		out = append(out, fact)
	}
	return out, nil
}

// Forget removes one fact.
func (m *Memory) Forget(ctx context.Context, namespace, id string) error {
	if m.store == nil {
		return ErrNoStore
	}
	if err := m.store.Delete(ctx, namespace, id); err != nil {
		return err
	}
	m.emit(observer.EventFactForgotten, namespace, map[string]any{"fact_id": id})
	return nil
}

// List returns the facts stored for a namespace.
func (m *Memory) List(ctx context.Context, namespace string, limit int) ([]Fact, error) {
	if m.store == nil {
		return nil, ErrNoStore
	}
	return m.store.List(ctx, namespace, limit)
}

// Store exposes the underlying store.
func (m *Memory) Store() Store { return m.store }

// embed produces a vector when an embedder is configured.
func (m *Memory) embed(ctx context.Context, text string) ([]float32, error) {
	if m.embedder == nil || strings.TrimSpace(text) == "" {
		return nil, nil
	}
	return m.embedder.Embed(ctx, text)
}

// Prompt renders recalled facts as a system-message fragment.
//
// Callers append this to the system prompt (or send it as its own
// system message) so the model sees durable context without the
// framework touching the conversation store.
func Prompt(facts []Fact) string {
	if len(facts) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteString("Known facts about this user:\n")
	for _, f := range facts {
		b.WriteString("- ")
		b.WriteString(f.Text)
		b.WriteByte('\n')
	}
	return strings.TrimRight(b.String(), "\n")
}

// factID derives a stable identifier from namespace and text.
//
// Stable means re-learning the same fact updates one row instead of
// piling up duplicates — the same deduplication production memory
// services get from an LLM "merge or add" decision, obtained here
// without an extra model call.
func factID(namespace, text string) string {
	return hashID(namespace + "\x00" + normalize(text))
}

// normalize lowercases and collapses whitespace so trivially different
// spellings of one fact share an ID.
func normalize(s string) string {
	return strings.ToLower(strings.Join(strings.Fields(s), " "))
}
