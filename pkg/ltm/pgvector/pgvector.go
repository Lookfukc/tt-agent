// Package pgvector implements ltm.Store on PostgreSQL with the
// pgvector extension, giving long-term memory a shared, durable home
// with true vector search.
//
// Vectors travel as pgvector's text format ("[1,2,3]"::vector): the
// store only ever writes embeddings and computes distances, never
// reads one back, so no client-side type registration — and no extra
// dependency — is needed.
//
// Migrate creates the extension and the table; the connecting role
// must be allowed to CREATE EXTENSION (the database owner is).
package pgvector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Lookfukc/tt-agent/pkg/ltm"
)

// Store persists facts in PostgreSQL with pgvector similarity search.
//
// Schema (created by Migrate):
//
//	ltm_facts(namespace, id, text, metadata jsonb, embedding vector(dim),
//	          created_at, updated_at, PRIMARY KEY(namespace, id))
//
// The vector column has a fixed dimension declared in Options.Dim.
// Embeddings whose length differs (an embedding provider changed, say)
// are stored as NULL rather than rejected: the fact stays available
// through keyword search, which is the same degrade-not-lose rule the
// in-process store follows.
type Store struct {
	pool    *pgxpool.Pool
	schema  string
	dim     int
	maxScan int
	ownPool bool
}

// Options configures the store.
type Options struct {
	// Dim is the embedding dimension. Required (a vector column must
	// declare one) and immutable after Migrate; changing embedding
	// providers to a different dimension means a new table.
	Dim int

	// Schema is the table namespace; empty means "public".
	Schema string

	// MaxKeywordScan caps how many facts the keyword fallback fetches
	// before ranking in Go. 0 means the default.
	MaxKeywordScan int
}

// defaultKeywordScan bounds the fallback scan: keyword ranking is a
// client-side operation, so an unbounded fetch would pull a whole
// namespace into memory.
const defaultKeywordScan = 1000

// New builds a store over an existing pool.
func New(pool *pgxpool.Pool, opts Options) (*Store, error) {
	if opts.Dim <= 0 {
		return nil, errors.New("pgvector: Options.Dim is required")
	}
	schema := opts.Schema
	if schema == "" {
		schema = "public"
	}
	scan := opts.MaxKeywordScan
	if scan <= 0 {
		scan = defaultKeywordScan
	}
	return &Store{pool: pool, schema: schema, dim: opts.Dim, maxScan: scan, ownPool: false}, nil
}

// NewFromURL connects by DSN and owns the resulting pool.
func NewFromURL(ctx context.Context, url string, opts Options) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("pgvector: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgvector: ping: %w", err)
	}
	s, err := New(pool, opts)
	if err != nil {
		pool.Close()
		return nil, err
	}
	s.ownPool = true
	return s, nil
}

// table returns the qualified table name.
func (s *Store) table() string {
	return quoteIdent(s.schema) + "." + quoteIdent("ltm_facts")
}

// quoteIdent double-quotes an SQL identifier.
func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// Migrate creates the extension and the table. Idempotent.
func (s *Store) Migrate(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `CREATE EXTENSION IF NOT EXISTS vector`); err != nil {
		return fmt.Errorf("pgvector: create extension (the connecting role must own the database): %w", err)
	}
	stmt := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		namespace  text NOT NULL,
		id         text NOT NULL,
		text       text NOT NULL,
		metadata   jsonb,
		embedding  vector(%d),
		created_at timestamptz NOT NULL DEFAULT now(),
		updated_at timestamptz NOT NULL DEFAULT now(),
		PRIMARY KEY (namespace, id)
	)`, s.table(), s.dim)
	if _, err := s.pool.Exec(ctx, stmt); err != nil {
		return fmt.Errorf("pgvector: migrate: %w", err)
	}
	idx := fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (namespace, updated_at DESC)`,
		quoteIdent("ltm_facts_namespace_updated_idx"), s.table())
	if _, err := s.pool.Exec(ctx, idx); err != nil {
		return fmt.Errorf("pgvector: migrate index: %w", err)
	}
	return nil
}

// Close releases the pool when this store owns it.
func (s *Store) Close() error {
	if s.ownPool && s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// Pool exposes the connection pool for operator queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Upsert implements ltm.Store.
//
// Vectors of the wrong dimension (or containing non-finite values, which
// pgvector rejects) are stored as NULL — the fact survives via keyword
// search instead of failing the write.
func (s *Store) Upsert(ctx context.Context, fact ltm.Fact, vector []float32) (ltm.Fact, error) {
	if fact.Namespace == "" {
		return ltm.Fact{}, ltm.ErrEmptyNamespace
	}
	if fact.ID == "" {
		return ltm.Fact{}, errors.New("pgvector: fact ID is required")
	}
	meta, err := json.Marshal(fact.Metadata)
	if err != nil {
		return ltm.Fact{}, fmt.Errorf("pgvector: encode metadata: %w", err)
	}
	var embedding any
	if lit, ok := vectorLiteral(vector, s.dim); ok {
		embedding = lit
	}
	err = s.pool.QueryRow(ctx, fmt.Sprintf(`
		INSERT INTO %s (namespace, id, text, metadata, embedding)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (namespace, id) DO UPDATE SET
			text = EXCLUDED.text,
			metadata = EXCLUDED.metadata,
			embedding = EXCLUDED.embedding,
			updated_at = now()
		RETURNING created_at, updated_at`, s.table()),
		fact.Namespace, fact.ID, fact.Text, string(meta), embedding,
	).Scan(&fact.CreatedAt, &fact.UpdatedAt)
	if err != nil {
		return ltm.Fact{}, fmt.Errorf("pgvector: upsert: %w", err)
	}
	return fact, nil
}

// Search implements ltm.Store.
//
// With a matching-dimension query vector it ranks by cosine distance
// in SQL (`embedding <=> query`); otherwise it fetches up to
// MaxKeywordScan facts and ranks them with the shared keyword scorer,
// so behavior stays aligned with the in-process store.
func (s *Store) Search(ctx context.Context, namespace, query string, vector []float32, limit int) ([]ltm.Fact, error) {
	if namespace == "" {
		return nil, ltm.ErrEmptyNamespace
	}
	if limit <= 0 {
		limit = 5
	}
	if lit, ok := vectorLiteral(vector, s.dim); ok {
		return s.searchByVector(ctx, namespace, lit, limit)
	}
	return s.searchByKeyword(ctx, namespace, query, limit)
}

// searchByVector ranks by cosine similarity in SQL.
func (s *Store) searchByVector(ctx context.Context, namespace, lit string, limit int) ([]ltm.Fact, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, text, metadata, created_at, updated_at,
		       1 - (embedding <=> $3::vector) AS score
		FROM %s
		WHERE namespace = $1 AND embedding IS NOT NULL
		ORDER BY embedding <=> $3::vector
		LIMIT $2`, s.table()), namespace, limit, lit)
	if err != nil {
		return nil, fmt.Errorf("pgvector: search: %w", err)
	}
	return collectFacts(rows)
}

// searchByKeyword fetches a bounded window and ranks in Go.
func (s *Store) searchByKeyword(ctx context.Context, namespace, query string, limit int) ([]ltm.Fact, error) {
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, text, metadata, created_at, updated_at
		FROM %s
		WHERE namespace = $1
		ORDER BY updated_at DESC
		LIMIT $2`, s.table()), namespace, s.maxScan)
	if err != nil {
		return nil, fmt.Errorf("pgvector: keyword scan: %w", err)
	}
	facts, err := collectFacts(rows)
	if err != nil {
		return nil, err
	}
	scored := make([]ltm.Fact, 0, len(facts))
	for _, f := range facts {
		f.Score = ltm.KeywordScore(query, f.Text)
		if f.Score > 0 {
			scored = append(scored, f)
		}
	}
	sort.SliceStable(scored, func(i, j int) bool { return scored[i].Score > scored[j].Score })
	if len(scored) > limit {
		scored = scored[:limit]
	}
	return scored, nil
}

// List implements ltm.Store, newest first.
func (s *Store) List(ctx context.Context, namespace string, limit int) ([]ltm.Fact, error) {
	if namespace == "" {
		return nil, ltm.ErrEmptyNamespace
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.pool.Query(ctx, fmt.Sprintf(`
		SELECT id, text, metadata, created_at, updated_at
		FROM %s
		WHERE namespace = $1
		ORDER BY updated_at DESC
		LIMIT $2`, s.table()), namespace, limit)
	if err != nil {
		return nil, fmt.Errorf("pgvector: list: %w", err)
	}
	return collectFacts(rows)
}

// Delete implements ltm.Store.
func (s *Store) Delete(ctx context.Context, namespace, id string) error {
	if namespace == "" {
		return ltm.ErrEmptyNamespace
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1 AND id = $2`, s.table()), namespace, id)
	if err != nil {
		return fmt.Errorf("pgvector: delete: %w", err)
	}
	return nil
}

// Clear implements ltm.Store.
func (s *Store) Clear(ctx context.Context, namespace string) error {
	if namespace == "" {
		return ltm.ErrEmptyNamespace
	}
	_, err := s.pool.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE namespace = $1`, s.table()), namespace)
	if err != nil {
		return fmt.Errorf("pgvector: clear: %w", err)
	}
	return nil
}

// collectFacts scans rows with the optional score column into facts.
//
// The score column is only present on the vector path; both query
// shapes select id/text/metadata/created_at/updated_at first, so one
// scanner serves both by asking pgx for the column list.
func collectFacts(rows pgx.Rows) ([]ltm.Fact, error) {
	defer rows.Close()
	fields := rows.FieldDescriptions()
	hasScore := len(fields) == 6
	var out []ltm.Fact
	for rows.Next() {
		var (
			f        ltm.Fact
			meta     []byte
			score    float64
			scorePtr *float64
		)
		if hasScore {
			scorePtr = &score
		}
		dest := []any{&f.ID, &f.Text, &meta, &f.CreatedAt, &f.UpdatedAt}
		if hasScore {
			dest = append(dest, scorePtr)
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("pgvector: scan: %w", err)
		}
		if len(meta) > 0 {
			_ = json.Unmarshal(meta, &f.Metadata)
		}
		if hasScore {
			f.Score = score
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// vectorLiteral renders a vector in pgvector's text format when it is
// storable: the right dimension and all values finite (pgvector
// rejects NaN/Infinity, and writing '[NaN]' would fail the whole
// upsert).
//
// returns: the literal and whether it is usable
func vectorLiteral(v []float32, dim int) (string, bool) {
	if len(v) != dim {
		return "", false
	}
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if math.IsNaN(float64(x)) || math.IsInf(float64(x), 0) {
			return "", false
		}
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String(), true
}

// compile-time proof the store satisfies ltm.Store.
var _ ltm.Store = (*Store)(nil)
