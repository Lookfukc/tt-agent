// Package postgres implements the memory driver contract on top of
// PostgreSQL.
//
// Postgres suits session history when a team already runs it: rows are
// ordered per session, writes are transactional, and the same database
// can later host vector search for long-term memory (pgvector) without
// adding another piece of infrastructure.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// Driver stores session memory in PostgreSQL.
//
// Schema (created by Migrate):
//
//	agent_messages(session_id text, seq bigserial, system bool, data bytea, created_at timestamptz)
//	agent_summaries(session_id text primary key, covered int, data bytea, updated_at timestamptz)
//
// Messages live in one table keyed by session_id with a monotonic seq,
// so "oldest first" is an index scan rather than a sort. `system` is
// stored in clear so trimming can select non-system rows without
// decoding (or decrypting) the payload.
type Driver struct {
	pool    *pgxpool.Pool
	schema  string
	ownPool bool
}

// Options configures the driver.
type Options struct {
	// Schema is the table namespace. Empty means "public".
	//
	// The name is quoted into SQL, so it can be a reserved word or
	// contain mixed case.
	Schema string
}

// New builds a driver over an existing pool. The driver does not close
// a pool it did not create.
func New(pool *pgxpool.Pool, opts Options) *Driver {
	schema := opts.Schema
	if schema == "" {
		schema = "public"
	}
	return &Driver{pool: pool, schema: schema, ownPool: false}
}

// NewFromURL builds a driver from a connection string and owns the
// resulting pool (Close releases it).
func NewFromURL(ctx context.Context, url string, opts Options) (*Driver, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("postgres: connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	d := New(pool, opts)
	d.ownPool = true
	return d, nil
}

// Memory wraps the driver as a core.Memory implementation ready for
// agent.NewLoop, also satisfying memory.Splitter, Trimmer and
// SummaryStore.
func (d *Driver) Memory(opts memorystore.Options) *memorystore.Store {
	opts.Backend = memorystore.DefaultBackend("postgres", opts.Backend)
	return memorystore.New(d, opts)
}

// Migrate creates the tables if they do not exist.
//
// It is idempotent and safe to call concurrently; call it once at
// startup, mirroring the explicit .setup() step other frameworks
// require before a checkpointer is usable.
func (d *Driver) Migrate(ctx context.Context) error {
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			session_id text NOT NULL,
			seq        bigint GENERATED ALWAYS AS IDENTITY,
			system     boolean NOT NULL DEFAULT false,
			data       bytea NOT NULL,
			created_at timestamptz NOT NULL DEFAULT now(),
			PRIMARY KEY (session_id, seq)
		)`, d.msgTable()),
		// PostgreSQL does not allow a schema-qualified index name in
		// CREATE INDEX (the index always lands in the table's schema),
		// so the index name is bare here.
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (session_id, seq)`,
			quoteIdent("agent_messages_session_seq_idx"), d.msgTable()),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			session_id text PRIMARY KEY,
			covered    integer NOT NULL DEFAULT 0,
			data       bytea NOT NULL,
			updated_at timestamptz NOT NULL DEFAULT now()
		)`, d.sumTable()),
	}
	for _, stmt := range stmts {
		if _, err := d.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("postgres: migrate: %w", err)
		}
	}
	return nil
}

// msgTable returns the qualified messages table name.
func (d *Driver) msgTable() string { return d.ident("agent_messages") }

// sumTable returns the qualified summaries table name.
func (d *Driver) sumTable() string { return d.ident("agent_summaries") }

// Pool exposes the connection pool, for callers that need to run their
// own queries or manage the schema this driver lives in.
func (d *Driver) Pool() *pgxpool.Pool { return d.pool }

// MsgTable returns the fully qualified messages table name.
//
// It is exported so operators can run their own maintenance queries
// (retention policy, analytics, vacuum hints) against the same tables
// without guessing at names.
func (d *Driver) MsgTable() string { return d.msgTable() }

// SumTable returns the fully qualified summaries table name.
func (d *Driver) SumTable() string { return d.sumTable() }

// ident qualifies and quotes an identifier with the configured schema.
func (d *Driver) ident(name string) string {
	return quoteIdent(d.schema) + "." + quoteIdent(name)
}

// quoteIdent wraps an identifier in double quotes, escaping embedded
// quotes. Using parameters is not possible for identifiers, so this is
// the injection barrier for schema/table names.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// AppendRecords implements memorystore.Driver.
//
// All rows of one call go in a single transaction: a concurrent reader
// therefore never sees a partially appended turn (user message without
// its assistant reply).
func (d *Driver) AppendRecords(ctx context.Context, sessionID string, recs []memorystore.EncodedRecord) error {
	if len(recs) == 0 {
		return nil
	}
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	batch := &pgx.Batch{}
	for _, rec := range recs {
		batch.Queue(
			fmt.Sprintf(`INSERT INTO %s (session_id, system, data) VALUES ($1, $2, $3)`, d.msgTable()),
			sessionID, rec.System, rec.Data,
		)
	}
	results := tx.SendBatch(ctx, batch)
	for range recs {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("postgres: append: %w", err)
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("postgres: append close: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit: %w", err)
	}
	return nil
}

// ScanRecords implements memorystore.Driver.
func (d *Driver) ScanRecords(ctx context.Context, sessionID string, sink memorystore.RecordSink) error {
	rows, err := d.pool.Query(ctx,
		fmt.Sprintf(`SELECT system, data FROM %s WHERE session_id = $1 ORDER BY seq ASC`, d.msgTable()),
		sessionID)
	if err != nil {
		return fmt.Errorf("postgres: scan: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if cerr := memorystore.ContextErr(ctx); cerr != nil {
			return cerr
		}
		var rec memorystore.EncodedRecord
		if err := rows.Scan(&rec.System, &rec.Data); err != nil {
			return fmt.Errorf("postgres: scan row: %w", err)
		}
		if err := sink(rec); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("postgres: scan iterate: %w", err)
	}
	return nil
}

// CountMessages implements memorystore.Driver.
func (d *Driver) CountMessages(ctx context.Context, sessionID string) (int, error) {
	var n int
	err := d.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT count(*) FROM %s WHERE session_id = $1`, d.msgTable()),
		sessionID).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("postgres: count: %w", err)
	}
	return n, nil
}

// TrimMessages implements memorystore.Driver.
//
// The n oldest non-system rows are located by seq and deleted in one
// statement; system rows are excluded in SQL, so the pairing invariant
// (non-system messages dropped oldest-first) holds without reading the
// session into memory.
func (d *Driver) TrimMessages(ctx context.Context, sessionID string, n int) error {
	if n <= 0 {
		return nil
	}
	_, err := d.pool.Exec(ctx, fmt.Sprintf(`
		DELETE FROM %s
		WHERE session_id = $1 AND system = false AND seq IN (
			SELECT seq FROM %s
			WHERE session_id = $1 AND system = false
			ORDER BY seq ASC
			LIMIT $2
		)`, d.msgTable(), d.msgTable()), sessionID, n)
	if err != nil {
		return fmt.Errorf("postgres: trim: %w", err)
	}
	return nil
}

// DeleteSession implements memorystore.Driver.
func (d *Driver) DeleteSession(ctx context.Context, sessionID string) error {
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("postgres: begin delete: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE session_id = $1`, d.msgTable()), sessionID); err != nil {
		return fmt.Errorf("postgres: delete messages: %w", err)
	}
	if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE session_id = $1`, d.sumTable()), sessionID); err != nil {
		return fmt.Errorf("postgres: delete summary: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("postgres: commit delete: %w", err)
	}
	return nil
}

// SaveSummary implements memorystore.Driver.
//
// The summary is one row per session, so an upsert keeps the table
// bounded regardless of how often compaction runs.
func (d *Driver) SaveSummary(ctx context.Context, sessionID string, rec memorystore.EncodedSummary) error {
	_, err := d.pool.Exec(ctx, fmt.Sprintf(`
		INSERT INTO %s (session_id, data, updated_at) VALUES ($1, $2, now())
		ON CONFLICT (session_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()`,
		d.sumTable()), sessionID, rec.Data)
	if err != nil {
		return fmt.Errorf("postgres: save summary: %w", err)
	}
	return nil
}

// LoadSummary implements memorystore.Driver.
func (d *Driver) LoadSummary(ctx context.Context, sessionID string) (memorystore.EncodedSummary, bool, error) {
	var data []byte
	err := d.pool.QueryRow(ctx,
		fmt.Sprintf(`SELECT data FROM %s WHERE session_id = $1`, d.sumTable()),
		sessionID).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return memorystore.EncodedSummary{}, false, nil
	}
	if err != nil {
		return memorystore.EncodedSummary{}, false, fmt.Errorf("postgres: load summary: %w", err)
	}
	return memorystore.EncodedSummary{Data: data}, true, nil
}

// Close implements memorystore.Driver, releasing the pool only when
// this driver created it.
func (d *Driver) Close() error {
	if d.ownPool && d.pool != nil {
		d.pool.Close()
	}
	return nil
}

// Ping verifies connectivity, for health endpoints.
func (d *Driver) Ping(ctx context.Context) error {
	if err := d.pool.Ping(ctx); err != nil {
		return fmt.Errorf("postgres: ping: %w", err)
	}
	return nil
}

// PruneIdleSessions deletes sessions untouched for longer than idle.
//
// Postgres has no per-row TTL, so bounded retention needs an explicit
// sweep; this is the portable equivalent of the Redis driver's native
// key expiry. Returns the number of sessions removed.
//
// Sessions with a summary are judged by the summary's updated_at;
// sessions that only ever accumulated messages are judged by their
// newest message. Covering both matters because a never-summarized
// session is the common case, and judging only by summaries would leak
// those rows forever.
func (d *Driver) PruneIdleSessions(ctx context.Context, idle time.Duration) (int64, error) {
	if idle <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-idle)
	tx, err := d.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("postgres: prune begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, fmt.Sprintf(`
		SELECT session_id FROM %s
		GROUP BY session_id
		HAVING max(created_at) < $1
		UNION
		SELECT session_id FROM %s
		WHERE updated_at < $1
		  AND session_id NOT IN (SELECT session_id FROM %s WHERE created_at >= $1)`,
		d.msgTable(), d.sumTable(), d.msgTable()), cutoff)
	if err != nil {
		return 0, fmt.Errorf("postgres: prune select: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("postgres: prune scan: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("postgres: prune iterate: %w", err)
	}

	var removed int64
	for _, id := range ids {
		tag, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE session_id = $1`, d.msgTable()), id)
		if err != nil {
			return 0, fmt.Errorf("postgres: prune messages: %w", err)
		}
		removed += tag.RowsAffected()
		if _, err := tx.Exec(ctx, fmt.Sprintf(`DELETE FROM %s WHERE session_id = $1`, d.sumTable()), id); err != nil {
			return 0, fmt.Errorf("postgres: prune summary: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("postgres: prune commit: %w", err)
	}
	return removed, nil
}

// compile-time proof that the backend contract is satisfied.
var _ memorystore.Driver = (*Driver)(nil)

// compile-time proof that the decorator-facing capabilities exist on a
// Store built over this driver.
var _ interface {
	memory.Splitter
	memory.Trimmer
	memory.SummaryStore
} = (*memorystore.Store)(nil)
