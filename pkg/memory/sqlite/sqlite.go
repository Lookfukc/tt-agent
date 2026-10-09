// Package sqlite implements the memory driver contract on top of
// SQLite via database/sql.
//
// Compared with the file-backed memory.Persistent, SQLite adds real
// transactions, multi-process-safe concurrent access, indexed range
// reads and SQL-level trimming — while keeping the zero-infrastructure
// property of a single local file. It is the recommended single-node
// backend when more than one process may touch the same data.
//
// The driver uses modernc.org/sqlite, a pure-Go implementation, so it
// needs no CGO and cross-compiles cleanly.
package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite" // registers the "sqlite" driver

	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// Driver stores session memory in a SQLite database.
//
// Schema (created by Migrate):
//
//	agent_messages(id integer primary key autoincrement, session_id text, system integer, data blob)
//	agent_summaries(session_id text primary key, covered integer, data blob, updated_at text)
//
// An autoincrement primary key gives a stable oldest-first order and
// makes trimming a bounded index delete, so a long session never needs
// to be read into memory just to drop its head.
type Driver struct {
	db  *sql.DB
	dsn string
}

// Options configures the driver.
type Options struct {
	// BusyTimeout is how long a writer waits for the lock. Zero uses
	// 5 seconds. Raising it trades latency for fewer SQLITE_BUSY
	// errors under concurrent writers.
	BusyTimeout time.Duration

	// NoSync disables fsync on commit, trading durability for speed.
	// Leave false unless the data is disposable.
	NoSync bool
}

// New opens (creating if needed) a SQLite database at path.
//
// The returned driver owns the connection and Close releases it.
// Use ":memory:" for an ephemeral database in tests.
func New(path string, opts Options) (*Driver, error) {
	dsn, err := buildDSN(path, opts)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	// SQLite tolerates exactly one writer; keeping the pool small
	// avoids a pile-up of goroutines waiting on the write lock.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: ping: %w", err)
	}
	return &Driver{db: db, dsn: dsn}, nil
}

// buildDSN assembles the connection string.
//
// Busy timeout and journal mode are set through the DSN because
// modernc's driver applies pragmas at connection open, which is also
// what makes them effective for every pooled connection.
func buildDSN(path string, opts Options) (string, error) {
	if path == "" {
		return "", errors.New("sqlite: path must not be empty")
	}
	timeout := opts.BusyTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	params := []string{
		"_pragma=busy_timeout(" + fmt.Sprintf("%d", timeout.Milliseconds()) + ")",
		"_pragma=journal_mode(WAL)",
		"_pragma=foreign_keys(1)",
	}
	if !opts.NoSync {
		params = append(params, "_pragma=synchronous(FULL)")
	} else {
		params = append(params, "_pragma=synchronous(OFF)")
	}
	if strings.Contains(path, "?") {
		return path + "&" + strings.Join(params, "&"), nil
	}
	return path + "?" + strings.Join(params, "&"), nil
}

// Memory wraps the driver as a core.Memory implementation, also
// satisfying memory.Splitter, Trimmer and SummaryStore.
func (d *Driver) Memory(opts memorystore.Options) *memorystore.Store {
	opts.Backend = memorystore.DefaultBackend("sqlite", opts.Backend)
	return memorystore.New(d, opts)
}

// DB exposes the underlying handle, for callers that want to run their
// own queries (analytics, exports) against the same file.
func (d *Driver) DB() *sql.DB { return d.db }

// Migrate creates the tables and indexes if absent. Idempotent.
func (d *Driver) Migrate(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS agent_messages (
			id         INTEGER PRIMARY KEY AUTOINCREMENT,
			session_id TEXT    NOT NULL,
			system     INTEGER NOT NULL DEFAULT 0,
			data       BLOB    NOT NULL,
			created_at TEXT    NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS agent_messages_session_id ON agent_messages (session_id, id)`,
		`CREATE INDEX IF NOT EXISTS agent_messages_session_system ON agent_messages (session_id, system, id)`,
		`CREATE TABLE IF NOT EXISTS agent_summaries (
			session_id TEXT PRIMARY KEY,
			covered    INTEGER NOT NULL DEFAULT 0,
			data       BLOB    NOT NULL,
			updated_at TEXT    NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := d.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("sqlite: migrate: %w", err)
		}
	}
	// created_at 参与空闲清理，但早期版本的库没有这一列；补列而不是
	// 要求重建，这样升级是就地完成的。SQLite 不支持 ADD COLUMN IF NOT
	// EXISTS，所以先探测再补。
	if err := d.ensureColumn(ctx, "agent_messages", "created_at",
		`ALTER TABLE agent_messages ADD COLUMN created_at TEXT NOT NULL DEFAULT ''`); err != nil {
		return err
	}
	// 清理依赖 created_at，给历史行补一个时间戳，否则它们会被
	// 当成「从未写入」而在第一次清理时被误删（'' < cutoff 恒真）。
	// 用 id 顺序推算不了真实时间，所以只在列刚被补上时统一填当前
	// 时间：宁可让旧会话多活一轮，也不能误删。
	if _, err := d.db.ExecContext(ctx,
		`UPDATE agent_messages SET created_at = ? WHERE created_at = ''`,
		time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		return fmt.Errorf("sqlite: backfill created_at: %w", err)
	}
	return nil
}

// ensureColumn adds a column when it does not exist yet.
//
// SQLite has no ADD COLUMN IF NOT EXISTS, so existence is probed via
// PRAGMA table_info; an already-present column is not an error.
func (d *Driver) ensureColumn(ctx context.Context, table, column, alter string) error {
	rows, err := d.db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
	if err != nil {
		return fmt.Errorf("sqlite: pragma %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			cid       int
			name      string
			ctype     string
			notnull   int
			dfltValue any
			pk        int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dfltValue, &pk); err != nil {
			return fmt.Errorf("sqlite: pragma scan: %w", err)
		}
		if name == column {
			return nil // 已存在
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: pragma iterate: %w", err)
	}
	if _, err := d.db.ExecContext(ctx, alter); err != nil {
		return fmt.Errorf("sqlite: add column %s.%s: %w", table, column, err)
	}
	return nil
}

// AppendRecords implements memorystore.Driver.
//
// One transaction per call keeps a turn atomic, so a concurrent reader
// never sees a user message without its assistant reply.
func (d *Driver) AppendRecords(ctx context.Context, sessionID string, recs []memorystore.EncodedRecord) error {
	if len(recs) == 0 {
		return nil
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx,
		`INSERT INTO agent_messages (session_id, system, data, created_at) VALUES (?, ?, ?, ?)`)
	if err != nil {
		return fmt.Errorf("sqlite: prepare: %w", err)
	}
	defer stmt.Close()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, rec := range recs {
		system := 0
		if rec.System {
			system = 1
		}
		if _, err := stmt.ExecContext(ctx, sessionID, system, rec.Data, now); err != nil {
			return fmt.Errorf("sqlite: append: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit: %w", err)
	}
	return nil
}

// ScanRecords implements memorystore.Driver.
func (d *Driver) ScanRecords(ctx context.Context, sessionID string, sink memorystore.RecordSink) error {
	rows, err := d.db.QueryContext(ctx,
		`SELECT system, data FROM agent_messages WHERE session_id = ? ORDER BY id ASC`, sessionID)
	if err != nil {
		return fmt.Errorf("sqlite: scan: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		if cerr := memorystore.ContextErr(ctx); cerr != nil {
			return cerr
		}
		var (
			system int
			data   []byte
		)
		if err := rows.Scan(&system, &data); err != nil {
			return fmt.Errorf("sqlite: scan row: %w", err)
		}
		if err := sink(memorystore.EncodedRecord{Data: data, System: system == 1}); err != nil {
			return err
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("sqlite: scan iterate: %w", err)
	}
	return nil
}

// CountMessages implements memorystore.Driver.
func (d *Driver) CountMessages(ctx context.Context, sessionID string) (int, error) {
	var n int
	if err := d.db.QueryRowContext(ctx,
		`SELECT count(*) FROM agent_messages WHERE session_id = ?`, sessionID).Scan(&n); err != nil {
		return 0, fmt.Errorf("sqlite: count: %w", err)
	}
	return n, nil
}

// TrimMessages implements memorystore.Driver.
//
// System rows are excluded in SQL, so the oldest non-system rows are
// removed without decoding anything.
func (d *Driver) TrimMessages(ctx context.Context, sessionID string, n int) error {
	if n <= 0 {
		return nil
	}
	_, err := d.db.ExecContext(ctx, `
		DELETE FROM agent_messages
		WHERE id IN (
			SELECT id FROM agent_messages
			WHERE session_id = ? AND system = 0
			ORDER BY id ASC
			LIMIT ?
		)`, sessionID, n)
	if err != nil {
		return fmt.Errorf("sqlite: trim: %w", err)
	}
	return nil
}

// DeleteSession implements memorystore.Driver.
func (d *Driver) DeleteSession(ctx context.Context, sessionID string) error {
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sqlite: begin delete: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM agent_messages WHERE session_id = ?`, sessionID); err != nil {
		return fmt.Errorf("sqlite: delete messages: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM agent_summaries WHERE session_id = ?`, sessionID); err != nil {
		return fmt.Errorf("sqlite: delete summary: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit delete: %w", err)
	}
	return nil
}

// SaveSummary implements memorystore.Driver using an upsert.
func (d *Driver) SaveSummary(ctx context.Context, sessionID string, rec memorystore.EncodedSummary) error {
	_, err := d.db.ExecContext(ctx, `
		INSERT INTO agent_summaries (session_id, data, updated_at, covered) VALUES (?, ?, ?, ?)
		ON CONFLICT (session_id) DO UPDATE SET data = excluded.data, updated_at = excluded.updated_at`,
		sessionID, rec.Data, time.Now().UTC().Format(time.RFC3339Nano), 0)
	if err != nil {
		return fmt.Errorf("sqlite: save summary: %w", err)
	}
	return nil
}

// LoadSummary implements memorystore.Driver.
func (d *Driver) LoadSummary(ctx context.Context, sessionID string) (memorystore.EncodedSummary, bool, error) {
	var data []byte
	err := d.db.QueryRowContext(ctx,
		`SELECT data FROM agent_summaries WHERE session_id = ?`, sessionID).Scan(&data)
	if errors.Is(err, sql.ErrNoRows) {
		return memorystore.EncodedSummary{}, false, nil
	}
	if err != nil {
		return memorystore.EncodedSummary{}, false, fmt.Errorf("sqlite: load summary: %w", err)
	}
	return memorystore.EncodedSummary{Data: data}, true, nil
}

// Close implements memorystore.Driver.
func (d *Driver) Close() error { return d.db.Close() }

// Ping verifies the database is reachable, for health endpoints.
func (d *Driver) Ping(ctx context.Context) error {
	if err := d.db.PingContext(ctx); err != nil {
		return fmt.Errorf("sqlite: ping: %w", err)
	}
	return nil
}

// PruneIdleSessions deletes sessions whose newest record is older than
// the session TTL.
//
// SQLite has no per-row expiry, so retention is enforced by an
// explicit sweep; hosts can call this on a timer. Returns the number
// of sessions removed.
//
// Both kinds of idle session are covered: sessions carrying a summary
// (judged by the summary's updated_at) and sessions that only ever
// accumulated messages (judged by their newest message timestamp).
// Sweeping only the first kind would leak every session that was never
// summarized, which is the common case for short conversations.
func (d *Driver) PruneIdleSessions(ctx context.Context, idle time.Duration) (int64, error) {
	if idle <= 0 {
		return 0, nil
	}
	cutoff := time.Now().Add(-idle).UTC().Format(time.RFC3339Nano)

	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// 两条件并集，都必须与「现在」比较：只有真正过期的会话会被选中，
	// 活跃会话（消息或摘要任一在阈值内）保持不变。
	rows, err := tx.QueryContext(ctx, `
		SELECT session_id FROM agent_messages
		GROUP BY session_id
		HAVING max(created_at) < ?
		UNION
		SELECT session_id FROM agent_summaries
		WHERE updated_at < ?
		  AND session_id NOT IN (SELECT session_id FROM agent_messages WHERE created_at >= ?)`,
		cutoff, cutoff, cutoff)
	if err != nil {
		return 0, fmt.Errorf("sqlite: prune select: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, fmt.Errorf("sqlite: prune scan: %w", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("sqlite: prune iterate: %w", err)
	}

	var removed int64
	for _, id := range ids {
		res, err := tx.ExecContext(ctx, `DELETE FROM agent_messages WHERE session_id = ?`, id)
		if err != nil {
			return 0, fmt.Errorf("sqlite: prune messages: %w", err)
		}
		if n, err := res.RowsAffected(); err == nil {
			removed += n
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM agent_summaries WHERE session_id = ?`, id); err != nil {
			return 0, fmt.Errorf("sqlite: prune summary: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("sqlite: prune commit: %w", err)
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
