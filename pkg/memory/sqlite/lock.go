package sqlite

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// lockTTL bounds how long a session lock survives a crashed holder,
// matching the Redis driver's lease.
const lockTTL = 30 * time.Second

// lockRetryInterval paces acquisition attempts while waiting.
const lockRetryInterval = 25 * time.Millisecond

// lockTimeLayout is a fixed-width UTC timestamp.
//
// Lock expiry compares expires_at strings in SQL, which is only sound
// when lexicographic order equals chronological order — RFC3339Nano's
// variable-length fractions break that ("…:00Z" sorts after
// "…:00.5Z"). Fixed milliseconds in UTC keep both sides of every
// comparison in one canonical shape, so plain string comparison is
// correct.
const lockTimeLayout = "2006-01-02T15:04:05.000Z"

// lockNow renders the canonical timestamp for right now.
func lockNow() string { return time.Now().UTC().Format(lockTimeLayout) }

// LockSession implements memorystore.SessionLocker.
//
// The lock is a row in a dedicated table: acquire is an INSERT that
// fails on the primary key when someone else holds the lock, release
// is a token-checked DELETE (a late release cannot delete a newer
// holder's row), and crash safety comes from the lease — an expired
// row is reclaimed by the next acquirer rather than deadlocking
// everyone.
//
// Unlike the Postgres advisory lock, nothing is held open between
// acquire and release: each step is a short statement through the
// driver's single pooled connection, so a lock held across a long
// critical section never pins a connection.
func (d *Driver) LockSession(ctx context.Context, sessionID string, wait time.Duration) (func() error, error) {
	// Lazy migration: older databases predate the lock table. Creating
	// it here (idempotently) means upgrading the driver binary is the
	// only step — no manual migration for operators.
	if err := d.ensureLockTable(ctx); err != nil {
		return nil, err
	}

	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, fmt.Errorf("sqlite: lock token: %w", err)
	}
	owner := hex.EncodeToString(token)
	expires := time.Now().Add(lockTTL).UTC().Format(lockTimeLayout)

	deadline := time.Now().Add(wait)
	for {
		// Purge expired leases first: a crashed holder's lock must not
		// block the next acquirer for the rest of the lease.
		if _, err := d.db.ExecContext(ctx,
			`DELETE FROM session_locks WHERE expires_at < ?`, lockNow()); err != nil {
			return nil, fmt.Errorf("sqlite: lock purge: %w", err)
		}
		_, err := d.db.ExecContext(ctx,
			`INSERT INTO session_locks (session_id, owner, expires_at) VALUES (?, ?, ?)`,
			sessionID, owner, expires)
		if err == nil {
			return func() error {
				// Token-checked delete: if this lease expired and
				// someone else acquired, owner no longer matches and
				// the delete is a no-op — never someone else's lock.
				if _, err := d.db.ExecContext(context.Background(),
					`DELETE FROM session_locks WHERE session_id = ? AND owner = ?`,
					sessionID, owner); err != nil {
					return fmt.Errorf("sqlite: unlock: %w", err)
				}
				return nil
			}, nil
		}
		if !isConstraintError(err) {
			return nil, fmt.Errorf("sqlite: lock: %w", err)
		}
		if time.Now().After(deadline) {
			return nil, memorystore.ErrLockTimeout
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(lockRetryInterval):
		}
	}
}

// ensureLockTable creates the lock table when absent. Idempotent.
func (d *Driver) ensureLockTable(ctx context.Context) error {
	_, err := d.db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS session_locks (
		session_id TEXT PRIMARY KEY,
		owner      TEXT NOT NULL,
		expires_at TEXT NOT NULL
	)`)
	if err != nil {
		return fmt.Errorf("sqlite: lock table: %w", err)
	}
	return nil
}

// isConstraintError reports whether an INSERT hit the primary key,
// meaning the lock is held. modernc surfaces SQLITE_CONSTRAINT as an
// error string; matching on "constraint" is the portable form across
// database/sql drivers.
func isConstraintError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "constraint")
}

// compile-time proof of the locker capability.
var _ memorystore.SessionLocker = (*Driver)(nil)
