package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// lockRetryInterval paces advisory-lock acquisition attempts.
const lockRetryInterval = 25 * time.Millisecond

// LockSession implements memorystore.SessionLocker.
//
// It uses a transaction-scoped advisory lock (pg_try_advisory_xact_lock
// keyed by the session ID), held on one connection acquired from the
// pool for the whole critical section. Transaction scoping is what
// makes the lock crash-safe: even if the process dies mid-section, the
// connection dies with it and the lock evaporates at transaction end —
// no lease to expire, no stale lock.
//
// hashtextextended maps the session ID to the bigint the advisory API
// takes; hash collisions would only cause unnecessary serialization,
// never corruption.
func (d *Driver) LockSession(ctx context.Context, sessionID string, wait time.Duration) (func() error, error) {
	deadline := time.Now().Add(wait)
	for {
		release, err := d.tryLockOnce(ctx, sessionID)
		if err == nil {
			return release, nil
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

// tryLockOnce makes one acquisition attempt on a dedicated connection.
func (d *Driver) tryLockOnce(ctx context.Context, sessionID string) (func() error, error) {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("postgres: lock acquire conn: %w", err)
	}
	tx, err := conn.Conn().Begin(ctx)
	if err != nil {
		conn.Release()
		return nil, fmt.Errorf("postgres: lock begin: %w", err)
	}
	var ok bool
	err = tx.QueryRow(ctx,
		`SELECT pg_try_advisory_xact_lock(hashtextextended($1, 0))`, sessionID).Scan(&ok)
	if err != nil {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, fmt.Errorf("postgres: advisory lock: %w", err)
	}
	if !ok {
		_ = tx.Rollback(ctx)
		conn.Release()
		return nil, memorystore.ErrLockTimeout // retried by the caller
	}
	release := func() error {
		// Rollback (not commit): the transaction exists only to carry
		// the advisory lock, and rollback releases it.
		err := tx.Rollback(context.Background())
		conn.Release()
		return err
	}
	return release, nil
}

// compile-time proof of the locker capability.
var _ memorystore.SessionLocker = (*Driver)(nil)
