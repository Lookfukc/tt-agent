package memorystore

import (
	"context"
	"errors"
	"time"
)

// ErrLockTimeout reports that a cross-process session lock could not
// be acquired within the wait budget.
var ErrLockTimeout = errors.New("memorystore: session lock busy")

// SessionLocker is the cross-process advisory-lock capability.
//
// A driver whose storage is shared between processes (Redis, Postgres)
// implements it so that multi-step sequences — capture-then-rollback
// being the motivating case — can exclude concurrent actors in other
// processes. Single-driver operations are already atomic by the Driver
// contract; the locker covers sequences of them.
//
// Locks are advisory and lease-based: a crashed holder's lock expires
// on its own rather than deadlocking everyone else. release must be
// called exactly once when acquired; calling it on a lost or expired
// lease is a harmless no-op.
type SessionLocker interface {
	// LockSession acquires the advisory lock for sessionID, waiting up
	// to wait for a current holder to release it.
	//
	// wait <= 0 means a single attempt. The returned release function
	// drops the lock; err is ErrLockTimeout when the lock stayed busy.
	LockSession(ctx context.Context, sessionID string, wait time.Duration) (release func() error, err error)
}
