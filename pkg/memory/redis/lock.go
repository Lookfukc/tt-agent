package redis

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// lockTTL bounds how long a session lock survives a crashed holder.
//
// The critical sections it guards (capture + rollback) run in
// milliseconds; half a minute tolerates a long GC pause or a slow disk
// without letting a crashed process lock everyone out for long.
const lockTTL = 30 * time.Second

// lockRetryInterval paces acquisition attempts while waiting.
const lockRetryInterval = 25 * time.Millisecond

// releaseScript deletes the lock only when the caller still owns it.
//
// Comparing the token prevents the classic mistake: holder A expires,
// holder B acquires, then A's late release deletes B's lock and a
// third actor piles in. The compare-and-delete must be one server-side
// step for the same reason the trim is a script.
var releaseScript = goredis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
	return redis.call('DEL', KEYS[1])
end
return 0
`)

// lockKey returns the advisory-lock key for a session.
func (d *Driver) lockKey(sessionID string) string { return d.prefix + sessionID + ":lock" }

// LockSession implements memorystore.SessionLocker.
//
// The lock is a single SET NX PX with a random token: acquire is one
// atomic server-side step, the TTL is the crash lease, and release
// reclaims only if the token still matches. Waiting is client-side
// polling, which is what every Redis lock does; the interval is short
// because contention here is rare (snapshot operations).
func (d *Driver) LockSession(ctx context.Context, sessionID string, wait time.Duration) (func() error, error) {
	token := make([]byte, 16)
	if _, err := rand.Read(token); err != nil {
		return nil, fmt.Errorf("redis: lock token: %w", err)
	}
	secret := hex.EncodeToString(token)
	key := d.lockKey(sessionID)

	deadline := time.Now().Add(wait)
	for {
		ok, err := d.client.SetNX(ctx, key, secret, lockTTL).Result()
		if err != nil {
			return nil, fmt.Errorf("redis: lock: %w", err)
		}
		if ok {
			return func() error {
				_, err := releaseScript.Run(context.Background(), d.client,
					[]string{key}, secret).Result()
				if err != nil {
					return fmt.Errorf("redis: unlock: %w", err)
				}
				return nil
			}, nil
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

// compile-time proof of the locker capability.
var _ memorystore.SessionLocker = (*Driver)(nil)
