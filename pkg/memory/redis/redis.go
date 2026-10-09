// Package redis implements the memory driver contract on top of Redis.
//
// Redis is the natural fit for session history: a session is an
// append-only list read newest-first, which maps onto RPUSH/LRANGE,
// and session lifetime maps onto native key expiry.
package redis

import (
	"context"
	"errors"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"

	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
)

// Driver stores session memory in Redis.
//
// Layout:
//
//	agent:mem:{sessionID}          LIST   encoded message records, oldest at head
//	agent:mem:{sessionID}:system   LIST   parallel flags marking system messages
//	agent:mem:{sessionID}:sum      STRING encoded summary record
//
// The parallel system list exists so TrimMessages can locate the
// oldest non-system records without decrypting the session: system
// messages are never trimmed, and their role alone is not sensitive.
type Driver struct {
	client goredis.UniversalClient
	prefix string
	ttl    time.Duration
}

// Options configures the driver.
type Options struct {
	// Prefix is prepended to every key. Empty means "agent:mem:".
	Prefix string

	// SessionTTL, when positive, is applied to every key a session
	// owns and refreshed on each append: an idle session then expires
	// on the Redis side without a sweeper goroutine.
	SessionTTL time.Duration
}

// defaultPrefix namespaces framework keys inside a shared Redis.
const defaultPrefix = "agent:mem:"

// New builds a driver over an existing client.
//
// The caller owns the client's lifetime when passing one in; Close
// still closes it, so pass a client dedicated to this driver if that
// is not what you want.
func New(client goredis.UniversalClient, opts Options) *Driver {
	prefix := opts.Prefix
	if prefix == "" {
		prefix = defaultPrefix
	}
	return &Driver{client: client, prefix: prefix, ttl: opts.SessionTTL}
}

// NewFromURL builds a driver from a redis:// URL.
//
// This is the convenience path for cmd/server and for applications
// that configure Redis by DSN rather than by constructing a client.
func NewFromURL(ctx context.Context, url string, opts Options) (*Driver, error) {
	parsed, err := goredis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("redis: parse url: %w", err)
	}
	client := goredis.NewClient(parsed)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redis: ping: %w", err)
	}
	return New(client, opts), nil
}

// Memory wraps the driver as a core.Memory implementation ready to
// hand to agent.NewLoop.
//
// The returned store also satisfies memory.Splitter, memory.Trimmer
// and memory.SummaryStore, so it can be decorated with
// memory.NewSummary / memory.NewTTL / memory.NewCompactingSummary.
func (d *Driver) Memory(opts memorystore.Options) *memorystore.Store {
	return memorystore.New(d, opts)
}

// key returns the message-list key for a session.
func (d *Driver) key(sessionID string) string { return d.prefix + sessionID }

// systemKey returns the parallel system-flag list key.
func (d *Driver) systemKey(sessionID string) string { return d.prefix + sessionID + ":system" }

// summaryKey returns the summary key.
func (d *Driver) summaryKey(sessionID string) string { return d.prefix + sessionID + ":sum" }

// AppendRecords implements memorystore.Driver.
//
// All writes for one call go through a single pipeline so a concurrent
// reader never observes the message list and the system-flag list at
// different lengths.
func (d *Driver) AppendRecords(ctx context.Context, sessionID string, recs []memorystore.EncodedRecord) error {
	if len(recs) == 0 {
		return nil
	}
	pipe := d.client.TxPipeline()
	for _, rec := range recs {
		flag := "0"
		if rec.System {
			flag = "1"
		}
		pipe.RPush(ctx, d.key(sessionID), rec.Data)
		pipe.RPush(ctx, d.systemKey(sessionID), flag)
	}
	if d.ttl > 0 {
		// Keys are created by this call; refreshing the TTL here is
		// what makes an idle session expire on the Redis side.
		pipe.Expire(ctx, d.key(sessionID), d.ttl)
		pipe.Expire(ctx, d.systemKey(sessionID), d.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: append: %w", err)
	}
	return nil
}

// ScanRecords implements memorystore.Driver.
//
// Messages and their system flags are fetched in one LRANGE pair and
// walked oldest-first. A flag/message count mismatch (possible only if
// someone wrote to these keys out of band) is tolerated by treating
// missing flags as non-system.
func (d *Driver) ScanRecords(ctx context.Context, sessionID string, sink memorystore.RecordSink) error {
	values, err := d.client.LRange(ctx, d.key(sessionID), 0, -1).Result()
	if err != nil {
		if errors.Is(err, goredis.Nil) {
			return nil
		}
		return fmt.Errorf("redis: scan: %w", err)
	}
	if len(values) == 0 {
		return nil
	}
	flags, err := d.client.LRange(ctx, d.systemKey(sessionID), 0, -1).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return fmt.Errorf("redis: scan flags: %w", err)
	}
	for i, v := range values {
		if cerr := memorystore.ContextErr(ctx); cerr != nil {
			return cerr
		}
		rec := memorystore.EncodedRecord{Data: []byte(v)}
		if i < len(flags) {
			rec.System = flags[i] == "1"
		}
		if err := sink(rec); err != nil {
			return err
		}
	}
	return nil
}

// CountMessages implements memorystore.Driver.
func (d *Driver) CountMessages(ctx context.Context, sessionID string) (int, error) {
	n, err := d.client.LLen(ctx, d.key(sessionID)).Result()
	if err != nil {
		return 0, fmt.Errorf("redis: llen: %w", err)
	}
	return int(n), nil
}

// trimScript performs the whole trim server-side, atomically.
//
// Doing read-rebuild-rename from the client has a lost-update race:
// between the client's LRANGE and its RENAME another process can
// append a message, and the rename then overwrites the list without
// it. Redis executes a script against a single-threaded server, so
// the delete-and-rebuild below is invisible to concurrent clients —
// which is the only correct fix once more than one process shares
// the database.
var trimScript = goredis.NewScript(`
local vals = redis.call('LRANGE', KEYS[1], 0, -1)
local flags = redis.call('LRANGE', KEYS[2], 0, -1)
local n = tonumber(ARGV[1])
local keepV = {}
local keepF = {}
local removed = 0
for i, v in ipairs(vals) do
	local isSys = (flags[i] == '1')
	if (not isSys) and removed < n then
		removed = removed + 1
	else
		keepV[#keepV + 1] = v
		keepF[#keepF + 1] = flags[i] or '0'
	end
end
if removed == 0 then
	return 0
end
redis.call('DEL', KEYS[1], KEYS[2])
-- RPUSH in bounded chunks: Lua's unpack is limited by the interpreter
-- stack (~8000 entries), and a long session would overflow it.
for i = 1, #keepV, 100 do
	redis.call('RPUSH', KEYS[1], unpack(keepV, i, math.min(i + 99, #keepV)))
	redis.call('RPUSH', KEYS[2], unpack(keepF, i, math.min(i + 99, #keepF)))
end
if ARGV[2] ~= '' then
	redis.call('PEXPIRE', KEYS[1], ARGV[2])
	redis.call('PEXPIRE', KEYS[2], ARGV[2])
end
return removed
`)

// TrimMessages implements memorystore.Driver.
//
// The whole operation runs inside trimScript on the Redis server, so
// a concurrent AppendRecords either lands entirely before the trim or
// entirely after it — never half-observed and never silently dropped.
func (d *Driver) TrimMessages(ctx context.Context, sessionID string, n int) error {
	if n <= 0 {
		return nil
	}
	ttlMS := ""
	if d.ttl > 0 {
		ttlMS = fmt.Sprintf("%d", d.ttl.Milliseconds())
	}
	_, err := trimScript.Run(ctx, d.client,
		[]string{d.key(sessionID), d.systemKey(sessionID)},
		n, ttlMS,
	).Int()
	if err != nil {
		return fmt.Errorf("redis: trim: %w", err)
	}
	return nil
}

// DeleteSession implements memorystore.Driver.
func (d *Driver) DeleteSession(ctx context.Context, sessionID string) error {
	if err := d.client.Del(ctx, d.key(sessionID), d.systemKey(sessionID), d.summaryKey(sessionID)).Err(); err != nil {
		return fmt.Errorf("redis: delete: %w", err)
	}
	return nil
}

// SaveSummary implements memorystore.Driver.
func (d *Driver) SaveSummary(ctx context.Context, sessionID string, rec memorystore.EncodedSummary) error {
	pipe := d.client.TxPipeline()
	pipe.Set(ctx, d.summaryKey(sessionID), rec.Data, d.ttl)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis: save summary: %w", err)
	}
	return nil
}

// LoadSummary implements memorystore.Driver.
func (d *Driver) LoadSummary(ctx context.Context, sessionID string) (memorystore.EncodedSummary, bool, error) {
	data, err := d.client.Get(ctx, d.summaryKey(sessionID)).Bytes()
	if errors.Is(err, goredis.Nil) {
		return memorystore.EncodedSummary{}, false, nil
	}
	if err != nil {
		return memorystore.EncodedSummary{}, false, fmt.Errorf("redis: load summary: %w", err)
	}
	return memorystore.EncodedSummary{Data: data}, true, nil
}

// Close implements memorystore.Driver.
func (d *Driver) Close() error { return d.client.Close() }

// compile-time proof that the memory backend contract is satisfied.
var _ memorystore.Driver = (*Driver)(nil)

// compile-time proof that a Store over this driver satisfies the
// decorator-facing capability interfaces, so memory.NewSummary /
// NewCompactingSummary / NewTTL can wrap it without adapters.
var _ interface {
	memory.Splitter
	memory.Trimmer
	memory.SummaryStore
} = (*memorystore.Store)(nil)
