package postgres_test

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/postgres"
)

// TestSessionLockAcquireRelease covers the basic advisory-lock cycle
// against a real Postgres.
func TestSessionLockAcquireRelease(t *testing.T) {
	d := newDriver(t)
	ctx := context.Background()

	release, err := d.LockSession(ctx, "s", time.Second)
	if err != nil {
		t.Fatalf("LockSession: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	// 释放后可立即重取
	release2, err := d.LockSession(ctx, "s", time.Second)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	_ = release2()
}

// TestSessionLockExcludesSecondHolder proves one held lock blocks
// another and times out cleanly.
func TestSessionLockExcludesSecondHolder(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("set %s to run postgres driver tests", dsnEnv)
	}
	ctx := context.Background()
	a, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: "tt_test_lock_a"})
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	defer a.Close()
	b, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: "tt_test_lock_b"})
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer b.Close()

	release, err := a.LockSession(ctx, "shared", 0)
	if err != nil {
		t.Fatalf("a lock: %v", err)
	}
	defer release()

	// b 用两个独立连接池等待：应当超时
	_, err = b.LockSession(ctx, "shared", 150*time.Millisecond)
	if !errors.Is(err, memorystore.ErrLockTimeout) {
		t.Fatalf("err = %v, want ErrLockTimeout", err)
	}

	// a 释放后 b 立即可取
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	got, err := b.LockSession(ctx, "shared", time.Second)
	if err != nil {
		t.Fatalf("b lock after release: %v", err)
	}
	_ = got()
}

// TestSessionLockCrashSafe proves an abandoned connection's lock does
// not outlive it: killing the backend (as a process crash would)
// releases the advisory lock server-side.
//
// The crash is simulated by pg_terminate_backend rather than closing
// the pool: pool.Close waits gracefully for acquired connections and
// would deadlock on the deliberately unreleased one — which is exactly
// what a graceful shutdown is not.
func TestSessionLockCrashSafe(t *testing.T) {
	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("set %s to run postgres driver tests", dsnEnv)
	}
	ctx := context.Background()
	a, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: "tt_test_lock_crash"})
	if err != nil {
		t.Fatalf("connect a: %v", err)
	}
	release, err := a.LockSession(ctx, "s", 0)
	if err != nil {
		t.Fatalf("lock: %v", err)
	}
	_ = release // 故意不释放：持锁连接保持 idle in transaction

	// 从另一个连接杀掉持锁后端，模拟进程崩溃
	killer, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: "tt_test_lock_killer"})
	if err != nil {
		t.Fatalf("connect killer: %v", err)
	}
	defer killer.Close()
	if _, err := killer.Pool().Exec(ctx, `
		SELECT pg_terminate_backend(pid) FROM pg_stat_activity
		WHERE datname = current_database()
		  AND pid <> pg_backend_pid()
		  AND query LIKE '%pg_try_advisory%'`); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	b, err := postgres.NewFromURL(ctx, dsn, postgres.Options{Schema: "tt_test_lock_crash_b"})
	if err != nil {
		t.Fatalf("connect b: %v", err)
	}
	defer b.Close()
	got, err := b.LockSession(ctx, "s", 3*time.Second)
	if err != nil {
		t.Fatalf("lock survived holder death: %v", err)
	}
	_ = got()
	// 不调用 a.Close()：那条连接被故意借出未归还，而 pool.Close 会
	// 等待所有借出连接归还——优雅关闭语义与"崩溃"测试天然矛盾。
	// 测试进程退出即清理。
}
