package sqlite_test

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/snapshot"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
)

// newDualSQLite simulates two processes: separate drivers and separate
// snapshot stores over one shared database file.
func newDualSQLite(t *testing.T, lockWait time.Duration) (memA, memB *memorystore.Store, snapsA, snapsB *snapshot.Store, a, b *sqlite.Driver, path string) {
	t.Helper()
	path = filepath.Join(t.TempDir(), "shared.db")
	mk := func() *sqlite.Driver {
		d, err := sqlite.New(path, sqlite.Options{})
		if err != nil {
			t.Fatalf("sqlite.New: %v", err)
		}
		t.Cleanup(func() { _ = d.Close() })
		if err := d.Migrate(context.Background()); err != nil {
			t.Fatalf("Migrate: %v", err)
		}
		return d
	}
	a, b = mk(), mk()
	return a.Memory(memorystore.Options{}), b.Memory(memorystore.Options{}),
		snapshot.New(a, snapshot.Options{LockWait: lockWait}),
		snapshot.New(b, snapshot.Options{LockWait: lockWait}),
		a, b, path
}

// TestLockAcquireRelease covers the basic cycle.
func TestLockAcquireRelease(t *testing.T) {
	d := newDriver(t)
	ctx := context.Background()

	release, err := d.LockSession(ctx, "s", time.Second)
	if err != nil {
		t.Fatalf("LockSession: %v", err)
	}
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	// 释放后立即可重取
	release2, err := d.LockSession(ctx, "s", time.Second)
	if err != nil {
		t.Fatalf("re-acquire: %v", err)
	}
	_ = release2()
}

// TestLockExcludesSecondProcess proves a held lock blocks another
// process and times out cleanly.
func TestLockExcludesSecondProcess(t *testing.T) {
	_, _, _, _, a, b, _ := newDualSQLite(t, 0)
	ctx := context.Background()

	release, err := a.LockSession(ctx, "s", 0)
	if err != nil {
		t.Fatalf("a lock: %v", err)
	}
	defer release()

	start := time.Now()
	if _, err := b.LockSession(ctx, "s", 150*time.Millisecond); !errors.Is(err, memorystore.ErrLockTimeout) {
		t.Fatalf("err = %v, want ErrLockTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %s, lock wait not honored", elapsed)
	}
}

// TestLockReleaseIsTokenSafe proves a late release cannot delete a
// lock that has since been acquired by someone else.
func TestLockReleaseIsTokenSafe(t *testing.T) {
	_, _, _, _, a, b, _ := newDualSQLite(t, 0)
	ctx := context.Background()

	release, err := a.LockSession(ctx, "s", 0)
	if err != nil {
		t.Fatalf("a lock: %v", err)
	}
	// 模拟租约到期：直接清掉锁行（真实场景由过期清扫完成）
	if _, err := a.DB().ExecContext(ctx, `DELETE FROM session_locks`); err != nil {
		t.Fatalf("expire: %v", err)
	}
	_ = release() // 迟到的释放：token 不匹配任何行，必须是空操作

	got, err := b.LockSession(ctx, "s", time.Second)
	if err != nil {
		t.Fatalf("b lock after expiry: %v", err)
	}
	_ = got()
}

// TestLockCrashSafe proves an expired lease is reclaimed by the next
// acquirer without anyone calling release.
func TestLockCrashSafe(t *testing.T) {
	_, _, _, _, a, b, _ := newDualSQLite(t, 0)
	ctx := context.Background()

	release, err := a.LockSession(ctx, "s", 0)
	if err != nil {
		t.Fatalf("a lock: %v", err)
	}
	_ = release // 故意不释放，然后把租约改到过去 = 持有者已崩溃
	if _, err := a.DB().ExecContext(ctx,
		`UPDATE session_locks SET expires_at = ?`, "2000-01-01T00:00:00.000Z"); err != nil {
		t.Fatalf("backdate: %v", err)
	}

	// b 的下一次获取应当顺手清扫过期租约并成功
	got, err := b.LockSession(ctx, "s", time.Second)
	if err != nil {
		t.Fatalf("expired lease not reclaimed: %v", err)
	}
	_ = got()
}

// TestLockTableAutoCreated proves old databases gain the lock table on
// first lock use — upgrading the binary is the only migration step.
func TestLockTableAutoCreated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	ctx := context.Background()

	// 只跑主迁移的旧库（不含锁表——锁表在首次 LockSession 时惰性创建）
	d, err := sqlite.New(path, sqlite.Options{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer d.Close()
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	var n int
	_ = d.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE name='session_locks'`).Scan(&n)
	if n != 0 {
		t.Fatal("precondition: lock table should not exist yet")
	}

	release, err := d.LockSession(ctx, "s", time.Second)
	if err != nil {
		t.Fatalf("LockSession on legacy db: %v", err)
	}
	_ = release()
	_ = d.DB().QueryRowContext(ctx,
		`SELECT count(*) FROM sqlite_master WHERE name='session_locks'`).Scan(&n)
	if n != 1 {
		t.Fatal("lock table not auto-created")
	}
}

// TestCrossProcessRollbackSafeIsSerialized mirrors the Redis scenario:
// two processes run RollbackSafe on one shared session concurrently;
// the lock-table must serialize capture-then-rollback sequences.
func TestCrossProcessRollbackSafeIsSerialized(t *testing.T) {
	memA, memB, snapsA, snapsB, _, _, _ := newDualSQLite(t, 5*time.Second)
	ctx := context.Background()

	_ = memA.Add(ctx, "s",
		core.Message{Role: core.RoleSystem, Content: "sys"},
		core.Message{Role: core.RoleUser, Content: "q1"},
	)
	early, err := snapsA.Capture(ctx, "s", "early")
	if err != nil {
		t.Fatalf("Capture on A: %v", err)
	}
	_ = memA.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "q2"})

	var wg sync.WaitGroup
	var fails atomic.Int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(useA bool) {
			defer wg.Done()
			snaps := snapsB
			if useA {
				snaps = snapsA
			}
			if _, _, err := snaps.RollbackSafe(ctx, "s", early.ID, "safe"); err != nil {
				t.Errorf("RollbackSafe: %v", err)
				fails.Add(1)
			}
		}(i == 0)
	}
	wg.Wait()
	if fails.Load() > 0 {
		t.Fatal("concurrent RollbackSafe failed")
	}

	got, err := memB.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 2 || got[0].Content != "sys" || got[1].Content != "q1" {
		t.Fatalf("final state wrong: %+v", got)
	}
	listB, _ := snapsB.List(ctx, "s")
	foundQ2 := false
	for _, e := range listB {
		if e.Label == "safe" && e.MessageCount == 3 {
			foundQ2 = true
		}
	}
	if !foundQ2 {
		t.Fatalf("safety snapshot incomplete: %+v", listB)
	}
}
