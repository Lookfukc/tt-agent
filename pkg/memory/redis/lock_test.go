package redis_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	ttredis "github.com/Lookfukc/tt-agent/pkg/memory/redis"
	"github.com/Lookfukc/tt-agent/pkg/memory/snapshot"
)

// dualFixture simulates two processes: separate drivers and separate
// snapshot stores over one shared Redis.
type dualFixture struct {
	srv        *miniredis.Miniredis
	memA, memB *memorystore.Store
	snapsA     *snapshot.Store
	snapsB     *snapshot.Store
	drvA, drvB *ttredis.Driver
}

// newDualFixture wires both processes.
func newDualFixture(t *testing.T, lockWait time.Duration) dualFixture {
	t.Helper()
	srv := miniredis.RunT(t)
	clientA := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	clientB := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = clientA.Close(); _ = clientB.Close() })
	drvA := ttredis.New(clientA, ttredis.Options{})
	drvB := ttredis.New(clientB, ttredis.Options{})
	return dualFixture{
		srv:  srv,
		drvA: drvA, drvB: drvB,
		memA:   drvA.Memory(memorystore.Options{}),
		memB:   drvB.Memory(memorystore.Options{}),
		snapsA: snapshot.New(drvA, snapshot.Options{LockWait: lockWait}),
		snapsB: snapshot.New(drvB, snapshot.Options{LockWait: lockWait}),
	}
}

// TestCrossProcessRollbackSafeIsSerialized is the regression guard for
// the in-process-only lock: two snapshot stores (two processes) run
// RollbackSafe on one shared session concurrently; the distributed
// lock must serialize them so no capture lands between another
// process's capture and rollback.
func TestCrossProcessRollbackSafeIsSerialized(t *testing.T) {
	f := newDualFixture(t, 5*time.Second)
	ctx := context.Background()

	// 准备共享历史
	_ = f.memA.Add(ctx, "s",
		core.Message{Role: core.RoleSystem, Content: "sys"},
		core.Message{Role: core.RoleUser, Content: "q1"},
	)
	early, err := f.snapsA.Capture(ctx, "s", "early")
	if err != nil {
		t.Fatalf("Capture on A: %v", err)
	}
	_ = f.memA.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "q2"})

	// 两进程同时安全回滚
	var wg sync.WaitGroup
	var fails atomic.Int32
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(useA bool) {
			defer wg.Done()
			snaps := f.snapsB
			if useA {
				snaps = f.snapsA
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

	// 最终状态：回滚到 early（两次幂等），历史应为 sys+q1
	got, err := f.memB.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 2 || got[0].Content != "sys" || got[1].Content != "q1" {
		t.Fatalf("final state wrong: %+v", got)
	}
	// 两个安全快照都完整保存了回滚前状态（q2 所在的历史）
	listB, _ := f.snapsB.List(ctx, "s")
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

// TestLockBlocksSecondProcess proves a held lock actually excludes
// another process and times out cleanly.
func TestLockBlocksSecondProcess(t *testing.T) {
	f := newDualFixture(t, 0)
	ctx := context.Background()

	release, err := f.drvA.LockSession(ctx, "s", 0)
	if err != nil {
		t.Fatalf("A LockSession: %v", err)
	}

	// B 同步等锁：短等待应当超时
	f.snapsB = snapshot.New(f.drvB, snapshot.Options{LockWait: 100 * time.Millisecond})
	start := time.Now()
	_, _, err = f.snapsB.RollbackSafe(ctx, "s", "whatever", "safe")
	if !errors.Is(err, memorystore.ErrLockTimeout) {
		t.Fatalf("err = %v, want ErrLockTimeout", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("timeout took %s, lock wait not honored", elapsed)
	}

	// A 释放后 B 立即可用
	if err := release(); err != nil {
		t.Fatalf("release: %v", err)
	}
	f.snapsB = snapshot.New(f.drvB, snapshot.Options{LockWait: time.Second})
	if _, _, err := f.snapsB.RollbackSafe(ctx, "s", "no-such", "safe"); !errors.Is(err, snapshot.ErrNoSnapshot) {
		t.Fatalf("after release, err = %v (want ErrNoSnapshot: lock acquired, target checked)", err)
	}
}

// TestLockReleaseIsTokenSafe proves a late release cannot delete a
// lock that has since been acquired by someone else.
func TestLockReleaseIsTokenSafe(t *testing.T) {
	f := newDualFixture(t, 0)
	ctx := context.Background()

	releaseA, err := f.drvA.LockSession(ctx, "s", 0)
	if err != nil {
		t.Fatalf("A lock: %v", err)
	}
	// 模拟 A 的租约到期：服务端直接清键（真实场景由 PX TTL 完成）
	f.srv.Del("agent:mem:s:lock")
	_ = releaseA() // 迟到的释放：token 已不匹配，必须是空操作

	releaseB, err := f.drvB.LockSession(ctx, "s", 0)
	if err != nil {
		t.Fatalf("B lock after expiry: %v", err)
	}
	if err := releaseB(); err != nil {
		t.Fatalf("B release: %v", err)
	}
}
