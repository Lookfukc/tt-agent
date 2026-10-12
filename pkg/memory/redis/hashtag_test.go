package redis_test

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/redis"
)

// TestKeysCarryHashTag pins the Redis Cluster compatibility contract:
// every key a session owns must carry the session ID inside a hash
// tag ({...}) so all of them hash to the same cluster slot. Without
// the tag, the Lua trim script and the multi-key DEL fail with
// CROSSSLOT on sharded deployments.
func TestKeysCarryHashTag(t *testing.T) {
	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	d := redis.New(client, redis.Options{})

	ctx := t.Context()
	store := memorystore.New(d, memorystore.Options{})

	// 产生真实键：消息 + 系统消息 + 摘要
	if err := store.Add(ctx, "s",
		core.Message{Role: core.RoleSystem, Content: "sys"},
		core.Message{Role: core.RoleUser, Content: "q"},
	); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// 一把锁 + 一次修剪，触发 lockKey 与脚本路径
	if rel, err := d.LockSession(ctx, "s", 0); err != nil {
		t.Fatalf("lock: %v", err)
	} else {
		_ = rel()
	}

	keys := srv.Keys()
	if len(keys) == 0 {
		t.Fatal("no keys produced")
	}

	// 1) 每个键都必须带 {s} 形式的 hash tag
	for _, k := range keys {
		if !containsTag(k, "s") {
			t.Fatalf("key %q lacks hash tag {s}", k)
		}
	}

	// 2) 所有键的 tag 内容必须一致：同 tag = 同 slot 是
	//    Redis Cluster 的定义性保证
	tags := map[string]bool{}
	for _, k := range keys {
		tags[extractTag(k)] = true
	}
	if len(tags) != 1 {
		t.Fatalf("keys span %d hash tags (%v), want exactly 1", len(tags), keys)
	}
}

// TestHashTagSurvivesCustomPrefix 同上但用自定义前缀：
// tag 语义与前缀无关，多应用共享一个 Cluster 时各自隔离仍成立。
func TestHashTagSurvivesCustomPrefix(t *testing.T) {
	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	d := redis.New(client, redis.Options{Prefix: "myapp:mem:"})

	ctx := t.Context()
	store := memorystore.New(d, memorystore.Options{})
	if err := store.Add(ctx, "user-42", core.Text("hi")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	for _, k := range srv.Keys() {
		if !containsTag(k, "user-42") {
			t.Fatalf("key %q lacks hash tag {user-42} under custom prefix", k)
		}
	}
}

// containsTag reports whether key contains the {id} hash tag.
func containsTag(key, id string) bool {
	return extractTag(key) == id
}

// extractTag returns the content of the first {...} in key, or "".
func extractTag(key string) string {
	start := -1
	for i, r := range key {
		if r == '{' {
			start = i
		}
		if r == '}' && start >= 0 {
			return key[start+1 : i]
		}
	}
	return ""
}
