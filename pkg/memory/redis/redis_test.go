package redis_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"

	"github.com/Lookfukc/tt-agent/pkg/agent"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/redis"
)

// newDriver spins up an in-process Redis and returns a driver bound to it.
func newDriver(t *testing.T, ttl time.Duration) (*redis.Driver, *miniredis.Miniredis) {
	t.Helper()
	srv := miniredis.RunT(t)
	client := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return redis.New(client, redis.Options{SessionTTL: ttl}), srv
}

// TestAddAndRecent covers the basic path plus ordering.
func TestAddAndRecent(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	if err := store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "one"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := store.Add(ctx, "s", core.Message{Role: core.RoleAssistant, Content: "two"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 2 || got[0].Content != "one" || got[1].Content != "two" {
		t.Fatalf("Recent = %+v", got)
	}
}

// TestNativeTTLExpiresSession proves the driver's headline advantage
// over the file backend: an idle session disappears without a sweeper.
func TestNativeTTLExpiresSession(t *testing.T) {
	d, srv := newDriver(t, 30*time.Second)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	if err := store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "hi"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, _ := store.Recent(ctx, "s", 1<<20)
	if len(got) != 1 {
		t.Fatalf("message missing before expiry: %+v", got)
	}

	// miniredis 的时间可以快进，验证 EXPIRE 真的设上了
	srv.FastForward(31 * time.Second)
	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent after expiry: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("session survived TTL: %+v", got)
	}
}

// TestTrimKeepsSystemMessagesAtHead covers the trickiest driver code:
// Redis cannot LTRIM here because system messages at the head survive.
func TestTrimKeepsSystemMessagesAtHead(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	msgs := []core.Message{{Role: core.RoleSystem, Content: "sys"}}
	for i := 0; i < 5; i++ {
		msgs = append(msgs, core.Message{Role: core.RoleUser, Content: string(rune('a' + i))})
	}
	if err := store.Add(ctx, "s", msgs...); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := store.Trim(ctx, "s", 2); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	got, _ := store.Recent(ctx, "s", 1<<20)
	if len(got) != 4 {
		t.Fatalf("after trim = %d, want 4: %+v", len(got), got)
	}
	if got[0].Role != core.RoleSystem || got[1].Content != "c" {
		t.Fatalf("wrong survivors: %+v", got)
	}
}

// TestTrimAllMessagesRemovesKeys covers the empty-result edge case.
func TestTrimAllMessagesRemovesKeys(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	_ = store.Add(ctx, "s",
		core.Message{Role: core.RoleUser, Content: "a"},
		core.Message{Role: core.RoleUser, Content: "b"},
	)
	if err := store.Trim(ctx, "s", 5); err != nil { // 多于实际条数
		t.Fatalf("Trim: %v", err)
	}
	got, _ := store.Recent(ctx, "s", 1<<20)
	if len(got) != 0 {
		t.Fatalf("messages survived full trim: %+v", got)
	}
	n, err := store.CountMessages(ctx, "s")
	if err != nil || n != 0 {
		t.Fatalf("CountMessages = %d, %v", n, err)
	}
}

// TestSummaryRoundTrip covers SummaryStore over Redis.
func TestSummaryRoundTrip(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	if err := store.SaveSummary(ctx, "s", 4, "short summary"); err != nil {
		t.Fatalf("SaveSummary: %v", err)
	}
	text, covered, err := store.LoadSummary(ctx, "s")
	if err != nil || text != "short summary" || covered != 4 {
		t.Fatalf("LoadSummary = (%q, %d, %v)", text, covered, err)
	}
}

// TestClearRemovesAllKeys verifies no orphan keys are left behind.
func TestClearRemovesAllKeys(t *testing.T) {
	d, srv := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	_ = store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"})
	_ = store.SaveSummary(ctx, "s", 1, "sum")
	if err := store.Clear(ctx, "s"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	for _, k := range srv.Keys() {
		if strings.Contains(k, "s") {
			t.Fatalf("key survived Clear: %q", k)
		}
	}
}

// TestEncryptedCodecOverRedis proves encryption rides on the codec and
// that plaintext never reaches the store.
func TestEncryptedCodecOverRedis(t *testing.T) {
	d, srv := newDriver(t, 0)
	codec, err := memorystore.NewEncryptedCodec([]byte("passphrase"), nil)
	if err != nil {
		t.Fatalf("NewEncryptedCodec: %v", err)
	}
	store := d.Memory(memorystore.Options{Codec: codec})
	ctx := context.Background()

	secret := "my credit card is 4111-1111-1111-1111"
	if err := store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: secret}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// 存储在 Redis 里的字节不得包含明文
	for _, k := range srv.Keys() {
		v, err := srv.Get(k)
		if err != nil {
			continue
		}
		if strings.Contains(v, "4111") {
			t.Fatalf("plaintext leaked into store key %q: %s", k, v)
		}
	}

	// 但读回来必须是明文
	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 1 || got[0].Content != secret {
		t.Fatalf("decrypted = %+v", got)
	}
}

// TestEncryptedCodecWrongKeyFails proves tampering or a wrong key is
// detected rather than returning garbage.
func TestEncryptedCodecWrongKeyFails(t *testing.T) {
	d, _ := newDriver(t, 0)
	writeCodec, _ := memorystore.NewEncryptedCodec([]byte("key-one"), nil)
	readCodec, _ := memorystore.NewEncryptedCodec([]byte("key-two"), nil)
	ctx := context.Background()

	writer := d.Memory(memorystore.Options{Codec: writeCodec})
	if err := writer.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "secret"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	reader := d.Memory(memorystore.Options{Codec: readCodec})
	got, err := reader.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	// 无法解密的记录按损坏行跳过：结果为空，而不是 panic 或返回垃圾
	if len(got) != 0 {
		t.Fatalf("wrong key produced readable data: %+v", got)
	}
}

// TestWorksWithTTLDecorator covers stacking the portable TTL decorator
// on top of native expiry.
//
// The decorator evicts on idle, and any Recent call counts as activity,
// so this test waits without touching the session — polling it would
// keep resetting the idle clock and prove nothing.
func TestWorksWithTTLDecorator(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mem := memory.NewTTL(ctx, store, 40*time.Millisecond, 10*time.Millisecond)
	if err := mem.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "x"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// 等足够久让 janitor 扫过（idle 40ms + sweep 10ms，留足余量），
	// 期间不访问：直接查内层存储，确认已被 Clear
	time.Sleep(300 * time.Millisecond)
	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("TTL decorator never evicted the session: %+v", got)
	}
}

// TestConcurrentAddAndTrimNeverLosesAppends is the regression guard
// for the lost-update race the client-side trim once had: between a
// client's LRANGE and RENAME, a concurrent append could land and then
// be silently overwritten by the rename. The Lua-based trim runs on
// the Redis server, so each append lands entirely before or after a
// trim — never half-observed, never dropped.
//
// The invariant asserted: once every trim has completed, any message
// appended afterwards is durably present. The chaos phase before it
// exists to interleave trims with appends; with the old
// read-rebuild-rename implementation the final count came up short.
func TestConcurrentAddAndTrimNeverLosesAppends(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	// 混沌阶段：并发追加 + 并发修剪
	const writers, perWriter = 6, 40
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				_ = store.Add(ctx, "s", core.Message{
					Role: core.RoleUser, Content: fmt.Sprintf("w%d-%d", i, j),
				})
			}
		}(i)
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_ = store.Trim(ctx, "s", 3)
			}
		}()
	}
	wg.Wait()

	// 静默阶段：记录基线，再追加固定数量，断言一条不少
	base, err := store.CountMessages(ctx, "s")
	if err != nil {
		t.Fatalf("baseline count: %v", err)
	}
	const tailAppends = 25
	for j := 0; j < tailAppends; j++ {
		if err := store.Add(ctx, "s", core.Message{
			Role: core.RoleUser, Content: fmt.Sprintf("tail-%d", j),
		}); err != nil {
			t.Fatalf("tail add: %v", err)
		}
	}
	final, err := store.CountMessages(ctx, "s")
	if err != nil {
		t.Fatalf("final count: %v", err)
	}
	if final != base+tailAppends {
		t.Fatalf("appends were lost: base=%d, added %d, final=%d (missing %d)",
			base, tailAppends, final, base+tailAppends-final)
	}
	// 尾部消息内容也必须在（计数对但内容错同样不可接受）
	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) == 0 || !strings.HasSuffix(got[len(got)-1].Content, fmt.Sprintf("tail-%d", tailAppends-1)) {
		t.Fatalf("newest tail message missing: last=%q", got[len(got)-1].Content)
	}
}

// TestTrimScriptKeepsSystemMessagesUnderConcurrency proves the system
// message survives no matter how trims interleave with appends.
func TestTrimScriptKeepsSystemMessagesUnderConcurrency(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	_ = store.Add(ctx, "s", core.Message{Role: core.RoleSystem, Content: "sys"})

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 30; j++ {
				_ = store.Add(ctx, "s", core.Message{Role: core.RoleUser, Content: "m"})
				_ = store.Trim(ctx, "s", 2)
			}
		}()
	}
	wg.Wait()

	got, err := store.Recent(ctx, "s", 1<<20)
	if err != nil {
		t.Fatalf("Recent: %v", err)
	}
	if len(got) == 0 || got[0].Role != core.RoleSystem {
		t.Fatalf("system message lost under concurrent trim: %+v", got)
	}
}

// TestNewFromURLFailureIsClean proves a bad URL is reported, not panicked.
func TestNewFromURLFailureIsClean(t *testing.T) {
	ctx := context.Background()
	if _, err := redis.NewFromURL(ctx, "not-a-url", redis.Options{}); err == nil {
		t.Fatal("expected error for malformed URL")
	}
	// 指向一个必然不可达的端口（不要用默认端口，避免本机正好有 Redis）
	_, err := redis.NewFromURL(ctx, "redis://127.0.0.1:1", redis.Options{})
	if err == nil {
		t.Fatal("expected error for unreachable server")
	}
}

// recordingLLM captures the messages it was asked to answer, so a test
// can assert exactly what the second instance's loop could see.
type recordingLLM struct {
	mu      sync.Mutex
	seen    [][]core.Message
	replies []string
}

// Chat is unused: the agent loop always goes through ChatStream.
func (r *recordingLLM) Chat(context.Context, core.ChatRequest) (*core.ChatResponse, error) {
	return nil, errors.New("not implemented")
}

// ChatStream returns the next scripted reply and records the request.
func (r *recordingLLM) ChatStream(_ context.Context, req core.ChatRequest) (<-chan core.StreamEvent, error) {
	r.mu.Lock()
	n := len(r.seen)
	r.seen = append(r.seen, append([]core.Message(nil), req.Messages...))
	reply := "ok"
	if n < len(r.replies) {
		reply = r.replies[n]
	}
	r.mu.Unlock()

	out := make(chan core.StreamEvent, 3)
	go func() {
		defer close(out)
		out <- core.StreamEvent{Type: core.StreamStart}
		out <- core.StreamEvent{Type: core.StreamDeltaText, Text: reply}
		out <- core.StreamEvent{Type: core.StreamDone}
	}()
	return out, nil
}

// requests returns the captured requests.
func (r *recordingLLM) requests() [][]core.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]core.Message, len(r.seen))
	copy(out, r.seen)
	return out
}

// TestDualInstanceSharedConversation is the multi-instance deployment
// story in miniature: two separate drivers (two clients, as two server
// processes would have) share one Redis. A conversation turn served by
// instance A must be visible to instance B on the next turn — that is
// the whole reason to run an external backend.
func TestDualInstanceSharedConversation(t *testing.T) {
	srv := miniredis.RunT(t)
	ctx := context.Background()

	clientA := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	clientB := goredis.NewClient(&goredis.Options{Addr: srv.Addr()})
	defer clientA.Close()
	defer clientB.Close()

	instanceA := redis.New(clientA, redis.Options{}).Memory(memorystore.Options{})
	instanceB := redis.New(clientB, redis.Options{}).Memory(memorystore.Options{})

	llm := &recordingLLM{replies: []string{"first answer from A", "second answer from B"}}
	// 两个实例各建一个 Loop：同一 sessionID，同一 LLM 记录器
	loopA := agent.NewLoop(llm, nil, instanceA, agent.Config{Model: "m"})
	loopB := agent.NewLoop(llm, nil, instanceB, agent.Config{Model: "m"})

	// 第一轮：打到实例 A
	if _, _, err := loopA.Run(ctx, "shared-1", "question from A"); err != nil {
		t.Fatalf("run on A: %v", err)
	}
	// 第二轮：打到实例 B
	if _, _, err := loopB.Run(ctx, "shared-1", "follow-up handled by B"); err != nil {
		t.Fatalf("run on B: %v", err)
	}

	reqs := llm.requests()
	if len(reqs) != 2 {
		t.Fatalf("captured %d requests, want 2", len(reqs))
	}
	// 第二个请求必须包含第一轮的问答（B 的循环从共享存储读到了 A 写入的历史）
	second := reqs[1]
	var sawFirstQ, sawFirstA, sawSecondQ bool
	for _, m := range second {
		switch m.Content {
		case "question from A":
			sawFirstQ = m.Role == core.RoleUser
		case "first answer from A":
			sawFirstA = m.Role == core.RoleAssistant
		case "follow-up handled by B":
			sawSecondQ = m.Role == core.RoleUser
		}
	}
	if !sawFirstQ || !sawFirstA || !sawSecondQ {
		t.Fatalf("instance B missed instance A's turn: %+v", second)
	}

	// A 再读一次，也必须看到 B 的回答（双向可见）
	got, err := instanceA.Recent(ctx, "shared-1", 1<<20)
	if err != nil {
		t.Fatalf("Recent on A: %v", err)
	}
	found := false
	for _, m := range got {
		if m.Content == "second answer from B" {
			found = true
		}
	}
	if !found {
		t.Fatalf("instance A cannot see instance B's answer: %+v", got)
	}
}

// TestInvalidSessionIDRejected covers input validation.
func TestInvalidSessionIDRejected(t *testing.T) {
	d, _ := newDriver(t, 0)
	store := d.Memory(memorystore.Options{})
	ctx := context.Background()

	err := store.Add(ctx, "../evil", core.Message{Role: core.RoleUser, Content: "x"})
	if !errors.Is(err, memorystore.ErrInvalidSessionID) {
		t.Fatalf("err = %v, want ErrInvalidSessionID", err)
	}
}
