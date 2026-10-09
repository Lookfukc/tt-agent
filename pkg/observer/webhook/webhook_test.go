package webhook_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/observer"
	"github.com/Lookfukc/tt-agent/pkg/observer/webhook"
)

// received records one delivered webhook.
type received struct {
	body      []byte
	signature string
}

// TestDeliversSignedEvent proves the happy path: JSON body, HMAC
// signature the receiver can verify, async delivery.
func TestDeliversSignedEvent(t *testing.T) {
	var (
		mu  sync.Mutex
		got []received
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		mu.Lock()
		got = append(got, received{body: body, signature: r.Header.Get(webhook.SignatureHeader)})
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fwd := webhook.New(webhook.Config{
		URL:         srv.URL,
		Secret:      "topsecret",
		MaxRetries:  0,
		BackoffBase: time.Millisecond,
	})
	event := observer.MemoryEvent{
		Kind:    observer.EventMessagesAppended,
		Backend: "sqlite",
		Session: "s-42",
		Detail:  map[string]any{"count": 3},
	}
	fwd.OnMemoryEvent(event)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n == 1 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	fwd.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 {
		t.Fatalf("delivered %d events, want 1", len(got))
	}

	// 验签：接收方用相同密钥重算 HMAC 必须一致
	mac := hmac.New(sha256.New, []byte("topsecret"))
	mac.Write(got[0].body)
	if want := hex.EncodeToString(mac.Sum(nil)); got[0].signature != want {
		t.Fatalf("signature = %q, want %q", got[0].signature, want)
	}

	// 载荷保持事件语义
	var decoded observer.MemoryEvent
	if err := json.Unmarshal(got[0].body, &decoded); err != nil {
		t.Fatalf("body decode: %v (%s)", err, got[0].body)
	}
	if decoded.Kind != observer.EventMessagesAppended || decoded.Backend != "sqlite" || decoded.Session != "s-42" {
		t.Fatalf("decoded = %+v", decoded)
	}
	// 事件不得包含消息内容——只有计数
	if _, hasCount := decoded.Detail["count"]; !hasCount {
		t.Fatalf("detail missing count: %+v", decoded.Detail)
	}
}

// TestRetriesOnServerError proves a 500 is retried and then succeeds.
func TestRetriesOnServerError(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if attempts.Add(1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	fwd := webhook.New(webhook.Config{
		URL:         srv.URL,
		MaxRetries:  3,
		BackoffBase: time.Millisecond,
	})
	fwd.OnMemoryEvent(observer.MemoryEvent{Kind: observer.EventSessionCleared})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() < 3 {
		time.Sleep(2 * time.Millisecond)
	}
	fwd.Close()

	if attempts.Load() != 3 {
		t.Fatalf("attempts = %d, want 3", attempts.Load())
	}
	if delivered, failed, _ := fwd.Stats(); delivered != 1 || failed != 0 {
		t.Fatalf("stats delivered=%d failed=%d, want 1/0", delivered, failed)
	}
}

// TestPermanentClientErrorNotRetried proves a 400 gives up
// immediately: resending identical bytes cannot fix a rejection.
func TestPermanentClientErrorNotRetried(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()

	fwd := webhook.New(webhook.Config{
		URL:         srv.URL,
		MaxRetries:  5,
		BackoffBase: time.Millisecond,
	})
	fwd.OnMemoryEvent(observer.MemoryEvent{Kind: observer.EventSessionCleared})

	// 等首次尝试到达（关停会丢弃排队事件，这是设计行为）
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && attempts.Load() == 0 {
		time.Sleep(2 * time.Millisecond)
	}
	fwd.Close()

	if attempts.Load() != 1 {
		t.Fatalf("attempts = %d, want 1 (4xx must not retry)", attempts.Load())
	}
	if _, failed, _ := fwd.Stats(); failed != 1 {
		t.Fatalf("failed = %d, want 1", failed)
	}
}

// TestQueueDropsOldest proves saturation drops the oldest event and
// keeps accepting newer ones.
func TestQueueDropsOldest(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block
	}))
	defer func() {
		close(block)
		srv.Close()
	}()

	fwd := webhook.New(webhook.Config{
		URL:        srv.URL,
		QueueSize:  2,
		MaxRetries: 0,
	})
	for i := 0; i < 5; i++ {
		fwd.OnMemoryEvent(observer.MemoryEvent{Kind: observer.EventFactRemembered})
	}
	if _, _, d := fwd.Stats(); d != 3 { // 5 events - queue 2 = 3 dropped
		t.Fatalf("dropped = %d, want 3", d)
	}
	fwd.Close()
}
