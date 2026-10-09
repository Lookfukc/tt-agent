// Package webhook forwards memory events to an HTTP endpoint.
//
// It implements observer.MemoryObserver, so it plugs into the same
// Observer options the memory stores expose. Delivery is asynchronous
// with a bounded queue, each POST is signed with HMAC-SHA256 so the
// receiver can authenticate it, and failures retry with exponential
// backoff before the event is counted as lost.
//
// Events never contain message content (by design of
// observer.MemoryEvent), so forwarding them off-process does not leak
// conversations.
package webhook

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// SignatureHeader carries the HMAC-SHA256 hex digest of the raw body.
const SignatureHeader = "X-TT-Agent-Signature"

// Config configures a Forwarder.
type Config struct {
	// URL is the delivery endpoint. Required.
	URL string

	// Secret keys the HMAC signature; empty disables signing (and the
	// receiver loses its authenticity check — set it in production).
	Secret string

	// Timeout bounds one HTTP attempt. Default 10s.
	Timeout time.Duration

	// MaxRetries is how often a failed delivery is retried with
	// exponential backoff before being counted as lost. Default 3.
	MaxRetries int

	// QueueSize bounds pending deliveries; the oldest event is dropped
	// when full. Default 256.
	QueueSize int

	// BackoffBase is the first retry delay; each subsequent retry
	// doubles it. Default 500ms.
	BackoffBase time.Duration
}

// defaults for unset fields.
const (
	defaultTimeout    = 10 * time.Second
	defaultMaxRetries = 3
	defaultQueueSize  = 256
	defaultBackoff    = 500 * time.Millisecond
)

// Forwarder delivers memory events to an HTTP endpoint.
//
// It satisfies observer.MemoryObserver. The worker goroutine starts
// lazily on the first event and stops on Close.
type Forwarder struct {
	cfg    Config
	client *http.Client

	mu      sync.Mutex
	queue   []observer.MemoryEvent
	started bool
	done    chan struct{}
	wg      sync.WaitGroup

	delivered atomic.Uint64
	failed    atomic.Uint64
	dropped   atomic.Uint64
}

// New builds a forwarder. The returned value must be Closed when done.
func New(cfg Config) *Forwarder {
	if cfg.Timeout <= 0 {
		cfg.Timeout = defaultTimeout
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = defaultQueueSize
	}
	if cfg.BackoffBase <= 0 {
		cfg.BackoffBase = defaultBackoff
	}
	return &Forwarder{
		cfg:    cfg,
		done:   make(chan struct{}),
		client: &http.Client{Timeout: cfg.Timeout},
	}
}

// OnMemoryEvent implements observer.MemoryObserver by enqueueing for
// asynchronous delivery.
//
// Enqueueing never blocks: a full queue drops the oldest event — newer
// events describe the latest state, and a memory write must never wait
// on an HTTP endpoint.
func (f *Forwarder) OnMemoryEvent(e observer.MemoryEvent) {
	f.mu.Lock()
	if len(f.queue) >= f.cfg.QueueSize {
		// Drop the oldest: under backpressure the freshest state wins.
		f.queue = f.queue[1:]
		f.dropped.Add(1)
	}
	f.queue = append(f.queue, e)
	start := !f.started && len(f.queue) > 0
	f.started = true
	f.mu.Unlock()

	if start {
		f.wg.Add(1)
		go f.worker()
	}
}

// Stats reports delivery counters.
func (f *Forwarder) Stats() (delivered, failed, dropped uint64) {
	return f.delivered.Load(), f.failed.Load(), f.dropped.Load()
}

// Close stops the worker. Pending events already handed to HTTP are
// finished; queued events are dropped (a shutdown must not block on a
// slow endpoint).
func (f *Forwarder) Close() {
	f.mu.Lock()
	started := f.started
	f.mu.Unlock()
	if started {
		close(f.done)
		f.wg.Wait()
	}
}

// worker drains the queue until done.
func (f *Forwarder) worker() {
	defer f.wg.Done()
	for {
		select {
		case <-f.done:
			return
		default:
		}
		f.mu.Lock()
		if len(f.queue) == 0 {
			f.mu.Unlock()
			// Nothing to do; park briefly instead of spinning.
			select {
			case <-f.done:
				return
			case <-time.After(50 * time.Millisecond):
			}
			continue
		}
		e := f.queue[0]
		f.queue = f.queue[1:]
		f.mu.Unlock()

		if err := f.deliver(e); err != nil {
			f.failed.Add(1)
		} else {
			f.delivered.Add(1)
		}
	}
}

// deliver POSTs one event, retrying with backoff.
func (f *Forwarder) deliver(e observer.MemoryEvent) error {
	body, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("webhook: encode event: %w", err) // not retried: permanent
	}
	var lastErr error
	for attempt := 0; attempt <= f.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			// Backoff before retry; honor shutdown so Close is prompt.
			select {
			case <-f.done:
				return lastErr
			case <-time.After(f.cfg.BackoffBase << (attempt - 1)):
			}
		}
		req, err := http.NewRequest(http.MethodPost, f.cfg.URL, bytes.NewReader(body))
		if err != nil {
			return fmt.Errorf("webhook: build request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		if f.cfg.Secret != "" {
			mac := hmac.New(sha256.New, []byte(f.cfg.Secret))
			mac.Write(body)
			req.Header.Set(SignatureHeader, hex.EncodeToString(mac.Sum(nil)))
		}
		resp, err := f.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("webhook: post: %w", err)
			continue
		}
		// Any 2xx is success; 4xx is permanent (the receiver rejected
		// the payload, retrying the same bytes cannot help); 5xx and
		// network errors are worth another attempt.
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			resp.Body.Close()
			return nil
		}
		lastErr = fmt.Errorf("webhook: status %d", resp.StatusCode)
		resp.Body.Close()
		if resp.StatusCode < 500 {
			return lastErr
		}
	}
	return lastErr
}

// compile-time proof the forwarder is an observer.
var _ observer.MemoryObserver = (*Forwarder)(nil)
