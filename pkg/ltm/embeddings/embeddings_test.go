package embeddings_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/ltm"
	"github.com/Lookfukc/tt-agent/pkg/ltm/embeddings"
)

// capture records what the fake provider received.
type capture struct {
	mu     sync.Mutex
	path   string
	method string
	header http.Header
	body   []byte
}

// handler serves one provider shape and records the request.
func handler(t *testing.T, cap *capture, responder func(w http.ResponseWriter, r *http.Request)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.mu.Lock()
		cap.path = r.URL.Path
		cap.method = r.Method
		cap.header = r.Header.Clone()
		cap.body = make([]byte, r.ContentLength)
		_, _ = r.Body.Read(cap.body)
		cap.mu.Unlock()
		responder(w, r)
	})
}

// TestOpenAICompatibleShape asserts the wire format of the shared
// OpenAI-style kernel: path, Bearer auth, {model, input} body.
func TestOpenAICompatibleShape(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(handler(t, cap, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"index":0,"embedding":[0.1,0.2,0.3]}]}`))
	}))
	defer srv.Close()

	c := embeddings.NewOpenAICompatible(srv.URL, "sk-test", "text-embedding-3-small")
	vec, err := c.Embed(context.Background(), "hello")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 3 || vec[0] != 0.1 {
		t.Fatalf("vec = %v", vec)
	}

	if cap.method != http.MethodPost {
		t.Fatalf("method = %s", cap.method)
	}
	if cap.path != "/embeddings" {
		t.Fatalf("path = %s", cap.path)
	}
	if got := cap.header.Get("Authorization"); got != "Bearer sk-test" {
		t.Fatalf("Authorization = %q", got)
	}
	var body struct {
		Model string   `json:"model"`
		Input []string `json:"input"`
	}
	if err := json.Unmarshal(cap.body, &body); err != nil {
		t.Fatalf("body decode: %v (raw %s)", err, cap.body)
	}
	if body.Model != "text-embedding-3-small" || len(body.Input) != 1 || body.Input[0] != "hello" {
		t.Fatalf("body = %+v", body)
	}
}

// TestAnthropicShape asserts Anthropic's auth headers while sharing the
// OpenAI wire shape.
func TestAnthropicShape(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(handler(t, cap, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.5]}]}`))
	}))
	defer srv.Close()

	c := embeddings.NewAnthropic("sk-ant-test", "voyage-3-large", embeddings.WithBaseURL(srv.URL))
	vec, err := c.Embed(context.Background(), "hi")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 1 || vec[0] != 0.5 {
		t.Fatalf("vec = %v", vec)
	}

	if cap.path != "/v1/embeddings" {
		t.Fatalf("path = %s", cap.path)
	}
	if got := cap.header.Get("x-api-key"); got != "sk-ant-test" {
		t.Fatalf("x-api-key = %q", got)
	}
	if got := cap.header.Get("anthropic-version"); got == "" {
		t.Fatal("anthropic-version header missing")
	}
	if got := cap.header.Get("Authorization"); got != "" {
		t.Fatalf("must not send Bearer auth to Anthropic, got %q", got)
	}
}

// TestGeminiShape asserts Gemini's endpoint, header and payload.
func TestGeminiShape(t *testing.T) {
	cap := &capture{}
	srv := httptest.NewServer(handler(t, cap, func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"embedding":{"values":[0.7,0.8]}}`))
	}))
	defer srv.Close()

	c := embeddings.NewGemini("g-key", "gemini-embedding-001", embeddings.WithBaseURL(srv.URL))
	vec, err := c.Embed(context.Background(), "hola")
	if err != nil {
		t.Fatalf("Embed: %v", err)
	}
	if len(vec) != 2 || vec[0] != 0.7 {
		t.Fatalf("vec = %v", vec)
	}

	if !strings.Contains(cap.path, "gemini-embedding-001:embedContent") {
		t.Fatalf("path = %s", cap.path)
	}
	if got := cap.header.Get("x-goog-api-key"); got != "g-key" {
		t.Fatalf("x-goog-api-key = %q", got)
	}
	if !strings.Contains(string(cap.body), `"text":"hola"`) {
		t.Fatalf("body missing text part: %s", cap.body)
	}
}

// TestProviderErrorCarriesBody proves errors include the provider's
// response, so a schema drift is diagnosable from the message alone.
func TestProviderErrorCarriesBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"message":"unknown model"}}`))
	}))
	defer srv.Close()

	c := embeddings.NewOpenAICompatible(srv.URL, "k", "bogus-model")
	_, err := c.Embed(context.Background(), "x")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "unknown model") {
		t.Fatalf("error lacks provider detail: %v", err)
	}
}

// TestProviderErrorFieldInside200 covers the JSON-200-with-error shape
// some gateways return.
func TestProviderErrorFieldInside200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"error":{"message":"quota exceeded"}}`))
	}))
	defer srv.Close()

	c := embeddings.NewOpenAICompatible(srv.URL, "k", "m")
	_, err := c.Embed(context.Background(), "x")
	if err == nil || !strings.Contains(err.Error(), "quota exceeded") {
		t.Fatalf("err = %v", err)
	}
}

// TestEmptyTextShortCircuits covers the facade's empty-input contract.
func TestEmptyTextShortCircuits(t *testing.T) {
	c := embeddings.NewOpenAICompatible("http://127.0.0.1:1", "k", "m")
	vec, err := c.Embed(context.Background(), "   ")
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if vec != nil {
		t.Fatalf("vec = %v, want nil", vec)
	}
}

// TestUnreachableEndpointFailsFast proves context cancellation is
// honored rather than hanging.
func TestUnreachableEndpointFailsFast(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-block // hang until teardown unblocks it
	}))
	// defer 是 LIFO：先解阻塞 handler，再关服务器——顺序反了会死锁
	defer srv.Close()
	defer close(block)

	c := embeddings.NewOpenAICompatible(srv.URL, "k", "m",
		embeddings.WithHTTPClient(&http.Client{Timeout: 100 * time.Millisecond}))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if _, err := c.Embed(ctx, "x"); err == nil {
		t.Fatal("expected timeout error")
	}
}

// TestConcurrentEmbeds covers the safe-concurrent-use contract.
func TestConcurrentEmbeds(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":[{"embedding":[1]}]}`))
	}))
	defer srv.Close()

	c := embeddings.NewOpenAICompatible(srv.URL, "k", "m")
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Embed(context.Background(), "x"); err != nil {
				errs <- err
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent embed: %v", err)
	}
}

// TestImplementsEmbedder is the compile-time plug-in guarantee.
var _ ltm.Embedder = (*embeddings.Client)(nil)
