// Package embeddings provides ready-to-use ltm.Embedder
// implementations for the major providers.
//
// Three constructors share one HTTP kernel:
//
//   - NewOpenAICompatible: the /embeddings endpoint spoken by OpenAI,
//     DeepSeek, GLM, and local ollama/vLLM servers (Bearer auth)
//   - NewAnthropic: Anthropic's Voyage-powered embeddings preview
//     (x-api-key + anthropic-version auth)
//   - NewGemini: Google's :embedContent endpoint (x-goog-api-key auth)
//
// The request/response shapes of the first two are nearly identical;
// only authentication differs, which is why they share a kernel. Error
// messages always carry a snippet of the provider's response body, so
// a schema drift shows up in the error instead of as a bare 400.
package embeddings

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client embeds text through one provider.
//
// It implements ltm.Embedder and is safe for concurrent use.
type Client struct {
	flavor     flavor
	httpClient *http.Client
	endpoint   string
	model      string
	// decorate sets provider-specific auth headers on each request.
	decorate func(*http.Request)
	// body builds the request payload; factored per provider shape.
	body func(model, text string) any
	// parse extracts the vector from the response body.
	parse func(data []byte) ([]float32, error)
}

// defaultTimeout bounds one embedding call. Embedding is a setup-cost
// operation on the memory path, not the chat path, so a generous
// timeout is fine — but unbounded is not.
const defaultTimeout = 30 * time.Second

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient injects a custom HTTP client (tests use this to point
// at an httptest server; production may want custom pooling or proxy
// settings).
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithBaseURL overrides the provider's default API root. Anthropic and
// Gemini variants use it for gateways and local proxies; the
// OpenAI-compatible constructor already takes baseURL explicitly.
func WithBaseURL(base string) Option {
	return func(c *Client) {
		if base != "" {
			c.endpoint = strings.TrimRight(base, "/") + c.endpointSuffix()
		}
	}
}

// endpointSuffix is the path the provider serves embeddings at.
func (c *Client) endpointSuffix() string {
	switch c.flavor {
	case flavorAnthropic:
		return "/v1/embeddings"
	case flavorGemini:
		return "/v1beta/models/" + c.model + ":embedContent"
	default:
		return "/embeddings"
	}
}

// flavor identifies the wire protocol variant.
type flavor int

const (
	flavorOpenAI flavor = iota
	flavorAnthropic
	flavorGemini
)

// NewOpenAICompatible builds an embedder for any /embeddings endpoint
// using Bearer authentication: OpenAI, DeepSeek, GLM, ollama, vLLM…
//
// baseURL is the API root (e.g. "https://api.openai.com/v1"); model is
// the embedding model id (e.g. "text-embedding-3-small").
func NewOpenAICompatible(baseURL, apiKey, model string, opts ...Option) *Client {
	c := &Client{
		flavor:   flavorOpenAI,
		endpoint: strings.TrimRight(baseURL, "/") + "/embeddings",
		model:    model,
		body:     openAIBody,
		parse:    parseOpenAI,
	}
	c.decorate = bearerAuth(apiKey)
	c.httpClient = &http.Client{Timeout: defaultTimeout}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// NewAnthropic builds an embedder for Anthropic's embeddings API
// (Voyage-powered, preview).
//
// The wire shape matches the OpenAI /embeddings format; the difference
// is authentication (x-api-key + anthropic-version headers). Because
// the API is a preview, keep the model name configurable (e.g.
// "voyage-3-large") and expect the endpoint contract to evolve.
func NewAnthropic(apiKey, model string, opts ...Option) *Client {
	c := &Client{
		flavor: flavorAnthropic,
		model:  model,
		body:   openAIBody,
		parse:  parseOpenAI,
	}
	c.endpoint = "https://api.anthropic.com" + c.endpointSuffix()
	c.decorate = anthropicAuth(apiKey)
	c.httpClient = &http.Client{Timeout: defaultTimeout}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// NewGemini builds an embedder for Google's embedContent endpoint.
//
// model is an embedding-capable model id (e.g.
// "gemini-embedding-001"); the request and response shapes are
// Gemini-specific and handled by this constructor.
func NewGemini(apiKey, model string, opts ...Option) *Client {
	c := &Client{
		flavor: flavorGemini,
		model:  model,
		body:   geminiBody,
		parse:  parseGemini,
	}
	c.endpoint = "https://generativelanguage.googleapis.com" + c.endpointSuffix()
	c.decorate = googleAuth(apiKey)
	c.httpClient = &http.Client{Timeout: defaultTimeout}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Embed implements ltm.Embedder.
func (c *Client) Embed(ctx context.Context, text string) ([]float32, error) {
	if strings.TrimSpace(text) == "" {
		return nil, nil // nothing to vectorize; callers treat nil as absent
	}
	payload, err := json.Marshal(c.body(c.model, text))
	if err != nil {
		return nil, fmt.Errorf("embeddings: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("embeddings: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	c.decorate(req)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embeddings: call %s: %w", c.endpoint, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("embeddings: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("embeddings: %s: status %d: %s",
			c.endpoint, resp.StatusCode, snippet(data))
	}
	vec, err := c.parse(data)
	if err != nil {
		// A parse failure on a 200 is the signature of a schema drift
		// (provider changed the payload shape); surface the body so
		// the mismatch is diagnosable without a packet capture.
		return nil, fmt.Errorf("embeddings: parse response: %w (body: %s)", err, snippet(data))
	}
	return vec, nil
}

// bearerAuth sets OpenAI-style headers.
func bearerAuth(apiKey string) func(*http.Request) {
	return func(r *http.Request) {
		r.Header.Set("Authorization", "Bearer "+apiKey)
	}
}

// anthropicAuth sets Anthropic's header pair.
func anthropicAuth(apiKey string) func(*http.Request) {
	return func(r *http.Request) {
		r.Header.Set("x-api-key", apiKey)
		r.Header.Set("anthropic-version", "2023-06-01")
	}
}

// googleAuth sets Google's API key header.
func googleAuth(apiKey string) func(*http.Request) {
	return func(r *http.Request) {
		r.Header.Set("x-goog-api-key", apiKey)
	}
}

// openAIRequest is the shared {model, input} payload.
type openAIRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

// openAIBody builds the shared OpenAI-shaped payload.
func openAIBody(model, text string) any {
	return openAIRequest{Model: model, Input: []string{text}}
}

// geminiRequest is Gemini's payload shape.
type geminiRequest struct {
	Content struct {
		Parts []struct {
			Text string `json:"text"`
		} `json:"parts"`
	} `json:"content"`
}

// geminiBody builds the Gemini-shaped payload.
func geminiBody(model, text string) any {
	var req geminiRequest
	req.Content.Parts = append(req.Content.Parts, struct {
		Text string `json:"text"`
	}{Text: text})
	return req
}

// parseOpenAI reads {"data":[{"embedding":[...]}]}.
func parseOpenAI(data []byte) ([]float32, error) {
	var resp struct {
		Data []struct {
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil && resp.Error.Message != "" {
		return nil, fmt.Errorf("provider error: %s", resp.Error.Message)
	}
	if len(resp.Data) == 0 {
		return nil, fmt.Errorf("no embedding in response")
	}
	return resp.Data[0].Embedding, nil
}

// parseGemini reads {"embedding":{"values":[...]}}.
func parseGemini(data []byte) ([]float32, error) {
	var resp struct {
		Embedding struct {
			Values []float32 `json:"values"`
		} `json:"embedding"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != nil && resp.Error.Message != "" {
		return nil, fmt.Errorf("provider error: %s", resp.Error.Message)
	}
	if len(resp.Embedding.Values) == 0 {
		return nil, fmt.Errorf("no embedding values in response")
	}
	return resp.Embedding.Values, nil
}

// snippet trims a response body for error messages.
func snippet(data []byte) string {
	s := strings.TrimSpace(string(data))
	if len(s) > 256 {
		s = s[:256] + "…"
	}
	return s
}
