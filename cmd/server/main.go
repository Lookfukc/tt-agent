// Package main starts the Agent framework HTTP server.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Lookfukc/tt-agent/pkg/adapters/provider"
	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/entry"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/memory/memorystore"
	"github.com/Lookfukc/tt-agent/pkg/memory/postgres"
	"github.com/Lookfukc/tt-agent/pkg/memory/redis"
	"github.com/Lookfukc/tt-agent/pkg/memory/sqlite"
	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

// memoryFlags carries the backend-specific flag values.
type memoryFlags struct {
	dir         string
	maxLoaded   int
	dsn         string
	redisPrefix string
	pgPoolMax   int
	ttl         time.Duration
	pruneEvery  time.Duration
}

// sweepLoop periodically invokes prune until stop is closed.
//
// SQLite and Postgres have no native expiry, so retention there is an
// explicit sweep; Redis needs none of this (its keys carry a TTL).
// Errors are logged and retried on the next tick: retention is
// best-effort and must never take the server down.
func sweepLoop(stop <-chan struct{}, every time.Duration, prune func(ctx context.Context) error) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			if err := prune(context.Background()); err != nil {
				log.Printf("memory prune: %v", err)
			}
		}
	}
}

// buildMemory constructs the session memory backend named by kind.
//
// It returns a nil memory (and nil closer) for the "file" backend when
// no directory is configured, which tells the server to use its own
// default. Every other backend must produce a memory or an error:
// silently falling back to a different store would send a user's data
// somewhere they did not ask for.
func buildMemory(kind string, f memoryFlags) (core.Memory, func(), error) {
	ctx := context.Background()
	switch kind {
	case "file":
		if f.dir == "" {
			log.Printf("session memory: file backend at default dir %s", entry.DefaultMemoryDir())
			return nil, nil, nil
		}
		mem, err := memory.NewPersistentWithLRU(f.dir, nil, f.maxLoaded)
		if err != nil {
			return nil, nil, err
		}
		log.Printf("session memory: file backend dir=%s maxLoaded=%d", f.dir, f.maxLoaded)
		return mem, nil, nil

	case "sqlite":
		if f.dsn == "" {
			return nil, nil, errors.New("--memory-dsn is required for the sqlite backend")
		}
		d, err := sqlite.New(f.dsn, sqlite.Options{})
		if err != nil {
			return nil, nil, err
		}
		if err := d.Migrate(ctx); err != nil {
			_ = d.Close()
			return nil, nil, err
		}
		log.Printf("session memory: sqlite backend dsn=%s", f.dsn)
		stopSweep := pruneSweeper(d.PruneIdleSessions, f)
		return d.Memory(memorystore.Options{}), func() { stopSweep(); _ = d.Close() }, nil

	case "redis":
		if f.dsn == "" {
			return nil, nil, errors.New("--memory-dsn is required for the redis backend (redis://host:port)")
		}
		d, err := redis.NewFromURL(ctx, f.dsn, redis.Options{
			Prefix:     f.redisPrefix,
			SessionTTL: f.ttl,
		})
		if err != nil {
			return nil, nil, err
		}
		log.Printf("session memory: redis backend ttl=%s prefix=%q", f.ttl, f.redisPrefix)
		return d.Memory(memorystore.Options{}), func() { _ = d.Close() }, nil

	case "postgres":
		if f.dsn == "" {
			return nil, nil, errors.New("--memory-dsn is required for the postgres backend")
		}
		d, err := postgres.NewFromURL(ctx, f.dsn, postgres.Options{})
		if err != nil {
			return nil, nil, err
		}
		if err := d.Migrate(ctx); err != nil {
			_ = d.Close()
			return nil, nil, err
		}
		log.Printf("session memory: postgres backend poolMax=%d", f.pgPoolMax)
		stopSweep := pruneSweeper(d.PruneIdleSessions, f)
		return d.Memory(memorystore.Options{}), func() { stopSweep(); _ = d.Close() }, nil

	default:
		return nil, nil, fmt.Errorf("unknown memory type %q (want file/sqlite/redis/postgres)", kind)
	}
}

// pruneSweeper starts a retention sweep for backends without native
// expiry and returns the function that stops it. When no --memory-ttl
// is configured there is nothing to sweep; the returned stop function
// is a no-op. Stopping the sweep never closes the driver — the caller
// composes both.
func pruneSweeper(prune func(ctx context.Context, idle time.Duration) (int64, error), f memoryFlags) func() {
	if f.ttl <= 0 {
		// No retention configured: nothing sweeps, data lives forever.
		// That is a legitimate choice; only note it so the operator knows.
		log.Printf("memory retention: --memory-ttl not set, sessions are kept indefinitely")
		return func() {}
	}
	every := f.pruneEvery
	if every <= 0 {
		every = 5 * time.Minute
	}
	stop := make(chan struct{})
	go sweepLoop(stop, every, func(ctx context.Context) error {
		n, err := prune(ctx, f.ttl)
		if err == nil && n > 0 {
			log.Printf("memory prune: removed %d idle sessions", n)
		}
		return err
	})
	return func() { close(stop) }
}

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	grpcAddr := flag.String("grpc", "", "gRPC listen address, empty to disable")
	defaultProvider := flag.String("provider", "main", "provider id label for routing/metrics")
	protocol := flag.String("protocol", "openai", "llm protocol: openai / anthropic / gemini")
	baseURL := flag.String("base-url", "", "provider api base url, e.g. https://api.deepseek.com/v1 (required)")
	apiKeyEnv := flag.String("api-key-env", "", "env var name holding the api key (required, key never lands on flags)")
	defaultModel := flag.String("model", "", "model id (required)")
	prompt := flag.String("prompt", "", "default system prompt")
	enableFetch := flag.Bool("enable-httpfetch", false, "register http_fetch tool (SSRF surface, off by default)")
	allowPrivate := flag.Bool("httpfetch-allow-private", false, "allow http_fetch to reach private networks")
	memType := flag.String("memory-type", "file", "session memory backend: file / sqlite / redis / postgres")
	memDir := flag.String("memory-dir", "", "file backend: session dir (default: shared temp dir)")
	memMaxLoaded := flag.Int("memory-max-loaded", 1024, "file backend: max sessions resident in memory (LRU evict beyond)")
	memDSN := flag.String("memory-dsn", "", "sqlite: database path; redis: redis:// URL; postgres: connection string")
	memRedisPrefix := flag.String("memory-redis-prefix", "", "redis: key prefix (default agent:mem:)")
	memPGPoolMax := flag.Int("memory-postgres-pool", 0, "postgres: max pool connections (0 = driver default)")
	memTTL := flag.Duration("memory-ttl", 0, "session retention: redis applies it natively; sqlite/postgres sweep it every --memory-prune-every (0 = keep forever)")
	memPruneEvery := flag.Duration("memory-prune-every", 5*time.Minute, "sqlite/postgres: how often the idle-session sweep runs")
	flag.Parse()

	// Vendor configuration is just flags+env; for multi-vendor setups
	// write your own main and assemble via the registry.
	if *baseURL == "" || *apiKeyEnv == "" || *defaultModel == "" {
		log.Fatalf("--base-url, --api-key-env and --model are required")
	}
	switch *protocol {
	case "openai", "anthropic", "gemini":
	default:
		log.Fatalf("unsupported protocol %q (want one of openai/anthropic/gemini)", *protocol)
	}
	u, err := url.Parse(*baseURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		log.Fatalf("--base-url must be absolute http(s) URL: %s", *baseURL)
	}
	registry := provider.NewRegistry()
	registry.Register(&provider.ProviderConfig{
		ID: *defaultProvider, Name: *defaultProvider, Protocol: *protocol,
		BaseURL: *baseURL, APIKeyEnv: *apiKeyEnv, DefaultModel: *defaultModel,
		Models: []provider.ModelConfig{{ID: *defaultModel}},
	})
	// A single-vendor server is definitely broken without the key;
	// report it at assembly time.
	if err := registry.MustGet(*defaultProvider).LoadAPIKeyFromEnv(); err != nil {
		log.Fatalf("%v", err)
	}

	toolReg := tools.NewRegistry()
	toolReg.Register(builtin.NewCalculator())
	toolReg.Register(builtin.NewClock())
	// http_fetch exposes an SSRF surface via model output; it is not
	// registered by default — enabling it and allowing private
	// networks requires the two separate switches.
	if *enableFetch {
		fetch := builtin.NewHTTPFetch()
		fetch.AllowPrivateNetwork = *allowPrivate
		toolReg.Register(fetch)
	}

	// Session memory: pick a backend. The file backend needs no
	// infrastructure; sqlite/redis/postgres let several processes or
	// instances share one session store.
	memOpts := []entry.Option{}
	mem, closeMem, err := buildMemory(*memType, memoryFlags{
		dir:         *memDir,
		maxLoaded:   *memMaxLoaded,
		dsn:         *memDSN,
		redisPrefix: *memRedisPrefix,
		pgPoolMax:   *memPGPoolMax,
		ttl:         *memTTL,
		pruneEvery:  *memPruneEvery,
	})
	if err != nil {
		log.Fatalf("init %s memory: %v", *memType, err)
	}
	if closeMem != nil {
		defer closeMem()
	}
	if mem != nil {
		memOpts = append(memOpts, entry.WithMemory(mem))
	}

	srv := entry.NewServer(registry, toolReg,
		append(memOpts, entry.WithConfig(entry.Config{
			Addr:            *addr,
			DefaultProvider: *defaultProvider,
			SystemPrompt:    *prompt,
			MaxIterations:   16,
			TokenBudget:     32_000,
		}))...,
	)

	httpSrv := &http.Server{
		Addr:              *addr,
		Handler:           srv.Handler(),
		ReadHeaderTimeout: 30 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", *addr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	var gs *grpc.Server
	if *grpcAddr != "" {
		gs = grpc.NewServer()
		srv.GRPCRegister(gs)
		gLis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			log.Fatalf("grpc listen: %v", err)
		}
		go func() {
			log.Printf("grpc listening on %s", *grpcAddr)
			_ = gs.Serve(gLis)
		}()
	}

	// On exit signals, give in-flight streaming responses a 10s
	// drain window.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	// Hijacked WS connections are outside Shutdown's purview; close
	// them explicitly before process exit.
	srv.CloseWebSockets()
	if gs != nil {
		// GracefulStop drains in-flight streams; if not done within
		// the window, Stop cuts them off hard.
		stopped := make(chan struct{})
		go func() {
			gs.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-ctx.Done():
			gs.Stop()
		}
	}
}
