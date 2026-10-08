// Package main starts the Agent framework HTTP server.
package main

import (
	"context"
	"errors"
	"flag"
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
	"github.com/Lookfukc/tt-agent/pkg/entry"
	"github.com/Lookfukc/tt-agent/pkg/memory"
	"github.com/Lookfukc/tt-agent/pkg/tools"
	"github.com/Lookfukc/tt-agent/pkg/tools/builtin"
)

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
	memDir := flag.String("memory-dir", "", "session memory dir (default: shared temp dir, restart-safe on same machine)")
	memMaxLoaded := flag.Int("memory-max-loaded", 1024, "max sessions resident in memory (LRU evict beyond)")
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

	// Session memory: an explicit directory wins; without one, persist
	// to a temp dir (recoverable across restarts on the same machine)
	// rather than falling back to process memory. For multi-instance
	// deployments, configure a dedicated dir per instance or swap in an
	// external storage implementation.
	memOpts := []entry.Option{}
	if *memDir != "" {
		mem, err := memory.NewPersistentWithLRU(*memDir, nil, *memMaxLoaded)
		if err != nil {
			log.Fatalf("init memory dir: %v", err)
		}
		memOpts = append(memOpts, entry.WithMemory(mem))
		log.Printf("session memory dir: %s (max loaded %d)", *memDir, *memMaxLoaded)
	} else {
		log.Printf("session memory dir: %s (default temp dir)", entry.DefaultMemoryDir())
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
