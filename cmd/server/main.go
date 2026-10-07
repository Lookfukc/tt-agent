// Package main 启动 Agent 框架 HTTP 服务
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
	flag.Parse()

	// 厂商配置即 flag+env，多厂商场景写自己的 main 用注册表装配
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
	// 单厂商服务缺密钥必坏，装配期即报
	if err := registry.MustGet(*defaultProvider).LoadAPIKeyFromEnv(); err != nil {
		log.Fatalf("%v", err)
	}

	toolReg := tools.NewRegistry()
	toolReg.Register(builtin.NewCalculator())
	toolReg.Register(builtin.NewClock())
	// http_fetch 面向模型输出属 SSRF 面，默认不注册，显式开启且放行内网需双开关
	if *enableFetch {
		fetch := builtin.NewHTTPFetch()
		fetch.AllowPrivateNetwork = *allowPrivate
		toolReg.Register(fetch)
	}

	srv := entry.NewServer(registry, toolReg,
		entry.WithConfig(entry.Config{
			Addr:            *addr,
			DefaultProvider: *defaultProvider,
			SystemPrompt:    *prompt,
			MaxIterations:   16,
			TokenBudget:     32_000,
		}),
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

	// 退出信号给在途流式响应 10s 排空窗口
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	log.Println("shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
	// 劫持的 WS 连接不在 Shutdown 管辖内，进程退出前显式关闭
	srv.CloseWebSockets()
	if gs != nil {
		// GracefulStop 排空在途流；窗口内未完成则 Stop 强断
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
