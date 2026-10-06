// Package main 启动 Agent 框架 HTTP 服务
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/config"
	"github.com/Lookfukc/send-agent/pkg/entry"
	"github.com/Lookfukc/send-agent/pkg/tools"
	"github.com/Lookfukc/send-agent/pkg/tools/builtin"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	grpcAddr := flag.String("grpc", "", "gRPC listen address, empty to disable")
	defaultProvider := flag.String("provider", "deepseek", "default provider id")
	prompt := flag.String("prompt", "", "default system prompt")
	configPath := flag.String("config", "", "custom providers json file")
	enableFetch := flag.Bool("enable-httpfetch", false, "register http_fetch tool (SSRF surface, off by default)")
	allowPrivate := flag.Bool("httpfetch-allow-private", false, "allow http_fetch to reach private networks")
	flag.Parse()

	registry := provider.NewRegistry()

	// 自定义厂商配置文件存在则追加注册
	if *configPath != "" {
		if _, err := os.Stat(*configPath); err == nil {
			customs, err := config.LoadProviders(*configPath)
			if err != nil {
				log.Fatalf("load config: %v", err)
			}
			for i := range customs {
				registry.Register(&customs[i])
				log.Printf("registered custom provider: %s", customs[i].ID)
			}
		} else {
			log.Fatalf("config file not found: %s", *configPath)
		}
	}

	// 启动期绑定全部已设密钥，缺失的留在使用时报错
	for _, id := range registry.List() {
		cfg, _ := registry.Get(id)
		if err := cfg.LoadAPIKeyFromEnv(); err != nil {
			log.Printf("skip provider %s: %v", id, err)
		}
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
