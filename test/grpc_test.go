package test

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/entry"
)

// newGRPCPair 启动 bufconn 上的 gRPC 服务并返回客户端连接
// returns: 已就绪的 gRPC 客户端与清理函数
func newGRPCPair(t *testing.T, llm core.LLM) *grpc.ClientConn {
	t.Helper()
	srv := newTestServer(t, llm)

	listener := bufconn.Listen(64 * 1024)
	gs := grpc.NewServer()
	srv.GRPCRegister(gs)
	go gs.Serve(listener)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// grpcChatRequest 构造动态请求
// returns: 填充好的请求消息
func grpcChatRequest(t *testing.T, input string) *dynamicpb.Message {
	t.Helper()
	msg := dynamicpb.NewMessage(entry.GRPCChatRequestDesc())
	setField(t, msg, "input", input)
	setField(t, msg, "session_id", "grpc-1")
	setField(t, msg, "provider_id", "deepseek")
	return msg
}

// setField 写字符串字段
func setField(t *testing.T, msg *dynamicpb.Message, name, value string) {
	t.Helper()
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if f == nil {
		t.Fatalf("field %s not in descriptor", name)
	}
	msg.Set(f, protoreflect.ValueOfString(value))
}

// readField 读字符串字段
// returns: 字段值
func readField(msg *dynamicpb.Message, name string) string {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if !msg.Has(f) {
		return ""
	}
	return msg.Get(f).String()
}

func TestGRPCUnaryChat(t *testing.T) {
	conn := newGRPCPair(t, &streamMockLLM{})
	resp := dynamicpb.NewMessage(entry.GRPCChatResponseDesc())

	err := conn.Invoke(t.Context(), "/agentframework.AgentService/Chat",
		grpcChatRequest(t, "hi"), resp)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if readField(resp, "event") != "done" || readField(resp, "content") != "你好" {
		t.Errorf("resp event=%q content=%q", readField(resp, "event"), readField(resp, "content"))
	}
}

func TestGRPCStreamChat(t *testing.T) {
	conn := newGRPCPair(t, &streamMockLLM{})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{StreamName: "ChatStream", ServerStreams: true},
		"/agentframework.AgentService/ChatStream")
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}
	if err := stream.SendMsg(grpcChatRequest(t, "hi")); err != nil {
		t.Fatalf("SendMsg: %v", err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}

	var deltas []string
	done := false
	for i := 0; i < 32; i++ {
		resp := dynamicpb.NewMessage(entry.GRPCChatResponseDesc())
		if err := stream.RecvMsg(resp); err != nil {
			t.Fatalf("RecvMsg: %v", err)
		}
		switch readField(resp, "event") {
		case "text":
			deltas = append(deltas, readField(resp, "delta"))
		case "done":
			done = true
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("no done event")
	}
	if len(deltas) != 2 || deltas[0] != "你" || deltas[1] != "好" {
		t.Errorf("deltas = %v", deltas)
	}
}

func TestGRPCValidationError(t *testing.T) {
	conn := newGRPCPair(t, &streamMockLLM{})
	resp := dynamicpb.NewMessage(entry.GRPCChatResponseDesc())

	// 空 input 服务层返回 error 事件而非 RPC 错误
	err := conn.Invoke(t.Context(), "/agentframework.AgentService/Chat",
		grpcChatRequest(t, ""), resp)
	if err != nil {
		t.Fatalf("Invoke: %v", err)
	}
	if readField(resp, "event") != "error" {
		t.Errorf("event = %q, want error", readField(resp, "event"))
	}
}
