package entry

import (
	"context"
	"errors"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/Lookfukc/send-agent/pkg/adapters/provider"
	"github.com/Lookfukc/send-agent/pkg/agent"
	"github.com/Lookfukc/send-agent/pkg/core"
)

// AgentServiceServer gRPC 服务端实现接口
type AgentServiceServer interface {
	// Chat 一元对话
	Chat(ctx context.Context, req *dynamicpb.Message) (*dynamicpb.Message, error)
	// ChatStream 服务端流对话
	ChatStream(req *dynamicpb.Message, stream grpc.ServerStream) error
}

// grpcAgentService 把 entry.Server 适配为 gRPC 服务
type grpcAgentService struct {
	s *Server
}

// GRPCRegister 将 AgentService 注册进 gRPC 服务器
// gs: 目标 gRPC 服务器
func (s *Server) GRPCRegister(gs *grpc.Server) {
	gs.RegisterService(&grpc.ServiceDesc{
		ServiceName: grpcServiceName,
		HandlerType: (*AgentServiceServer)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Chat",
			Handler:    s.grpcChatHandler,
		}},
		Streams: []grpc.StreamDesc{{
			StreamName:    "ChatStream",
			Handler:       s.grpcStreamHandler,
			ServerStreams: true,
		}},
	}, &grpcAgentService{s})
}

// grpcChatHandler 一元方法分发
func (s *Server) grpcChatHandler(srv any, ctx context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
	in := dynamicpb.NewMessage(chatReqDesc)
	if err := dec(in); err != nil {
		return nil, err
	}
	return srv.(AgentServiceServer).Chat(ctx, in)
}

// grpcStreamHandler 流式方法分发
func (s *Server) grpcStreamHandler(srv any, stream grpc.ServerStream) error {
	in := dynamicpb.NewMessage(chatReqDesc)
	if err := stream.RecvMsg(in); err != nil {
		return err
	}
	return srv.(AgentServiceServer).ChatStream(in, stream)
}

// Chat 实现一元对话
func (g *grpcAgentService) Chat(ctx context.Context, req *dynamicpb.Message) (*dynamicpb.Message, error) {
	cfg, model, llm, body, err := g.s.resolveGRPC(req)
	if err != nil {
		return grpcErrorEvent(err), nil
	}
	loop := g.s.loop(llm, model, body.SystemPrompt, nil)
	msg, usage, runErr := loop.Run(ctx, body.SessionID, body.Input)
	g.s.metrics.Record(cfg.ID, usage, runErr)
	if runErr != nil && !errors.Is(runErr, agent.ErrMaxIterations) {
		out := dynamicpb.NewMessage(chatRespDesc)
		setDynStr(out, "event", "error")
		setDynStr(out, "error", runErr.Error())
		return out, nil
	}

	out := dynamicpb.NewMessage(chatRespDesc)
	setDynStr(out, "event", "done")
	setDynStr(out, "content", msg.Content)
	setDynInt(out, "input_tokens", usage.InputTokens)
	setDynInt(out, "output_tokens", usage.OutputTokens)
	setDynInt(out, "reasoning_tokens", usage.ReasoningTokens)
	setDynFloat(out, "cost_usd", g.s.costOf(cfg, model, usage))
	return out, nil
}

// ChatStream 实现服务端流对话
func (g *grpcAgentService) ChatStream(req *dynamicpb.Message, stream grpc.ServerStream) error {
	ctx := stream.Context()
	cfg, model, llm, body, err := g.s.resolveGRPC(req)
	if err != nil {
		return stream.SendMsg(grpcErrorEvent(err))
	}

	onEvent := func(e agent.LoopEvent) {
		if ctx.Err() != nil {
			return
		}
		out := dynamicpb.NewMessage(chatRespDesc)
		switch e.Type {
		case agent.EventDeltaText:
			setDynStr(out, "event", "text")
			setDynStr(out, "delta", e.Text)
		case agent.EventDeltaReasoning:
			setDynStr(out, "event", "reasoning")
			setDynStr(out, "delta", e.Reasoning)
		case agent.EventToolCall:
			setDynStr(out, "event", "tool_call")
			setDynStr(out, "delta", e.Call.Name)
		case agent.EventToolResult:
			setDynStr(out, "event", "tool_result")
			setDynStr(out, "delta", e.Call.Name)
			if e.Err != nil {
				setDynStr(out, "error", e.Err.Error())
			}
		default:
			return
		}
		// 发送失败只记录：ctx 取消后循环自身会退出
		_ = stream.SendMsg(out)
	}

	loop := g.s.loop(llm, model, body.SystemPrompt, onEvent)
	msg, usage, runErr := loop.Run(ctx, body.SessionID, body.Input)
	g.s.metrics.Record(cfg.ID, usage, runErr)

	out := dynamicpb.NewMessage(chatRespDesc)
	setDynStr(out, "event", "done")
	setDynStr(out, "content", msg.Content)
	setDynInt(out, "input_tokens", usage.InputTokens)
	setDynInt(out, "output_tokens", usage.OutputTokens)
	setDynInt(out, "reasoning_tokens", usage.ReasoningTokens)
	setDynFloat(out, "cost_usd", g.s.costOf(cfg, model, usage))
	if runErr != nil {
		setDynStr(out, "error", runErr.Error())
	}
	return stream.SendMsg(out)
}

// resolveGRPC 从动态请求解析对话参数
// returns: 提供商配置、模型、LLM、请求体或错误
func (s *Server) resolveGRPC(req *dynamicpb.Message) (*provider.ProviderConfig, string, core.LLM, ChatBody, error) {
	body := ChatBody{
		SessionID:    dynStr(req, "session_id"),
		ProviderID:   dynStr(req, "provider_id"),
		Model:        dynStr(req, "model"),
		Input:        dynStr(req, "input"),
		SystemPrompt: dynStr(req, "system_prompt"),
	}
	if body.Input == "" {
		return nil, "", nil, body, errors.New("input is required")
	}
	if body.SessionID == "" {
		body.SessionID = "default"
	}
	cfg, model, err := s.resolveModel(body.ProviderID, body.Model)
	if err != nil {
		return nil, "", nil, body, err
	}
	llm, err := s.llmFor(cfg.ID)
	if err != nil {
		return nil, "", nil, body, err
	}
	return cfg, model, llm, body, nil
}

// grpcErrorEvent 构造错误响应消息
// returns: event=error 的动态消息
func grpcErrorEvent(err error) *dynamicpb.Message {
	out := dynamicpb.NewMessage(chatRespDesc)
	setDynStr(out, "event", "error")
	setDynStr(out, "error", err.Error())
	return out
}
