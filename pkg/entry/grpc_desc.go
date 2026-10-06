package entry

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// gRPC 服务契约（等价 .proto 定义）：
//
// package agentframework;
// service AgentService {
//   rpc Chat(ChatRequest) returns (ChatResponse);
//   rpc ChatStream(ChatRequest) returns (stream ChatResponse);
// }
// message ChatRequest  { string session_id=1; string provider_id=2; string model=3;
//                        string input=4; string system_prompt=5; }
// message ChatResponse { string event=1; string delta=2; string content=3; string error=4;
//                        int64 input_tokens=5; int64 output_tokens=6; int64 reasoning_tokens=7;
//                        double cost_usd=8; }
//
// 环境离线无 protoc，用运行时构造的 descriptor + dynamicpb 替代生成代码；
// 消息只含标量类型，无需任何 WKT 依赖，protodesc 可独立编译

const (
	grpcServiceName = "agentframework.AgentService"
	grpcChatMethod  = "/agentframework.AgentService/Chat"
	grpcStreamPath  = "/agentframework.AgentService/ChatStream"
)

var (
	grpcFileDesc protoreflect.FileDescriptor
	chatReqDesc  protoreflect.MessageDescriptor
	chatRespDesc protoreflect.MessageDescriptor
)

// 初始化运行时 descriptor
func init() {
	str := func(s string) *string { return proto.String(s) }
	pbString := descriptorpb.FieldDescriptorProto_TYPE_STRING
	pbInt64 := descriptorpb.FieldDescriptorProto_TYPE_INT64
	pbDouble := descriptorpb.FieldDescriptorProto_TYPE_DOUBLE
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL

	fd := func(name string, num int32, typ descriptorpb.FieldDescriptorProto_Type) *descriptorpb.FieldDescriptorProto {
		return &descriptorpb.FieldDescriptorProto{
			Name:   str(name),
			Number: proto.Int32(num),
			Label:  optional.Enum(),
			Type:   typ.Enum(),
		}
	}

	file := &descriptorpb.FileDescriptorProto{
		Name:    str("agent_service.proto"),
		Package: str("agentframework"),
		Syntax:  str("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: str("ChatRequest"),
				Field: []*descriptorpb.FieldDescriptorProto{
					fd("session_id", 1, pbString),
					fd("provider_id", 2, pbString),
					fd("model", 3, pbString),
					fd("input", 4, pbString),
					fd("system_prompt", 5, pbString),
				},
			},
			{
				Name: str("ChatResponse"),
				Field: []*descriptorpb.FieldDescriptorProto{
					fd("event", 1, pbString),
					fd("delta", 2, pbString),
					fd("content", 3, pbString),
					fd("error", 4, pbString),
					fd("input_tokens", 5, pbInt64),
					fd("output_tokens", 6, pbInt64),
					fd("reasoning_tokens", 7, pbInt64),
					fd("cost_usd", 8, pbDouble),
				},
			},
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			{
				Name: str("AgentService"),
				Method: []*descriptorpb.MethodDescriptorProto{
					{
						Name:       str("Chat"),
						InputType:  str(".agentframework.ChatRequest"),
						OutputType: str(".agentframework.ChatResponse"),
					},
					{
						Name:            str("ChatStream"),
						InputType:       str(".agentframework.ChatRequest"),
						OutputType:      str(".agentframework.ChatResponse"),
						ServerStreaming: proto.Bool(true),
					},
				},
			},
		},
	}

	parsed, err := protodesc.NewFile(file, nil)
	if err != nil {
		// descriptor 是编译期常量，构造失败属编程错误，panic 快速暴露
		panic("entry: bad grpc descriptor: " + err.Error())
	}
	grpcFileDesc = parsed
	chatReqDesc = parsed.Messages().ByName("ChatRequest")
	chatRespDesc = parsed.Messages().ByName("ChatResponse")
}

// GRPCChatRequestDesc 暴露请求消息描述符，供无生成代码的客户端构造动态消息
// returns: ChatRequest 的 MessageDescriptor
func GRPCChatRequestDesc() protoreflect.MessageDescriptor { return chatReqDesc }

// GRPCChatResponseDesc 暴露响应消息描述符
// returns: ChatResponse 的 MessageDescriptor
func GRPCChatResponseDesc() protoreflect.MessageDescriptor { return chatRespDesc }

// dynStr 读动态消息字符串字段
// msg: 动态消息
// name: 字段名
// returns: 字段值，缺字段返回空串
func dynStr(msg *dynamicpb.Message, name string) string {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if f == nil || !msg.Has(f) {
		return ""
	}
	return msg.Get(f).String()
}

// setDynStr 写动态消息字符串字段
func setDynStr(msg *dynamicpb.Message, name, value string) {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if value != "" {
		msg.Set(f, protoreflect.ValueOfString(value))
	}
}

// setDynInt 写动态消息整数字段
func setDynInt(msg *dynamicpb.Message, name string, value int64) {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if value != 0 {
		msg.Set(f, protoreflect.ValueOfInt64(value))
	}
}

// setDynFloat 写动态消息浮点字段
func setDynFloat(msg *dynamicpb.Message, name string, value float64) {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if value != 0 {
		msg.Set(f, protoreflect.ValueOfFloat64(value))
	}
}
