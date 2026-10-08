package entry

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// gRPC service contract (equivalent .proto definition):
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
// The build environment is offline with no protoc available, so we use a
// runtime-constructed descriptor + dynamicpb instead of generated code;
// the messages contain only scalar types, need no WKT dependencies, and
// protodesc compiles standalone

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

// init initializes the runtime descriptor.
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
		// The descriptor is a compile-time constant; a construction
		// failure is a programming error, so panic to expose it fast.
		panic("entry: bad grpc descriptor: " + err.Error())
	}
	grpcFileDesc = parsed
	chatReqDesc = parsed.Messages().ByName("ChatRequest")
	chatRespDesc = parsed.Messages().ByName("ChatResponse")
}

// GRPCChatRequestDesc exposes the request message descriptor so clients
// without generated code can construct dynamic messages.
// returns: the MessageDescriptor of ChatRequest.
func GRPCChatRequestDesc() protoreflect.MessageDescriptor { return chatReqDesc }

// GRPCChatResponseDesc exposes the response message descriptor.
// returns: the MessageDescriptor of ChatResponse.
func GRPCChatResponseDesc() protoreflect.MessageDescriptor { return chatRespDesc }

// dynStr reads a string field from a dynamic message.
// msg: the dynamic message.
// name: the field name.
// returns: the field value, or an empty string if the field is absent.
func dynStr(msg *dynamicpb.Message, name string) string {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if f == nil || !msg.Has(f) {
		return ""
	}
	return msg.Get(f).String()
}

// setDynStr writes a string field of a dynamic message.
func setDynStr(msg *dynamicpb.Message, name, value string) {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if value != "" {
		msg.Set(f, protoreflect.ValueOfString(value))
	}
}

// setDynInt writes an integer field of a dynamic message.
func setDynInt(msg *dynamicpb.Message, name string, value int64) {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if value != 0 {
		msg.Set(f, protoreflect.ValueOfInt64(value))
	}
}

// setDynFloat writes a float field of a dynamic message.
func setDynFloat(msg *dynamicpb.Message, name string, value float64) {
	f := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if value != 0 {
		msg.Set(f, protoreflect.ValueOfFloat64(value))
	}
}
