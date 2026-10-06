package test

import (
	"context"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/send-agent/pkg/core"
)

const dataURI = "data:image/png;base64,aGVsbG8="

func TestOpenAIMultimodal(t *testing.T) {
	got, srv := probeBody(t, `{"choices":[{"message":{"content":"图里有字"}}]}`)
	defer srv.Close()

	p := protocol.NewOpenAI("t", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:    "m",
		Messages: []core.Message{core.UserImage("这是什么", dataURI)},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	content := got["messages"].([]any)[0].(map[string]any)["content"].([]any)
	if content[0].(map[string]any)["type"] != "text" {
		t.Error("first part should be text")
	}
	img := content[1].(map[string]any)
	if img["type"] != "image_url" {
		t.Errorf("second part = %v", img)
	}
	if img["image_url"].(map[string]any)["url"] != dataURI {
		t.Error("image url mismatch")
	}
}

func TestAnthropicMultimodal(t *testing.T) {
	got, srv := probeBody(t, `{"content":[{"type":"text","text":"ok"}]}`)
	defer srv.Close()

	p := protocol.NewAnthropic("a", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:    "m",
		Messages: []core.Message{core.UserImage("看图", dataURI)},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	blocks := got["messages"].([]any)[0].(map[string]any)["content"].([]any)
	img := blocks[1].(map[string]any)
	if img["type"] != "image" {
		t.Fatalf("block = %v", img)
	}
	src := img["source"].(map[string]any)
	if src["type"] != "base64" || src["media_type"] != "image/png" || src["data"] != "aGVsbG8=" {
		t.Errorf("source = %v", src)
	}
}

func TestAnthropicMultimodalURL(t *testing.T) {
	got, srv := probeBody(t, `{"content":[{"type":"text","text":"ok"}]}`)
	defer srv.Close()

	p := protocol.NewAnthropic("a", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:    "m",
		Messages: []core.Message{core.UserImage("看图", "https://x/img.png")},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	blocks := got["messages"].([]any)[0].(map[string]any)["content"].([]any)
	src := blocks[1].(map[string]any)["source"].(map[string]any)
	if src["type"] != "url" || src["url"] != "https://x/img.png" {
		t.Errorf("source = %v", src)
	}
}

func TestGeminiMultimodal(t *testing.T) {
	got, srv := probeBody(t, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}]}`)
	defer srv.Close()

	p := protocol.NewGemini("g", srv.URL, "k", protocol.Quirks{})
	_, err := p.Chat(context.Background(), core.ChatRequest{
		Model:    "m",
		Messages: []core.Message{core.UserImage("看图", dataURI)},
	})
	if err != nil {
		t.Fatalf("Chat: %v", err)
	}
	parts := got["contents"].([]any)[0].(map[string]any)["parts"].([]any)
	inline := parts[1].(map[string]any)["inlineData"].(map[string]any)
	if inline["mimeType"] != "image/png" || inline["data"] != "aGVsbG8=" {
		t.Errorf("inlineData = %v", inline)
	}
}

func TestImageDataParse(t *testing.T) {
	// 非法 Data URI 应回退 URL 语义
	p := core.ContentPart{Type: "image", ImageURL: "https://x/a.png"}
	if _, _, ok := p.ImageData(); ok {
		t.Error("http url is not a data uri")
	}
	p.ImageURL = "data:;base64,QUJD"
	mime, data, ok := p.ImageData()
	if !ok || data != "QUJD" || mime == "" {
		t.Errorf("parse = %q %q %v", mime, data, ok)
	}
}
