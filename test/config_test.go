package test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Lookfukc/send-agent/pkg/config"
)

func TestLoadProviders(t *testing.T) {
	raw := `{
  "providers": [
    {
      "id": "my-llm",
      "name": "Internal Gateway",
      "protocol": "openai",
      "base_url": "http://llm.internal:8000/v1",
      "api_key_env": "MY_LLM_API_KEY",
      "models": [
        {"id": "qwen3-72b", "capabilities": {"streaming": true, "tool_calls": true, "context_window": 131072}}
      ]
    }
  ]
}`
	path := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	providers, err := config.LoadProviders(path)
	if err != nil {
		t.Fatalf("LoadProviders: %v", err)
	}
	if len(providers) != 1 {
		t.Fatalf("providers = %d", len(providers))
	}
	p := providers[0]
	if p.ID != "my-llm" || p.DefaultModel != "qwen3-72b" {
		t.Errorf("provider = %+v", p)
	}
	if !p.SupportsModel("qwen3-72b") || p.SupportsModel("other") {
		t.Error("model lookup failed")
	}
}

func TestLoadProvidersValidation(t *testing.T) {
	cases := []string{
		`{"providers":[{"id":"x","protocol":"openai","base_url":"http://a","api_key_env":"K"}]}`,              // 无 models
		`{"providers":[{"id":"x","protocol":"openai","base_url":"http://a","models":[{"id":"m"}]}]}`,          // 无 api_key_env
		`{"providers":[{"protocol":"openai","base_url":"http://a","api_key_env":"K","models":[{"id":"m"}]}]}`, // 无 id
	}
	for i, raw := range cases {
		path := filepath.Join(t.TempDir(), "providers.json")
		if err := os.WriteFile(path, []byte(raw), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if _, err := config.LoadProviders(path); err == nil {
			t.Errorf("case %d: want validation error", i)
		}
	}
}
