package test

import (
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// errFake 测试用错误
func errFake() error { return errors.New("boom") }

func TestMetricsRecordAndSnapshot(t *testing.T) {
	m := observer.NewMetrics()
	m.Record("deepseek", core.Usage{InputTokens: 100, OutputTokens: 50}, nil)
	m.Record("deepseek", core.Usage{InputTokens: 200, ReasoningTokens: 10}, nil)
	m.Record("glm", core.Usage{}, errFake())

	snap := m.Snapshot()
	if snap["deepseek"].Calls != 2 || snap["deepseek"].InputTokens != 300 || snap["deepseek"].ReasoningTokens != 10 {
		t.Errorf("deepseek = %+v", snap["deepseek"])
	}
	if snap["glm"].Calls != 1 || snap["glm"].Errors != 1 {
		t.Errorf("glm = %+v", snap["glm"])
	}
	// 副本语义：改快照不影响内部
	delete(snap, "deepseek")
	if len(m.Snapshot()) != 2 {
		t.Error("snapshot must be a copy")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := newTestServer(t, &streamMockLLM{})
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	// 先打一次 chat 产生指标
	_, _ = ts.Client().Post(ts.URL+"/api/chat", "application/json",
		strings.NewReader(`{"input":"hi"}`))

	resp, err := ts.Client().Get(ts.URL + "/api/metrics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	var snap map[string]struct {
		Calls       int64 `json:"calls"`
		InputTokens int64 `json:"input_tokens"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap["deepseek"].Calls != 1 {
		t.Errorf("deepseek metrics = %+v", snap["deepseek"])
	}
}
