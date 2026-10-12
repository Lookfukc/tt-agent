package test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Lookfukc/tt-agent/pkg/core"
	"github.com/Lookfukc/tt-agent/pkg/ltm"
	"github.com/Lookfukc/tt-agent/pkg/memory/snapshot"
	"github.com/Lookfukc/tt-agent/pkg/observer"
)

// TestPublicJSONContract pins the exact JSON shape of every public type
// that consumers serialize across process boundaries. It exists because
// a json-tag change compiles cleanly, passes every other test, and
// silently reshapes every consumer's API — the demo project hit this
// class of drift while hand-aligning test assertions (Fact used to
// marshal as "ID"/"Namespace" while Entry marshaled as "id").
//
// This test turns such a change into a loud failure with a diff,
// forcing a conscious decision: revert, or bump the major version.
//
// To change a shape deliberately: update the expected string here,
// understand you are breaking wire consumers, and say so in the
// release notes.
func TestPublicJSONContract(t *testing.T) {
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)

	cases := []struct {
		name string
		v    any
		want string
	}{
		{
			"ltm.Fact",
			ltm.Fact{ID: "f1", Namespace: "u1", Text: "likes Go",
				Metadata: map[string]string{"src": "learn"}, CreatedAt: ts, UpdatedAt: ts, Score: 0.5},
			`{"id":"f1","namespace":"u1","text":"likes Go","metadata":{"src":"learn"},` +
				`"created_at":"2026-01-02T03:04:05Z","updated_at":"2026-01-02T03:04:05Z","score":0.5}`,
		},
		{
			"ltm.Message",
			ltm.Message{Role: "user", Content: "hi"},
			`{"role":"user","content":"hi"}`,
		},
		{
			"snapshot.Entry",
			snapshot.Entry{ID: "s1", Label: "before", MessageCount: 4, CreatedAt: ts},
			`{"id":"s1","label":"before","message_count":4,"created_at":"2026-01-02T03:04:05Z"}`,
		},
		{
			"core.Message (full)",
			core.Message{
				Role: core.RoleAssistant, Content: "ans", Reasoning: "think",
				ToolCalls:    []core.ToolCall{{ID: "c1", Name: "calc", Arguments: `{"x":1}`}},
				FinishReason: core.FinishToolCalls,
			},
			`{"role":"assistant","content":"ans","tool_calls":` +
				`[{"id":"c1","name":"calc","arguments":"{\"x\":1}"}],` +
				`"reasoning":"think","finish_reason":"tool_calls"}`,
		},
		{
			"core.Message (minimal)",
			core.Message{Role: core.RoleUser, Content: "q"},
			`{"role":"user","content":"q"}`,
		},
		{
			"core.Usage",
			core.Usage{InputTokens: 10, OutputTokens: 20},
			`{"input_tokens":10,"output_tokens":20}`,
		},
		{
			"observer.MemoryEvent",
			observer.MemoryEvent{
				Kind: observer.EventMessagesAppended, Backend: "redis", Session: "s1", At: ts,
				Detail: map[string]any{"count": 1},
			},
			`{"kind":"messages_appended","backend":"redis","session":"s1",` +
				`"at":"2026-01-02T03:04:05Z","detail":{"count":1}}`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := json.Marshal(tc.v)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if string(got) != tc.want {
				t.Fatalf("JSON wire shape changed!\n  got:  %s\n  want: %s\n\n"+
					"This is a wire-contract change: either revert it, or update this "+
					"expectation AND bump the major version (consumers' APIs reshape silently).",
					got, tc.want)
			}
		})
	}
}
