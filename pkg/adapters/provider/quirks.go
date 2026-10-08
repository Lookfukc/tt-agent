package provider

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Lookfukc/tt-agent/pkg/adapters/protocol"
	"github.com/Lookfukc/tt-agent/pkg/core"
)

// quirkEntry is a named quirks entry: a patch function plus the protocols it applies to.
type quirkEntry struct {
	patch     protocol.PatchFunc
	protocols []string // empty means unrestricted
}

// quirksLibrary is the library of named quirks.
//
// Behavior corrections are a code concept that configuration files reference
// by name; when adding a vendor behavioral deviation, first evaluate whether it
// can go into the library for reuse rather than writing a one-off each time.
var quirksLibrary = map[string]quirkEntry{
	// GLM controls thinking mode via a top-level thinking field, which the standard OpenAI protocol does not have.
	"glm-thinking": {patch: glmThinkingPatch, protocols: []string{"openai"}},
	// reasoner-series models return an immediate 400 when given sampling parameters; they must be removed.
	"deepseek-reasoner": {patch: deepSeekReasonerPatch, protocols: []string{"openai"}},
}

// QuirkNames lists all available quirk names.
// returns: the sorted list of names.
func QuirkNames() []string {
	names := make([]string, 0, len(quirksLibrary))
	for n := range quirksLibrary {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// ComposeQuirks composes quirks by name.
// names: the list of names declared in configuration; an empty list returns the zero value.
// proto: the provider protocol, used to validate patch applicability.
// returns: the composed result, with multiple patches executed in declaration order; errors list available names when a name is unknown or the protocol does not match.
func ComposeQuirks(names []string, proto string) (protocol.Quirks, error) {
	var q protocol.Quirks
	var patches []protocol.PatchFunc
	for _, n := range names {
		entry, ok := quirksLibrary[n]
		if !ok {
			return q, fmt.Errorf("unknown quirk %q (available: %s)", n, strings.Join(QuirkNames(), ", "))
		}
		if len(entry.protocols) > 0 && !contains(entry.protocols, proto) {
			return q, fmt.Errorf("quirk %q applies to protocol %s, provider uses %q", n, strings.Join(entry.protocols, "/"), proto)
		}
		if entry.patch != nil {
			patches = append(patches, entry.patch)
		}
	}
	if len(patches) > 0 {
		q.PatchRequest = func(body map[string]any, req core.ChatRequest) {
			for _, p := range patches {
				p(body, req)
			}
		}
	}
	return q, nil
}

// contains reports whether the slice contains the string.
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// glmThinkingPatch translates the unified Thinking configuration into GLM's proprietary field.
func glmThinkingPatch(body map[string]any, req core.ChatRequest) {
	if req.Thinking == nil {
		return
	}
	if req.Thinking.Enabled {
		body["thinking"] = map[string]any{"type": "enabled"}
	} else {
		body["thinking"] = map[string]any{"type": "disabled"}
	}
}

// deepSeekReasonerPatch removes the sampling parameters that reasoner models do not accept.
func deepSeekReasonerPatch(body map[string]any, req core.ChatRequest) {
	if req.Model != "deepseek-reasoner" {
		return
	}
	delete(body, "temperature")
	delete(body, "top_p")
}
