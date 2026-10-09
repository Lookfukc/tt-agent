package ltm

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
)

// hashID returns a short stable hex digest of s.
func hashID(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:16])
}

// scoreCosine returns the cosine similarity of two vectors.
//
// Cosine (rather than dot product or Euclidean) is used because
// embedding magnitudes carry no meaning across providers, so only
// direction should matter. Mismatched lengths and zero vectors score
// 0 rather than panicking: a provider that changes its output
// dimension must degrade, not crash a live request.
func scoreCosine(a, b []float32) float64 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (sqrt(na) * sqrt(nb))
}

// sqrt is Newton's method, avoiding a math import for one call.
func sqrt(x float64) float64 {
	if x <= 0 {
		return 0
	}
	z := x
	for i := 0; i < 32; i++ {
		z -= (z*z - x) / (2 * z)
	}
	return z
}

// KeywordScore ranks a fact's text against a query by token overlap.
//
// It is exported so external Store implementations can offer the same
// keyword fallback the built-in store uses, keeping ranking behavior
// consistent across backends when no embedding is available.
func KeywordScore(query, text string) float64 {
	return scoreKeyword(query, text)
}

// scoreKeyword ranks a fact against a query by token overlap.
//
// This is the fallback when no embedder is configured or embedding
// failed. It is deliberately simple: for the common case of a query
// like "what food does the user like" against a handful of stored
// facts, token overlap finds the right one without infrastructure.
func scoreKeyword(query, text string) float64 {
	terms := tokenize(query)
	if len(terms) == 0 {
		return 0
	}
	haystack := " " + strings.ToLower(text) + " "
	var hits float64
	for _, term := range terms {
		if strings.Contains(haystack, " "+term+" ") {
			hits += 1
			continue
		}
		// Substring credit for CJK and compounds, where whitespace
		// tokenization does not split words.
		if strings.Contains(haystack, term) {
			hits += 0.5
		}
	}
	return hits / float64(len(terms))
}

// tokenize lowercases and splits on whitespace.
func tokenize(s string) []string {
	fields := strings.Fields(strings.ToLower(s))
	out := fields[:0]
	for _, f := range fields {
		if len(f) > 1 {
			out = append(out, f)
		}
	}
	return out
}

// ranked pairs a fact with its score for sorting.
type ranked struct {
	fact  Fact
	score float64
	// seq is the insertion order of the source fact, used only to break
	// score ties deterministically.
	seq uint64
}

// sortByScore orders results best-first.
//
// Ties break on insertion order (newest first) rather than on
// timestamps, because facts written in the same nanosecond would
// otherwise be ordered arbitrarily and recall results would shuffle
// between identical calls.
func sortByScore(items []ranked) {
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].score != items[j].score {
			return items[i].score > items[j].score
		}
		return items[i].seq > items[j].seq
	})
}
