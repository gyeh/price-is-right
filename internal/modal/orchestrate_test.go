package modal

import (
	"encoding/json"
	"testing"

	"github.com/gyeh/npi-rates/internal/mrf"
)

func TestShardURLs(t *testing.T) {
	urls := []string{"a", "b", "c", "d", "e"}

	// 5 URLs across 3 shards: round-robin
	shards := shardURLs(urls, 3)
	if len(shards) != 3 {
		t.Fatalf("expected 3 shards, got %d", len(shards))
	}
	want := [][]string{{"a", "d"}, {"b", "e"}, {"c"}}
	for i, s := range shards {
		if len(s) != len(want[i]) {
			t.Errorf("shard %d: got %v, want %v", i, s, want[i])
			continue
		}
		for j := range s {
			if s[j] != want[i][j] {
				t.Errorf("shard %d[%d]: got %q, want %q", i, j, s[j], want[i][j])
			}
		}
	}

	// More shards than URLs: capped to len(urls)
	shards = shardURLs(urls, 10)
	if len(shards) != 5 {
		t.Fatalf("expected 5 shards (capped), got %d", len(shards))
	}
	for i, s := range shards {
		if len(s) != 1 || s[0] != urls[i] {
			t.Errorf("shard %d: got %v, want [%s]", i, s, urls[i])
		}
	}

	// Single shard: all URLs in one
	shards = shardURLs(urls, 1)
	if len(shards) != 1 {
		t.Fatalf("expected 1 shard, got %d", len(shards))
	}
	if len(shards[0]) != 5 {
		t.Errorf("expected 5 urls in single shard, got %d", len(shards[0]))
	}

	// Empty input
	shards = shardURLs(nil, 3)
	if len(shards) != 0 {
		t.Errorf("expected 0 shards for empty input, got %d", len(shards))
	}

	// Zero shards requested: treated as 1
	shards = shardURLs(urls, 0)
	if len(shards) != 1 {
		t.Errorf("expected 1 shard for n=0, got %d", len(shards))
	}
}

func TestMergeResults(t *testing.T) {
	out1 := mrf.SearchOutput{
		SearchParams: mrf.SearchParams{
			NPIs:            []int64{1770671182},
			SearchedFiles:   5,
			MatchedFiles:    2,
			DurationSeconds: 10.5,
		},
		Results: []mrf.RateResult{
			{NPI: 1770671182, BillingCode: "99213", NegotiatedRate: 100.0},
		},
	}
	out2 := mrf.SearchOutput{
		SearchParams: mrf.SearchParams{
			NPIs:            []int64{1770671182},
			SearchedFiles:   3,
			MatchedFiles:    1,
			DurationSeconds: 15.2,
		},
		Results: []mrf.RateResult{
			{NPI: 1770671182, BillingCode: "99214", NegotiatedRate: 150.0},
			{NPI: 1770671182, BillingCode: "99215", NegotiatedRate: 200.0},
		},
	}

	data1, _ := json.Marshal(out1)
	data2, _ := json.Marshal(out2)

	merged, err := mergeResults([][]byte{data1, data2})
	if err != nil {
		t.Fatalf("mergeResults: %v", err)
	}

	if merged.SearchParams.SearchedFiles != 8 {
		t.Errorf("searched_files: got %d, want 8", merged.SearchParams.SearchedFiles)
	}
	if merged.SearchParams.MatchedFiles != 3 {
		t.Errorf("matched_files: got %d, want 3", merged.SearchParams.MatchedFiles)
	}
	if merged.SearchParams.DurationSeconds != 15.2 {
		t.Errorf("duration: got %f, want 15.2", merged.SearchParams.DurationSeconds)
	}
	if len(merged.Results) != 3 {
		t.Errorf("results: got %d, want 3", len(merged.Results))
	}
	if len(merged.SearchParams.NPIs) != 1 || merged.SearchParams.NPIs[0] != 1770671182 {
		t.Errorf("npis: got %v, want [1770671182]", merged.SearchParams.NPIs)
	}
}

func TestMergeResultsEmpty(t *testing.T) {
	merged, err := mergeResults(nil)
	if err != nil {
		t.Fatalf("mergeResults: %v", err)
	}
	if merged.Results == nil {
		t.Error("expected non-nil empty results slice")
	}
	if len(merged.Results) != 0 {
		t.Errorf("expected 0 results, got %d", len(merged.Results))
	}
}

func TestMergeResultsSkipsInvalidJSON(t *testing.T) {
	valid := mrf.SearchOutput{
		SearchParams: mrf.SearchParams{
			NPIs:          []int64{1234},
			SearchedFiles: 5,
			MatchedFiles:  2,
		},
		Results: []mrf.RateResult{
			{NPI: 1234, BillingCode: "99213"},
		},
	}
	validData, _ := json.Marshal(valid)

	merged, err := mergeResults([][]byte{[]byte("not json"), validData})
	if err != nil {
		t.Fatalf("mergeResults: %v", err)
	}
	if merged.SearchParams.SearchedFiles != 5 {
		t.Errorf("searched_files: got %d, want 5", merged.SearchParams.SearchedFiles)
	}
	if len(merged.Results) != 1 {
		t.Errorf("results: got %d, want 1", len(merged.Results))
	}
}
