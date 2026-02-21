// Package modal implements distributed MRF search orchestration using deployed Modal functions.
// It shards URLs across parallel function calls, collects results, and merges them locally.
package modal

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	modalsdk "github.com/modal-labs/libmodal/modal-go"

	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/output"
)

// Config holds configuration for a Modal-based distributed search.
type Config struct {
	NPI             string
	URLs            []string
	OutputFile      string
	Shards          int
	WorkersPerShard int
}

type shardResult struct {
	index int
	data  []byte
	err   error
}

// RunSearch executes a distributed search using deployed Modal functions.
func RunSearch(ctx context.Context, cfg Config) error {
	shards := shardURLs(cfg.URLs, cfg.Shards)

	logf("NPI: %s", cfg.NPI)
	logf("Files: %d URLs across %d shards", len(cfg.URLs), len(shards))
	logf("Workers per shard: %d", cfg.WorkersPerShard)

	// Create Modal client
	client, err := modalsdk.NewClient()
	if err != nil {
		return fmt.Errorf("creating Modal client: %w", err)
	}
	defer client.Close()

	// Look up the deployed function and app
	fn, err := client.Functions.FromName(ctx, "npi-rates", "run_search", nil)
	if err != nil {
		return fmt.Errorf("looking up function: %w", err)
	}

	// Spawn all shards
	logf("Spawning %d shards...", len(shards))
	start := time.Now()

	calls := make([]*modalsdk.FunctionCall, len(shards))
	for i, urls := range shards {
		fc, err := fn.Spawn(ctx, []any{i, urls, cfg.NPI, cfg.WorkersPerShard}, nil)
		if err != nil {
			return fmt.Errorf("spawning shard %d: %w", i, err)
		}
		calls[i] = fc
	}

	// Collect results concurrently
	var completedCount atomic.Int32
	results := make([]shardResult, len(shards))
	var wg sync.WaitGroup
	for i, fc := range calls {
		wg.Add(1)
		go func(idx int, fc *modalsdk.FunctionCall) {
			defer wg.Done()
			result, err := fc.Get(ctx, nil)
			if err != nil {
				results[idx] = shardResult{index: idx, err: fmt.Errorf("shard %d: %w", idx, err)}
				n := completedCount.Add(1)
				logf("Shard %d/%d complete (error)", n, len(shards))
				return
			}
			data, ok := result.([]byte)
			if !ok {
				results[idx] = shardResult{index: idx, err: fmt.Errorf("shard %d: unexpected result type %T", idx, result)}
				n := completedCount.Add(1)
				logf("Shard %d/%d complete (error)", n, len(shards))
				return
			}
			results[idx] = shardResult{index: idx, data: data}
			n := completedCount.Add(1)
			logf("Shard %d/%d complete", n, len(shards))
		}(i, fc)
	}

	wg.Wait()
	wallTime := time.Since(start)

	// Collect successful results
	var successData [][]byte
	var failCount int
	for _, r := range results {
		if r.err != nil {
			logf("Shard %d failed: %v", r.index, r.err)
			failCount++
			continue
		}
		successData = append(successData, r.data)
	}

	if len(successData) == 0 {
		return fmt.Errorf("all %d shards failed", len(results))
	}

	merged, err := mergeResults(successData)
	if err != nil {
		return fmt.Errorf("merging results: %w", err)
	}
	merged.SearchParams.DurationSeconds = wallTime.Seconds()

	if err := output.WriteResults(cfg.OutputFile, merged.SearchParams, merged.Results); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	logf("Search complete: %d files searched, %d matched, %d rates found in %.1fs",
		merged.SearchParams.SearchedFiles,
		merged.SearchParams.MatchedFiles,
		len(merged.Results),
		wallTime.Seconds(),
	)
	if failCount > 0 {
		logf("Warning: %d/%d shards failed", failCount, len(results))
	}
	logf("Results saved to %s", cfg.OutputFile)

	return nil
}

func shardURLs(urls []string, n int) [][]string {
	if n <= 0 {
		n = 1
	}
	if n > len(urls) {
		n = len(urls)
	}
	shards := make([][]string, n)
	for i, url := range urls {
		shards[i%n] = append(shards[i%n], url)
	}
	var result [][]string
	for _, s := range shards {
		if len(s) > 0 {
			result = append(result, s)
		}
	}
	return result
}

func mergeResults(outputs [][]byte) (*mrf.SearchOutput, error) {
	var merged mrf.SearchOutput
	first := true

	for i, data := range outputs {
		var out mrf.SearchOutput
		if err := json.Unmarshal(data, &out); err != nil {
			logf("Warning: skipping shard output %d (%d bytes): %v", i, len(data), err)
			continue
		}
		if first {
			merged.SearchParams.NPIs = out.SearchParams.NPIs
			first = false
		}
		merged.SearchParams.SearchedFiles += out.SearchParams.SearchedFiles
		merged.SearchParams.MatchedFiles += out.SearchParams.MatchedFiles
		if out.SearchParams.DurationSeconds > merged.SearchParams.DurationSeconds {
			merged.SearchParams.DurationSeconds = out.SearchParams.DurationSeconds
		}
		merged.Results = append(merged.Results, out.Results...)
	}

	if merged.Results == nil {
		merged.Results = []mrf.RateResult{}
	}

	return &merged, nil
}

func logf(format string, args ...any) {
	ts := time.Now().Format("15:04:05")
	fmt.Fprintf(os.Stderr, "%s %s\n", ts, fmt.Sprintf(format, args...))
}
