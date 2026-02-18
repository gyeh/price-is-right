package worker

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/progress"
)

// PipelineResult holds results from processing a single MRF file.
type PipelineResult struct {
	URL     string
	Results []mrf.RateResult
	Err     error
}

// RunPipeline processes a single MRF URL: download → split → parse → cleanup.
func RunPipeline(
	ctx context.Context,
	url string,
	targetNPIs map[int64]struct{},
	tmpDir string,
	tracker progress.Tracker,
) *PipelineResult {
	result := &PipelineResult{URL: url}

	// Create a temp directory for this file's split output
	splitDir, err := os.MkdirTemp(tmpDir, "split-*")
	if err != nil {
		result.Err = fmt.Errorf("creating split dir: %w", err)
		return result
	}
	defer os.RemoveAll(splitDir)

	// Step 1: Download and decompress
	tracker.SetStage("Downloading")
	dlResult, err := DownloadAndDecompress(ctx, url, tmpDir, func(downloaded, total int64) {
		tracker.SetProgress(downloaded, total)
	})
	if err != nil {
		result.Err = fmt.Errorf("download: %w", err)
		return result
	}
	defer os.Remove(dlResult.FilePath)

	// Step 2: Split JSON into NDJSON
	tracker.SetStage("Splitting")
	splitResult, err := mrf.SplitFile(dlResult.FilePath, splitDir)
	if err != nil {
		result.Err = fmt.Errorf("split: %w", err)
		return result
	}

	// Remove the decompressed JSON file immediately after splitting to save disk
	os.Remove(dlResult.FilePath)

	// Step 3: Phase A — Parse provider references
	tracker.SetStage("Parsing: provider_references")
	var refsScanned int64
	matchedProviders, err := mrf.ParseProviderReferences(
		splitResult.ProviderReferenceFiles,
		targetNPIs,
		func() {
			atomic.AddInt64(&refsScanned, 1)
			tracker.SetCounter("refs_scanned", atomic.LoadInt64(&refsScanned))
		},
	)
	if err != nil {
		result.Err = fmt.Errorf("parse provider_references: %w", err)
		return result
	}

	// Early termination: if no NPI matches in provider_references AND no in_network
	// files to check for inline provider_groups, we're done
	hasRefMatches := len(matchedProviders.ByGroupID) > 0
	tracker.SetCounter("npi_matches", int64(len(matchedProviders.ByGroupID)))

	if !hasRefMatches && len(splitResult.InNetworkFiles) == 0 {
		tracker.SetStage("Done (no matches)")
		return result
	}

	// Step 4: Phase B — Parse in_network rates
	tracker.SetStage("Parsing: in_network")
	var codesScanned int64
	var mu sync.Mutex

	err = mrf.ParseInNetwork(
		splitResult.InNetworkFiles,
		targetNPIs,
		matchedProviders,
		url,
		func() {
			atomic.AddInt64(&codesScanned, 1)
			tracker.SetCounter("codes_scanned", atomic.LoadInt64(&codesScanned))
		},
		func(r mrf.RateResult) {
			mu.Lock()
			result.Results = append(result.Results, r)
			mu.Unlock()
			tracker.SetCounter("rates_found", int64(len(result.Results)))
		},
	)
	if err != nil {
		result.Err = fmt.Errorf("parse in_network: %w", err)
		return result
	}

	if len(result.Results) > 0 {
		tracker.SetStage(fmt.Sprintf("Done (%d rates)", len(result.Results)))
	} else {
		tracker.SetStage("Done (no matches)")
	}

	return result
}
