package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

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
//
// Decompression streams directly into jsplit via a FIFO (named pipe), so the full
// decompressed JSON never exists on disk. Peak disk usage = NDJSON output only,
// roughly equal to the decompressed size. This is critical for large files (50GB+
// compressed) where the decompressed data can exceed available storage.
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

	// Create a FIFO for streaming decompression directly into jsplit.
	// This eliminates the intermediate decompressed file, halving peak disk usage.
	fifoPath := filepath.Join(tmpDir, fmt.Sprintf("stream-%d-%d.fifo", os.Getpid(), time.Now().UnixNano()))
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		// FIFO not supported — fall back to file-based pipeline
		return runPipelineWithFile(ctx, url, targetNPIs, tmpDir, splitDir, tracker)
	}
	defer os.Remove(fifoPath)

	// Start download + decompress to FIFO in background goroutine.
	// The goroutine blocks on os.OpenFile until jsplit opens the FIFO for reading.
	tracker.SetStage("Downloading + Splitting")
	dlErrCh := make(chan error, 1)
	go func() {
		dlErrCh <- StreamDecompressToPath(ctx, url, fifoPath, func(downloaded, total int64) {
			tracker.SetProgress(downloaded, total)
		})
	}()

	// Split JSON into NDJSON — reads from the FIFO as jsplit's input.
	// This call blocks until all data is read (download complete + EOF).
	splitResult, err := mrf.SplitFile(fifoPath, splitDir)
	if err != nil {
		result.Err = fmt.Errorf("split: %w", err)
		return result
	}

	// Wait for the download goroutine to finish (should already be done when jsplit returns)
	if dlErr := <-dlErrCh; dlErr != nil {
		result.Err = fmt.Errorf("download: %w", dlErr)
		return result
	}

	// Parse phases A & B
	return runParsePhases(ctx, result, splitResult, targetNPIs, url, tracker)
}

// runPipelineWithFile is the fallback pipeline that writes the decompressed file to disk
// before splitting. Used when FIFOs are not available.
func runPipelineWithFile(
	ctx context.Context,
	url string,
	targetNPIs map[int64]struct{},
	tmpDir string,
	splitDir string,
	tracker progress.Tracker,
) *PipelineResult {
	result := &PipelineResult{URL: url}

	tracker.SetStage("Downloading")
	dlResult, err := DownloadAndDecompress(ctx, url, tmpDir, func(downloaded, total int64) {
		tracker.SetProgress(downloaded, total)
	})
	if err != nil {
		result.Err = fmt.Errorf("download: %w", err)
		return result
	}
	defer os.Remove(dlResult.FilePath)

	tracker.SetStage("Splitting")
	splitResult, err := mrf.SplitFile(dlResult.FilePath, splitDir)
	if err != nil {
		result.Err = fmt.Errorf("split: %w", err)
		return result
	}

	// Remove decompressed file immediately to free disk
	os.Remove(dlResult.FilePath)

	return runParsePhases(ctx, result, splitResult, targetNPIs, url, tracker)
}

// runParsePhases runs Phase A (provider_references) and Phase B (in_network) parsing.
func runParsePhases(
	ctx context.Context,
	result *PipelineResult,
	splitResult *mrf.SplitResult,
	targetNPIs map[int64]struct{},
	url string,
	tracker progress.Tracker,
) *PipelineResult {
	// Phase A — Parse provider references
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

	hasRefMatches := len(matchedProviders.ByGroupID) > 0
	tracker.SetCounter("npi_matches", int64(len(matchedProviders.ByGroupID)))

	if !hasRefMatches && len(splitResult.InNetworkFiles) == 0 {
		tracker.SetStage("Done (no matches)")
		return result
	}

	// Phase B — Parse in_network rates
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
