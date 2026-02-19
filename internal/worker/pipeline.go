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

const maxPipelineRetries = 3

// RunPipeline processes a single MRF URL: download → split → parse → cleanup.
//
// Decompression streams directly into jsplit via a FIFO (named pipe), so the full
// decompressed JSON never exists on disk. Peak disk usage = NDJSON output only,
// roughly equal to the decompressed size. This is critical for large files (50GB+
// compressed) where the decompressed data can exceed available storage.
//
// On failure (e.g. CDN throttling truncating the stream), the pipeline retries up to
// 3 times. The final attempt falls back to a file-based pipeline that downloads the
// full file to disk before splitting, which is more resilient to stream interruptions.
func RunPipeline(
	ctx context.Context,
	url string,
	targetNPIs map[int64]struct{},
	tmpDir string,
	tracker progress.Tracker,
) *PipelineResult {
	// Check if FIFOs are supported (they aren't on all platforms)
	testFifo := filepath.Join(tmpDir, fmt.Sprintf("fifo-probe-%d.fifo", os.Getpid()))
	fifoSupported := syscall.Mkfifo(testFifo, 0o600) == nil
	os.Remove(testFifo)

	var lastErr error
	for attempt := 1; attempt <= maxPipelineRetries; attempt++ {
		if ctx.Err() != nil {
			return &PipelineResult{URL: url, Err: ctx.Err()}
		}

		// Final attempt or no FIFO support: use file-based pipeline (more resilient)
		useFile := !fifoSupported || attempt == maxPipelineRetries

		splitDir, err := os.MkdirTemp(tmpDir, "split-*")
		if err != nil {
			return &PipelineResult{URL: url, Err: fmt.Errorf("creating split dir: %w", err)}
		}

		var result *PipelineResult
		if useFile {
			result = runPipelineWithFile(ctx, url, targetNPIs, tmpDir, splitDir, tracker)
		} else {
			result = runPipelineWithFIFO(ctx, url, targetNPIs, tmpDir, splitDir, tracker)
		}

		if result.Err == nil {
			// Success — splitDir cleanup is handled by the caller via defer in the sub-functions,
			// but we need to ensure it's cleaned up here since we created it.
			os.RemoveAll(splitDir)
			return result
		}

		// Clean up failed attempt
		os.RemoveAll(splitDir)
		lastErr = result.Err

		if ctx.Err() != nil {
			return result // context cancelled, don't retry
		}

		if attempt < maxPipelineRetries {
			delay := time.Duration(attempt) * 2 * time.Second
			tracker.SetStage(fmt.Sprintf("Retry %d/%d (in %s)", attempt, maxPipelineRetries-1, delay))
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return &PipelineResult{URL: url, Err: ctx.Err()}
			}
		}
	}

	return &PipelineResult{URL: url, Err: lastErr}
}

// runPipelineWithFIFO streams decompressed data through a FIFO into jsplit.
func runPipelineWithFIFO(
	ctx context.Context,
	url string,
	targetNPIs map[int64]struct{},
	tmpDir string,
	splitDir string,
	tracker progress.Tracker,
) *PipelineResult {
	result := &PipelineResult{URL: url}

	fifoPath := filepath.Join(tmpDir, fmt.Sprintf("stream-%d-%d.fifo", os.Getpid(), time.Now().UnixNano()))
	if err := syscall.Mkfifo(fifoPath, 0o600); err != nil {
		result.Err = fmt.Errorf("creating FIFO: %w", err)
		return result
	}
	defer os.Remove(fifoPath)

	tracker.SetStage("Downloading + Splitting")
	dlErrCh := make(chan error, 1)
	go func() {
		dlErrCh <- StreamDecompressToPath(ctx, url, fifoPath, func(downloaded, total int64) {
			tracker.SetProgress(downloaded, total)
		})
	}()

	splitResult, err := mrf.SplitFile(fifoPath, splitDir)

	// Always drain the download goroutine
	dlErr := <-dlErrCh

	if err != nil {
		result.Err = fmt.Errorf("split: %w", err)
		return result
	}
	if dlErr != nil {
		result.Err = fmt.Errorf("download: %w", dlErr)
		return result
	}

	return runParsePhases(ctx, result, splitResult, targetNPIs, url, tracker)
}

// runPipelineWithFile downloads the full decompressed file to disk before splitting.
// More resilient than FIFO streaming since the download completes fully before jsplit runs.
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
