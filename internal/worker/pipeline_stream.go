package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/progress"
)

// runPipelineStreaming processes a single MRF URL by streaming directly from
// HTTP → gzip → json.Decoder → parse with zero intermediate files on disk.
// Memory usage is bounded to one JSON array element at a time.
func runPipelineStreaming(
	ctx context.Context,
	url string,
	targetNPIs map[int64]struct{},
	useStdGzip bool,
	tracker progress.Tracker,
) *PipelineResult {
	result := &PipelineResult{URL: url}

	tracker.SetStage("Streaming")

	resp, err := downloadHTTP(ctx, url)
	if err != nil {
		result.Err = fmt.Errorf("download: %w", err)
		return result
	}
	defer resp.Body.Close()

	totalBytes := resp.ContentLength

	// Wrap body: progress → counting → gzip → StreamParse
	progReader := &progressReader{
		reader:   resp.Body,
		total:    totalBytes,
		callback: func(downloaded, total int64) { tracker.SetProgress(downloaded, total) },
	}
	countReader := &countingReader{reader: progReader}

	gzReader, err := newGzipReader(countReader, useStdGzip)
	if err != nil {
		result.Err = fmt.Errorf("gzip reader: %w", err)
		return result
	}
	defer gzReader.Close()

	var refsScanned int64
	var codesScanned int64
	var mu sync.Mutex

	err = mrf.StreamParse(
		gzReader,
		targetNPIs,
		url,
		mrf.StreamCallbacks{
			OnRefScanned: func() {
				atomic.AddInt64(&refsScanned, 1)
				tracker.SetCounter("refs_scanned", atomic.LoadInt64(&refsScanned))
			},
			OnCodeScanned: func() {
				atomic.AddInt64(&codesScanned, 1)
				tracker.SetCounter("codes_scanned", atomic.LoadInt64(&codesScanned))
			},
			OnStageChange: func(stage string) {
				tracker.SetStage(stage)
			},
			OnWarning: func(msg string) {
				tracker.LogWarning(msg)
			},
		},
		func(r mrf.RateResult) {
			mu.Lock()
			result.Results = append(result.Results, r)
			n := int64(len(result.Results))
			mu.Unlock()
			tracker.SetCounter("rates_found", n)
		},
	)
	if err != nil {
		result.Err = fmt.Errorf("stream parse: %w", err)
		return result
	}

	// Verify the full compressed payload was received.
	if totalBytes > 0 && countReader.n != totalBytes {
		result.Err = fmt.Errorf("download truncated: got %d of %d compressed bytes", countReader.n, totalBytes)
		return result
	}

	if len(result.Results) > 0 {
		tracker.SetStage(fmt.Sprintf("Done (%d rates)", len(result.Results)))
	} else {
		tracker.SetStage("Done (no matches)")
	}

	return result
}
