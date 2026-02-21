package worker

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/progress"
)

// downloadAndParse downloads the URL, sets up the gzip reader pipeline, and
// runs StreamParse. Returns the StreamResult or an error.
func downloadAndParse(
	ctx context.Context,
	url string,
	targetNPIs map[int64]struct{},
	useStdGzip bool,
	tracker progress.Tracker,
	callbacks mrf.StreamCallbacks,
	emit func(mrf.RateResult),
	prebuilt *mrf.MatchedProviders,
) (*mrf.StreamResult, error) {
	resp, err := downloadHTTP(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("download: %w", err)
	}
	defer resp.Body.Close()

	progReader := &progressReader{
		reader:   resp.Body,
		total:    resp.ContentLength,
		callback: func(downloaded, total int64) { tracker.SetProgress(downloaded, total) },
	}
	countReader := &countingReader{reader: progReader}

	gzReader, err := newGzipReader(countReader, useStdGzip)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
	}
	defer gzReader.Close()

	sr, err := mrf.StreamParse(gzReader, targetNPIs, url, callbacks, emit, prebuilt)
	if err != nil {
		return nil, fmt.Errorf("stream parse: %w", err)
	}

	if resp.ContentLength > 0 && countReader.n != resp.ContentLength {
		return nil, fmt.Errorf("download truncated: got %d of %d compressed bytes", countReader.n, resp.ContentLength)
	}

	return sr, nil
}

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

	var refsScanned int64
	var codesScanned int64
	var mu sync.Mutex

	callbacks := mrf.StreamCallbacks{
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
	}
	emitFunc := func(r mrf.RateResult) {
		mu.Lock()
		result.Results = append(result.Results, r)
		n := int64(len(result.Results))
		mu.Unlock()
		tracker.SetCounter("rates_found", n)
	}

	streamResult, err := downloadAndParse(ctx, url, targetNPIs, useStdGzip, tracker, callbacks, emitFunc, nil)
	if err != nil {
		result.Err = err
		return result
	}

	if streamResult.NeedSecondPass {
		tracker.SetStage("Re-downloading for in_network")

		_, err = downloadAndParse(ctx, url, targetNPIs, useStdGzip, tracker, callbacks, emitFunc, streamResult.MatchedProviders)
		if err != nil {
			result.Err = fmt.Errorf("second pass: %w", err)
			return result
		}
	}

	if len(result.Results) > 0 {
		tracker.SetStage(fmt.Sprintf("Done (%d rates)", len(result.Results)))
	} else {
		tracker.SetStage("Done (no matches)")
	}

	return result
}
