package worker

import (
	"context"
	"sync"

	"github.com/gyeh/npi-rates/internal/progress"
)

// Pool manages concurrent processing of MRF files.
type Pool struct {
	Workers    int
	TargetNPIs map[int64]struct{}
	TmpDir     string
	Progress   progress.Manager
}

// Run processes all URLs concurrently and returns all results.
func (p *Pool) Run(ctx context.Context, urls []string) []PipelineResult {
	results := make([]PipelineResult, len(urls))

	p.Progress.StartDiskMonitor(p.TmpDir)
	defer p.Progress.StopDiskMonitor()

	sem := make(chan struct{}, p.Workers)
	var wg sync.WaitGroup

	for i, url := range urls {
		wg.Add(1)
		go func(idx int, u string) {
			defer wg.Done()

			// Acquire a semaphore slot to limit concurrency to p.Workers.
			// If all slots are taken, this blocks until one frees up.
			select {
			case sem <- struct{}{}:
				// Slot acquired — proceed with pipeline.
			case <-ctx.Done():
				// Context cancelled while waiting — bail out early.
				results[idx] = PipelineResult{URL: u, Err: ctx.Err()}
				return
			}
			// Release the semaphore slot when this goroutine finishes.
			defer func() { <-sem }()

			tracker := p.Progress.NewTracker(idx, len(urls), FileNameFromURL(u))
			result := RunPipeline(ctx, u, p.TargetNPIs, p.TmpDir, tracker)
			results[idx] = *result
			tracker.Done()
		}(i, url)
	}

	wg.Wait()
	return results
}
