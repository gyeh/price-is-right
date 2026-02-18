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

	sem := make(chan struct{}, p.Workers)
	var wg sync.WaitGroup

	for i, url := range urls {
		wg.Add(1)
		go func(idx int, u string) {
			defer wg.Done()

			// Acquire semaphore
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				results[idx] = PipelineResult{URL: u, Err: ctx.Err()}
				return
			}
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
