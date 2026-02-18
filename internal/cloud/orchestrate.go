package cloud

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/output"
)

// CloudSearchConfig holds configuration for a cloud-based distributed search.
type CloudSearchConfig struct {
	URLs        []string
	NPIs        []int64
	OutputFile  string
	S3Bucket    string
	Region      string
	Subnets     []string
	URLsPerTask int
}

// RunCloudSearch distributes URL processing across Fargate tasks, monitors them,
// collects results from S3, and merges into a single output file.
func RunCloudSearch(ctx context.Context, cfg CloudSearchConfig) error {
	startTime := time.Now()

	// Create clients
	s3Client, err := NewS3Client(ctx, cfg.S3Bucket, cfg.Region)
	if err != nil {
		return fmt.Errorf("creating S3 client: %w", err)
	}

	orch, err := NewFargateOrchestrator(ctx, cfg.Region, cfg.S3Bucket, cfg.Subnets)
	if err != nil {
		return fmt.Errorf("creating Fargate orchestrator: %w", err)
	}

	logStreamer, err := NewLogStreamer(ctx, cfg.Region)
	if err != nil {
		return fmt.Errorf("creating log streamer: %w", err)
	}

	// Split URLs into chunks
	chunks := chunkURLs(cfg.URLs, cfg.URLsPerTask)
	fmt.Fprintf(os.Stderr, "Distributing %d URLs across %d Fargate tasks (%d URLs/task)\n\n",
		len(cfg.URLs), len(chunks), cfg.URLsPerTask)

	// Upload URL chunks to S3
	urlKeys := make([]string, len(chunks))
	for i, chunk := range chunks {
		key := fmt.Sprintf("urls/chunk-%03d.txt", i)
		data := []byte(strings.Join(chunk, "\n"))
		if err := s3Client.UploadBytes(ctx, key, data, "text/plain"); err != nil {
			return fmt.Errorf("uploading URL chunk %d: %w", i, err)
		}
		urlKeys[i] = key
	}
	fmt.Fprintf(os.Stderr, "Uploaded %d URL chunks to s3://%s/urls/\n", len(chunks), cfg.S3Bucket)

	// Launch Fargate tasks
	taskArns := make([]string, 0, len(chunks))
	resultKeys := make([]string, len(chunks))

	// Ensure cleanup on exit — stop running tasks and delete URL chunks
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cleanupCancel()

		// Stop any still-running tasks
		if len(taskArns) > 0 {
			tasks, descErr := orch.DescribeTasks(cleanupCtx, taskArns)
			if descErr == nil {
				var running []string
				for _, t := range tasks {
					if aws.ToString(t.LastStatus) != "STOPPED" {
						running = append(running, aws.ToString(t.TaskArn))
					}
				}
				if len(running) > 0 {
					fmt.Fprintf(os.Stderr, "\nStopping %d running Fargate tasks...\n", len(running))
					errs := orch.StopAllTasks(cleanupCtx, running)
					for _, e := range errs {
						fmt.Fprintf(os.Stderr, "  Warning: %v\n", e)
					}
				}
			}
		}

		// Clean up URL chunks from S3
		for _, key := range urlKeys {
			_ = s3Client.DeleteObject(cleanupCtx, key)
		}
	}()

	for i, chunk := range chunks {
		resultKey := fmt.Sprintf("results/task-%03d.json", i)
		resultKeys[i] = resultKey

		arn, err := orch.LaunchTask(ctx, TaskInput{
			URLsS3Key: urlKeys[i],
			NPIs:      cfg.NPIs,
			TaskIndex: i,
			OutputKey: resultKey,
		})
		if err != nil {
			return fmt.Errorf("launching task %d: %w", i, err)
		}
		taskArns = append(taskArns, arn)
		fmt.Fprintf(os.Stderr, "  Launched task %d/%d: %s (%d URLs)\n",
			i+1, len(chunks), TaskIDFromARN(arn), len(chunk))
	}
	fmt.Fprintf(os.Stderr, "\n")

	// Start log streaming for all tasks
	logCtx, logCancel := context.WithCancel(ctx)
	defer logCancel()

	for i, arn := range taskArns {
		taskIdx := i
		taskARN := arn
		go logStreamer.StreamLogs(logCtx, taskARN, func(line string) {
			fmt.Fprintf(os.Stderr, "[task-%03d] %s\n", taskIdx, line)
		})
	}

	// Wait for all tasks to complete
	fmt.Fprintf(os.Stderr, "Waiting for %d tasks to complete...\n", len(taskArns))
	taskResults, err := orch.WaitForTasks(ctx, taskArns, func(running, pending, stopped int) {
		fmt.Fprintf(os.Stderr, "  Tasks: %d running, %d pending, %d stopped (of %d total)\n",
			running, pending, stopped, len(taskArns))
	})
	if err != nil {
		return fmt.Errorf("waiting for tasks: %w", err)
	}

	// Stop log streaming
	logCancel()

	// Report task results
	succeeded, failed := 0, 0
	for _, tr := range taskResults {
		if tr.Success {
			succeeded++
		} else {
			failed++
			fmt.Fprintf(os.Stderr, "  Task %s failed: exit code %d, reason: %s\n",
				TaskIDFromARN(tr.TaskArn), tr.ExitCode, tr.Reason)
		}
	}
	fmt.Fprintf(os.Stderr, "\nAll tasks finished: %d succeeded, %d failed\n", succeeded, failed)

	// Collect and merge results from successful tasks
	fmt.Fprintf(os.Stderr, "Collecting results from S3...\n")
	var allResults []mrf.RateResult
	matchedFiles := 0

	for i, tr := range taskResults {
		if !tr.Success {
			continue
		}

		searchOut, dlErr := s3Client.DownloadSearchOutput(ctx, resultKeys[i])
		if dlErr != nil {
			fmt.Fprintf(os.Stderr, "  Warning: failed to download results for task %d: %v\n", i, dlErr)
			continue
		}
		allResults = append(allResults, searchOut.Results...)
		matchedFiles += searchOut.SearchParams.MatchedFiles

		// Clean up result file from S3
		_ = s3Client.DeleteObject(ctx, resultKeys[i])
	}

	duration := time.Since(startTime)

	// Write merged output
	params := mrf.SearchParams{
		NPIs:            cfg.NPIs,
		SearchedFiles:   len(cfg.URLs),
		MatchedFiles:    matchedFiles,
		DurationSeconds: duration.Seconds(),
	}

	if err := output.WriteResults(cfg.OutputFile, params, allResults); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	fmt.Fprintf(os.Stderr, "\nCloud search complete: %d files searched, %d matched, %d rates found in %.1fs\n",
		len(cfg.URLs), matchedFiles, len(allResults), duration.Seconds())
	fmt.Fprintf(os.Stderr, "Results written to %s\n", cfg.OutputFile)

	return nil
}

func chunkURLs(urls []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(urls); i += chunkSize {
		end := i + chunkSize
		if end > len(urls) {
			end = len(urls)
		}
		chunks = append(chunks, urls[i:end])
	}
	return chunks
}
