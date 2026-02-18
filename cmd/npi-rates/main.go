package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/gyeh/npi-rates/internal/cloud"
	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/output"
	"github.com/gyeh/npi-rates/internal/progress"
	"github.com/gyeh/npi-rates/internal/worker"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "npi-rates",
		Short: "Search CMS Price Transparency MRF files for negotiated rates by NPI",
	}

	rootCmd.AddCommand(newSearchCmd())
	rootCmd.AddCommand(newCloudSetupCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newSearchCmd() *cobra.Command {
	var (
		urlsFile   string
		npiList    string
		outputFile string
		workers    int
		tmpDir     string
		noProgress bool

		// Cloud mode flags (orchestrator)
		cloudMode   bool
		s3Bucket    string
		region      string
		subnets     []string
		urlsPerTask int

		// Worker mode flags (used by Fargate tasks)
		urlsS3      string
		outputS3    string
		cloudRegion string
	)

	cmd := &cobra.Command{
		Use:   "search",
		Short: "Search MRF files for negotiated rates matching specified NPIs",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Parse NPIs
			npis, err := parseNPIs(npiList)
			if err != nil {
				return fmt.Errorf("parsing NPIs: %w", err)
			}
			if len(npis) == 0 {
				return fmt.Errorf("no NPIs specified")
			}

			// Handle signals
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				fmt.Fprintln(os.Stderr, "\nInterrupted, cleaning up...")
				cancel()
			}()

			// --- Cloud mode: distribute to Fargate ---
			if cloudMode {
				if urlsFile == "" {
					return fmt.Errorf("--urls-file is required for cloud mode")
				}
				if s3Bucket == "" {
					return fmt.Errorf("--s3-bucket is required for cloud mode")
				}
				if len(subnets) == 0 {
					return fmt.Errorf("--subnets is required for cloud mode")
				}

				urls, readErr := readURLs(urlsFile)
				if readErr != nil {
					return fmt.Errorf("reading URLs: %w", readErr)
				}
				if len(urls) == 0 {
					return fmt.Errorf("no URLs found in %s", urlsFile)
				}

				return cloud.RunCloudSearch(ctx, cloud.CloudSearchConfig{
					URLs:        urls,
					NPIs:        npis,
					OutputFile:  outputFile,
					S3Bucket:    s3Bucket,
					Region:      region,
					Subnets:     subnets,
					URLsPerTask: urlsPerTask,
				})
			}

			// --- Read URLs from file or S3 ---
			var urls []string
			if urlsS3 != "" {
				// Worker mode: download URL file from S3
				bucket, key, parseErr := cloud.ParseS3URI(urlsS3)
				if parseErr != nil {
					return fmt.Errorf("parsing --urls-s3: %w", parseErr)
				}
				s3Client, s3Err := cloud.NewS3Client(ctx, bucket, cloudRegion)
				if s3Err != nil {
					return fmt.Errorf("creating S3 client: %w", s3Err)
				}
				data, dlErr := s3Client.DownloadBytes(ctx, key)
				if dlErr != nil {
					return fmt.Errorf("downloading URLs from S3: %w", dlErr)
				}
				for _, line := range strings.Split(string(data), "\n") {
					line = strings.TrimSpace(line)
					if line == "" || strings.HasPrefix(line, "#") {
						continue
					}
					urls = append(urls, line)
				}
			} else if urlsFile != "" {
				urls, err = readURLs(urlsFile)
				if err != nil {
					return fmt.Errorf("reading URLs: %w", err)
				}
			} else {
				return fmt.Errorf("either --urls-file or --urls-s3 is required")
			}
			if len(urls) == 0 {
				return fmt.Errorf("no URLs found")
			}

			// Build NPI lookup set
			npiSet := make(map[int64]struct{}, len(npis))
			for _, n := range npis {
				npiSet[n] = struct{}{}
			}

			// Set up temp dir
			if tmpDir == "" {
				tmpDir = os.TempDir()
			}
			if err := os.MkdirAll(tmpDir, 0o755); err != nil {
				return fmt.Errorf("creating temp dir: %w", err)
			}

			// Set up progress
			var mgr progress.Manager
			if noProgress {
				mgr = &progress.NoopManager{}
			} else {
				mgr = progress.NewMPBManager()
			}

			// Log parser info
			fmt.Fprintf(os.Stderr, "Parser: %s\n", mrf.ParserName())
			fmt.Fprintf(os.Stderr, "Searching %d files for %d NPIs with %d workers\n\n", len(urls), len(npis), workers)

			// Run the worker pool
			startTime := time.Now()

			pool := &worker.Pool{
				Workers:    workers,
				TargetNPIs: npiSet,
				TmpDir:     tmpDir,
				Progress:   mgr,
			}

			results := pool.Run(ctx, urls)
			mgr.Wait()

			// Collect results
			var allRates []mrf.RateResult
			matchedFiles := 0
			for _, r := range results {
				if r.Err != nil {
					fmt.Fprintf(os.Stderr, "Error processing %s: %v\n", worker.FileNameFromURL(r.URL), r.Err)
					continue
				}
				if len(r.Results) > 0 {
					matchedFiles++
					allRates = append(allRates, r.Results...)
				}
			}

			duration := time.Since(startTime)

			// Write output
			params := mrf.SearchParams{
				NPIs:            npis,
				SearchedFiles:   len(urls),
				MatchedFiles:    matchedFiles,
				DurationSeconds: duration.Seconds(),
			}

			if err := output.WriteResults(outputFile, params, allRates); err != nil {
				return fmt.Errorf("writing output: %w", err)
			}

			fmt.Fprintf(os.Stderr, "\nSearch complete: %d files searched, %d matched, %d rates found in %.1fs\n",
				len(urls), matchedFiles, len(allRates), duration.Seconds())
			fmt.Fprintf(os.Stderr, "Results written to %s\n", outputFile)

			// Upload results to S3 if in worker mode
			if outputS3 != "" {
				bucket, key, parseErr := cloud.ParseS3URI(outputS3)
				if parseErr != nil {
					return fmt.Errorf("parsing --output-s3: %w", parseErr)
				}

				searchOut := mrf.SearchOutput{
					SearchParams: params,
					Results:      allRates,
				}
				if searchOut.Results == nil {
					searchOut.Results = []mrf.RateResult{}
				}
				data, jsonErr := json.Marshal(searchOut)
				if jsonErr != nil {
					return fmt.Errorf("marshaling results for S3: %w", jsonErr)
				}

				s3Client, s3Err := cloud.NewS3Client(ctx, bucket, cloudRegion)
				if s3Err != nil {
					return fmt.Errorf("creating S3 client for upload: %w", s3Err)
				}
				if uploadErr := s3Client.UploadBytes(ctx, key, data, "application/json"); uploadErr != nil {
					return fmt.Errorf("uploading results to S3: %w", uploadErr)
				}
				fmt.Fprintf(os.Stderr, "Results uploaded to s3://%s/%s\n", bucket, key)
			}

			return nil
		},
	}

	// Standard flags
	cmd.Flags().StringVar(&urlsFile, "urls-file", "", "File containing MRF URLs (one per line)")
	cmd.Flags().StringVar(&npiList, "npi", "", "Comma-separated NPI numbers to search for")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "results.json", "Output file path (use '-' for stdout)")
	cmd.Flags().IntVar(&workers, "workers", 3, "Number of concurrent file workers")
	cmd.Flags().StringVar(&tmpDir, "tmp-dir", "", "Temp directory for intermediate files (default: system temp)")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress bars")

	cmd.MarkFlagRequired("npi")

	// Cloud mode flags (orchestrator)
	cmd.Flags().BoolVar(&cloudMode, "cloud", false, "Run in cloud mode (distribute to Fargate)")
	cmd.Flags().StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket for URL chunks and results (cloud mode)")
	cmd.Flags().StringVar(&region, "region", "us-east-1", "AWS region (cloud mode)")
	cmd.Flags().StringSliceVar(&subnets, "subnets", nil, "VPC subnet IDs for Fargate tasks (cloud mode)")
	cmd.Flags().IntVar(&urlsPerTask, "urls-per-task", 5, "Number of URLs per Fargate task (cloud mode)")

	// Worker mode flags (used by Fargate containers, hidden)
	cmd.Flags().StringVar(&urlsS3, "urls-s3", "", "S3 URI for URL file (worker mode)")
	cmd.Flags().StringVar(&outputS3, "output-s3", "", "S3 URI to upload results to (worker mode)")
	cmd.Flags().StringVar(&cloudRegion, "cloud-region", "us-east-1", "AWS region for S3 operations (worker mode)")
	cmd.Flags().MarkHidden("urls-s3")
	cmd.Flags().MarkHidden("output-s3")
	cmd.Flags().MarkHidden("cloud-region")

	return cmd
}

func parseNPIs(s string) ([]int64, error) {
	parts := strings.Split(s, ",")
	var npis []int64
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.ParseInt(p, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid NPI %q: %w", p, err)
		}
		if n < 1000000000 || n > 9999999999 {
			return nil, fmt.Errorf("NPI %d is not a valid 10-digit NPI", n)
		}
		npis = append(npis, n)
	}
	return npis, nil
}

func newCloudSetupCmd() *cobra.Command {
	var (
		region   string
		s3Bucket string
	)

	cmd := &cobra.Command{
		Use:   "cloud-setup",
		Short: "Provision AWS infrastructure for Fargate-based distributed processing",
		RunE: func(cmd *cobra.Command, args []string) error {
			return cloud.Setup(context.Background(), cloud.SetupConfig{
				Region:   region,
				S3Bucket: s3Bucket,
			})
		},
	}

	cmd.Flags().StringVar(&region, "region", "us-east-1", "AWS region")
	cmd.Flags().StringVar(&s3Bucket, "s3-bucket", "", "S3 bucket for results")
	cmd.MarkFlagRequired("s3-bucket")

	return cmd
}

func readURLs(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var urls []string
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024) // URLs can be long (signed URLs)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		urls = append(urls, line)
	}
	return urls, scanner.Err()
}
