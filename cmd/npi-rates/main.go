package main

import (
	"bufio"
	"context"
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

			// Read URLs
			urls, err := readURLs(urlsFile)
			if err != nil {
				return fmt.Errorf("reading URLs: %w", err)
			}
			if len(urls) == 0 {
				return fmt.Errorf("no URLs found in %s", urlsFile)
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

			return nil
		},
	}

	cmd.Flags().StringVar(&urlsFile, "urls-file", "", "File containing MRF URLs (one per line)")
	cmd.Flags().StringVar(&npiList, "npi", "", "Comma-separated NPI numbers to search for")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "results.json", "Output file path (use '-' for stdout)")
	cmd.Flags().IntVar(&workers, "workers", 3, "Number of concurrent file workers")
	cmd.Flags().StringVar(&tmpDir, "tmp-dir", "", "Temp directory for intermediate files (default: system temp)")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress bars")

	cmd.MarkFlagRequired("urls-file")
	cmd.MarkFlagRequired("npi")

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
