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
	"github.com/gyeh/npi-rates/internal/npi"
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
		urlsFile     string // Used during Cloud mode or local mode
		npiList      string
		providerName string
		state        string
		outputFile   string
		workers      int
		tmpDir       string
		noProgress   bool

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
			// Resolve NPIs — either from --npi or --provider-name
			var npis []int64
			if providerName != "" {
				selected, err := searchAndSelectProvider(providerName, state)
				if err != nil {
					return err
				}
				npis = []int64{selected.NPI}
			} else if npiList != "" {
				var err error
				npis, err = parseNPIs(npiList)
				if err != nil {
					return fmt.Errorf("parsing NPIs: %w", err)
				}
			}
			if len(npis) == 0 {
				return fmt.Errorf("specify either --npi or --provider-name")
			}

			// Handle signals: first ^C cancels context for graceful shutdown,
			// second ^C force-exits immediately.
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sigCh := make(chan os.Signal, 2)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				sig := <-sigCh
				fmt.Fprintf(os.Stderr, "\nReceived %s, shutting down... (^C again to force quit)\n", sig)
				cancel()
				sig = <-sigCh
				fmt.Fprintf(os.Stderr, "\nReceived %s, force quit.\n", sig)
				os.Exit(1)
			}()

			// Look up NPI provider info (skip in worker mode — no user to display to)
			if urlsS3 == "" {
				if notFound := printProviderInfo(ctx, npis); len(notFound) > 0 {
					if !confirmContinue(notFound) {
						return fmt.Errorf("aborted: %d NPI(s) not found in NPPES registry", len(notFound))
					}
				}
			}

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
				// Local mode or Cloud orchestration mode
				var readErr error
				urls, readErr = readURLs(urlsFile)
				if readErr != nil {
					return fmt.Errorf("reading URLs: %w", readErr)
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

			// Check available disk space and warn if low
			avail := availableDiskSpace(tmpDir)
			if avail > 0 && avail < 50*1024*1024*1024 { // < 50 GB
				fmt.Fprintf(os.Stderr, "WARNING: Only %s available in temp dir %s\n", humanBytesCLI(avail), tmpDir)
				fmt.Fprintf(os.Stderr, "  MRF files decompress to 5-40 GB each. Use --tmp-dir to point to a larger volume.\n")
				fmt.Fprintf(os.Stderr, "  Consider --workers 1 to reduce concurrent disk usage.\n\n")
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
			fmt.Fprintf(os.Stderr, "Temp dir: %s (%s available)\n", tmpDir, humanBytesCLI(avail))
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
	cmd.Flags().StringVar(&providerName, "provider-name", "", "Search by provider name (\"First Last\")")
	cmd.Flags().StringVar(&state, "state", "", "State filter for provider name search (2-letter code, e.g. NY)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "results.json", "Output file path (use '-' for stdout)")
	cmd.Flags().IntVar(&workers, "workers", 3, "Number of concurrent file workers")
	cmd.Flags().StringVar(&tmpDir, "tmp-dir", "", "Temp directory for intermediate files (default: system temp)")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress bars")

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

// printProviderInfo looks up and displays provider details for each NPI.
// Returns the list of NPI numbers that were not found in the NPPES registry.
func printProviderInfo(ctx context.Context, npis []int64) []int64 {
	lookupCtx, lookupCancel := context.WithTimeout(ctx, 15*time.Second)
	defer lookupCancel()

	results, errs := npi.LookupAll(lookupCtx, npis)

	var notFound []int64
	for i, info := range results {
		if errs[i] != nil {
			fmt.Fprintf(os.Stderr, "NPI %d: lookup failed (%v)\n", npis[i], errs[i])
			continue
		}
		if info == nil {
			fmt.Fprintf(os.Stderr, "NPI %d: not found in NPPES registry\n", npis[i])
			notFound = append(notFound, npis[i])
			continue
		}

		// Build display line
		fmt.Fprintf(os.Stderr, "NPI %d: %s", info.NPI, info.Name)
		if info.Credential != "" {
			fmt.Fprintf(os.Stderr, ", %s", info.Credential)
		}
		fmt.Fprintln(os.Stderr)

		if info.PrimaryTaxonomy != "" {
			fmt.Fprintf(os.Stderr, "  Specialty: %s\n", info.PrimaryTaxonomy)
		}
		if info.PracticeAddress != "" {
			line := "  Location:  " + info.PracticeAddress
			if info.PracticePhone != "" {
				line += "  |  " + info.PracticePhone
			}
			fmt.Fprintln(os.Stderr, line)
		}
		if info.Status != "A" {
			fmt.Fprintf(os.Stderr, "  WARNING:   NPI status is %q (not active)\n", info.Status)
		}
	}
	fmt.Fprintln(os.Stderr)
	return notFound
}

// confirmContinue prompts the user to continue despite not-found NPIs.
// Returns true if the user wants to continue, false to abort.
func confirmContinue(notFound []int64) bool {
	npiStrs := make([]string, len(notFound))
	for i, n := range notFound {
		npiStrs[i] = fmt.Sprintf("%d", n)
	}
	fmt.Fprintf(os.Stderr, "NPI(s) %s not found. Continue anyway? [y/N]: ", strings.Join(npiStrs, ", "))

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return false
	}
	answer := strings.TrimSpace(strings.ToLower(scanner.Text()))
	return answer == "y" || answer == "yes"
}

// searchAndSelectProvider queries the NPPES registry by name and prompts the user
// to select a single provider from the results.
func searchAndSelectProvider(name, state string) (*npi.ProviderInfo, error) {
	// Split "First Last" — first token is first name, rest is last name
	parts := strings.Fields(name)
	if len(parts) < 2 {
		return nil, fmt.Errorf("--provider-name requires first and last name (e.g. \"John Smith\")")
	}
	firstName := parts[0]
	lastName := strings.Join(parts[1:], " ")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	fmt.Fprintf(os.Stderr, "Searching NPPES registry for \"%s %s\"", firstName, lastName)
	if state != "" {
		fmt.Fprintf(os.Stderr, " in %s", strings.ToUpper(state))
	}
	fmt.Fprintln(os.Stderr, "...")

	providers, err := npi.SearchByName(ctx, firstName, lastName, strings.ToUpper(state))
	if err != nil {
		return nil, fmt.Errorf("searching NPI registry: %w", err)
	}
	if len(providers) == 0 {
		return nil, fmt.Errorf("no providers found matching \"%s\"", name)
	}

	// Display results
	fmt.Fprintf(os.Stderr, "\nFound %d provider(s):\n\n", len(providers))
	for i, p := range providers {
		fmt.Fprintf(os.Stderr, "  [%d] %s (NPI %d)", i+1, p.Name, p.NPI)
		if p.Credential != "" {
			fmt.Fprintf(os.Stderr, ", %s", p.Credential)
		}
		fmt.Fprintln(os.Stderr)
		if p.PrimaryTaxonomy != "" {
			fmt.Fprintf(os.Stderr, "      Specialty: %s\n", p.PrimaryTaxonomy)
		}
		if p.PracticeAddress != "" {
			fmt.Fprintf(os.Stderr, "      Location:  %s\n", p.PracticeAddress)
		}
	}

	// Single result — auto-select
	if len(providers) == 1 {
		fmt.Fprintf(os.Stderr, "\nAuto-selected the only match: NPI %d\n\n", providers[0].NPI)
		return providers[0], nil
	}

	// Prompt for selection
	fmt.Fprintf(os.Stderr, "\nSelect a provider [1-%d]: ", len(providers))
	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		return nil, fmt.Errorf("no input received")
	}
	input := strings.TrimSpace(scanner.Text())
	choice, err := strconv.Atoi(input)
	if err != nil || choice < 1 || choice > len(providers) {
		return nil, fmt.Errorf("invalid selection %q — enter a number between 1 and %d", input, len(providers))
	}

	selected := providers[choice-1]
	fmt.Fprintf(os.Stderr, "\nSelected: %s (NPI %d)\n\n", selected.Name, selected.NPI)
	return selected, nil
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

// availableDiskSpace returns the available bytes on the filesystem containing path.
// Returns 0 if the check fails.
func availableDiskSpace(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	return stat.Bavail * uint64(stat.Bsize)
}

func humanBytesCLI(b uint64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)
	switch {
	case b >= tb:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(tb))
	case b >= gb:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(gb))
	case b >= mb:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(mb))
	case b >= kb:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(kb))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
