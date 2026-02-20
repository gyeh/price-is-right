package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	rootCmd.AddCommand(newDownloadCmd())
	rootCmd.AddCommand(newSplitCmd())
	rootCmd.AddCommand(newCloudSetupCmd())

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newSearchCmd() *cobra.Command {
	var (
		urlsFile     string   // Used during Cloud mode or local mode
		urlsList     []string // URLs passed directly on the command line
		npiList      string
		providerName string
		state        string
		outputFile   string
		workers      int
		tmpDir       string
		noProgress   bool
		logProgress  bool
		noFIFO       bool
		streamMode   bool
		noSimd       bool

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
			if noSimd {
				mrf.DisableSimd()
			}

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
				if urlsFile == "" && len(urlsList) == 0 {
					return fmt.Errorf("--urls-file or --url is required for cloud mode")
				}
				if s3Bucket == "" {
					return fmt.Errorf("--s3-bucket is required for cloud mode")
				}
				if len(subnets) == 0 {
					return fmt.Errorf("--subnets is required for cloud mode")
				}

				var urls []string
				if len(urlsList) > 0 {
					urls = urlsList
				} else {
					var readErr error
					urls, readErr = readURLs(urlsFile)
					if readErr != nil {
						return fmt.Errorf("reading URLs: %w", readErr)
					}
				}
				if len(urls) == 0 {
					return fmt.Errorf("no URLs provided")
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

			// --- Read URLs from file, S3, or command-line ---
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
			} else if len(urlsList) > 0 {
				urls = urlsList
			} else if urlsFile != "" {
				// Local mode or Cloud orchestration mode
				var readErr error
				urls, readErr = readURLs(urlsFile)
				if readErr != nil {
					return fmt.Errorf("reading URLs: %w", readErr)
				}
			} else {
				return fmt.Errorf("either --urls-file or --url is required")
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

			// Check available disk space and warn if low (skip for streaming mode — no disk used)
			var avail uint64
			if !streamMode {
				avail = availableDiskSpace(tmpDir)
				if avail > 0 && avail < 50*1024*1024*1024 { // < 50 GB
					fmt.Fprintf(os.Stderr, "WARNING: Only %s available in temp dir %s\n", humanBytesCLI(avail), tmpDir)
					fmt.Fprintf(os.Stderr, "  MRF files decompress to 5-40 GB each. Use --tmp-dir to point to a larger volume.\n")
					fmt.Fprintf(os.Stderr, "  Consider --workers 1 to reduce concurrent disk usage.\n\n")
				}
			}

			// Set up progress
			var mgr progress.Manager
			if logProgress {
				mgr = progress.NewLogManager()
			} else if noProgress {
				mgr = &progress.NoopManager{}
			} else {
				mgr = progress.NewMPBManager()
			}

			// Log URL and environment info
			logURLInfo(ctx, urls)
			fmt.Fprintf(os.Stderr, "Parser: %s\n", mrf.ParserName())
			if streamMode {
				fmt.Fprintf(os.Stderr, "Mode: streaming (no disk)\n")
			} else {
				fmt.Fprintf(os.Stderr, "Temp dir: %s (%s available)\n", tmpDir, humanBytesCLI(avail))
			}
			fmt.Fprintf(os.Stderr, "Workers: %d\n\n", workers)

			// Run the worker pool
			startTime := time.Now()

			pool := &worker.Pool{
				Workers:    workers,
				TargetNPIs: npiSet,
				TmpDir:     tmpDir,
				Progress:   mgr,
				NoFIFO:     noFIFO,
				Stream:     streamMode,
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
	cmd.Flags().StringSliceVar(&urlsList, "url", nil, "MRF URL(s) to search (can be repeated or comma-separated)")
	cmd.Flags().StringVar(&npiList, "npi", "", "Comma-separated NPI numbers to search for")
	cmd.Flags().StringVar(&providerName, "provider-name", "", "Search by provider name (\"First Last\")")
	cmd.Flags().StringVar(&state, "state", "", "State filter for provider name search (2-letter code, e.g. NY)")
	cmd.Flags().StringVarP(&outputFile, "output", "o", "results.json", "Output file path (use '-' for stdout)")
	cmd.Flags().IntVar(&workers, "workers", 3, "Number of concurrent file workers")
	cmd.Flags().StringVar(&tmpDir, "tmp-dir", "", "Temp directory for intermediate files (default: system temp)")
	cmd.Flags().BoolVar(&noProgress, "no-progress", false, "Disable progress bars")
	cmd.Flags().BoolVar(&logProgress, "log-progress", false, "Use line-based progress logging (for non-TTY environments)")
	cmd.Flags().BoolVar(&noFIFO, "no-fifo", false, "Use file-based pipeline instead of FIFO streaming")
	cmd.Flags().BoolVar(&streamMode, "stream", false, "Stream directly from download to parsing (no disk, constant memory)")
	cmd.Flags().BoolVar(&noSimd, "no-simd", false, "Disable simdjson and use stdlib encoding/json")

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

func newDownloadCmd() *cobra.Command {
	var (
		outputPath string
		tmpDir     string
	)

	cmd := &cobra.Command{
		Use:   "download <url>",
		Short: "Download and decompress a single MRF file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			url := args[0]
			filename := worker.FileNameFromURL(url)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			sigCh := make(chan os.Signal, 2)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			go func() {
				<-sigCh
				cancel()
				<-sigCh
				os.Exit(1)
			}()

			if tmpDir == "" {
				tmpDir = "."
			}

			fmt.Fprintf(os.Stderr, "Downloading %s ...\n", filename)
			startTime := time.Now()

			result, err := worker.DownloadAndDecompress(ctx, url, tmpDir, false, func(downloaded, total int64) {
				// periodic progress is printed below
			})
			if err != nil {
				return fmt.Errorf("download failed: %w", err)
			}

			// Move temp file to the final output path
			dest := outputPath
			if dest == "" {
				// Strip .gz suffix for the default name
				dest = strings.TrimSuffix(filename, ".gz")
			}
			if err := os.Rename(result.FilePath, dest); err != nil {
				// Rename failed (cross-device), fall back to keeping temp file
				dest = result.FilePath
			}

			elapsed := time.Since(startTime).Truncate(time.Second)
			info, _ := os.Stat(dest)
			decompressedSize := int64(0)
			if info != nil {
				decompressedSize = info.Size()
			}

			fmt.Fprintf(os.Stderr, "Downloaded and decompressed in %s\n", elapsed)
			if result.TotalBytes > 0 {
				fmt.Fprintf(os.Stderr, "  Compressed:   %s\n", humanBytesCLI(uint64(result.TotalBytes)))
			}
			if decompressedSize > 0 {
				fmt.Fprintf(os.Stderr, "  Decompressed: %s\n", humanBytesCLI(uint64(decompressedSize)))
			}
			fmt.Fprintf(os.Stderr, "  Output: %s\n", dest)

			return nil
		},
	}

	cmd.Flags().StringVarP(&outputPath, "output", "o", "", "Output file path (default: filename without .gz)")
	cmd.Flags().StringVar(&tmpDir, "tmp-dir", "", "Temp directory for intermediate files (default: current dir)")

	return cmd
}

func newSplitCmd() *cobra.Command {
	var outputDir string

	cmd := &cobra.Command{
		Use:   "split <file>",
		Short: "Split a decompressed MRF JSON file into NDJSON chunks",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			inputPath := args[0]

			// Verify the input file exists
			info, err := os.Stat(inputPath)
			if err != nil {
				return fmt.Errorf("cannot read input file: %w", err)
			}

			if outputDir == "" {
				outputDir = strings.TrimSuffix(inputPath, ".json") + "_split"
			}
			if err := os.MkdirAll(outputDir, 0o755); err != nil {
				return fmt.Errorf("creating output dir: %w", err)
			}

			fmt.Fprintf(os.Stderr, "Splitting %s (%s) ...\n", inputPath, humanBytesCLI(uint64(info.Size())))
			startTime := time.Now()

			result, err := mrf.SplitFile(inputPath, outputDir)
			if err != nil {
				return fmt.Errorf("split failed: %w", err)
			}

			elapsed := time.Since(startTime).Truncate(time.Second)

			fmt.Fprintf(os.Stderr, "Split complete in %s\n", elapsed)
			fmt.Fprintf(os.Stderr, "  Output dir: %s\n", outputDir)
			fmt.Fprintf(os.Stderr, "  provider_references: %d file(s)\n", len(result.ProviderReferenceFiles))
			fmt.Fprintf(os.Stderr, "  in_network:          %d file(s)\n", len(result.InNetworkFiles))

			return nil
		},
	}

	cmd.Flags().StringVarP(&outputDir, "output-dir", "o", "", "Output directory for split files (default: <input>_split)")

	return cmd
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

// logURLInfo analyzes the URLs and logs CDN/vendor, region, and file size distribution.
func logURLInfo(ctx context.Context, urls []string) {
	if len(urls) == 0 {
		return
	}

	fmt.Fprintf(os.Stderr, "Files: %d\n", len(urls))

	// Detect CDN/vendor and region from URL hostnames
	vendors := map[string]int{}
	regions := map[string]int{}
	for _, rawURL := range urls {
		vendor, region := detectCDN(rawURL)
		if vendor != "" {
			vendors[vendor]++
		}
		if region != "" {
			regions[region]++
		}
	}
	if len(vendors) > 0 {
		parts := make([]string, 0, len(vendors))
		for v, count := range vendors {
			if count == len(urls) {
				parts = append(parts, v)
			} else {
				parts = append(parts, fmt.Sprintf("%s (%d)", v, count))
			}
		}
		sort.Strings(parts)
		fmt.Fprintf(os.Stderr, "CDN: %s\n", strings.Join(parts, ", "))
	}
	// If no region detected from URLs, try IP-based geolocation on first URL's host
	if len(regions) == 0 && len(urls) > 0 {
		if r := detectRegionFromIP(ctx, urls[0]); r != "" {
			regions[r] = len(urls)
		}
	}
	if len(regions) > 0 {
		parts := make([]string, 0, len(regions))
		for r, count := range regions {
			if count == len(urls) {
				parts = append(parts, r)
			} else {
				parts = append(parts, fmt.Sprintf("%s (%d)", r, count))
			}
		}
		sort.Strings(parts)
		fmt.Fprintf(os.Stderr, "Region: %s\n", strings.Join(parts, ", "))
	}

	// Fetch file sizes via HEAD requests (concurrent, with timeout)
	sizes := fetchFileSizes(ctx, urls)
	var known []int64
	for _, s := range sizes {
		if s > 0 {
			known = append(known, s)
		}
	}
	if len(known) > 0 {
		sort.Slice(known, func(i, j int) bool { return known[i] < known[j] })
		var total int64
		for _, s := range known {
			total += s
		}
		min, max := known[0], known[len(known)-1]
		avg := total / int64(len(known))
		fmt.Fprintf(os.Stderr, "Size (compressed): %s total, %s avg, %s min, %s max",
			humanBytesCLI(uint64(total)), humanBytesCLI(uint64(avg)),
			humanBytesCLI(uint64(min)), humanBytesCLI(uint64(max)))
		if len(known) < len(urls) {
			fmt.Fprintf(os.Stderr, " (%d/%d responded)", len(known), len(urls))
		}
		fmt.Fprintln(os.Stderr)
	}
}

// detectCDN identifies the CDN vendor and region from a URL.
func detectCDN(rawURL string) (vendor, region string) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", ""
	}
	host := strings.ToLower(u.Hostname())

	switch {
	case strings.HasSuffix(host, ".cloudfront.net"):
		return "CloudFront", ""
	case strings.Contains(u.RawQuery, "Key-Pair-Id="):
		// CloudFront signed URL on custom domain
		return "CloudFront", ""
	case strings.HasSuffix(host, ".amazonaws.com"):
		// S3: s3.us-east-1.amazonaws.com or bucket.s3.region.amazonaws.com
		parts := strings.Split(host, ".")
		for i, p := range parts {
			if p == "s3" && i+1 < len(parts) && parts[i+1] != "amazonaws" {
				return "AWS S3", parts[i+1]
			}
		}
		return "AWS S3", ""
	case strings.HasSuffix(host, ".storage.googleapis.com") || host == "storage.googleapis.com":
		return "Google Cloud Storage", ""
	case strings.HasSuffix(host, ".blob.core.windows.net"):
		return "Azure Blob Storage", ""
	case strings.Contains(host, ".akamai"):
		return "Akamai", ""
	case strings.HasSuffix(host, ".fastly.net"):
		return "Fastly", ""
	case strings.HasSuffix(host, ".cloudflare.com") || strings.HasSuffix(host, ".r2.dev"):
		return "Cloudflare", ""
	case strings.HasSuffix(host, ".bcbs.com"):
		// BCBS MRF hosting — typically CloudFront behind custom domain
		if strings.Contains(u.RawQuery, "Key-Pair-Id=") || strings.Contains(u.RawQuery, "Signature=") {
			return "CloudFront (BCBS)", ""
		}
		return "BCBS", ""
	}
	return "", ""
}

// detectRegionFromIP resolves the hostname from a URL and uses IP geolocation
// to determine the server's geographic region. Uses ip-api.com (free, no key needed).
func detectRegionFromIP(ctx context.Context, rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	host := u.Hostname()
	if host == "" {
		return ""
	}

	// Resolve hostname to IP
	resolveCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	ips, err := net.DefaultResolver.LookupHost(resolveCtx, host)
	if err != nil || len(ips) == 0 {
		return ""
	}
	ip := ips[0]

	// Query ip-api.com for geolocation
	apiCtx, apiCancel := context.WithTimeout(ctx, 5*time.Second)
	defer apiCancel()

	req, err := http.NewRequestWithContext(apiCtx, "GET",
		fmt.Sprintf("http://ip-api.com/json/%s?fields=status,regionName,country,city,isp", ip), nil)
	if err != nil {
		return ""
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return ""
	}

	var geo struct {
		Status     string `json:"status"`
		Country    string `json:"country"`
		RegionName string `json:"regionName"`
		City       string `json:"city"`
		ISP        string `json:"isp"`
	}
	if json.Unmarshal(body, &geo) != nil || geo.Status != "success" {
		return ""
	}

	// Build a human-readable location string
	parts := []string{}
	if geo.City != "" {
		parts = append(parts, geo.City)
	}
	if geo.RegionName != "" {
		parts = append(parts, geo.RegionName)
	}
	if geo.Country != "" && geo.Country != "United States" {
		parts = append(parts, geo.Country)
	}
	if len(parts) == 0 {
		return ""
	}
	location := strings.Join(parts, ", ")
	if geo.ISP != "" {
		location += " (" + geo.ISP + ")"
	}
	return location
}

// fetchFileSizes does concurrent HEAD requests to get Content-Length for each URL.
func fetchFileSizes(ctx context.Context, urls []string) []int64 {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	sizes := make([]int64, len(urls))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 10) // limit concurrent HEAD requests

	client := &http.Client{Timeout: 10 * time.Second}

	for i, rawURL := range urls {
		wg.Add(1)
		go func(idx int, u string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			req, err := http.NewRequestWithContext(ctx, "HEAD", u, nil)
			if err != nil {
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				return
			}
			resp.Body.Close()
			if resp.ContentLength > 0 {
				sizes[idx] = resp.ContentLength
			}
		}(i, rawURL)
	}
	wg.Wait()
	return sizes
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
