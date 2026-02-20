// Package main implements a Modal-based distributed deployment CLI for npi-rates.
// It shards URLs across parallel Modal sandboxes, runs the npi-rates search binary
// in each, and merges results locally.
//
// The binary is baked into a Modal Image (via cross-compile + SnapshotFilesystem),
// matching the Python deploy_modal.py approach where the image contains /npi-rates.
// URLs are passed to each worker via stdin, avoiding volume consistency issues.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	modal "github.com/modal-labs/libmodal/modal-go"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"

	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/output"
)

type config struct {
	npi      string
	urlsFile string
	shards   int
	workers  int
	cpu      float64
	memory   int
	timeout  time.Duration
	cloud    string
	region   string
	image    string
	output   string
	progress bool
}

type shardResult struct {
	index int
	data  []byte
	err   error
}

// --- Progress bar support ---

var (
	reURLLine = regexp.MustCompile(`\[URL\|\s*\d+/\d+\]\s+\[([^\]]+)\]\s+(.+)`)
	rePct     = regexp.MustCompile(`\((\d+)%\)`)
	reRates   = regexp.MustCompile(`(\d+) rates`)
)

type shardTracker struct {
	bar      *mpb.Bar
	namePtr  *atomic.Value
	stagePtr *atomic.Value
}

func newShardTracker(p *mpb.Progress, index, total int) *shardTracker {
	namePtr := &atomic.Value{}
	namePtr.Store("waiting...")
	stagePtr := &atomic.Value{}
	stagePtr.Store("")

	width := len(fmt.Sprintf("%d", total))
	bar := p.AddBar(100,
		mpb.PrependDecorators(
			decor.Any(func(s decor.Statistics) string {
				name := namePtr.Load().(string)
				return fmt.Sprintf("[%*d/%d] %s", width, index+1, total, name)
			}, decor.WCSyncSpaceR),
		),
		mpb.AppendDecorators(
			decor.Any(func(s decor.Statistics) string {
				return stagePtr.Load().(string)
			}),
		),
	)

	return &shardTracker{bar: bar, namePtr: namePtr, stagePtr: stagePtr}
}

func (t *shardTracker) handleLine(line string) {
	if t == nil {
		return
	}
	m := reURLLine.FindStringSubmatch(line)
	if m == nil {
		return
	}
	name := strings.TrimSpace(m[1])
	msg := m[2]

	// Truncate filename for display
	if len(name) > 45 {
		name = "..." + name[len(name)-42:]
	}
	t.namePtr.Store(name)

	// Extract percentage → update bar
	if pm := rePct.FindStringSubmatch(msg); pm != nil {
		if pct, err := strconv.ParseInt(pm[1], 10, 64); err == nil {
			t.bar.SetCurrent(pct)
		}
	}

	// Update status text
	t.stagePtr.Store(msg)

	// On Done/Finished, snap bar to 100%
	if strings.HasPrefix(msg, "Done") || strings.HasPrefix(msg, "Finished") {
		t.bar.SetCurrent(100)
	}
}

func (t *shardTracker) fail(err error) {
	if t == nil || err == nil {
		return
	}
	t.stagePtr.Store(fmt.Sprintf("FAILED: %v", err))
	t.bar.SetCurrent(100)
	t.bar.Abort(false)
}

func (t *shardTracker) complete() {
	if t == nil {
		return
	}
	t.bar.SetCurrent(100)
	t.bar.Abort(false)
}

// isTerminal returns true if stderr is connected to a terminal.
func isTerminal() bool {
	fi, err := os.Stderr.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func main() {
	cfg := parseFlags()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		logf("Interrupted, cleaning up...")
		cancel()
		<-sigCh
		os.Exit(1)
	}()

	if err := run(ctx, cfg); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func parseFlags() config {
	var cfg config
	flag.StringVar(&cfg.npi, "npi", "", "NPI number(s) (required)")
	flag.StringVar(&cfg.urlsFile, "urls-file", "", "File containing MRF URLs (required)")
	flag.IntVar(&cfg.shards, "shards", 100, "Number of URL shards")
	flag.IntVar(&cfg.workers, "workers", 1, "Workers per shard")
	flag.Float64Var(&cfg.cpu, "cpu", 2.0, "CPU cores per sandbox")
	flag.IntVar(&cfg.memory, "memory", 4096, "Memory MB per sandbox")
	flag.DurationVar(&cfg.timeout, "timeout", time.Hour, "Timeout per sandbox")
	flag.StringVar(&cfg.cloud, "cloud", "aws", "Cloud provider")
	flag.StringVar(&cfg.region, "region", "us-east-1", "Region")
	flag.StringVar(&cfg.image, "image", "", "Pre-built Docker image (skip cross-compile)")
	flag.StringVar(&cfg.output, "o", "", "Output file path")
	flag.BoolVar(&cfg.progress, "progress", isTerminal(), "Show progress bars (default: auto-detect TTY)")
	flag.Parse()

	if cfg.npi == "" {
		log.Fatal("--npi is required")
	}
	if cfg.urlsFile == "" {
		log.Fatal("--urls-file is required")
	}
	if cfg.output == "" {
		cfg.output = fmt.Sprintf("results_%s.json", time.Now().Format("20060102_150405"))
	}

	return cfg
}

func run(ctx context.Context, cfg config) error {
	urls, err := readURLs(cfg.urlsFile)
	if err != nil {
		return fmt.Errorf("reading URLs: %w", err)
	}
	shards := shardURLs(urls, cfg.shards)

	logf("NPI: %s", cfg.npi)
	logf("Files: %d URLs across %d shards", len(urls), len(shards))
	logf("Infra: %.0f CPU, %d MB memory, %s/%s", cfg.cpu, cfg.memory, cfg.cloud, cfg.region)
	logf("Workers per shard: %d", cfg.workers)

	// Create Modal client
	client, err := modal.NewClient()
	if err != nil {
		return fmt.Errorf("creating Modal client: %w", err)
	}
	defer client.Close()

	// Get app
	app, err := client.Apps.FromName(ctx, "npi-rates-deploy", &modal.AppFromNameParams{
		CreateIfMissing: true,
	})
	if err != nil {
		return fmt.Errorf("getting app: %w", err)
	}

	// Build image with /npi-rates binary baked in
	var img *modal.Image
	if cfg.image != "" {
		logf("Using pre-built image: %s", cfg.image)
		img = client.Images.FromRegistry(cfg.image, nil)
	} else {
		img, err = buildImage(ctx, client, app)
		if err != nil {
			return fmt.Errorf("building image: %w", err)
		}
	}

	// Run all shards — URLs are passed via stdin, no volume needed
	start := time.Now()
	results := runShards(ctx, client, app, img, cfg, shards)
	wallTime := time.Since(start)

	// Collect results
	var successData [][]byte
	var failCount int
	for _, r := range results {
		if r.err != nil {
			logf("Shard %d failed: %v", r.index, r.err)
			failCount++
			continue
		}
		successData = append(successData, r.data)
	}

	if len(successData) == 0 {
		return fmt.Errorf("all %d shards failed", len(results))
	}

	merged, err := mergeResults(successData)
	if err != nil {
		return fmt.Errorf("merging results: %w", err)
	}
	merged.SearchParams.DurationSeconds = wallTime.Seconds()

	if err := output.WriteResults(cfg.output, merged.SearchParams, merged.Results); err != nil {
		return fmt.Errorf("writing output: %w", err)
	}

	logf("Search complete: %d files searched, %d matched, %d rates found in %.1fs",
		merged.SearchParams.SearchedFiles,
		merged.SearchParams.MatchedFiles,
		len(merged.Results),
		wallTime.Seconds(),
	)
	if failCount > 0 {
		logf("Warning: %d/%d shards failed", failCount, len(results))
	}
	logf("Results saved to %s", cfg.output)

	return nil
}

// buildImage cross-compiles the npi-rates binary, uploads it into a temporary
// sandbox, and snapshots the filesystem to produce a Modal Image with /npi-rates
// baked in. This mirrors deploy_modal.py's from_dockerfile approach.
func buildImage(ctx context.Context, client *modal.Client, app *modal.App) (*modal.Image, error) {
	// Cross-compile
	logf("Cross-compiling npi-rates for linux/amd64...")
	binaryPath, err := crossCompile(ctx)
	if err != nil {
		return nil, fmt.Errorf("cross-compile: %w", err)
	}
	defer os.RemoveAll(filepath.Dir(binaryPath))

	binaryData, err := os.ReadFile(binaryPath)
	if err != nil {
		return nil, fmt.Errorf("reading binary: %w", err)
	}
	logf("Binary compiled (%d MB)", len(binaryData)/(1024*1024))

	// Build base image
	base := client.Images.FromRegistry("alpine:3.21", nil)
	base = base.DockerfileCommands([]string{"RUN apk add --no-cache ca-certificates"}, nil)

	logf("Building base image...")
	base, err = base.Build(ctx, app)
	if err != nil {
		return nil, fmt.Errorf("building base image: %w", err)
	}

	// Create builder sandbox, upload binary, snapshot filesystem
	logf("Uploading binary to image...")
	sb, err := client.Sandboxes.Create(ctx, app, base, &modal.SandboxCreateParams{
		Command: []string{"sleep", "3600"},
		Timeout: 10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("creating builder sandbox: %w", err)
	}
	defer sb.Terminate(ctx)

	// Write binary in chunks (Modal has a 16 MiB per-request limit)
	f, err := sb.Open(ctx, "/npi-rates", "w")
	if err != nil {
		return nil, fmt.Errorf("opening /npi-rates: %w", err)
	}
	const chunkSize = 8 * 1024 * 1024
	for off := 0; off < len(binaryData); off += chunkSize {
		end := off + chunkSize
		if end > len(binaryData) {
			end = len(binaryData)
		}
		if _, err := f.Write(binaryData[off:end]); err != nil {
			f.Close()
			return nil, fmt.Errorf("writing binary chunk at offset %d: %w", off, err)
		}
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("closing binary: %w", err)
	}

	proc, err := sb.Exec(ctx, []string{"chmod", "+x", "/npi-rates"}, nil)
	if err != nil {
		return nil, fmt.Errorf("chmod: %w", err)
	}
	if _, err := proc.Wait(ctx); err != nil {
		return nil, fmt.Errorf("chmod wait: %w", err)
	}

	// Snapshot the sandbox filesystem → image with /npi-rates baked in
	logf("Snapshotting image...")
	img, err := sb.SnapshotFilesystem(ctx, 2*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("snapshotting filesystem: %w", err)
	}

	logf("Image ready")
	return img, nil
}

func crossCompile(ctx context.Context) (string, error) {
	tmpDir, err := os.MkdirTemp("", "npi-rates-build-*")
	if err != nil {
		return "", err
	}

	outPath := filepath.Join(tmpDir, "npi-rates")
	cmd := exec.CommandContext(ctx, "go", "build", "-o", outPath, "./cmd/npi-rates")
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		os.RemoveAll(tmpDir)
		return "", fmt.Errorf("go build: %w", err)
	}

	return outPath, nil
}

func runShards(ctx context.Context, client *modal.Client, app *modal.App, img *modal.Image, cfg config, shards [][]string) []shardResult {
	results := make([]shardResult, len(shards))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 50)

	var container *mpb.Progress
	var trackers []*shardTracker
	var shardsComplete int64
	var statusStop chan struct{}

	if cfg.progress {
		container = mpb.New(mpb.WithWidth(60), mpb.WithOutput(os.Stderr))
		trackers = make([]*shardTracker, len(shards))
		for i := range shards {
			trackers[i] = newShardTracker(container, i, len(shards))
		}

		// Overall status bar at bottom
		statusVal := &atomic.Value{}
		statusVal.Store("")
		statusBar := container.AddBar(0,
			mpb.PrependDecorators(
				decor.Any(func(s decor.Statistics) string {
					return statusVal.Load().(string)
				}),
			),
		)

		statusStop = make(chan struct{})
		start := time.Now()
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for {
				elapsed := time.Since(start).Truncate(time.Second)
				done := atomic.LoadInt64(&shardsComplete)
				statusVal.Store(fmt.Sprintf("Elapsed: %s  |  %d/%d shards", elapsed, done, len(shards)))
				select {
				case <-ticker.C:
				case <-statusStop:
					statusBar.Abort(false)
					return
				}
			}
		}()
	}

	for i, urls := range shards {
		wg.Add(1)
		sem <- struct{}{}
		go func(idx int, urls []string) {
			defer wg.Done()
			defer func() { <-sem }()
			var tracker *shardTracker
			if trackers != nil {
				tracker = trackers[idx]
			}
			results[idx] = runShard(ctx, client, app, img, cfg, idx, urls, tracker)
			if cfg.progress {
				atomic.AddInt64(&shardsComplete, 1)
			}
		}(i, urls)
	}

	wg.Wait()
	if statusStop != nil {
		close(statusStop)
	}
	if container != nil {
		container.Wait()
	}
	return results
}

// runShard creates a worker sandbox that receives URLs via stdin, writes them
// to a local temp file, then runs /npi-rates search. This avoids volume
// consistency issues — each sandbox is fully self-contained.
func runShard(ctx context.Context, client *modal.Client, app *modal.App, img *modal.Image, cfg config, shardIndex int, urls []string, tracker *shardTracker) shardResult {
	result := shardResult{index: shardIndex}
	prefix := fmt.Sprintf("[shard-%03d]", shardIndex)

	defer func() { tracker.fail(result.err) }()

	if tracker == nil {
		logf("%s Starting (%d URLs)", prefix, len(urls))
	}

	// Create a long-running sandbox so we can write files, exec the search,
	// and read results back via sb.Open — avoids stdout streaming truncation.
	sb, err := client.Sandboxes.Create(ctx, app, img, &modal.SandboxCreateParams{
		Command:   []string{"sleep", "3600"},
		CPU:       cfg.cpu,
		MemoryMiB: cfg.memory,
		Timeout:   cfg.timeout,
		Cloud:     cfg.cloud,
		Regions:   []string{cfg.region},
	})
	if err != nil {
		result.err = fmt.Errorf("creating sandbox: %w", err)
		return result
	}
	defer sb.Terminate(ctx)

	// Write URL file into sandbox
	urlData := strings.Join(urls, "\n") + "\n"
	uf, err := sb.Open(ctx, "/tmp/urls.txt", "w")
	if err != nil {
		result.err = fmt.Errorf("opening urls file: %w", err)
		return result
	}
	if _, err := uf.Write([]byte(urlData)); err != nil {
		uf.Close()
		result.err = fmt.Errorf("writing urls: %w", err)
		return result
	}
	if err := uf.Close(); err != nil {
		result.err = fmt.Errorf("closing urls file: %w", err)
		return result
	}

	// Run the search via sb.Exec
	cmd := []string{
		"/npi-rates", "search",
		"--npi", cfg.npi,
		"--urls-file", "/tmp/urls.txt",
		"--workers", fmt.Sprintf("%d", cfg.workers),
		"-o", "/tmp/results.json",
		"--stream", "--log-progress",
	}
	proc, err := sb.Exec(ctx, cmd, nil)
	if err != nil {
		result.err = fmt.Errorf("exec search: %w", err)
		return result
	}

	// Stream stderr with shard prefix (or feed to progress tracker)
	var stderrWg sync.WaitGroup
	stderrWg.Add(1)
	go func() {
		defer stderrWg.Done()
		scanner := bufio.NewScanner(proc.Stderr)
		scanner.Buffer(make([]byte, 1024*1024), 1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			if tracker != nil {
				tracker.handleLine(line)
			} else {
				fmt.Fprintf(os.Stderr, "%s %s\n", prefix, line)
			}
		}
	}()

	// Drain stdout
	io.Copy(io.Discard, proc.Stdout)

	exitCode, err := proc.Wait(ctx)
	if err != nil {
		result.err = fmt.Errorf("waiting for search: %w", err)
		return result
	}
	stderrWg.Wait()

	if exitCode != 0 {
		result.err = fmt.Errorf("exit code %d", exitCode)
		return result
	}

	// Read results file from the sandbox filesystem
	rf, err := sb.Open(ctx, "/tmp/results.json", "r")
	if err != nil {
		result.err = fmt.Errorf("opening results file: %w", err)
		return result
	}
	data, err := io.ReadAll(rf)
	rf.Close()
	if err != nil {
		result.err = fmt.Errorf("reading results file: %w", err)
		return result
	}

	if len(data) == 0 {
		result.err = fmt.Errorf("empty output")
		return result
	}

	result.data = data
	if tracker != nil {
		tracker.complete()
	} else {
		logf("%s Completed (%d bytes)", prefix, len(data))
	}
	return result
}

func shardURLs(urls []string, n int) [][]string {
	if n <= 0 {
		n = 1
	}
	if n > len(urls) {
		n = len(urls)
	}
	shards := make([][]string, n)
	for i, url := range urls {
		shards[i%n] = append(shards[i%n], url)
	}
	var result [][]string
	for _, s := range shards {
		if len(s) > 0 {
			result = append(result, s)
		}
	}
	return result
}

func mergeResults(outputs [][]byte) (*mrf.SearchOutput, error) {
	var merged mrf.SearchOutput
	first := true

	for i, data := range outputs {
		var out mrf.SearchOutput
		if err := json.Unmarshal(data, &out); err != nil {
			logf("Warning: skipping shard output %d (%d bytes): %v", i, len(data), err)
			continue
		}
		if first {
			merged.SearchParams.NPIs = out.SearchParams.NPIs
			first = false
		}
		merged.SearchParams.SearchedFiles += out.SearchParams.SearchedFiles
		merged.SearchParams.MatchedFiles += out.SearchParams.MatchedFiles
		if out.SearchParams.DurationSeconds > merged.SearchParams.DurationSeconds {
			merged.SearchParams.DurationSeconds = out.SearchParams.DurationSeconds
		}
		merged.Results = append(merged.Results, out.Results...)
	}

	if merged.Results == nil {
		merged.Results = []mrf.RateResult{}
	}

	return &merged, nil
}

func readURLs(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var urls []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			urls = append(urls, line)
		}
	}
	if len(urls) == 0 {
		return nil, fmt.Errorf("no URLs found in %s", path)
	}
	return urls, nil
}

func logf(format string, args ...any) {
	ts := time.Now().Format("15:04:05")
	fmt.Fprintf(os.Stderr, "%s %s\n", ts, fmt.Sprintf(format, args...))
}
