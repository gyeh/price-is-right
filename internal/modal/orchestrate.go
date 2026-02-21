// Package modal implements distributed MRF search orchestration via `modal run`.
// It shells out to `modal run python/deploy_modal.py` which handles sharding, spawning
// parallel workers, merging results, and streaming logs to stderr.
package modal

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Config holds configuration for a Modal-based distributed search.
type Config struct {
	NPI             string
	URLsFile        string   // path to URLs file (already exists on disk)
	URLs            []string // if set, written to temp file
	OutputFile      string
	Shards          int
	WorkersPerShard int
}

// RunSearch executes a distributed search by shelling out to `modal run python/deploy_modal.py`.
func RunSearch(ctx context.Context, cfg Config) error {
	// Resolve URLs file: use existing file or write URLs to a temp file
	urlsFile := cfg.URLsFile
	if urlsFile == "" && len(cfg.URLs) > 0 {
		f, err := os.CreateTemp("", "npi-urls-*.txt")
		if err != nil {
			return fmt.Errorf("creating temp urls file: %w", err)
		}
		urlsFile = f.Name()
		defer os.Remove(urlsFile)

		if _, err := f.WriteString(strings.Join(cfg.URLs, "\n")); err != nil {
			f.Close()
			return fmt.Errorf("writing temp urls file: %w", err)
		}
		f.Close()
	}
	if urlsFile == "" {
		return fmt.Errorf("no URLs provided: set URLsFile or URLs")
	}

	// Locate python/deploy_modal.py relative to the project root.
	// We look for it in the working directory first, then walk up.
	scriptPath, err := findScript(filepath.Join("python", "deploy_modal.py"))
	if err != nil {
		return err
	}

	args := []string{
		"run", scriptPath,
		"--npi", cfg.NPI,
		"--urls-file", urlsFile,
		"--shards", strconv.Itoa(cfg.Shards),
		"--workers", strconv.Itoa(cfg.WorkersPerShard),
	}
	if cfg.OutputFile != "" {
		args = append(args, "--output", cfg.OutputFile)
	}

	logf("Running: modal %s", strings.Join(args, " "))
	start := time.Now()

	cmd := exec.CommandContext(ctx, "modal", args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("modal run failed: %w", err)
	}

	logf("modal run completed in %.1fs", time.Since(start).Seconds())
	return nil
}

// findScript walks up from the working directory looking for the given script file.
func findScript(name string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("getting working directory: %w", err)
	}
	for {
		candidate := filepath.Join(dir, name)
		if _, err := os.Stat(candidate); err == nil {
			return candidate, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("%s not found in working directory or any parent", name)
}

func logf(format string, args ...any) {
	ts := time.Now().Format("15:04:05")
	fmt.Fprintf(os.Stderr, "%s %s\n", ts, fmt.Sprintf(format, args...))
}
