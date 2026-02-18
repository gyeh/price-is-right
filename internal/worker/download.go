package worker

import (
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/klauspost/pgzip"
)

var httpClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 10,
		MaxIdleConns:        100,
		IdleConnTimeout:     90 * time.Second,
	},
	Timeout: 30 * time.Minute,
}

// DownloadResult holds the result of a download operation.
type DownloadResult struct {
	FilePath    string // path to decompressed temp file
	TotalBytes  int64  // compressed size from Content-Length (or -1)
}

// DownloadAndDecompress downloads a gzipped URL, decompresses with pgzip, and writes to a temp file.
// onProgress is called with (bytesDownloaded, totalBytes) during download.
func DownloadAndDecompress(ctx context.Context, url string, tmpDir string, onProgress func(downloaded, total int64)) (*DownloadResult, error) {
	var resp *http.Response
	var err error

	// Retry with exponential backoff
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			delay := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(delay):
			}
		}

		req, reqErr := http.NewRequestWithContext(ctx, "GET", url, nil)
		if reqErr != nil {
			return nil, fmt.Errorf("creating request: %w", reqErr)
		}

		resp, err = httpClient.Do(req)
		if err != nil {
			continue
		}
		if resp.StatusCode == http.StatusOK {
			break
		}
		resp.Body.Close()
		err = fmt.Errorf("HTTP %d", resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return nil, err // don't retry client errors
		}
	}

	if err != nil {
		return nil, fmt.Errorf("download failed after retries: %w", err)
	}
	defer resp.Body.Close()

	totalBytes := resp.ContentLength

	// Wrap body in a counting reader for progress
	var reader io.Reader = resp.Body
	if onProgress != nil {
		reader = &progressReader{
			reader:   resp.Body,
			total:    totalBytes,
			callback: onProgress,
		}
	}

	// Decompress with pgzip
	gzReader, err := pgzip.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("pgzip reader: %w", err)
	}
	defer gzReader.Close()

	// Write decompressed data to temp file
	tmpFile, err := os.CreateTemp(tmpDir, "mrf-*.json")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %w", err)
	}

	_, err = io.Copy(tmpFile, gzReader)
	if closeErr := tmpFile.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	if err != nil {
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("writing decompressed data: %w", err)
	}

	return &DownloadResult{
		FilePath:   tmpFile.Name(),
		TotalBytes: totalBytes,
	}, nil
}

// FileNameFromURL extracts a human-readable filename from a URL.
func FileNameFromURL(url string) string {
	// Parse path portion, ignore query params
	path := url
	if idx := len(url) - 1; idx > 0 {
		for i, c := range url {
			if c == '?' {
				path = url[:i]
				break
			}
		}
	}
	return filepath.Base(path)
}

type progressReader struct {
	reader     io.Reader
	downloaded int64
	total      int64
	callback   func(downloaded, total int64)
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.reader.Read(p)
	if n > 0 {
		pr.downloaded += int64(n)
		pr.callback(pr.downloaded, pr.total)
	}
	return n, err
}
