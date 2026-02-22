package worker

import (
	"compress/gzip"
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
	Timeout: 3 * time.Hour, // large files (50GB+) at slow CDN speeds can take over an hour
}

// DownloadResult holds the result of a download operation.
type DownloadResult struct {
	FilePath   string // path to decompressed temp file
	TotalBytes int64  // compressed size from Content-Length (or -1)
}

// DownloadHTTP performs an HTTP GET with retries and returns the response.
// Caller is responsible for closing resp.Body.
func DownloadHTTP(ctx context.Context, url string) (*http.Response, error) {
	var resp *http.Response
	var err error

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
			return resp, nil
		}
		resp.Body.Close()
		err = fmt.Errorf("HTTP %d", resp.StatusCode)
		if resp.StatusCode >= 400 && resp.StatusCode < 500 {
			return nil, err // don't retry client errors
		}
	}

	return nil, fmt.Errorf("download failed after retries: %w", err)
}

// NewGzipReader creates a gzip decompression reader. When useStdGzip is true,
// it uses the standard library's single-threaded compress/gzip (more reliable).
// Otherwise it uses pgzip (parallel, faster, but can produce mid-stream corruption
// on very large files).
func NewGzipReader(r io.Reader, useStdGzip bool) (io.ReadCloser, error) {
	if useStdGzip {
		return gzip.NewReader(r)
	}
	return pgzip.NewReader(r)
}

// DownloadAndDecompress downloads a gzipped URL, decompresses, and writes to a temp file.
// When useStdGzip is true, uses standard compress/gzip instead of pgzip for more reliable decompression.
// onProgress is called with (bytesDownloaded, totalBytes) during download.
func DownloadAndDecompress(ctx context.Context, url string, tmpDir string, useStdGzip bool, onProgress func(downloaded, total int64)) (*DownloadResult, error) {
	resp, err := DownloadHTTP(ctx, url)
	if err != nil {
		return nil, err
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

	// Count compressed bytes actually read
	countReader := &countingReader{reader: reader}

	// Decompress
	gzReader, err := NewGzipReader(countReader, useStdGzip)
	if err != nil {
		return nil, fmt.Errorf("gzip reader: %w", err)
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

	// Verify the full compressed payload was received
	if totalBytes > 0 && countReader.n != totalBytes {
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("download truncated: got %d of %d compressed bytes", countReader.n, totalBytes)
	}

	// Verify decompressed JSON is structurally intact (starts with '{', ends with '}')
	if err := verifyJSONBrackets(tmpFile.Name()); err != nil {
		os.Remove(tmpFile.Name())
		return nil, fmt.Errorf("decompression corrupt: %w", err)
	}

	return &DownloadResult{
		FilePath:   tmpFile.Name(),
		TotalBytes: totalBytes,
	}, nil
}

// StreamDecompressToPath downloads a gzipped URL, decompresses, and writes
// to the specified path. The path can be a regular file or a FIFO (named pipe).
// When useStdGzip is true, uses standard compress/gzip instead of pgzip.
// For FIFOs, this blocks on open until a reader opens the other end.
func StreamDecompressToPath(ctx context.Context, url string, destPath string, useStdGzip bool, onProgress func(downloaded, total int64)) error {
	resp, err := DownloadHTTP(ctx, url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	totalBytes := resp.ContentLength

	var reader io.Reader = resp.Body
	if onProgress != nil {
		reader = &progressReader{
			reader:   resp.Body,
			total:    totalBytes,
			callback: onProgress,
		}
	}

	countReader := &countingReader{reader: reader}

	gzReader, err := NewGzipReader(countReader, useStdGzip)
	if err != nil {
		return fmt.Errorf("gzip reader: %w", err)
	}
	defer gzReader.Close()

	// Open destination for writing. For FIFOs, this blocks until a reader opens the other end.
	f, err := os.OpenFile(destPath, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("opening dest path: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, gzReader); err != nil {
		return fmt.Errorf("writing decompressed data: %w", err)
	}

	// Verify the full compressed payload was received
	if totalBytes > 0 && countReader.n != totalBytes {
		return fmt.Errorf("download truncated: got %d of %d compressed bytes", countReader.n, totalBytes)
	}

	return nil
}

// verifyJSONBrackets checks that a file starts with '{' and ends with '}'.
// This is a cheap integrity check for MRF JSON files (always top-level objects)
// that catches truncation and mid-stream decompression corruption without
// reading the entire file.
func verifyJSONBrackets(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Check first non-whitespace byte is '{'
	var buf [1]byte
	for {
		if _, err := f.Read(buf[:]); err != nil {
			return fmt.Errorf("empty or unreadable file")
		}
		if buf[0] != ' ' && buf[0] != '\t' && buf[0] != '\n' && buf[0] != '\r' {
			break
		}
	}
	if buf[0] != '{' {
		return fmt.Errorf("file does not start with '{' (got %q)", string(buf[:]))
	}

	// Check last non-whitespace byte is '}'
	info, err := f.Stat()
	if err != nil {
		return err
	}
	// Read the last 32 bytes to find the closing brace (may have trailing whitespace/newlines)
	tailSize := int64(32)
	if info.Size() < tailSize {
		tailSize = info.Size()
	}
	tail := make([]byte, tailSize)
	if _, err := f.ReadAt(tail, info.Size()-tailSize); err != nil {
		return err
	}
	// Scan backwards for last non-whitespace byte
	for i := len(tail) - 1; i >= 0; i-- {
		b := tail[i]
		if b == ' ' || b == '\t' || b == '\n' || b == '\r' {
			continue
		}
		if b == '}' {
			return nil
		}
		return fmt.Errorf("file does not end with '}' (last non-whitespace byte: %q)", string([]byte{b}))
	}
	return fmt.Errorf("file tail is all whitespace")
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

type countingReader struct {
	reader io.Reader
	n      int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.reader.Read(p)
	cr.n += int64(n)
	return n, err
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
