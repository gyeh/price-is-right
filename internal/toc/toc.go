package toc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/gyeh/npi-rates/internal/worker"
)

// ReportingPlan represents a single plan entry within a reporting structure.
type ReportingPlan struct {
	PlanName   string `json:"plan_name"`
	PlanIDType string `json:"plan_id_type"`
	PlanID     string `json:"plan_id"`
}

// InNetworkFile represents an in-network MRF file reference in a TOC.
type InNetworkFile struct {
	Description string `json:"description"`
	Location    string `json:"location"`
}

// ResolveResult holds the output of a TOC resolution.
type ResolveResult struct {
	ReportingEntityName string
	URLs                []string // deduplicated, insertion-ordered
	MatchedStructures   int
}

// ResolveTOC streams a TOC JSON file from r and extracts in-network MRF URLs
// for any reporting_structure whose reporting_plans contain a plan matching
// planID (case-insensitive exact match on plan_id).
//
// onStructure, if non-nil, is called with the count of structures processed so far.
func ResolveTOC(r io.Reader, planID string, onStructure func(int)) (*ResolveResult, error) {
	dec := json.NewDecoder(r)

	// Expect opening '{'.
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("reading opening token: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("expected '{', got %v", tok)
	}

	result := &ResolveResult{}
	seen := map[string]struct{}{}
	planIDLower := []byte(strings.ToLower(planID))

	for dec.More() {
		// Read the key name.
		tok, err = dec.Token()
		if err != nil {
			return nil, fmt.Errorf("reading key: %w", err)
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("expected string key, got %T", tok)
		}

		switch key {
		case "reporting_entity_name":
			var name string
			if err := dec.Decode(&name); err != nil {
				return nil, fmt.Errorf("decoding reporting_entity_name: %w", err)
			}
			result.ReportingEntityName = name

		case "reporting_structure":
			if err := streamReportingStructure(dec, planID, planIDLower, result, seen, onStructure); err != nil {
				return nil, fmt.Errorf("streaming reporting_structure: %w", err)
			}

		default:
			if err := skipValue(dec); err != nil {
				return nil, fmt.Errorf("skipping key %q: %w", key, err)
			}
		}
	}

	// Expect closing '}'.
	tok, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("reading closing token: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '}' {
		return nil, fmt.Errorf("expected '}', got %v", tok)
	}

	return result, nil
}

// streamReportingStructure reads the reporting_structure array element by
// element. Each element is decoded as raw JSON, pre-filtered by planID
// substring, then fully unmarshalled only if it might match.
func streamReportingStructure(
	dec *json.Decoder,
	planID string,
	planIDLower []byte,
	result *ResolveResult,
	seen map[string]struct{},
	onStructure func(int),
) error {
	// Expect opening '['.
	tok, err := dec.Token()
	if err != nil {
		return fmt.Errorf("reading array start: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected '[', got %v", tok)
	}

	structCount := 0
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return fmt.Errorf("decoding element: %w", err)
		}

		structCount++
		if onStructure != nil {
			onStructure(structCount)
		}

		// Pre-filter: skip elements that don't contain the plan ID as substring.
		if !bytes.Contains(bytes.ToLower(raw), planIDLower) {
			continue
		}

		// Full unmarshal of matching candidate.
		var entry struct {
			ReportingPlans []ReportingPlan `json:"reporting_plans"`
			InNetworkFiles []InNetworkFile `json:"in_network_files"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			continue // skip malformed
		}

		// Check for exact case-insensitive match on plan_id.
		matched := false
		for _, plan := range entry.ReportingPlans {
			if strings.EqualFold(plan.PlanID, planID) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}

		result.MatchedStructures++

		// Collect deduplicated URLs in insertion order.
		for _, f := range entry.InNetworkFiles {
			if f.Location == "" {
				continue
			}
			if _, exists := seen[f.Location]; !exists {
				seen[f.Location] = struct{}{}
				result.URLs = append(result.URLs, f.Location)
			}
		}
	}

	// Expect closing ']'.
	tok, err = dec.Token()
	if err != nil {
		return fmt.Errorf("reading array end: %w", err)
	}

	return nil
}

// FetchAndResolve downloads a TOC file from tocURL, optionally decompresses
// gzip, and resolves in-network MRF URLs for the given planID.
func FetchAndResolve(ctx context.Context, tocURL, planID string, onProgress func(downloaded, total int64)) (*ResolveResult, error) {
	resp, err := worker.DownloadHTTP(ctx, tocURL)
	if err != nil {
		return nil, fmt.Errorf("downloading TOC: %w", err)
	}
	defer resp.Body.Close()

	var reader io.Reader = resp.Body
	if onProgress != nil {
		reader = &progressReader{
			reader:   resp.Body,
			total:    resp.ContentLength,
			callback: onProgress,
		}
	}

	// Detect gzip: check Content-Type or URL suffix.
	contentType := resp.Header.Get("Content-Type")
	isGzip := strings.Contains(contentType, "gzip") || strings.HasSuffix(strings.ToLower(tocURL), ".gz")

	if isGzip {
		gzReader, err := worker.NewGzipReader(reader, false)
		if err != nil {
			return nil, fmt.Errorf("gzip reader: %w", err)
		}
		defer gzReader.Close()
		reader = gzReader
	}

	return ResolveTOC(reader, planID, nil)
}

// skipValue reads and discards the next JSON value from the decoder.
// Handles objects, arrays, and primitive values.
func skipValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}

	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			for dec.More() {
				if _, err := dec.Token(); err != nil {
					return err
				}
				if err := skipValue(dec); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
		case '[':
			for dec.More() {
				if err := skipValue(dec); err != nil {
					return err
				}
			}
			if _, err := dec.Token(); err != nil {
				return err
			}
		}
	default:
		// Primitive value — already consumed.
	}
	return nil
}

// progressReader wraps an io.Reader and calls a callback with download progress.
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
