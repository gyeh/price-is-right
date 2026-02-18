package mrf

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/danielchalef/jsplit/pkg/jsplit"
)

// SplitResult holds the paths to the NDJSON files produced by jsplit.
type SplitResult struct {
	Dir                    string
	ProviderReferenceFiles []string
	InNetworkFiles         []string
}

// SplitFile splits a JSON file (optionally gzipped) into NDJSON files using jsplit.
// Returns the paths to the provider_references and in_network NDJSON files.
func SplitFile(inputPath, outputDir string) (*SplitResult, error) {
	// Suppress jsplit's stdout prints
	origStdout := os.Stdout
	devNull, err := os.Open(os.DevNull)
	if err != nil {
		return nil, fmt.Errorf("failed to open /dev/null: %w", err)
	}
	os.Stdout = devNull
	err = jsplit.Split(inputPath, outputDir, true)
	os.Stdout = origStdout
	devNull.Close()
	if err != nil {
		return nil, fmt.Errorf("jsplit split failed: %w", err)
	}

	result := &SplitResult{Dir: outputDir}

	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return nil, fmt.Errorf("failed to read split output dir: %w", err)
	}

	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".jsonl") {
			continue
		}
		fullPath := filepath.Join(outputDir, name)
		if strings.HasPrefix(name, "provider_references_") {
			result.ProviderReferenceFiles = append(result.ProviderReferenceFiles, fullPath)
		} else if strings.HasPrefix(name, "in_network_") {
			result.InNetworkFiles = append(result.InNetworkFiles, fullPath)
		}
	}

	sort.Strings(result.ProviderReferenceFiles)
	sort.Strings(result.InNetworkFiles)

	return result, nil
}
