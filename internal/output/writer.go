package output

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/gyeh/npi-rates/internal/mrf"
)

// WriteResults writes the final JSON output to the specified file.
func WriteResults(outputPath string, params mrf.SearchParams, results []mrf.RateResult) error {
	if results == nil {
		results = []mrf.RateResult{}
	}

	output := mrf.SearchOutput{
		SearchParams: params,
		Results:      results,
	}

	data, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling output: %w", err)
	}

	if outputPath == "-" {
		_, err = os.Stdout.Write(data)
		fmt.Fprintln(os.Stdout)
		return err
	}

	return os.WriteFile(outputPath, data, 0o644)
}
