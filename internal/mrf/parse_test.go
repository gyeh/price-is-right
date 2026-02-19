package mrf

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func writeTestFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestParseProviderReferences_MatchesByNPI(t *testing.T) {
	dir := t.TempDir()

	// Two provider references, one matching NPI 1234567890
	ndjson := `{"provider_group_id":1,"provider_groups":[{"npi":[1234567890],"tin":{"type":"ein","value":"12-3456789"}}]}
{"provider_group_id":2,"provider_groups":[{"npi":[9999999999],"tin":{"type":"ein","value":"99-9999999"}}]}
{"provider_group_id":3,"provider_groups":[{"npi":[1234567890,5555555555],"tin":{"type":"ein","value":"55-5555555"}}]}`

	f := writeTestFile(t, dir, "provider_references_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	var scanned int
	matched, err := ParseProviderReferences([]string{f}, targetNPIs, func() { scanned++ })
	if err != nil {
		t.Fatal(err)
	}

	if scanned != 3 {
		t.Errorf("expected 3 refs scanned, got %d", scanned)
	}

	// Should match group 1 and group 3
	if len(matched.ByGroupID) != 2 {
		t.Errorf("expected 2 matched groups, got %d", len(matched.ByGroupID))
	}

	infos1, ok := matched.ByGroupID[1]
	if !ok || len(infos1) != 1 {
		t.Errorf("expected group 1 to have 1 match, got %v", infos1)
	} else if infos1[0].NPI != 1234567890 {
		t.Errorf("expected NPI 1234567890, got %d", infos1[0].NPI)
	}

	infos3, ok := matched.ByGroupID[3]
	if !ok || len(infos3) != 1 {
		t.Errorf("expected group 3 to have 1 match, got %v", infos3)
	}
}

func TestParseProviderReferences_NoMatches(t *testing.T) {
	dir := t.TempDir()

	ndjson := `{"provider_group_id":1,"provider_groups":[{"npi":[9999999999],"tin":{"type":"ein","value":"99-9999999"}}]}`
	f := writeTestFile(t, dir, "provider_references_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	matched, err := ParseProviderReferences([]string{f}, targetNPIs, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(matched.ByGroupID) != 0 {
		t.Errorf("expected 0 matched groups, got %d", len(matched.ByGroupID))
	}
}

func TestParseInNetwork_ViaProviderReferences(t *testing.T) {
	dir := t.TempDir()

	ndjson := `{"billing_code_type":"CPT","billing_code":"99213","name":"Office visit","negotiation_arrangement":"ffs","negotiated_rates":[{"provider_references":[1],"negotiated_prices":[{"negotiated_rate":125.50,"negotiated_type":"negotiated","billing_class":"professional","setting":"outpatient","expiration_date":"2025-12-31"}]}]}`
	f := writeTestFile(t, dir, "in_network_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	matchedProviders := &MatchedProviders{
		ByGroupID: map[float64][]ProviderInfo{
			1: {{NPI: 1234567890, TIN: TIN{Type: "ein", Value: "12-3456789"}}},
		},
	}

	var results []RateResult
	var scanned int

	err := ParseInNetwork(
		[]string{f},
		targetNPIs,
		matchedProviders,
		"https://example.com/test.json.gz",
		func() { scanned++ },
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatal(err)
	}

	if scanned != 1 {
		t.Errorf("expected 1 code scanned, got %d", scanned)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	r := results[0]
	if r.NPI != 1234567890 {
		t.Errorf("expected NPI 1234567890, got %d", r.NPI)
	}
	if r.BillingCode != "99213" {
		t.Errorf("expected billing code 99213, got %s", r.BillingCode)
	}
	if r.NegotiatedRate != 125.50 {
		t.Errorf("expected rate 125.50, got %f", r.NegotiatedRate)
	}
	if r.BillingClass != "professional" {
		t.Errorf("expected billing class professional, got %s", r.BillingClass)
	}
}

func TestParseInNetwork_InlineProviderGroups(t *testing.T) {
	dir := t.TempDir()

	// In-network item with inline provider_groups instead of provider_references
	ndjson := `{"billing_code_type":"HCPCS","billing_code":"J0129","name":"Injection","negotiation_arrangement":"ffs","negotiated_rates":[{"provider_groups":[{"npi":[1234567890],"tin":{"type":"ein","value":"12-3456789"}}],"negotiated_prices":[{"negotiated_rate":50.00,"negotiated_type":"negotiated","billing_class":"professional","setting":"outpatient","expiration_date":"2025-06-30"}]}]}`
	f := writeTestFile(t, dir, "in_network_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	// Empty matchedProviders — testing inline path only
	matchedProviders := &MatchedProviders{ByGroupID: map[float64][]ProviderInfo{}}

	var results []RateResult
	err := ParseInNetwork(
		[]string{f},
		targetNPIs,
		matchedProviders,
		"https://example.com/test.json.gz",
		nil,
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].BillingCode != "J0129" {
		t.Errorf("expected billing code J0129, got %s", results[0].BillingCode)
	}
	if results[0].NegotiatedRate != 50.0 {
		t.Errorf("expected rate 50.0, got %f", results[0].NegotiatedRate)
	}
}

func TestParseInNetwork_NoMatchSkipped(t *testing.T) {
	dir := t.TempDir()

	ndjson := `{"billing_code_type":"CPT","billing_code":"99213","name":"Office visit","negotiation_arrangement":"ffs","negotiated_rates":[{"provider_references":[99],"negotiated_prices":[{"negotiated_rate":100.00,"negotiated_type":"negotiated","billing_class":"professional","setting":"outpatient","expiration_date":"2025-12-31"}]}]}`
	f := writeTestFile(t, dir, "in_network_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	matchedProviders := &MatchedProviders{ByGroupID: map[float64][]ProviderInfo{}}

	var results []RateResult
	err := ParseInNetwork(
		[]string{f},
		targetNPIs,
		matchedProviders,
		"https://example.com/test.json.gz",
		nil,
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatal(err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestParseInNetwork_MultipleProvidersPrices(t *testing.T) {
	dir := t.TempDir()

	// One in_network item with 2 matching provider references and 2 prices = 4 results
	ndjson := `{"billing_code_type":"CPT","billing_code":"99214","name":"Office visit","negotiation_arrangement":"ffs","negotiated_rates":[{"provider_references":[1,2],"negotiated_prices":[{"negotiated_rate":150.00,"negotiated_type":"negotiated","billing_class":"professional","setting":"outpatient","expiration_date":"2025-12-31"},{"negotiated_rate":175.00,"negotiated_type":"negotiated","billing_class":"institutional","setting":"inpatient","expiration_date":"2025-12-31"}]}]}`
	f := writeTestFile(t, dir, "in_network_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1111111111: {}, 2222222222: {}}
	matchedProviders := &MatchedProviders{
		ByGroupID: map[float64][]ProviderInfo{
			1: {{NPI: 1111111111, TIN: TIN{Type: "ein", Value: "11-1111111"}}},
			2: {{NPI: 2222222222, TIN: TIN{Type: "ein", Value: "22-2222222"}}},
		},
	}

	var results []RateResult
	err := ParseInNetwork(
		[]string{f},
		targetNPIs,
		matchedProviders,
		"https://example.com/test.json.gz",
		nil,
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatal(err)
	}

	// 2 providers × 2 prices = 4 results
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
}

func TestSplitFile(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "output")

	// Create a minimal MRF-like JSON file
	mrfJSON := `{
		"reporting_entity_name": "Test Insurer",
		"provider_references": [
			{"provider_group_id": 1, "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}]}
		],
		"in_network": [
			{"billing_code_type": "CPT", "billing_code": "99213", "name": "Office visit", "negotiation_arrangement": "ffs", "negotiated_rates": [{"provider_references": [1], "negotiated_prices": [{"negotiated_rate": 125.50, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}]}]}
		]
	}`

	inputFile := filepath.Join(dir, "test.json")
	if err := os.WriteFile(inputFile, []byte(mrfJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	result, err := SplitFile(inputFile, outputDir)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.ProviderReferenceFiles) != 1 {
		t.Errorf("expected 1 provider_references file, got %d", len(result.ProviderReferenceFiles))
	}
	if len(result.InNetworkFiles) != 1 {
		t.Errorf("expected 1 in_network file, got %d", len(result.InNetworkFiles))
	}

	// Verify content of provider_references file
	if len(result.ProviderReferenceFiles) > 0 {
		data, err := os.ReadFile(result.ProviderReferenceFiles[0])
		if err != nil {
			t.Fatal(err)
		}
		if len(data) == 0 {
			t.Error("provider_references file is empty")
		}
	}
}

// TestEndToEnd tests the full split → parse pipeline with a small JSON file.
func TestEndToEnd(t *testing.T) {
	dir := t.TempDir()
	outputDir := filepath.Join(dir, "split")

	mrfJSON := `{
		"reporting_entity_name": "Test Insurer",
		"provider_references": [
			{"provider_group_id": 10, "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}]},
			{"provider_group_id": 20, "provider_groups": [{"npi": [9999999999], "tin": {"type": "ein", "value": "99-9999999"}}]}
		],
		"in_network": [
			{
				"billing_code_type": "CPT", "billing_code": "99213", "name": "Office visit, established",
				"negotiation_arrangement": "ffs",
				"negotiated_rates": [
					{
						"provider_references": [10],
						"negotiated_prices": [
							{"negotiated_rate": 125.50, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}
						]
					}
				]
			},
			{
				"billing_code_type": "CPT", "billing_code": "99214", "name": "Office visit, new",
				"negotiation_arrangement": "ffs",
				"negotiated_rates": [
					{
						"provider_references": [20],
						"negotiated_prices": [
							{"negotiated_rate": 200.00, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}
						]
					}
				]
			}
		]
	}`

	inputFile := filepath.Join(dir, "test.json")
	if err := os.WriteFile(inputFile, []byte(mrfJSON), 0o644); err != nil {
		t.Fatal(err)
	}

	// Step 1: Split
	splitResult, err := SplitFile(inputFile, outputDir)
	if err != nil {
		t.Fatal(err)
	}

	// Step 2: Parse provider references
	targetNPIs := map[int64]struct{}{1234567890: {}}
	matched, err := ParseProviderReferences(splitResult.ProviderReferenceFiles, targetNPIs, nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(matched.ByGroupID) != 1 {
		t.Fatalf("expected 1 matched group, got %d", len(matched.ByGroupID))
	}

	// Step 3: Parse in_network
	var results []RateResult
	err = ParseInNetwork(
		splitResult.InNetworkFiles,
		targetNPIs,
		matched,
		"test-source",
		nil,
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatal(err)
	}

	// Should only match 99213 (group 10 = NPI 1234567890), not 99214 (group 20 = NPI 9999999999)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].BillingCode != "99213" {
		t.Errorf("expected billing code 99213, got %s", results[0].BillingCode)
	}
	if results[0].NegotiatedRate != 125.50 {
		t.Errorf("expected rate 125.50, got %f", results[0].NegotiatedRate)
	}
	if results[0].NPI != 1234567890 {
		t.Errorf("expected NPI 1234567890, got %d", results[0].NPI)
	}
}

func TestParserName(t *testing.T) {
	name := ParserName()
	if runtime.GOARCH == "amd64" {
		// On amd64, simdjson may or may not be available depending on CPU features
		t.Logf("Parser on amd64: %s (useSimd=%v)", name, useSimd)
	} else {
		// On non-amd64 (arm64, etc.), simdjson is never available
		if useSimd {
			t.Errorf("useSimd should be false on %s, got true", runtime.GOARCH)
		}
		if name != "encoding/json (standard)" {
			t.Errorf("expected stdlib parser on %s, got %s", runtime.GOARCH, name)
		}
		t.Logf("Parser on %s: %s (correctly using stdlib fallback)", runtime.GOARCH, name)
	}
}

// TestStdlibProviderRefsDirectly tests the stdlib path explicitly.
func TestStdlibProviderRefsDirectly(t *testing.T) {
	dir := t.TempDir()
	ndjson := `{"provider_group_id":1,"provider_groups":[{"npi":[1234567890],"tin":{"type":"ein","value":"12-3456789"}}]}`
	f := writeTestFile(t, dir, "provider_references_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	matched := &MatchedProviders{ByGroupID: make(map[float64][]ProviderInfo)}

	patterns := npiBytePatterns(targetNPIs)
	err := scanProviderRefFileStdlib(f, targetNPIs, patterns, matched, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(matched.ByGroupID) != 1 {
		t.Errorf("stdlib: expected 1 matched group, got %d", len(matched.ByGroupID))
	}
}

// TestSimdProviderRefsDirectly tests the simdjson path explicitly (skipped if CPU unsupported).
func TestSimdProviderRefsDirectly(t *testing.T) {
	if !useSimd {
		t.Skipf("simdjson not available on %s (requires AVX2+CLMUL)", runtime.GOARCH)
	}

	dir := t.TempDir()
	ndjson := `{"provider_group_id":1,"provider_groups":[{"npi":[1234567890],"tin":{"type":"ein","value":"12-3456789"}}]}`
	f := writeTestFile(t, dir, "provider_references_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	matched := &MatchedProviders{ByGroupID: make(map[float64][]ProviderInfo)}

	patterns := npiBytePatterns(targetNPIs)
	err := scanProviderRefFileSimd(f, targetNPIs, patterns, matched, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(matched.ByGroupID) != 1 {
		t.Errorf("simd: expected 1 matched group, got %d", len(matched.ByGroupID))
	}
	infos := matched.ByGroupID[1]
	if len(infos) != 1 || infos[0].NPI != 1234567890 {
		t.Errorf("simd: expected NPI 1234567890, got %v", infos)
	}
	if infos[0].TIN.Type != "ein" || infos[0].TIN.Value != "12-3456789" {
		t.Errorf("simd: expected TIN ein/12-3456789, got %v", infos[0].TIN)
	}
}

// TestSimdInNetworkDirectly tests the simdjson in_network path explicitly.
func TestSimdInNetworkDirectly(t *testing.T) {
	if !useSimd {
		t.Skipf("simdjson not available on %s (requires AVX2+CLMUL)", runtime.GOARCH)
	}

	dir := t.TempDir()
	ndjson := `{"billing_code_type":"CPT","billing_code":"99213","name":"Office visit","negotiation_arrangement":"ffs","negotiated_rates":[{"provider_references":[1],"negotiated_prices":[{"negotiated_rate":125.50,"negotiated_type":"negotiated","billing_class":"professional","setting":"outpatient","expiration_date":"2025-12-31"}]}]}`
	f := writeTestFile(t, dir, "in_network_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	matchedProviders := &MatchedProviders{
		ByGroupID: map[float64][]ProviderInfo{
			1: {{NPI: 1234567890, TIN: TIN{Type: "ein", Value: "12-3456789"}}},
		},
	}

	var results []RateResult
	err := scanInNetworkFileSimd(f, targetNPIs, matchedProviders, "test", nil,
		func(r RateResult) { results = append(results, r) })
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("simd: expected 1 result, got %d", len(results))
	}
	if results[0].NegotiatedRate != 125.50 {
		t.Errorf("simd: expected rate 125.50, got %f", results[0].NegotiatedRate)
	}
}

// TestSimdInNetworkInlineProviderGroups tests simdjson with inline provider_groups.
func TestSimdInNetworkInlineProviderGroups(t *testing.T) {
	if !useSimd {
		t.Skipf("simdjson not available on %s (requires AVX2+CLMUL)", runtime.GOARCH)
	}

	dir := t.TempDir()
	ndjson := `{"billing_code_type":"HCPCS","billing_code":"J0129","name":"Injection","negotiation_arrangement":"ffs","negotiated_rates":[{"provider_groups":[{"npi":[1234567890],"tin":{"type":"ein","value":"12-3456789"}}],"negotiated_prices":[{"negotiated_rate":50.00,"negotiated_type":"negotiated","billing_class":"professional","setting":"outpatient","expiration_date":"2025-06-30"}]}]}`
	f := writeTestFile(t, dir, "in_network_00.jsonl", ndjson)

	targetNPIs := map[int64]struct{}{1234567890: {}}
	matchedProviders := &MatchedProviders{ByGroupID: map[float64][]ProviderInfo{}}

	var results []RateResult
	err := scanInNetworkFileSimd(f, targetNPIs, matchedProviders, "test", nil,
		func(r RateResult) { results = append(results, r) })
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Fatalf("simd: expected 1 result, got %d", len(results))
	}
	if results[0].BillingCode != "J0129" {
		t.Errorf("simd: expected J0129, got %s", results[0].BillingCode)
	}
}
