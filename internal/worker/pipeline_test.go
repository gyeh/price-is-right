package worker

import (
	"compress/gzip"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gyeh/npi-rates/internal/mrf"
	"github.com/gyeh/npi-rates/internal/progress"
)

// buildTestMRF creates a realistic MRF JSON string with float provider_group_ids.
func buildTestMRF() string {
	return `{
	"reporting_entity_name": "Test Health Plan",
	"reporting_entity_type": "health_insurance_issuer",
	"provider_references": [
		{
			"provider_group_id": 302.257054942,
			"provider_groups": [
				{
					"npi": [1316924913],
					"tin": {"type": "ein", "value": "16-0960964"}
				}
			]
		},
		{
			"provider_group_id": 302.100000001,
			"provider_groups": [
				{
					"npi": [9999999999],
					"tin": {"type": "ein", "value": "99-9999999"}
				}
			]
		},
		{
			"provider_group_id": 302.300000003,
			"provider_groups": [
				{
					"npi": [1316924913, 5555555555],
					"tin": {"type": "ein", "value": "55-5555555"}
				}
			]
		}
	],
	"in_network": [
		{
			"billing_code_type": "CPT",
			"billing_code": "99213",
			"name": "Office visit, established patient",
			"negotiation_arrangement": "ffs",
			"negotiated_rates": [
				{
					"provider_references": [302.257054942],
					"negotiated_prices": [
						{
							"negotiated_rate": 125.50,
							"negotiated_type": "negotiated",
							"billing_class": "professional",
							"setting": "outpatient",
							"expiration_date": "2025-12-31",
							"service_code": ["11"],
							"billing_code_modifier": []
						},
						{
							"negotiated_rate": 140.00,
							"negotiated_type": "negotiated",
							"billing_class": "institutional",
							"setting": "inpatient",
							"expiration_date": "2025-12-31",
							"service_code": ["21"],
							"billing_code_modifier": ["26"]
						}
					]
				}
			]
		},
		{
			"billing_code_type": "CPT",
			"billing_code": "99214",
			"name": "Office visit, new patient",
			"negotiation_arrangement": "ffs",
			"negotiated_rates": [
				{
					"provider_references": [302.100000001],
					"negotiated_prices": [
						{
							"negotiated_rate": 200.00,
							"negotiated_type": "negotiated",
							"billing_class": "professional",
							"setting": "outpatient",
							"expiration_date": "2025-12-31"
						}
					]
				}
			]
		},
		{
			"billing_code_type": "HCPCS",
			"billing_code": "J0129",
			"name": "Abatacept injection",
			"negotiation_arrangement": "ffs",
			"negotiated_rates": [
				{
					"provider_references": [302.300000003],
					"negotiated_prices": [
						{
							"negotiated_rate": 50.00,
							"negotiated_type": "negotiated",
							"billing_class": "professional",
							"setting": "outpatient",
							"expiration_date": "2025-06-30"
						}
					]
				}
			]
		},
		{
			"billing_code_type": "CPT",
			"billing_code": "36415",
			"name": "Venipuncture",
			"negotiation_arrangement": "ffs",
			"negotiated_rates": [
				{
					"provider_groups": [
						{
							"npi": [1316924913],
							"tin": {"type": "ein", "value": "16-0960964"}
						}
					],
					"negotiated_prices": [
						{
							"negotiated_rate": 12.75,
							"negotiated_type": "negotiated",
							"billing_class": "professional",
							"setting": "outpatient",
							"expiration_date": "2025-12-31"
						}
					]
				}
			]
		}
	]
}`
}

// serveGzippedMRF starts a test HTTP server that serves the given JSON as gzipped content.
func serveGzippedMRF(t *testing.T, jsonData string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/gzip")
		gz := gzip.NewWriter(w)
		if _, err := gz.Write([]byte(jsonData)); err != nil {
			t.Errorf("failed to write gzipped response: %v", err)
			return
		}
		if err := gz.Close(); err != nil {
			t.Errorf("failed to close gzip writer: %v", err)
		}
	}))
}

// TestPipelineEndToEnd exercises the full pipeline: HTTP download → gzip decompress →
// jsplit → Phase A (provider_references with float IDs) → Phase B (in_network) → rate results.
// This validates the fix for float provider_group_id (e.g. 302.257054942) and both
// provider_references and inline provider_groups code paths.
func TestPipelineEndToEnd(t *testing.T) {
	mrfJSON := buildTestMRF()
	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	url := server.URL + "/test-mrf.json.gz"
	targetNPIs := map[int64]struct{}{1316924913: {}}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, false,
		tracker.NewTracker(0, 1, "test-mrf.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("pipeline failed: %v", result.Err)
	}

	// Expected results:
	// 1. 99213 via provider_references [302.257054942] → 2 prices (professional + institutional)
	// 2. 99214 via provider_references [302.100000001] → NPI 9999999999, NO match
	// 3. J0129 via provider_references [302.300000003] → NPI 1316924913 matched, 1 price
	// 4. 36415 via inline provider_groups → NPI 1316924913, 1 price
	// Total: 2 + 0 + 1 + 1 = 4 results
	if len(result.Results) != 4 {
		for i, r := range result.Results {
			t.Logf("  result[%d]: code=%s rate=%.2f NPI=%d TIN=%s", i, r.BillingCode, r.NegotiatedRate, r.NPI, r.TIN.Value)
		}
		t.Fatalf("expected 4 results, got %d", len(result.Results))
	}

	// Validate each result
	resultsByCode := map[string][]mrf.RateResult{}
	for _, r := range result.Results {
		resultsByCode[r.BillingCode] = append(resultsByCode[r.BillingCode], r)
	}

	// 99213: 2 prices via float provider_group_id 302.257054942
	if rates, ok := resultsByCode["99213"]; !ok {
		t.Error("missing results for billing code 99213")
	} else {
		if len(rates) != 2 {
			t.Errorf("expected 2 results for 99213, got %d", len(rates))
		}
		for _, r := range rates {
			if r.NPI != 1316924913 {
				t.Errorf("99213: expected NPI 1316924913, got %d", r.NPI)
			}
			if r.TIN.Value != "16-0960964" {
				t.Errorf("99213: expected TIN 16-0960964, got %s", r.TIN.Value)
			}
			if r.BillingCodeType != "CPT" {
				t.Errorf("99213: expected CPT, got %s", r.BillingCodeType)
			}
			if r.BillingCodeDescription != "Office visit, established patient" {
				t.Errorf("99213: expected description 'Office visit, established patient', got %s", r.BillingCodeDescription)
			}
		}
		// Check both price variants exist
		rateSet := map[float64]bool{}
		for _, r := range rates {
			rateSet[r.NegotiatedRate] = true
		}
		if !rateSet[125.50] {
			t.Error("99213: missing rate 125.50")
		}
		if !rateSet[140.00] {
			t.Error("99213: missing rate 140.00")
		}
	}

	// 99214: should NOT appear (NPI 9999999999 doesn't match)
	if rates, ok := resultsByCode["99214"]; ok {
		t.Errorf("expected no results for 99214, got %d", len(rates))
	}

	// J0129: 1 result via float provider_group_id 302.300000003
	if rates, ok := resultsByCode["J0129"]; !ok {
		t.Error("missing results for billing code J0129")
	} else {
		if len(rates) != 1 {
			t.Errorf("expected 1 result for J0129, got %d", len(rates))
		}
		r := rates[0]
		if r.NPI != 1316924913 {
			t.Errorf("J0129: expected NPI 1316924913, got %d", r.NPI)
		}
		if r.NegotiatedRate != 50.00 {
			t.Errorf("J0129: expected rate 50.00, got %.2f", r.NegotiatedRate)
		}
		if r.TIN.Value != "55-5555555" {
			t.Errorf("J0129: expected TIN 55-5555555, got %s", r.TIN.Value)
		}
	}

	// 36415: 1 result via inline provider_groups
	if rates, ok := resultsByCode["36415"]; !ok {
		t.Error("missing results for billing code 36415")
	} else {
		if len(rates) != 1 {
			t.Errorf("expected 1 result for 36415, got %d", len(rates))
		}
		r := rates[0]
		if r.NPI != 1316924913 {
			t.Errorf("36415: expected NPI 1316924913, got %d", r.NPI)
		}
		if r.NegotiatedRate != 12.75 {
			t.Errorf("36415: expected rate 12.75, got %.2f", r.NegotiatedRate)
		}
		if r.TIN.Value != "16-0960964" {
			t.Errorf("36415: expected TIN 16-0960964, got %s", r.TIN.Value)
		}
	}

	// Verify source URL is set on all results
	for i, r := range result.Results {
		if r.SourceFile != url {
			t.Errorf("result[%d]: expected source_file=%s, got %s", i, url, r.SourceFile)
		}
	}
}

// TestPipelineEndToEnd_NoMatch verifies the pipeline correctly returns zero results
// when no NPIs match any providers in the file.
func TestPipelineEndToEnd_NoMatch(t *testing.T) {
	mrfJSON := buildTestMRF()
	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	url := server.URL + "/test-mrf.json.gz"
	targetNPIs := map[int64]struct{}{1111111111: {}} // NPI not in the file
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, false,
		tracker.NewTracker(0, 1, "test-mrf.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("pipeline failed: %v", result.Err)
	}

	if len(result.Results) != 0 {
		t.Errorf("expected 0 results, got %d", len(result.Results))
	}
}

// TestPipelineEndToEnd_MultipleNPIs verifies the pipeline handles multiple target NPIs.
func TestPipelineEndToEnd_MultipleNPIs(t *testing.T) {
	mrfJSON := buildTestMRF()
	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	url := server.URL + "/test-mrf.json.gz"
	// Target both NPIs that exist in the file
	targetNPIs := map[int64]struct{}{
		1316924913: {},
		9999999999: {},
	}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, false,
		tracker.NewTracker(0, 1, "test-mrf.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("pipeline failed: %v", result.Err)
	}

	// With both NPIs:
	// 99213: 2 prices × NPI 1316924913 = 2
	// 99214: 1 price × NPI 9999999999 = 1
	// J0129: 1 price × 2 NPIs (1316924913 + 5555555555 but only 1316924913 matches) = 1
	//        Wait — 5555555555 is NOT in targetNPIs. So only 1316924913 matches = 1
	// 36415: 1 price × NPI 1316924913 = 1
	// Total: 2 + 1 + 1 + 1 = 5
	if len(result.Results) != 5 {
		t.Errorf("expected 5 results, got %d", len(result.Results))
		for i, r := range result.Results {
			t.Logf("  result[%d]: code=%s rate=%.2f NPI=%d", i, r.BillingCode, r.NegotiatedRate, r.NPI)
		}
	}

	// Verify 99214 now appears for NPI 9999999999
	found99214 := false
	for _, r := range result.Results {
		if r.BillingCode == "99214" {
			found99214 = true
			if r.NPI != 9999999999 {
				t.Errorf("99214: expected NPI 9999999999, got %d", r.NPI)
			}
			if r.NegotiatedRate != 200.00 {
				t.Errorf("99214: expected rate 200.00, got %.2f", r.NegotiatedRate)
			}
		}
	}
	if !found99214 {
		t.Error("expected billing code 99214 to appear for NPI 9999999999")
	}
}

// TestPoolEndToEnd tests the pool processing multiple URLs concurrently.
func TestPoolEndToEnd(t *testing.T) {
	mrfJSON := buildTestMRF()
	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	// Create 3 URLs pointing to the same test server
	urls := []string{
		server.URL + "/file1.json.gz",
		server.URL + "/file2.json.gz",
		server.URL + "/file3.json.gz",
	}

	pool := &Pool{
		Workers:    2,
		TargetNPIs: map[int64]struct{}{1316924913: {}},
		TmpDir:     t.TempDir(),
		Progress:   &progress.NoopManager{},
	}

	results := pool.Run(context.Background(), urls)

	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	totalRates := 0
	for i, r := range results {
		if r.Err != nil {
			t.Errorf("file %d failed: %v", i, r.Err)
			continue
		}
		// Each file should produce 4 results (same MRF content)
		if len(r.Results) != 4 {
			t.Errorf("file %d: expected 4 results, got %d", i, len(r.Results))
		}
		totalRates += len(r.Results)

		// Verify URL is different for each result set
		expectedURL := urls[i]
		for _, rate := range r.Results {
			if rate.SourceFile != expectedURL {
				t.Errorf("file %d: expected source_file=%s, got %s", i, expectedURL, rate.SourceFile)
				break
			}
		}
	}

	if totalRates != 12 {
		t.Errorf("expected 12 total rates across 3 files, got %d", totalRates)
	}
}

// TestPipelineEndToEnd_ContextCancellation verifies the pipeline exits cleanly on cancellation.
func TestPipelineEndToEnd_ContextCancellation(t *testing.T) {
	// Serve a response that hangs to simulate a slow download
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Never respond — context cancellation should unblock the pipeline
		<-r.Context().Done()
	}))
	defer server.Close()

	url := server.URL + "/slow.json.gz"
	targetNPIs := map[int64]struct{}{1316924913: {}}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	result := RunPipeline(
		ctx,
		url,
		targetNPIs,
		tmpDir,
		false, false,
		tracker.NewTracker(0, 1, "slow.json.gz"),
	)

	if result.Err == nil {
		t.Error("expected error from cancelled context, got nil")
	}
	if result.Err != context.Canceled {
		// The error might be wrapped, so just check it's not nil
		t.Logf("got error: %v (expected context.Canceled or wrapped variant)", result.Err)
	}
}

// TestPipelineEndToEnd_FloatProviderGroupID specifically validates that float
// provider_group_ids like 302.257054942 are correctly matched between
// provider_references and in_network sections.
func TestPipelineEndToEnd_FloatProviderGroupID(t *testing.T) {
	// Use IDs that would be different when truncated to int
	mrfJSON := `{
	"reporting_entity_name": "Float ID Test",
	"provider_references": [
		{
			"provider_group_id": 42.123456789,
			"provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}]
		},
		{
			"provider_group_id": 42.987654321,
			"provider_groups": [{"npi": [9876543210], "tin": {"type": "ein", "value": "98-7654321"}}]
		}
	],
	"in_network": [
		{
			"billing_code_type": "CPT", "billing_code": "99213",
			"name": "Test code A", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [42.123456789],
				"negotiated_prices": [{"negotiated_rate": 100.00, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}]
			}]
		},
		{
			"billing_code_type": "CPT", "billing_code": "99214",
			"name": "Test code B", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [42.987654321],
				"negotiated_prices": [{"negotiated_rate": 200.00, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}]
			}]
		}
	]
}`

	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	// Only target NPI 1234567890 — should only match 42.123456789, NOT 42.987654321
	url := server.URL + "/float-test.json.gz"
	targetNPIs := map[int64]struct{}{1234567890: {}}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, false,
		tracker.NewTracker(0, 1, "float-test.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("pipeline failed: %v", result.Err)
	}

	// Should only get 99213 (via 42.123456789), NOT 99214 (via 42.987654321)
	// If int truncation happened, both would map to 42 and we'd get both
	if len(result.Results) != 1 {
		for i, r := range result.Results {
			t.Logf("  result[%d]: code=%s rate=%.2f", i, r.BillingCode, r.NegotiatedRate)
		}
		t.Fatalf("expected 1 result (float IDs correctly distinguished), got %d", len(result.Results))
	}

	r := result.Results[0]
	if r.BillingCode != "99213" {
		t.Errorf("expected billing code 99213, got %s", r.BillingCode)
	}
	if r.NegotiatedRate != 100.00 {
		t.Errorf("expected rate 100.00, got %.2f", r.NegotiatedRate)
	}
	if r.NPI != 1234567890 {
		t.Errorf("expected NPI 1234567890, got %d", r.NPI)
	}
}

// TestPipelineEndToEnd_ServiceCodeAndModifiers validates that service_code and
// billing_code_modifier arrays are correctly extracted.
func TestPipelineEndToEnd_ServiceCodeAndModifiers(t *testing.T) {
	mrfJSON := buildTestMRF()
	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	url := server.URL + "/test-mrf.json.gz"
	targetNPIs := map[int64]struct{}{1316924913: {}}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, false,
		tracker.NewTracker(0, 1, "test-mrf.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("pipeline failed: %v", result.Err)
	}

	// Find the 99213 institutional/inpatient result which has service_code and modifier
	for _, r := range result.Results {
		if r.BillingCode == "99213" && r.BillingClass == "institutional" {
			if len(r.ServiceCode) != 1 || r.ServiceCode[0] != "21" {
				t.Errorf("expected service_code [21], got %v", r.ServiceCode)
			}
			if len(r.BillingCodeModifier) != 1 || r.BillingCodeModifier[0] != "26" {
				t.Errorf("expected billing_code_modifier [26], got %v", r.BillingCodeModifier)
			}
			if r.Setting != "inpatient" {
				t.Errorf("expected setting inpatient, got %s", r.Setting)
			}
			return
		}
	}
	t.Error("99213 institutional result not found")
}

// TestStreamPipelineEndToEnd exercises the streaming pipeline (--stream mode):
// HTTP download → gzip decompress → json.Decoder streaming → rate results.
// This should produce identical results to the non-streaming pipeline.
func TestStreamPipelineEndToEnd(t *testing.T) {
	mrfJSON := buildTestMRF()
	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	url := server.URL + "/test-mrf.json.gz"
	targetNPIs := map[int64]struct{}{1316924913: {}}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, true, // stream=true
		tracker.NewTracker(0, 1, "test-mrf.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("streaming pipeline failed: %v", result.Err)
	}

	// Same expected results as TestPipelineEndToEnd: 4 results
	if len(result.Results) != 4 {
		for i, r := range result.Results {
			t.Logf("  result[%d]: code=%s rate=%.2f NPI=%d TIN=%s", i, r.BillingCode, r.NegotiatedRate, r.NPI, r.TIN.Value)
		}
		t.Fatalf("expected 4 results, got %d", len(result.Results))
	}

	resultsByCode := map[string][]mrf.RateResult{}
	for _, r := range result.Results {
		resultsByCode[r.BillingCode] = append(resultsByCode[r.BillingCode], r)
	}

	// 99213: 2 prices via provider_references
	if rates := resultsByCode["99213"]; len(rates) != 2 {
		t.Errorf("expected 2 results for 99213, got %d", len(rates))
	}

	// 99214: should NOT appear
	if rates := resultsByCode["99214"]; len(rates) != 0 {
		t.Errorf("expected 0 results for 99214, got %d", len(rates))
	}

	// J0129: 1 result via provider_references
	if rates := resultsByCode["J0129"]; len(rates) != 1 {
		t.Errorf("expected 1 result for J0129, got %d", len(rates))
	}

	// 36415: 1 result via inline provider_groups
	if rates := resultsByCode["36415"]; len(rates) != 1 {
		t.Errorf("expected 1 result for 36415, got %d", len(rates))
	}
}

// TestStreamPipelineEndToEnd_NoMatch verifies the streaming pipeline returns
// zero results when no NPIs match.
func TestStreamPipelineEndToEnd_NoMatch(t *testing.T) {
	mrfJSON := buildTestMRF()
	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	url := server.URL + "/test-mrf.json.gz"
	targetNPIs := map[int64]struct{}{1111111111: {}}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, true,
		tracker.NewTracker(0, 1, "test-mrf.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("streaming pipeline failed: %v", result.Err)
	}
	if len(result.Results) != 0 {
		t.Errorf("expected 0 results, got %d", len(result.Results))
	}
}

// TestStreamPipelineEndToEnd_FloatIDs validates that streaming mode correctly
// distinguishes float provider_group_ids.
func TestStreamPipelineEndToEnd_FloatIDs(t *testing.T) {
	mrfJSON := `{
	"reporting_entity_name": "Float ID Test",
	"provider_references": [
		{
			"provider_group_id": 42.123456789,
			"provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}]
		},
		{
			"provider_group_id": 42.987654321,
			"provider_groups": [{"npi": [9876543210], "tin": {"type": "ein", "value": "98-7654321"}}]
		}
	],
	"in_network": [
		{
			"billing_code_type": "CPT", "billing_code": "99213",
			"name": "Test code A", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [42.123456789],
				"negotiated_prices": [{"negotiated_rate": 100.00, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}]
			}]
		},
		{
			"billing_code_type": "CPT", "billing_code": "99214",
			"name": "Test code B", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [42.987654321],
				"negotiated_prices": [{"negotiated_rate": 200.00, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}]
			}]
		}
	]
}`

	server := serveGzippedMRF(t, mrfJSON)
	defer server.Close()

	url := server.URL + "/float-test.json.gz"
	targetNPIs := map[int64]struct{}{1234567890: {}}
	tmpDir := t.TempDir()
	tracker := &progress.NoopManager{}

	result := RunPipeline(
		context.Background(),
		url,
		targetNPIs,
		tmpDir,
		false, true,
		tracker.NewTracker(0, 1, "float-test.json.gz"),
	)

	if result.Err != nil {
		t.Fatalf("streaming pipeline failed: %v", result.Err)
	}

	if len(result.Results) != 1 {
		for i, r := range result.Results {
			t.Logf("  result[%d]: code=%s rate=%.2f", i, r.BillingCode, r.NegotiatedRate)
		}
		t.Fatalf("expected 1 result, got %d", len(result.Results))
	}

	if result.Results[0].BillingCode != "99213" {
		t.Errorf("expected billing code 99213, got %s", result.Results[0].BillingCode)
	}
}

