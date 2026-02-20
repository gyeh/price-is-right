package mrf

import (
	"strings"
	"testing"
)

func TestStreamParse_BasicMRF(t *testing.T) {
	mrfJSON := `{
	"reporting_entity_name": "Test Health Plan",
	"provider_references": [
		{
			"provider_group_id": 1,
			"provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}]
		},
		{
			"provider_group_id": 2,
			"provider_groups": [{"npi": [9999999999], "tin": {"type": "ein", "value": "99-9999999"}}]
		}
	],
	"in_network": [
		{
			"billing_code_type": "CPT", "billing_code": "99213",
			"name": "Office visit", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [1],
				"negotiated_prices": [{
					"negotiated_rate": 125.50, "negotiated_type": "negotiated",
					"billing_class": "professional", "setting": "outpatient",
					"expiration_date": "2025-12-31"
				}]
			}]
		},
		{
			"billing_code_type": "CPT", "billing_code": "99214",
			"name": "No match code", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [2],
				"negotiated_prices": [{
					"negotiated_rate": 200.00, "negotiated_type": "negotiated",
					"billing_class": "professional", "setting": "outpatient",
					"expiration_date": "2025-12-31"
				}]
			}]
		}
	]
}`

	targetNPIs := map[int64]struct{}{1234567890: {}}
	var results []RateResult
	var refsScanned, codesScanned int
	var stages []string

	err := StreamParse(
		strings.NewReader(mrfJSON),
		targetNPIs,
		"test-source.json.gz",
		StreamCallbacks{
			OnRefScanned:  func() { refsScanned++ },
			OnCodeScanned: func() { codesScanned++ },
			OnStageChange: func(stage string) { stages = append(stages, stage) },
		},
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatalf("StreamParse failed: %v", err)
	}

	if refsScanned != 2 {
		t.Errorf("expected 2 refs scanned, got %d", refsScanned)
	}
	if codesScanned != 2 {
		t.Errorf("expected 2 codes scanned, got %d", codesScanned)
	}
	if len(stages) != 2 {
		t.Errorf("expected 2 stage changes, got %d: %v", len(stages), stages)
	}

	// Should only match 99213 (group 1 → NPI 1234567890)
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
	if results[0].SourceFile != "test-source.json.gz" {
		t.Errorf("expected source test-source.json.gz, got %s", results[0].SourceFile)
	}
}

func TestStreamParse_InlineProviderGroups(t *testing.T) {
	mrfJSON := `{
	"reporting_entity_name": "Test",
	"provider_references": [],
	"in_network": [
		{
			"billing_code_type": "CPT", "billing_code": "36415",
			"name": "Venipuncture", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
				"negotiated_prices": [{
					"negotiated_rate": 12.75, "negotiated_type": "negotiated",
					"billing_class": "professional", "setting": "outpatient",
					"expiration_date": "2025-12-31"
				}]
			}]
		}
	]
}`

	targetNPIs := map[int64]struct{}{1234567890: {}}
	var results []RateResult

	err := StreamParse(
		strings.NewReader(mrfJSON),
		targetNPIs,
		"test.json.gz",
		StreamCallbacks{},
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatalf("StreamParse failed: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].BillingCode != "36415" {
		t.Errorf("expected 36415, got %s", results[0].BillingCode)
	}
	if results[0].NegotiatedRate != 12.75 {
		t.Errorf("expected 12.75, got %f", results[0].NegotiatedRate)
	}
}

func TestStreamParse_InNetworkBeforeProviderRefs(t *testing.T) {
	// in_network appears before provider_references — should still work
	// but only inline provider_groups will match
	mrfJSON := `{
	"reporting_entity_name": "Test",
	"in_network": [
		{
			"billing_code_type": "CPT", "billing_code": "99213",
			"name": "Office visit", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [1],
				"negotiated_prices": [{
					"negotiated_rate": 100.00, "negotiated_type": "negotiated",
					"billing_class": "professional", "setting": "outpatient",
					"expiration_date": "2025-12-31"
				}]
			}]
		},
		{
			"billing_code_type": "CPT", "billing_code": "36415",
			"name": "Venipuncture", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}],
				"negotiated_prices": [{
					"negotiated_rate": 12.75, "negotiated_type": "negotiated",
					"billing_class": "professional", "setting": "outpatient",
					"expiration_date": "2025-12-31"
				}]
			}]
		}
	],
	"provider_references": [
		{
			"provider_group_id": 1,
			"provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}]
		}
	]
}`

	targetNPIs := map[int64]struct{}{1234567890: {}}
	var results []RateResult
	var warnings []string

	err := StreamParse(
		strings.NewReader(mrfJSON),
		targetNPIs,
		"test.json.gz",
		StreamCallbacks{
			OnWarning: func(msg string) { warnings = append(warnings, msg) },
		},
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatalf("StreamParse failed: %v", err)
	}

	// Should warn about order
	if len(warnings) != 1 {
		t.Errorf("expected 1 warning, got %d", len(warnings))
	}

	// Only inline provider_groups should match (36415), not provider_references (99213)
	if len(results) != 1 {
		t.Fatalf("expected 1 result (inline only), got %d", len(results))
	}
	if results[0].BillingCode != "36415" {
		t.Errorf("expected 36415, got %s", results[0].BillingCode)
	}
}

func TestStreamParse_NoMatches(t *testing.T) {
	mrfJSON := `{
	"reporting_entity_name": "Test",
	"provider_references": [
		{"provider_group_id": 1, "provider_groups": [{"npi": [9999999999], "tin": {"type": "ein", "value": "99-9999999"}}]}
	],
	"in_network": [
		{
			"billing_code_type": "CPT", "billing_code": "99213",
			"name": "Office visit", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{
				"provider_references": [1],
				"negotiated_prices": [{
					"negotiated_rate": 100.00, "negotiated_type": "negotiated",
					"billing_class": "professional", "setting": "outpatient",
					"expiration_date": "2025-12-31"
				}]
			}]
		}
	]
}`

	targetNPIs := map[int64]struct{}{1234567890: {}}
	var results []RateResult

	err := StreamParse(
		strings.NewReader(mrfJSON),
		targetNPIs,
		"test.json.gz",
		StreamCallbacks{},
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatalf("StreamParse failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestStreamParse_SkipsUnknownKeys(t *testing.T) {
	mrfJSON := `{
	"reporting_entity_name": "Test Health Plan",
	"reporting_entity_type": "health_insurance_issuer",
	"last_updated_on": "2025-01-15",
	"version": "1.0",
	"provider_references": [],
	"in_network": []
}`

	targetNPIs := map[int64]struct{}{1234567890: {}}
	var results []RateResult

	err := StreamParse(
		strings.NewReader(mrfJSON),
		targetNPIs,
		"test.json.gz",
		StreamCallbacks{},
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatalf("StreamParse failed: %v", err)
	}

	if len(results) != 0 {
		t.Errorf("expected 0 results, got %d", len(results))
	}
}

func TestStreamParse_FloatProviderGroupIDs(t *testing.T) {
	mrfJSON := `{
	"provider_references": [
		{"provider_group_id": 42.123456789, "provider_groups": [{"npi": [1234567890], "tin": {"type": "ein", "value": "12-3456789"}}]},
		{"provider_group_id": 42.987654321, "provider_groups": [{"npi": [9876543210], "tin": {"type": "ein", "value": "98-7654321"}}]}
	],
	"in_network": [
		{
			"billing_code_type": "CPT", "billing_code": "99213",
			"name": "Code A", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{"provider_references": [42.123456789], "negotiated_prices": [{"negotiated_rate": 100.00, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}]}]
		},
		{
			"billing_code_type": "CPT", "billing_code": "99214",
			"name": "Code B", "negotiation_arrangement": "ffs",
			"negotiated_rates": [{"provider_references": [42.987654321], "negotiated_prices": [{"negotiated_rate": 200.00, "negotiated_type": "negotiated", "billing_class": "professional", "setting": "outpatient", "expiration_date": "2025-12-31"}]}]
		}
	]
}`

	targetNPIs := map[int64]struct{}{1234567890: {}}
	var results []RateResult

	err := StreamParse(
		strings.NewReader(mrfJSON),
		targetNPIs,
		"test.json.gz",
		StreamCallbacks{},
		func(r RateResult) { results = append(results, r) },
	)
	if err != nil {
		t.Fatalf("StreamParse failed: %v", err)
	}

	if len(results) != 1 {
		for i, r := range results {
			t.Logf("  result[%d]: code=%s rate=%.2f", i, r.BillingCode, r.NegotiatedRate)
		}
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].BillingCode != "99213" {
		t.Errorf("expected 99213, got %s", results[0].BillingCode)
	}
}
