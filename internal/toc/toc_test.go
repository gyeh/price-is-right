package toc

import (
	"strings"
	"testing"
)

func TestResolveTOC_BasicMatch(t *testing.T) {
	tocJSON := `{
	"reporting_entity_name": "Test Health Plan",
	"reporting_structure": [
		{
			"reporting_plans": [
				{"plan_name": "Gold Plan", "plan_id_type": "HIOS", "plan_id": "12345"}
			],
			"in_network_files": [
				{"description": "In-Network File 1", "location": "https://example.com/mrf1.json.gz"},
				{"description": "In-Network File 2", "location": "https://example.com/mrf2.json.gz"}
			]
		}
	]
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "12345", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.MatchedStructures != 1 {
		t.Errorf("expected 1 matched structure, got %d", result.MatchedStructures)
	}
	if len(result.URLs) != 2 {
		t.Fatalf("expected 2 URLs, got %d", len(result.URLs))
	}
	if result.URLs[0] != "https://example.com/mrf1.json.gz" {
		t.Errorf("unexpected URL[0]: %s", result.URLs[0])
	}
	if result.URLs[1] != "https://example.com/mrf2.json.gz" {
		t.Errorf("unexpected URL[1]: %s", result.URLs[1])
	}
}

func TestResolveTOC_CaseInsensitive(t *testing.T) {
	tocJSON := `{
	"reporting_structure": [
		{
			"reporting_plans": [
				{"plan_name": "Plan", "plan_id_type": "HIOS", "plan_id": "ABC123"}
			],
			"in_network_files": [
				{"description": "File", "location": "https://example.com/mrf.json.gz"}
			]
		}
	]
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "abc123", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.MatchedStructures != 1 {
		t.Errorf("expected 1 matched structure, got %d", result.MatchedStructures)
	}
	if len(result.URLs) != 1 {
		t.Fatalf("expected 1 URL, got %d", len(result.URLs))
	}
}

func TestResolveTOC_MultipleStructures(t *testing.T) {
	tocJSON := `{
	"reporting_structure": [
		{
			"reporting_plans": [
				{"plan_name": "Plan A", "plan_id_type": "HIOS", "plan_id": "MATCH"}
			],
			"in_network_files": [
				{"description": "File A", "location": "https://example.com/a.json.gz"}
			]
		},
		{
			"reporting_plans": [
				{"plan_name": "Plan B", "plan_id_type": "EIN", "plan_id": "NOMATCH"}
			],
			"in_network_files": [
				{"description": "File B", "location": "https://example.com/b.json.gz"}
			]
		},
		{
			"reporting_plans": [
				{"plan_name": "Plan C", "plan_id_type": "HIOS", "plan_id": "MATCH"}
			],
			"in_network_files": [
				{"description": "File C", "location": "https://example.com/c.json.gz"}
			]
		}
	]
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "MATCH", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.MatchedStructures != 2 {
		t.Errorf("expected 2 matched structures, got %d", result.MatchedStructures)
	}
	if len(result.URLs) != 2 {
		t.Fatalf("expected 2 URLs, got %d: %v", len(result.URLs), result.URLs)
	}
	if result.URLs[0] != "https://example.com/a.json.gz" {
		t.Errorf("unexpected URL[0]: %s", result.URLs[0])
	}
	if result.URLs[1] != "https://example.com/c.json.gz" {
		t.Errorf("unexpected URL[1]: %s", result.URLs[1])
	}
}

func TestResolveTOC_Deduplication(t *testing.T) {
	tocJSON := `{
	"reporting_structure": [
		{
			"reporting_plans": [
				{"plan_name": "Plan A", "plan_id_type": "HIOS", "plan_id": "DUP"}
			],
			"in_network_files": [
				{"description": "File", "location": "https://example.com/shared.json.gz"}
			]
		},
		{
			"reporting_plans": [
				{"plan_name": "Plan B", "plan_id_type": "HIOS", "plan_id": "DUP"}
			],
			"in_network_files": [
				{"description": "File", "location": "https://example.com/shared.json.gz"},
				{"description": "File 2", "location": "https://example.com/unique.json.gz"}
			]
		}
	]
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "DUP", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.MatchedStructures != 2 {
		t.Errorf("expected 2 matched structures, got %d", result.MatchedStructures)
	}
	if len(result.URLs) != 2 {
		t.Fatalf("expected 2 URLs (deduplicated), got %d: %v", len(result.URLs), result.URLs)
	}
	if result.URLs[0] != "https://example.com/shared.json.gz" {
		t.Errorf("unexpected URL[0]: %s", result.URLs[0])
	}
	if result.URLs[1] != "https://example.com/unique.json.gz" {
		t.Errorf("unexpected URL[1]: %s", result.URLs[1])
	}
}

func TestResolveTOC_NoMatch(t *testing.T) {
	tocJSON := `{
	"reporting_structure": [
		{
			"reporting_plans": [
				{"plan_name": "Plan", "plan_id_type": "HIOS", "plan_id": "OTHER"}
			],
			"in_network_files": [
				{"description": "File", "location": "https://example.com/mrf.json.gz"}
			]
		}
	]
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "NOPE", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.MatchedStructures != 0 {
		t.Errorf("expected 0 matched structures, got %d", result.MatchedStructures)
	}
	if len(result.URLs) != 0 {
		t.Errorf("expected 0 URLs, got %d", len(result.URLs))
	}
}

func TestResolveTOC_EmptyInNetworkFiles(t *testing.T) {
	tocJSON := `{
	"reporting_structure": [
		{
			"reporting_plans": [
				{"plan_name": "Plan", "plan_id_type": "HIOS", "plan_id": "EMPTY"}
			],
			"in_network_files": []
		}
	]
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "EMPTY", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.MatchedStructures != 1 {
		t.Errorf("expected 1 matched structure, got %d", result.MatchedStructures)
	}
	if len(result.URLs) != 0 {
		t.Errorf("expected 0 URLs, got %d", len(result.URLs))
	}
}

func TestResolveTOC_SkipsUnknownKeys(t *testing.T) {
	tocJSON := `{
	"reporting_entity_name": "Test",
	"reporting_entity_type": "health_insurance_issuer",
	"last_updated_on": "2025-01-15",
	"version": "1.0",
	"boilerplate": {"nested": [1, 2, 3]},
	"reporting_structure": [
		{
			"reporting_plans": [
				{"plan_name": "Plan", "plan_id_type": "HIOS", "plan_id": "FOUND"}
			],
			"in_network_files": [
				{"description": "File", "location": "https://example.com/mrf.json.gz"}
			]
		}
	]
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "FOUND", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.MatchedStructures != 1 {
		t.Errorf("expected 1 matched structure, got %d", result.MatchedStructures)
	}
	if len(result.URLs) != 1 {
		t.Fatalf("expected 1 URL, got %d", len(result.URLs))
	}
}

func TestResolveTOC_ReportingEntityName(t *testing.T) {
	tocJSON := `{
	"reporting_entity_name": "Acme Health Insurance",
	"reporting_structure": []
}`

	result, err := ResolveTOC(strings.NewReader(tocJSON), "anything", nil)
	if err != nil {
		t.Fatalf("ResolveTOC failed: %v", err)
	}

	if result.ReportingEntityName != "Acme Health Insurance" {
		t.Errorf("expected reporting entity name %q, got %q", "Acme Health Insurance", result.ReportingEntityName)
	}
}
