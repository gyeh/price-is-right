package mrf

// TIN represents a Tax Identification Number.
type TIN struct {
	Type  string `json:"type"`  // "ein" or "npi"
	Value string `json:"value"` // e.g. "12-3456789"
}

// ProviderGroup represents a group of providers within a provider reference.
type ProviderGroup struct {
	NPI []int64 `json:"npi"`
	TIN TIN     `json:"tin"`
}

// ProviderReference is a top-level provider_references entry.
type ProviderReference struct {
	ProviderGroupID int             `json:"provider_group_id"`
	ProviderGroups  []ProviderGroup `json:"provider_groups"`
}

// NegotiatedPrice represents a single negotiated price entry.
type NegotiatedPrice struct {
	NegotiatedRate    float64  `json:"negotiated_rate"`
	NegotiatedType    string   `json:"negotiated_type"`
	BillingClass      string   `json:"billing_class"`
	Setting           string   `json:"setting"`
	ExpirationDate    string   `json:"expiration_date"`
	ServiceCode       []string `json:"service_code"`
	BillingCodeModifier []string `json:"billing_code_modifier"`
}

// NegotiatedRate is a rate entry within an in_network item.
type NegotiatedRate struct {
	ProviderReferences []int           `json:"provider_references"`
	ProviderGroups     []ProviderGroup `json:"provider_groups"`
	NegotiatedPrices   []NegotiatedPrice `json:"negotiated_prices"`
}

// InNetworkItem represents a single in_network array entry.
type InNetworkItem struct {
	BillingCodeType        string           `json:"billing_code_type"`
	BillingCode            string           `json:"billing_code"`
	Name                   string           `json:"name"`
	Description            string           `json:"description"`
	NegotiationArrangement string           `json:"negotiation_arrangement"`
	NegotiatedRates        []NegotiatedRate `json:"negotiated_rates"`
}

// ProviderInfo holds matched provider details from Phase A.
type ProviderInfo struct {
	NPI int64
	TIN TIN
}

// RateResult is a single output record for a matched rate.
type RateResult struct {
	SourceFile             string   `json:"source_file"`
	NPI                    int64    `json:"npi"`
	TIN                    TIN      `json:"tin"`
	BillingCodeType        string   `json:"billing_code_type"`
	BillingCode            string   `json:"billing_code"`
	BillingCodeDescription string   `json:"billing_code_description"`
	NegotiationArrangement string   `json:"negotiation_arrangement"`
	NegotiatedRate         float64  `json:"negotiated_rate"`
	NegotiatedType         string   `json:"negotiated_type"`
	BillingClass           string   `json:"billing_class"`
	Setting                string   `json:"setting"`
	ExpirationDate         string   `json:"expiration_date"`
	ServiceCode            []string `json:"service_code"`
	BillingCodeModifier    []string `json:"billing_code_modifier"`
}

// SearchOutput is the top-level output JSON structure.
type SearchOutput struct {
	SearchParams SearchParams  `json:"search_params"`
	Results      []RateResult  `json:"results"`
}

// SearchParams holds metadata about the search.
type SearchParams struct {
	NPIs            []int64 `json:"npis"`
	SearchedFiles   int     `json:"searched_files"`
	MatchedFiles    int     `json:"matched_files"`
	DurationSeconds float64 `json:"duration_seconds"`
}
