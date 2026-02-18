package npi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const registryURL = "https://npiregistry.cms.hhs.gov/api/?version=2.1"

var client = &http.Client{Timeout: 10 * time.Second}

// ProviderInfo holds the key details returned by the NPPES NPI Registry.
type ProviderInfo struct {
	NPI              int64
	Name             string // "LAST, FIRST MIDDLE" for individuals, org name for organizations
	Credential       string // e.g. "MD", "DO", "MA, OTR"
	Type             string // "Individual" or "Organization"
	PrimaryTaxonomy  string // e.g. "Internal Medicine"
	TaxonomyCode     string // e.g. "207R00000X"
	PracticeAddress  string // city, state
	PracticePhone    string
	EnumerationDate  string
	Status           string // "A" = active
}

type apiResponse struct {
	ResultCount int         `json:"result_count"`
	Results     []apiResult `json:"results"`
}

type apiResult struct {
	Number          string        `json:"number"`
	EnumerationType string        `json:"enumeration_type"`
	Basic           apiBasic      `json:"basic"`
	Addresses       []apiAddress  `json:"addresses"`
	Taxonomies      []apiTaxonomy `json:"taxonomies"`
}

type apiBasic struct {
	// Individual fields
	FirstName  string `json:"first_name"`
	MiddleName string `json:"middle_name"`
	LastName   string `json:"last_name"`
	Credential string `json:"credential"`

	// Organization fields
	OrganizationName string `json:"organization_name"`

	EnumerationDate string `json:"enumeration_date"`
	Status          string `json:"status"`
}

type apiAddress struct {
	Address1       string `json:"address_1"`
	Address2       string `json:"address_2"`
	City           string `json:"city"`
	State          string `json:"state"`
	PostalCode     string `json:"postal_code"`
	AddressPurpose string `json:"address_purpose"` // "LOCATION" or "MAILING"
	Phone          string `json:"telephone_number"`
}

type apiTaxonomy struct {
	Code    string `json:"code"`
	Desc    string `json:"desc"`
	Primary bool   `json:"primary"`
	State   string `json:"state"`
	License string `json:"license"`
}

// SearchByName queries the NPPES NPI Registry for individual providers matching
// the given first/last name. An optional state (2-letter code) narrows results.
// Returns up to 20 matching providers.
func SearchByName(ctx context.Context, firstName, lastName, state string) ([]*ProviderInfo, error) {
	u := fmt.Sprintf("%s&enumeration_type=NPI-1&limit=20&first_name=%s&last_name=%s",
		registryURL, firstName, lastName)
	if state != "" {
		u += "&state=" + state
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying NPI registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("NPI registry returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp apiResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("parsing NPI registry response: %w", err)
	}

	if apiResp.ResultCount == 0 || len(apiResp.Results) == 0 {
		return nil, nil
	}

	var results []*ProviderInfo
	for _, r := range apiResp.Results {
		info := resultToProviderInfo(r)
		results = append(results, info)
	}
	return results, nil
}

// Lookup queries the NPPES NPI Registry for a single NPI number.
// Returns nil if the NPI is not found.
func Lookup(ctx context.Context, number int64) (*ProviderInfo, error) {
	url := fmt.Sprintf("%s&number=%d", registryURL, number)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("querying NPI registry: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("NPI registry returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp apiResponse
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, fmt.Errorf("parsing NPI registry response: %w", err)
	}

	if apiResp.ResultCount == 0 || len(apiResp.Results) == 0 {
		return nil, nil
	}

	return resultToProviderInfo(apiResp.Results[0]), nil
}

func resultToProviderInfo(r apiResult) *ProviderInfo {
	npiNum, _ := strconv.ParseInt(r.Number, 10, 64)
	info := &ProviderInfo{
		NPI:             npiNum,
		EnumerationDate: r.Basic.EnumerationDate,
		Status:          r.Basic.Status,
	}

	// Name and type
	if r.EnumerationType == "NPI-1" {
		info.Type = "Individual"
		info.Name = formatIndividualName(r.Basic)
		info.Credential = cleanField(r.Basic.Credential)
	} else {
		info.Type = "Organization"
		info.Name = r.Basic.OrganizationName
	}

	// Primary taxonomy (specialty)
	for _, t := range r.Taxonomies {
		if t.Primary {
			info.PrimaryTaxonomy = t.Desc
			info.TaxonomyCode = t.Code
			break
		}
	}
	if info.PrimaryTaxonomy == "" && len(r.Taxonomies) > 0 {
		info.PrimaryTaxonomy = r.Taxonomies[0].Desc
		info.TaxonomyCode = r.Taxonomies[0].Code
	}

	// Practice location address
	for _, addr := range r.Addresses {
		if addr.AddressPurpose == "LOCATION" {
			info.PracticeAddress = formatAddress(addr)
			info.PracticePhone = formatPhone(addr.Phone)
			break
		}
	}
	if info.PracticeAddress == "" && len(r.Addresses) > 0 {
		info.PracticeAddress = formatAddress(r.Addresses[0])
		info.PracticePhone = formatPhone(r.Addresses[0].Phone)
	}

	return info
}

// LookupAll queries the NPPES NPI Registry for multiple NPIs concurrently.
// Returns results in the same order as input. Missing NPIs have nil entries.
func LookupAll(ctx context.Context, npis []int64) ([]*ProviderInfo, []error) {
	results := make([]*ProviderInfo, len(npis))
	errs := make([]error, len(npis))

	type indexedResult struct {
		idx  int
		info *ProviderInfo
		err  error
	}

	ch := make(chan indexedResult, len(npis))
	for i, n := range npis {
		go func(idx int, number int64) {
			info, err := Lookup(ctx, number)
			ch <- indexedResult{idx, info, err}
		}(i, n)
	}

	for range npis {
		r := <-ch
		results[r.idx] = r.info
		errs[r.idx] = r.err
	}

	return results, errs
}

func formatIndividualName(b apiBasic) string {
	parts := []string{cleanField(b.LastName)}
	if first := cleanField(b.FirstName); first != "" {
		parts = append(parts, first)
	}
	name := strings.Join(parts, ", ")
	if middle := cleanField(b.MiddleName); middle != "" {
		name += " " + middle
	}
	return name
}

func formatAddress(a apiAddress) string {
	parts := []string{}
	if a.City != "" {
		parts = append(parts, a.City)
	}
	if a.State != "" {
		parts = append(parts, a.State)
	}
	loc := strings.Join(parts, ", ")
	if a.PostalCode != "" {
		zip := a.PostalCode
		if len(zip) > 5 {
			zip = zip[:5]
		}
		loc += " " + zip
	}
	return loc
}

func formatPhone(phone string) string {
	p := strings.ReplaceAll(phone, "-", "")
	p = strings.TrimSpace(p)
	if len(p) == 10 {
		return fmt.Sprintf("(%s) %s-%s", p[:3], p[3:6], p[6:])
	}
	return phone
}

func cleanField(s string) string {
	s = strings.TrimSpace(s)
	if s == "--" || s == "" {
		return ""
	}
	return s
}
