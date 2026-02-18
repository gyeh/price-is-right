package mrf

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
)

// MatchedProviders maps provider_group_id → list of ProviderInfo that matched target NPIs.
type MatchedProviders struct {
	// ByGroupID maps provider_group_id to the matched provider details.
	ByGroupID map[int][]ProviderInfo
}

// ParseProviderReferences scans provider_references NDJSON files for NPI matches (Phase A).
// Returns a map of provider_group_id → matched ProviderInfo entries.
func ParseProviderReferences(files []string, targetNPIs map[int64]struct{}, onRefScanned func()) (*MatchedProviders, error) {
	matched := &MatchedProviders{
		ByGroupID: make(map[int][]ProviderInfo),
	}

	for _, filePath := range files {
		if err := scanProviderRefFile(filePath, targetNPIs, matched, onRefScanned); err != nil {
			return nil, fmt.Errorf("parsing %s: %w", filePath, err)
		}
	}

	return matched, nil
}

func scanProviderRefFile(filePath string, targetNPIs map[int64]struct{}, matched *MatchedProviders, onRefScanned func()) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 64*1024*1024) // up to 64MB per line

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var ref ProviderReference
		if err := json.Unmarshal(line, &ref); err != nil {
			continue // skip malformed lines
		}

		if onRefScanned != nil {
			onRefScanned()
		}

		for _, pg := range ref.ProviderGroups {
			for _, npi := range pg.NPI {
				if _, ok := targetNPIs[npi]; ok {
					matched.ByGroupID[ref.ProviderGroupID] = append(
						matched.ByGroupID[ref.ProviderGroupID],
						ProviderInfo{NPI: npi, TIN: pg.TIN},
					)
				}
			}
		}
	}

	return scanner.Err()
}

// ParseInNetwork scans in_network NDJSON files and emits RateResults for matching NPIs (Phase B).
// It checks both provider_references (via matchedProviders) and inline provider_groups.
func ParseInNetwork(
	files []string,
	targetNPIs map[int64]struct{},
	matchedProviders *MatchedProviders,
	sourceFile string,
	onCodeScanned func(),
	emit func(RateResult),
) error {
	for _, filePath := range files {
		if err := scanInNetworkFile(filePath, targetNPIs, matchedProviders, sourceFile, onCodeScanned, emit); err != nil {
			return fmt.Errorf("parsing %s: %w", filePath, err)
		}
	}
	return nil
}

func scanInNetworkFile(
	filePath string,
	targetNPIs map[int64]struct{},
	matchedProviders *MatchedProviders,
	sourceFile string,
	onCodeScanned func(),
	emit func(RateResult),
) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 64*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var item InNetworkItem
		if err := json.Unmarshal(line, &item); err != nil {
			continue
		}

		if onCodeScanned != nil {
			onCodeScanned()
		}

		description := item.Name
		if description == "" {
			description = item.Description
		}

		for _, nr := range item.NegotiatedRates {
			// Collect matched providers from both sources
			var providers []ProviderInfo

			// Case A: provider_references IDs
			if matchedProviders != nil {
				for _, refID := range nr.ProviderReferences {
					if infos, ok := matchedProviders.ByGroupID[refID]; ok {
						providers = append(providers, infos...)
					}
				}
			}

			// Case B: inline provider_groups
			for _, pg := range nr.ProviderGroups {
				for _, npi := range pg.NPI {
					if _, ok := targetNPIs[npi]; ok {
						providers = append(providers, ProviderInfo{NPI: npi, TIN: pg.TIN})
					}
				}
			}

			if len(providers) == 0 {
				continue
			}

			// Emit one result per provider × price combination
			for _, prov := range providers {
				for _, price := range nr.NegotiatedPrices {
					emit(RateResult{
						SourceFile:             sourceFile,
						NPI:                    prov.NPI,
						TIN:                    prov.TIN,
						BillingCodeType:        item.BillingCodeType,
						BillingCode:            item.BillingCode,
						BillingCodeDescription: description,
						NegotiationArrangement: item.NegotiationArrangement,
						NegotiatedRate:         price.NegotiatedRate,
						NegotiatedType:         price.NegotiatedType,
						BillingClass:           price.BillingClass,
						Setting:                price.Setting,
						ExpirationDate:         price.ExpirationDate,
						ServiceCode:            price.ServiceCode,
						BillingCodeModifier:    price.BillingCodeModifier,
					})
				}
			}
		}
	}

	return scanner.Err()
}
