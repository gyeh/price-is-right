package mrf

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"

	simdjson "github.com/minio/simdjson-go"
)

// useSimd is true if the CPU supports AVX2+CLMUL for simdjson acceleration.
var useSimd = simdjson.SupportedCPU()

// DisableSimd forces the stdlib JSON parser even on CPUs that support simdjson.
func DisableSimd() {
	useSimd = false
}

// ParserName returns which JSON parser is active.
func ParserName() string {
	if useSimd {
		return "simdjson (SIMD-accelerated)"
	}
	return "encoding/json (standard)"
}

// MatchedProviders maps provider_group_id → list of ProviderInfo that matched target NPIs.
type MatchedProviders struct {
	ByGroupID map[float64][]ProviderInfo
}

// npiBytePatterns builds byte patterns for pre-filtering raw JSON lines.
func npiBytePatterns(targetNPIs map[int64]struct{}) [][]byte {
	patterns := make([][]byte, 0, len(targetNPIs))
	for npi := range targetNPIs {
		patterns = append(patterns, []byte(strconv.FormatInt(npi, 10)))
	}
	return patterns
}

// lineContainsAny returns true if the line contains any of the byte patterns.
func lineContainsAny(line []byte, patterns [][]byte) bool {
	for _, p := range patterns {
		if bytes.Contains(line, p) {
			return true
		}
	}
	return false
}

// ParseProviderReferences scans provider_references NDJSON files for NPI matches (Phase A).
func ParseProviderReferences(files []string, targetNPIs map[int64]struct{}, onRefScanned func()) (*MatchedProviders, error) {
	matched := &MatchedProviders{
		ByGroupID: make(map[float64][]ProviderInfo),
	}

	patterns := npiBytePatterns(targetNPIs)

	for _, filePath := range files {
		var err error
		if useSimd {
			err = scanProviderRefFileSimd(filePath, targetNPIs, patterns, matched, onRefScanned)
		} else {
			err = scanProviderRefFileStdlib(filePath, targetNPIs, patterns, matched, onRefScanned)
		}
		if err != nil {
			return nil, fmt.Errorf("parsing %s: %w", filePath, err)
		}
	}

	return matched, nil
}

// ParseInNetwork scans in_network NDJSON files and emits RateResults for matching NPIs (Phase B).
func ParseInNetwork(
	files []string,
	targetNPIs map[int64]struct{},
	matchedProviders *MatchedProviders,
	sourceFile string,
	onCodeScanned func(),
	emit func(RateResult),
) error {
	for _, filePath := range files {
		var err error
		if useSimd {
			err = scanInNetworkFileSimd(filePath, targetNPIs, matchedProviders, sourceFile, onCodeScanned, emit)
		} else {
			err = scanInNetworkFileStdlib(filePath, targetNPIs, matchedProviders, sourceFile, onCodeScanned, emit)
		}
		if err != nil {
			return fmt.Errorf("parsing %s: %w", filePath, err)
		}
	}
	return nil
}

// emitInNetworkResults extracts and emits rate results from a parsed InNetworkItem.
// Shared by both stdlib and simd code paths.
func emitInNetworkResults(
	item *InNetworkItem,
	targetNPIs map[int64]struct{},
	matchedProviders *MatchedProviders,
	sourceFile string,
	emit func(RateResult),
) {
	description := item.Name
	if description == "" {
		description = item.Description
	}

	for _, nr := range item.NegotiatedRates {
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

// --- stdlib (encoding/json) implementations ---

func scanProviderRefFileStdlib(filePath string, targetNPIs map[int64]struct{}, npiPatterns [][]byte, matched *MatchedProviders, onRefScanned func()) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 512*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if onRefScanned != nil {
			onRefScanned()
		}

		// Pre-filter: skip lines that don't contain any target NPI as a substring.
		// This avoids expensive json.Unmarshal on 99.99%+ of lines.
		if !lineContainsAny(line, npiPatterns) {
			continue
		}

		var ref ProviderReference
		if err := json.Unmarshal(line, &ref); err != nil {
			continue
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

func scanInNetworkFileStdlib(
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
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 512*1024*1024)

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

		emitInNetworkResults(&item, targetNPIs, matchedProviders, sourceFile, emit)
	}

	return scanner.Err()
}
