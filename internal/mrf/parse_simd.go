package mrf

import (
	"bufio"
	"encoding/json"
	"os"

	simdjson "github.com/minio/simdjson-go"
)

// scanProviderRefFileSimd parses provider_references NDJSON using simdjson.
// Full native extraction — no json.Unmarshal needed.
func scanProviderRefFileSimd(filePath string, targetNPIs map[int64]struct{}, npiPatterns [][]byte, matched *MatchedProviders, onRefScanned func()) error {
	f, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 4*1024*1024), 512*1024*1024)

	var pj *simdjson.ParsedJson

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if onRefScanned != nil {
			onRefScanned()
		}

		// Pre-filter: skip lines that don't contain any target NPI as a substring.
		if !lineContainsAny(line, npiPatterns) {
			continue
		}

		pj, err = simdjson.Parse(line, pj)
		if err != nil {
			continue
		}

		pj.ForEach(func(i simdjson.Iter) error {
			extractProviderRef(i, targetNPIs, matched)
			return nil
		})
	}

	return scanner.Err()
}

// extractProviderRef extracts provider_group_id and checks NPIs using simdjson.
func extractProviderRef(i simdjson.Iter, targetNPIs map[int64]struct{}, matched *MatchedProviders) {
	// Get provider_group_id (FindElement resets position each call)
	idElem, err := i.FindElement(nil, "provider_group_id")
	if err != nil {
		return
	}
	groupID, err := idElem.Iter.Float()
	if err != nil {
		return
	}

	// Get provider_groups array
	pgElem, err := i.FindElement(nil, "provider_groups")
	if err != nil {
		return
	}
	pgArr, err := pgElem.Iter.Array(nil)
	if err != nil {
		return
	}

	pgArr.ForEach(func(pgIter simdjson.Iter) {
		// Get NPI array
		npiElem, npiErr := pgIter.FindElement(nil, "npi")
		if npiErr != nil {
			return
		}
		npiArr, npiErr := npiElem.Iter.Array(nil)
		if npiErr != nil {
			return
		}
		npis, npiErr := npiArr.AsInteger()
		if npiErr != nil {
			return
		}

		// Quick check: does any NPI match?
		hasMatch := false
		for _, npi := range npis {
			if _, ok := targetNPIs[npi]; ok {
				hasMatch = true
				break
			}
		}
		if !hasMatch {
			return
		}

		// Match found — extract TIN (only for matched groups)
		var tin TIN
		if e, tinErr := pgIter.FindElement(nil, "tin", "type"); tinErr == nil {
			tin.Type, _ = e.Iter.String()
		}
		if e, tinErr := pgIter.FindElement(nil, "tin", "value"); tinErr == nil {
			tin.Value, _ = e.Iter.String()
		}

		for _, npi := range npis {
			if _, ok := targetNPIs[npi]; ok {
				matched.ByGroupID[groupID] = append(
					matched.ByGroupID[groupID],
					ProviderInfo{NPI: npi, TIN: tin},
				)
			}
		}
	})
}

// scanInNetworkFileSimd uses simdjson for fast NPI match checking,
// then stdlib json.Unmarshal only for records that actually match.
// This is a hybrid approach: simdjson filters (fast), stdlib extracts (simple).
func scanInNetworkFileSimd(
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

	var pj *simdjson.ParsedJson

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		pj, err = simdjson.Parse(line, pj)
		if err != nil {
			continue
		}

		if onCodeScanned != nil {
			onCodeScanned()
		}

		// Quick match check using simdjson — skip non-matching records fast
		matched := false
		pj.ForEach(func(i simdjson.Iter) error {
			matched = checkNPIMatchSimd(i, targetNPIs, matchedProviders)
			return nil
		})

		if !matched {
			continue
		}

		// Match found — full extraction via stdlib (simpler for deeply nested structures)
		var item InNetworkItem
		if err := json.Unmarshal(line, &item); err != nil {
			continue
		}

		emitInNetworkResults(&item, targetNPIs, matchedProviders, sourceFile, emit)
	}

	return scanner.Err()
}

// checkNPIMatchSimd quickly checks if an in_network record contains any matching NPIs.
// Checks both provider_references (via matchedProviders) and inline provider_groups.
func checkNPIMatchSimd(i simdjson.Iter, targetNPIs map[int64]struct{}, matchedProviders *MatchedProviders) bool {
	ratesElem, err := i.FindElement(nil, "negotiated_rates")
	if err != nil {
		return false
	}
	ratesArr, err := ratesElem.Iter.Array(nil)
	if err != nil {
		return false
	}

	found := false
	ratesArr.ForEach(func(rateIter simdjson.Iter) {
		if found {
			return // already found a match, skip remaining
		}

		// Check provider_references IDs
		if matchedProviders != nil && len(matchedProviders.ByGroupID) > 0 {
			if refsElem, refErr := rateIter.FindElement(nil, "provider_references"); refErr == nil {
				if refsArr, arrErr := refsElem.Iter.Array(nil); arrErr == nil {
					if refs, floatErr := refsArr.AsFloat(); floatErr == nil {
						for _, ref := range refs {
							if _, ok := matchedProviders.ByGroupID[ref]; ok {
								found = true
								return
							}
						}
					}
				}
			}
		}

		// Check inline provider_groups
		if pgElem, pgErr := rateIter.FindElement(nil, "provider_groups"); pgErr == nil {
			if pgArr, arrErr := pgElem.Iter.Array(nil); arrErr == nil {
				pgArr.ForEach(func(pgIter simdjson.Iter) {
					if found {
						return
					}
					if npiElem, npiErr := pgIter.FindElement(nil, "npi"); npiErr == nil {
						if npiArr, arrErr := npiElem.Iter.Array(nil); arrErr == nil {
							if npis, intErr := npiArr.AsInteger(); intErr == nil {
								for _, npi := range npis {
									if _, ok := targetNPIs[npi]; ok {
										found = true
										return
									}
								}
							}
						}
					}
				})
			}
		}
	})

	return found
}
