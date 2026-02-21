package mrf

import (
	"encoding/json"
	"fmt"
	"io"
	"runtime"
	"sync"

	simdjson "github.com/minio/simdjson-go"
)

// StreamResult is the return value from StreamParse, indicating whether a
// second pass is needed (when in_network appeared before provider_references).
type StreamResult struct {
	NeedSecondPass   bool
	MatchedProviders *MatchedProviders
}

// StreamCallbacks holds all callbacks for StreamParse progress reporting.
type StreamCallbacks struct {
	OnRefScanned  func()             // called for each provider_references element
	OnCodeScanned func()             // called for each in_network element
	OnStageChange func(stage string) // called when transitioning between phases
	OnWarning     func(msg string)   // called for non-fatal issues
}

// StreamParse walks a top-level MRF JSON object from r using a streaming
// json.Decoder (Token/More), processing one array element at a time with
// constant memory. Zero intermediate files.
//
// Expected structure: { "provider_references": [...], "in_network": [...], ... }
//
// If in_network appears before provider_references (rare), it is skipped on
// the first pass and the caller is signaled via StreamResult.NeedSecondPass to
// re-download and re-parse with the prebuilt MatchedProviders.
//
// If prebuilt is non-nil (second pass), provider_references is skipped and
// in_network is processed using the prebuilt index.
func StreamParse(
	r io.Reader,
	targetNPIs map[int64]struct{},
	sourceFile string,
	cb StreamCallbacks,
	emit func(RateResult),
	prebuilt *MatchedProviders,
) (*StreamResult, error) {
	dec := json.NewDecoder(r)

	// Expect opening '{' of the top-level object.
	tok, err := dec.Token()
	if err != nil {
		return nil, fmt.Errorf("reading opening token: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '{' {
		return nil, fmt.Errorf("expected '{', got %v", tok)
	}

	// On second pass, use the prebuilt index directly.
	var matched *MatchedProviders
	if prebuilt != nil {
		matched = prebuilt
	} else {
		matched = &MatchedProviders{
			ByGroupID: make(map[float64][]ProviderInfo),
		}
	}
	patterns := npiBytePatterns(targetNPIs)

	var pj *simdjson.ParsedJson // reused across simdjson.Parse calls

	seenProviderRefs := false
	skippedInNetwork := false
	var refsCount int64

	for dec.More() {
		// Read the key name.
		tok, err = dec.Token()
		if err != nil {
			return nil, fmt.Errorf("reading key: %w", err)
		}
		key, ok := tok.(string)
		if !ok {
			return nil, fmt.Errorf("expected string key, got %T", tok)
		}

		switch key {
		case "provider_references":
			if prebuilt != nil {
				// Second pass: skip provider_references, already indexed.
				if err := skipValue(dec); err != nil {
					return nil, fmt.Errorf("skipping provider_references (second pass): %w", err)
				}
				continue
			}
			seenProviderRefs = true
			if cb.OnStageChange != nil {
				cb.OnStageChange("Streaming: provider_references")
			}
			countingOnRef := func() {
				refsCount++
				if cb.OnRefScanned != nil {
					cb.OnRefScanned()
				}
			}
			pj, err = streamProviderReferences(dec, targetNPIs, patterns, matched, pj, countingOnRef)
			if err != nil {
				return nil, fmt.Errorf("streaming provider_references: %w", err)
			}

		case "in_network":
			if prebuilt == nil && !seenProviderRefs {
				// First pass, reversed order: skip in_network, process provider_references first.
				if cb.OnWarning != nil {
					cb.OnWarning("in_network appeared before provider_references; will require second pass")
				}
				skippedInNetwork = true
				if err := skipValue(dec); err != nil {
					return nil, fmt.Errorf("skipping in_network (reversed order): %w", err)
				}
				continue
			}
			if prebuilt == nil && seenProviderRefs && refsCount > 0 && len(matched.ByGroupID) == 0 {
				// provider_references had entries but yielded no NPI matches; skip in_network.
				if cb.OnStageChange != nil {
					cb.OnStageChange("Skipping: in_network (no matching providers)")
				}
				if err := skipValue(dec); err != nil {
					return nil, fmt.Errorf("skipping in_network (no matches): %w", err)
				}
				continue
			}
			if cb.OnStageChange != nil {
				cb.OnStageChange("Streaming: in_network")
			}
			pj, err = streamInNetwork(dec, targetNPIs, matched, sourceFile, pj, cb.OnCodeScanned, emit)
			if err != nil {
				return nil, fmt.Errorf("streaming in_network: %w", err)
			}

		default:
			// Skip unneeded keys (reporting_entity_name, etc.)
			if err := skipValue(dec); err != nil {
				return nil, fmt.Errorf("skipping key %q: %w", key, err)
			}
		}
	}

	// Expect closing '}'.
	tok, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("reading closing token: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '}' {
		return nil, fmt.Errorf("expected '}', got %v", tok)
	}

	return &StreamResult{
		NeedSecondPass:   skippedInNetwork && len(matched.ByGroupID) > 0,
		MatchedProviders: matched,
	}, nil
}

// streamProviderReferences reads the provider_references JSON array element by
// element, building MatchedProviders. Returns the (possibly reused) ParsedJson.
func streamProviderReferences(
	dec *json.Decoder,
	targetNPIs map[int64]struct{},
	patterns [][]byte,
	matched *MatchedProviders,
	pj *simdjson.ParsedJson,
	onRefScanned func(),
) (*simdjson.ParsedJson, error) {
	// Expect opening '['.
	tok, err := dec.Token()
	if err != nil {
		return pj, fmt.Errorf("reading array start: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return pj, fmt.Errorf("expected '[', got %v", tok)
	}

	for dec.More() {
		// Read one element as raw JSON (byte array).
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			return pj, fmt.Errorf("decoding element: %w", err)
		}

		if onRefScanned != nil {
			onRefScanned()
		}

		// Pre-filter: skip elements that don't contain any target NPI as substring.
		if !lineContainsAny(raw, patterns) {
			continue
		}

		// Parse with simdjson if available, else stdlib.
		if useSimd {
			pj, err = simdjson.Parse(raw, pj)
			if err != nil {
				continue // skip malformed
			}
			pj.ForEach(func(i simdjson.Iter) error {
				extractProviderRef(i, targetNPIs, matched)
				return nil
			})
		} else {
			var ref ProviderReference
			if err := json.Unmarshal(raw, &ref); err != nil {
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
	}

	// Expect closing ']'.
	tok, err = dec.Token()
	if err != nil {
		return pj, fmt.Errorf("reading array end: %w", err)
	}

	return pj, nil
}

// streamInNetwork reads the in_network JSON array element by element.
// Decoding is serial (json.Decoder requires it), but simdjson matching and
// stdlib unmarshalling are fanned out to GOMAXPROCS workers for parallel
// processing. Each worker holds its own *simdjson.ParsedJson.
func streamInNetwork(
	dec *json.Decoder,
	targetNPIs map[int64]struct{},
	matched *MatchedProviders,
	sourceFile string,
	pj *simdjson.ParsedJson,
	onCodeScanned func(),
	emit func(RateResult),
) (*simdjson.ParsedJson, error) {
	// Expect opening '['.
	tok, err := dec.Token()
	if err != nil {
		return pj, fmt.Errorf("reading array start: %w", err)
	}
	if delim, ok := tok.(json.Delim); !ok || delim != '[' {
		return pj, fmt.Errorf("expected '[', got %v", tok)
	}

	// Fan out element processing to workers.
	numWorkers := runtime.GOMAXPROCS(0)
	ch := make(chan json.RawMessage, numWorkers*2)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var workerPJ *simdjson.ParsedJson
			for raw := range ch {
				processInNetworkElement(raw, targetNPIs, matched, sourceFile, &workerPJ, emit)
			}
		}()
	}

	// Decode loop — serial, feeds workers via channel.
	var decErr error
	for dec.More() {
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			decErr = fmt.Errorf("decoding element: %w", err)
			break
		}

		if onCodeScanned != nil {
			onCodeScanned()
		}

		ch <- raw
	}
	close(ch)
	wg.Wait()

	if decErr != nil {
		return nil, decErr
	}

	// Expect closing ']'.
	tok, err = dec.Token()
	if err != nil {
		return nil, fmt.Errorf("reading array end: %w", err)
	}

	return nil, nil
}

// processInNetworkElement checks a single in_network element for NPI matches
// and emits results. Called from worker goroutines — targetNPIs and matched
// are read-only at this point; emit must be safe for concurrent calls.
func processInNetworkElement(
	raw json.RawMessage,
	targetNPIs map[int64]struct{},
	matched *MatchedProviders,
	sourceFile string,
	pj **simdjson.ParsedJson,
	emit func(RateResult),
) {
	if useSimd {
		var err error
		*pj, err = simdjson.Parse(raw, *pj)
		if err != nil {
			return
		}
		isMatch := false
		(*pj).ForEach(func(i simdjson.Iter) error {
			isMatch = checkNPIMatchSimd(i, targetNPIs, matched)
			return nil
		})
		if !isMatch {
			return
		}
	}

	var item InNetworkItem
	if err := json.Unmarshal(raw, &item); err != nil {
		return
	}

	emitInNetworkResults(&item, targetNPIs, matched, sourceFile, emit)
}

// skipValue reads and discards the next JSON value from the decoder.
// Handles objects, arrays, and primitive values.
func skipValue(dec *json.Decoder) error {
	tok, err := dec.Token()
	if err != nil {
		return err
	}

	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			// Skip object contents.
			for dec.More() {
				// Skip key.
				if _, err := dec.Token(); err != nil {
					return err
				}
				// Skip value (recursive).
				if err := skipValue(dec); err != nil {
					return err
				}
			}
			// Read closing '}'.
			if _, err := dec.Token(); err != nil {
				return err
			}
		case '[':
			// Skip array contents.
			for dec.More() {
				if err := skipValue(dec); err != nil {
					return err
				}
			}
			// Read closing ']'.
			if _, err := dec.Token(); err != nil {
				return err
			}
		}
	default:
		// Primitive value (string, number, bool, null) — already consumed.
	}
	return nil
}
