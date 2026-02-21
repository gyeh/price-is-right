# The Price is Right

Search payer Machine-Readable Files (MRFs) for negotiated procedure rates by provider NPI number.

The [CMS Price Transparency rule](https://www.cms.gov/hospital-price-transparency) requires insurers to publish every negotiated rate for every provider and procedure as machine-readable JSON. The result is one of the largest public datasets ever created: a single insurer's MRF index can list thousands of gzipped JSON files, each 5-40 GB compressed. Finding rates for a specific doctor means downloading and parsing terabytes of data.

npi-rates streams these files directly from CDN to parser with zero disk I/O and constant memory, extracting matching rates in minutes instead of hours.

## Why this exists

If you want to know what your insurance company has agreed to pay your doctor for a specific procedure, the data is technically public. In practice, it's buried in multi-gigabyte JSON files spread across hundreds of URLs. There's no search API. Each file can take 30+ minutes to download and contains millions of records.

This tool makes that data accessible:

```
$ npi-rates search --npi 1770671182 --urls-file anthem_ny.txt
NPI 1770671182: DOE, JANE, MD
  Specialty: Diagnostic Radiology
  Location:  123 Medical Dr, New York, NY 10001

Files: 12
CDN: CloudFront (BCBS)
Size (compressed): 37.2 GB total, 3.1 GB avg, 891.2 MB min, 8.3 GB max
Parser: simdjson (SIMD-accelerated)
Mode: streaming (no disk)
Workers: 3

Search complete: 12 files searched, 2 matched, 127 rates found in 194.6s
Results written to results_20260220_153634.json
```

## Install

Requires Go 1.23+.

```bash
go install github.com/gyeh/npi-rates/cmd/npi-rates@latest
```

Or build from source:

```bash
git clone https://github.com/gyeh/price_is_right.git
cd price_is_right
go build -o npi-rates ./cmd/npi-rates
```

## Usage

### Search by NPI

```bash
# Search specific URLs
npi-rates search --npi 1234567890 --url "https://example.com/mrf_file_1.json.gz"

# Search from a URL list file
npi-rates search --npi 1234567890 --urls-file urls.txt

# Multiple NPIs
npi-rates search --npi 1234567890,9876543210 --urls-file urls.txt

# Custom output path
npi-rates search --npi 1234567890 --urls-file urls.txt -o my_rates.json
```

### Search by provider name

If you don't know the NPI, search the NPPES registry by name:

```bash
npi-rates search --provider-name "Jane Doe" --state NY --urls-file urls.txt
```

This queries the [NPPES NPI Registry](https://npiregistry.cms.hhs.gov/), shows matching providers, and lets you select one interactively.

### Cloud mode (Modal)

For large URL lists (100+ files), distribute across parallel [Modal](https://modal.com) functions:

```bash
# One-time setup: deploy the function to Modal
modal deploy deploy_modal.py

# Search using deployed functions
npi-rates search --npi 1234567890 --urls-file urls.txt --cloud --shards 50
```

This shards the URL list across 50 parallel Modal function calls and merges results locally. Each function call runs an independent `npi-rates search` instance. The image is built once at deploy time, so subsequent searches start instantly.

```bash
# Adjust workers per shard
npi-rates search --npi 1234567890 --urls-file urls.txt \
  --cloud --shards 100 --cloud-workers 2
```

Infrastructure settings (CPU, memory, cloud provider, region) are configured in `deploy_modal.py` and applied at deploy time. Re-deploy after changing them:

```bash
# Edit deploy_modal.py to change _CPU, _MEMORY, _CLOUD, _REGION, etc.
modal deploy deploy_modal.py
```

### Other commands

```bash
# Download and decompress a single MRF file
npi-rates download "https://example.com/mrf_file.json.gz" -o mrf_file.json

# Split a decompressed MRF into NDJSON chunks
npi-rates split mrf_file.json -o mrf_file_split/
```

## Output format

```json
{
  "search_params": {
    "npis": [1770671182],
    "searched_files": 12,
    "matched_files": 2,
    "duration_seconds": 194.55
  },
  "results": [
    {
      "source_file": "https://anthembcca.mrf.bcbs.com/2026-02_..._in-network-rates.json.gz",
      "npi": 1770671182,
      "tin": {
        "type": "ein",
        "value": "15-0584188"
      },
      "billing_code_type": "CPT",
      "billing_code": "76705",
      "billing_code_description": "ABDOMINAL ULTRASOUND",
      "negotiation_arrangement": "ffs",
      "negotiated_rate": 531.21,
      "negotiated_type": "derived",
      "billing_class": "institutional",
      "setting": "outpatient",
      "expiration_date": "2026-01-01",
      "service_code": ["22"],
      "billing_code_modifier": null
    }
  ]
}
```

Use `-o -` to write to stdout for piping into `jq` or other tools.

## How it works

### Streaming parser

MRF files follow a schema with two top-level arrays: `provider_references` (maps NPIs to TINs and provider groups) and `in_network` (contains every negotiated rate). A single file can contain tens of millions of rate entries.

npi-rates streams directly from the HTTP response through gzip decompression into a JSON token parser. No intermediate files are written to disk. Memory usage stays constant regardless of file size.

The parser works in two phases:
1. **provider_references**: Builds an in-memory index mapping NPI numbers to TIN (Tax Identification Number) values and provider group IDs
2. **in_network**: Streams rate entries, checks each against the NPI index, and emits matches

Before JSON parsing, each raw line is checked for the target NPI as a substring. This skips 99%+ of entries without invoking the parser.

### SIMD acceleration

On CPUs with AVX2 and CLMUL support, npi-rates uses [simdjson-go](https://github.com/minio/simdjson-go) for parsing matched entries. This is used for fast NPI detection in provider group arrays and rate extraction. Falls back to `encoding/json` on unsupported CPUs or with `--no-simd`.

### Cloud orchestration

Cloud mode uses [Modal](https://modal.com) to run searches in parallel:

1. A Modal function is deployed once via `modal deploy deploy_modal.py` (builds the container image with the `npi-rates` binary)
2. At search time, URLs are sharded round-robin across N function calls
3. Each function call receives its URL shard and runs `npi-rates search` independently
4. Results are returned directly and merged locally

With 100 shards, a 400+ file search that would take hours locally finishes in minutes. A progress bar shows shard completion when running from a terminal.

## Where to get MRF URLs

Insurers publish a Table of Contents (TOC) index file linking to all their MRF files. These are typically at a URL like:

```
https://<insurer-domain>/transparency-in-coverage/index.json
```

The [CMS MRF lookup tool](https://transparency-in-coverage.cms.gov/) can help find insurer TOC URLs. You'll need to extract the `in-network` file URLs from the TOC and save them to a text file (one URL per line).

The URL list file format:

```
# Comments and blank lines are ignored
https://example.com/2026-02_plan_in-network-rates_1_of_3.json.gz
https://example.com/2026-02_plan_in-network-rates_2_of_3.json.gz
https://example.com/2026-02_plan_in-network-rates_3_of_3.json.gz
```

## Scale

Some reference points for dataset sizes:

| Insurer | Files | Compressed size | Search time (3 workers) | Search time (cloud, 50 shards) |
|---------|------:|----------------:|------------------------:|-------------------------------:|
| Regional BCBS (1 state) | ~12 | ~37 GB | ~3 min | ~30s |
| National BCBS (multi-state) | ~280 | ~440 GB | ~3 hrs | ~5 min |

Individual files range from hundreds of megabytes to 10+ GB compressed. The streaming parser processes them at roughly CDN download speed since parsing is faster than the network.

## Limitations

- **No TOC parser**: You need to provide MRF URLs directly. Extracting URLs from insurer Table of Contents files is a separate step.
- **Schema coverage**: Supports the CMS in-network-rates MRF schema. Does not parse allowed-amounts or prescription drug files.
- **Provider reference resolution**: If `in_network` appears before `provider_references` in a file (non-standard but occurs), only inline `provider_groups` are matched. Rates referenced by `provider_group_id` require `provider_references` to appear first.
- **Signed URLs**: Some insurers use time-limited signed URLs (CloudFront, S3). These expire, so URL lists may need to be regenerated before each search.
- **Rate deduplication**: Results are emitted as-is from the MRF files. The same rate may appear in multiple files or with different negotiation arrangements.
- **Cloud mode requires Modal account**: The `--cloud` flag requires a [Modal](https://modal.com) account, API token configured locally, and a one-time `modal deploy deploy_modal.py`.

## References

- [CMS Price Transparency Final Rule](https://www.cms.gov/hospital-price-transparency/resources) - The federal regulation requiring insurers to publish MRF files
- [CMS MRF Schema](https://github.com/CMSgov/price-transparency-guide/tree/master/schemas/in-network-rates) - JSON schema specification for in-network-rates files
- [CMS Transparency in Coverage Lookup](https://transparency-in-coverage.cms.gov/) - CMS tool for finding insurer MRF index URLs
- [NPPES NPI Registry](https://npiregistry.cms.hhs.gov/) - National Plan & Provider Enumeration System for NPI lookups
- [Dolthub Price Transparency Data](https://www.dolthub.com/blog/2022-09-02-a-hospital-price-transparency-story/) - Prior work analyzing the scale of price transparency data
- [Turquoise Health](https://turquoise.health/) - Commercial service built on price transparency data
- [Serif Health](https://www.serifhealth.com/) - Another commercial MRF data processor

## License

MIT
