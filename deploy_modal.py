#!/usr/bin/env python3
"""
Deploy npi-rates to Modal, shard URLs across parallel workers, merge results.

Usage:
    modal run deploy_modal.py
"""

import json
import subprocess
import sys
import time
from datetime import datetime

import modal

VOLUME_NAME = "npi-rates-data"
NUM_SHARDS = 20
NPI = "1770671182"

app = modal.App("npi-rates")

image = (
    modal.Image.from_dockerfile("Dockerfile")
    .run_commands("apk add --no-cache python3")
    .dockerfile_commands(["ENTRYPOINT []"])
)

volume = modal.Volume.from_name(VOLUME_NAME, create_if_missing=True)


@app.function(
    image=image,
    cpu=8,
    memory=16384,  # 16 GB
    volumes={"/data": volume},
    timeout=7200  # 2 hours
)
def run_search(shard_index: int, urls: list[str]):
    import os
    import subprocess as sp
    import tempfile

    work_dir = f"/data/shard-{shard_index}"
    tmp_dir = os.path.join(work_dir, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    # Write URLs to a temp file for --urls-file
    urls_path = os.path.join(work_dir, "urls.txt")
    with open(urls_path, "w") as f:
        f.write("\n".join(urls))

    output_path = os.path.join(work_dir, "results.json")

    proc = sp.run(
        [
            "/npi-rates", "search",
            "--npi", NPI,
            "--urls-file", urls_path,
            "--workers", "8",
            "--log-progress",
            "--tmp-dir", tmp_dir,
            "-o", output_path,
        ],
    )

    if proc.returncode != 0:
        raise RuntimeError(f"Shard {shard_index} failed with exit code {proc.returncode}")

    with open(output_path, "rb") as f:
        return f.read()


def cleanup_volume():
    """Delete the remote Modal volume."""
    print("Cleaning up remote volume...")
    subprocess.run(
        [sys.executable, "-m", "modal", "volume", "delete", VOLUME_NAME, "--yes"],
        capture_output=True,
    )


def read_urls(path: str) -> list[str]:
    """Read URLs from a file, skipping blank lines and comments."""
    urls = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                urls.append(line)
    return urls


def shard_urls(urls: list[str], n: int) -> list[list[str]]:
    """Split URLs into n roughly-equal shards via round-robin."""
    shards: list[list[str]] = [[] for _ in range(n)]
    for i, url in enumerate(urls):
        shards[i % n].append(url)
    return [s for s in shards if s]  # drop empty shards


def merge_results(shard_outputs: list[bytes]) -> dict:
    """Merge shard results into a single SearchOutput."""
    all_results = []
    total_searched = 0
    total_matched = 0
    total_duration = 0.0
    npis = []

    for data in shard_outputs:
        output = json.loads(data)
        params = output.get("search_params", {})
        total_searched += params.get("searched_files", 0)
        total_matched += params.get("matched_files", 0)
        total_duration = max(total_duration, params.get("duration_seconds", 0))
        if not npis:
            npis = params.get("npis", [])
        all_results.extend(output.get("results", []))

    return {
        "search_params": {
            "npis": npis,
            "searched_files": total_searched,
            "matched_files": total_matched,
            "duration_seconds": total_duration,
        },
        "results": all_results,
    }


@app.local_entrypoint()
def main(urls_file: str = "ny_urls.txt"):
    urls = read_urls(urls_file)
    shards = shard_urls(urls, NUM_SHARDS)

    print(f"Deploying npi-rates to Modal ({len(urls)} URLs across {len(shards)} shards, 8 CPU / 16GB each)...")
    start = time.time()

    try:
        # Launch all shards in parallel via Modal's .map()
        shard_outputs = list(run_search.starmap(
            [(i, shard) for i, shard in enumerate(shards)]
        ))
    except Exception as e:
        print(f"\nSearch failed: {e}", file=sys.stderr)
        cleanup_volume()
        sys.exit(1)

    wall_time = time.time() - start

    # Merge all shard results
    merged = merge_results(shard_outputs)
    merged["search_params"]["duration_seconds"] = wall_time

    # Save locally with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"results_{timestamp}.json"
    with open(output_path, "w") as f:
        json.dump(merged, f, indent=2)

    count = len(merged["results"])
    searched = merged["search_params"]["searched_files"]
    matched = merged["search_params"]["matched_files"]
    print(f"\nSearch complete: {searched} files searched, {matched} matched, {count} rates found in {wall_time:.1f}s")
    print(f"Results saved to {output_path}")

    # Tear down remote volume
    cleanup_volume()
    print("Done.")
