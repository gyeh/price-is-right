#!/usr/bin/env python3
"""
Deploy npi-rates to Modal, run the search, download results, and tear down.

Usage:
    modal run deploy_modal.py
"""

import json
import subprocess
import sys
from datetime import datetime

import modal

VOLUME_NAME = "npi-rates-data"

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
    timeout=7200,  # 2 hours
)
def run_search():
    import os
    import subprocess as sp

    os.makedirs("/data/tmp", exist_ok=True)

    proc = sp.run(
        [
            "/npi-rates", "search",
            "--npi", "1770671182",
            "--urls-file", "/ny_urls.txt",
            "--workers", "8",
            "--no-progress",
            "--tmp-dir", "/data/tmp",
            "-o", "/data/results.json",
        ],
    )

    if proc.returncode != 0:
        raise RuntimeError(f"npi-rates exited with code {proc.returncode}")

    with open("/data/results.json", "rb") as f:
        return f.read()


def cleanup_volume():
    """Delete the remote Modal volume."""
    print("Cleaning up remote volume...")
    subprocess.run(
        [sys.executable, "-m", "modal", "volume", "delete", VOLUME_NAME, "--yes"],
        capture_output=True,
    )


@app.local_entrypoint()
def main():
    print("Deploying npi-rates to Modal (8 CPU, 16GB RAM, 200GB volume)...")

    try:
        data = run_search.remote()
    except Exception as e:
        print(f"\nSearch failed: {e}", file=sys.stderr)
        cleanup_volume()
        sys.exit(1)

    # Save results locally with timestamp suffix
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"results_{timestamp}.json"
    with open(output_path, "wb") as f:
        f.write(data)

    results = json.loads(data)
    count = len(results.get("results", []))
    print(f"\nResults saved to {output_path} ({count} rates, {len(data):,} bytes)")

    # Tear down remote volume
    cleanup_volume()
    print("Done.")
