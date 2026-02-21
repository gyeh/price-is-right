#!/usr/bin/env python3
"""
Deploy npi-rates to Modal, shard URLs across parallel workers, merge results.

Usage:
    modal deploy python/deploy_modal.py           # one-time deploy
    modal run python/deploy_modal.py --npi 1770671182 --urls-file ny_urls.txt
"""

import json
import os
import sys
import time
from datetime import datetime

import modal


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    task_id = os.environ.get("MODAL_TASK_ID", "")
    if not task_id:
        import socket
        task_id = socket.gethostname()
    task_id = task_id[-8:]
    prefix = f"[ID|{task_id}] " if task_id else ""
    print(f"{ts} {prefix}{msg}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Module-level CLI parsing for params that must be known at decorator time.
# Modal's @app.function decorator evaluates at import, before main() runs,
# so we extract infra params from sys.argv early.
# ---------------------------------------------------------------------------
def _cli_arg(name, default, type_fn=str):
    flag = f"--{name}"
    for i, arg in enumerate(sys.argv):
        if arg == flag and i + 1 < len(sys.argv):
            return type_fn(sys.argv[i + 1])
        if arg.startswith(f"{flag}="):
            return type_fn(arg.split("=", 1)[1])
    return default


_MEMORY = _cli_arg("memory", 4096, int)
_CPU = _cli_arg("cpu", 2, int)
_WORKERS = 1
_SHARDS = 100

_TIMEOUT = _cli_arg("timeout", 3600, int)
_CLOUD = _cli_arg("cloud", "aws")
_REGION = _cli_arg("region", "us-east-1")

# ---------------------------------------------------------------------------
# Modal app setup
# ---------------------------------------------------------------------------
app = modal.App("npi-rates")

image = (
    modal.Image.from_dockerfile("Dockerfile")
    .run_commands("apk add --no-cache python3")
    .dockerfile_commands(["ENTRYPOINT []"])
)


@app.function(
    image=image,
    cpu=_CPU,
    memory=_MEMORY,
    timeout=_TIMEOUT,
    cloud=_CLOUD,
    region=_REGION,
)
def run_search(shard_index: int, urls: list[str], npi: str, workers: int):
    import os
    import subprocess as sp

    work_dir = f"/tmp/shard-{shard_index}"
    tmp_dir = os.path.join(work_dir, "tmp")
    os.makedirs(tmp_dir, exist_ok=True)

    urls_path = os.path.join(work_dir, "urls.txt")
    with open(urls_path, "w") as f:
        f.write("\n".join(urls))

    output_path = os.path.join(work_dir, "results.json")

    proc = sp.run(
        [
            "/npi-rates", "search",
            "--npi", npi,
            "--urls-file", urls_path,
            "--workers", str(workers),
            "--log-progress",
            "--stream",
            "--tmp-dir", tmp_dir,
            "-o", output_path,
        ],
    )

    if proc.returncode != 0:
        raise RuntimeError(f"Shard {shard_index} failed with exit code {proc.returncode}")

    with open(output_path, "rb") as f:
        return f.read()


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
    return [s for s in shards if s]


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
def main(
    npi: str,
    urls_file: str = None,
    shards: int = _SHARDS,
    workers: int = _WORKERS,
    output: str = "",
):
    if workers == 0:
        workers = _CPU

    urls = read_urls(urls_file)
    url_shards = shard_urls(urls, shards)

    log(f"NPI: {npi}")
    log(f"Files: {len(urls)} URLs across {len(url_shards)} shards")
    log(f"Infra: {_CPU} CPU, {_MEMORY} MB memory, {_CLOUD}/{_REGION}")
    log(f"Workers per shard: {workers}")

    start = time.time()

    try:
        shard_outputs = list(run_search.starmap(
            [(i, shard, npi, workers) for i, shard in enumerate(url_shards)]
        ))
    except Exception as e:
        log(f"Search failed: {e}")
        sys.exit(1)

    wall_time = time.time() - start

    merged = merge_results(shard_outputs)
    merged["search_params"]["duration_seconds"] = wall_time

    if output:
        output_path = output
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = f"results_{timestamp}.json"
    with open(output_path, "w") as f:
        json.dump(merged, f, indent=2)

    count = len(merged["results"])
    searched = merged["search_params"]["searched_files"]
    matched = merged["search_params"]["matched_files"]
    log(f"Search complete: {searched} files searched, {matched} matched, {count} rates found in {wall_time:.1f}s")
    log(f"Results saved to {output_path}")
    log("Function run completed")
