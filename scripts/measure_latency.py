"""Measures end-to-end latency: manual event sent to Kafka -> aggregate visible in the REST API.

Each run injects one reading for a new meter (m-latency-<n>) through the producer container and
polls GET /latest?meter_id=... until the 5-minute window appears.
Usage: python scripts/measure_latency.py [--runs 5]
"""
import argparse
import json
import re
import subprocess
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=5)
    ap.add_argument("--url", default="http://localhost:8000")
    a = ap.parse_args()
    tag = datetime.now().strftime("%H%M%S")
    results = []
    for i in range(1, a.runs + 1):
        meter = f"m-latency-{tag}-{i}"
        out = subprocess.run(["docker", "compose", "exec", "-T", "--index", "1", "producer",
                              "python", "inject_event.py", "--meter-id", meter],
                             cwd=ROOT, capture_output=True, text=True, check=True).stdout
        sent = datetime.fromisoformat(re.search(r'"sent_at": "([^"]+)"', out).group(1))
        while True:
            with urllib.request.urlopen(f"{a.url}/latest?meter_id={meter}", timeout=5) as r:
                if json.load(r):
                    break
            time.sleep(0.2)
        latency = (datetime.now(timezone.utc) - sent).total_seconds()
        results.append(latency)
        print(f"run {i}: {latency:.1f} s")
        time.sleep(3)
    print(f"min {min(results):.1f} s, mean {sum(results) / len(results):.1f} s, max {max(results):.1f} s")


if __name__ == "__main__":
    main()
