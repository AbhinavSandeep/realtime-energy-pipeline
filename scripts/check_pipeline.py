"""End-to-end check from the host: is data flowing through every stage?

Calls the API twice (with a pause) and shows whether readings and windows are growing.
Usage: python scripts/check_pipeline.py [--url http://localhost:8000] [--key API_KEY] [--wait 15]
"""
import argparse
import json
import time
import urllib.request


def get(url: str, key: str | None):
    req = urllib.request.Request(url, headers={"X-API-Key": key} if key else {})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--key", default=None)
    ap.add_argument("--wait", type=int, default=15)
    a = ap.parse_args()

    print("health:", get(f"{a.url}/health", None))
    first = get(f"{a.url}/stats", a.key)
    print(f"waiting {a.wait} s ...")
    time.sleep(a.wait)
    second = get(f"{a.url}/stats", a.key)

    ok = True
    for k in ("readings", "windows_5min", "windows_15min", "windows_daily", "invalid_readings"):
        d = second[k] - first[k]
        print(f"  {k:<18} {first[k]:>10,} -> {second[k]:>10,}  (+{d:,})")
        if k in ("readings", "windows_5min") and d <= 0:
            ok = False
    for m in get(f"{a.url}/latest", a.key):
        print(f"  latest window {m['meter_id']}: {m['window_start']}  avg {m['avg_active_power']:.3f} kW "
              f"({m['reading_count']} readings)")
    print("PIPELINE OK: data is flowing" if ok else "NO PROGRESS: check `docker compose logs producer spark-job`")


if __name__ == "__main__":
    main()
