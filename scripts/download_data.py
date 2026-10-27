"""Downloads the UCI household power data set into ./data (same as the producer does on first start).

Usage: python scripts/download_data.py
"""
import urllib.request
import zipfile
from pathlib import Path

URL = ("https://archive.ics.uci.edu/static/public/235/"
       "individual+household+electric+power+consumption.zip")
TARGET = Path(__file__).resolve().parents[1] / "data" / "household_power_consumption.txt"


def main() -> None:
    if TARGET.exists():
        print(f"already there: {TARGET}")
        return
    zip_path = TARGET.with_suffix(".zip")
    print(f"downloading {URL} ...")
    urllib.request.urlretrieve(URL, zip_path)
    with zipfile.ZipFile(zip_path) as zf:
        member = next(n for n in zf.namelist() if n.endswith(".txt"))
        with zf.open(member) as src, open(TARGET, "wb") as dst:
            dst.write(src.read())
    zip_path.unlink()
    lines = sum(1 for _ in open(TARGET, encoding="utf-8", errors="replace")) - 1
    print(f"saved {TARGET} ({lines:,} data rows)")


if __name__ == "__main__":
    main()
