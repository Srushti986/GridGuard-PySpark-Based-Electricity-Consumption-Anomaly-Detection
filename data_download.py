"""Download and extract the UCI "Individual Household Electric Power Consumption" dataset.

    python data_download.py                 # download + extract into ./data
    python data_download.py --dest /content/data
    python data_download.py --check         # only verify an existing file

Official page: https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption

The original ZIP and the extracted text file are never modified afterwards; cleaned
data is written to separate output tables by the pipeline.
"""
from __future__ import annotations

import argparse
import shutil
import sys
import urllib.request
import zipfile
from pathlib import Path

from src.config import PROJECT_ROOT

DATASET_URLS = [
    # Current UCI static download link
    "https://archive.ics.uci.edu/static/public/235/individual+household+electric+power+consumption.zip",
    # Legacy UCI location (kept as a fallback)
    "https://archive.ics.uci.edu/ml/machine-learning-databases/00235/household_power_consumption.zip",
]
ZIP_NAME = "individual_household_electric_power_consumption.zip"
DATA_FILE = "household_power_consumption.txt"
EXPECTED_HEADER = "Date;Time;Global_active_power;Global_reactive_power;Voltage;Global_intensity;Sub_metering_1;Sub_metering_2;Sub_metering_3"

MANUAL_HELP = f"""
Automatic download failed. To continue manually:
  1. Open https://archive.ics.uci.edu/dataset/235/individual+household+electric+power+consumption
  2. Click "Download" and unzip the archive.
  3. Place '{DATA_FILE}' in the data/ folder (or set GRIDGUARD_DATASET_PATH to its location).
  In Google Colab: use the Files panel (left sidebar) -> Upload, or files.upload(), then
  move the file to /content/GridGuard/data/{DATA_FILE}.
"""


def _progress(block_count: int, block_size: int, total_size: int) -> None:
    if total_size <= 0:
        return
    pct = min(100, block_count * block_size * 100 // total_size)
    sys.stdout.write(f"\r  downloading... {pct:3d}%")
    sys.stdout.flush()


def check_data_file(path: Path) -> tuple[bool, str]:
    """Verify the file exists and has the expected header line."""
    if not path.is_file():
        return False, f"{path} not found"
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        header = fh.readline().strip().lstrip("\ufeff")
    if header != EXPECTED_HEADER:
        return False, f"unexpected header in {path}: {header[:80]!r}"
    return True, f"{path} looks valid ({path.stat().st_size / 1e6:.1f} MB)"


def download_and_extract(dest: Path, force: bool = False) -> Path:
    dest.mkdir(parents=True, exist_ok=True)
    target = dest / DATA_FILE
    ok, msg = check_data_file(target)
    if ok and not force:
        print(f"Already present: {msg}")
        return target

    zip_path = dest / ZIP_NAME
    last_error = None
    for url in DATASET_URLS:
        try:
            print(f"Downloading {url}")
            urllib.request.urlretrieve(url, zip_path, _progress)  # noqa: S310 (fixed https URLs)
            print()
            break
        except Exception as exc:  # network error, 404, ...
            last_error = exc
            print(f"\n  failed: {exc}")
    else:
        raise RuntimeError(f"Could not download the dataset ({last_error}).{MANUAL_HELP}")

    try:
        with zipfile.ZipFile(zip_path) as zf:
            members = [m for m in zf.namelist() if m.endswith(DATA_FILE)]
            if not members:
                raise RuntimeError(f"{DATA_FILE} not found inside the ZIP (contains: {zf.namelist()})")
            print(f"Extracting {members[0]} ...")
            with zf.open(members[0]) as src, open(target, "wb") as out:
                shutil.copyfileobj(src, out)
    except zipfile.BadZipFile as exc:
        raise RuntimeError(f"Downloaded file is not a valid ZIP ({exc}).{MANUAL_HELP}") from exc

    ok, msg = check_data_file(target)
    if not ok:
        raise RuntimeError(f"Extraction finished but validation failed: {msg}{MANUAL_HELP}")
    print(f"Ready: {msg}")
    return target


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dest", type=Path, default=PROJECT_ROOT / "data", help="target folder (default: ./data)")
    p.add_argument("--force", action="store_true", help="download again even if the file exists")
    p.add_argument("--check", action="store_true", help="only validate an existing file")
    args = p.parse_args(argv)

    if args.check:
        ok, msg = check_data_file(args.dest / DATA_FILE)
        print(("OK: " if ok else "PROBLEM: ") + msg)
        return 0 if ok else 1
    try:
        download_and_extract(args.dest, args.force)
    except RuntimeError as exc:
        print(f"\nERROR: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
