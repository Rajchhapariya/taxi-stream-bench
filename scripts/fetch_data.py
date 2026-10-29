#!/usr/bin/env python3
"""Downloads NYC TLC Yellow Taxi parquet data and verifies SHA-256 integrity."""

import argparse
import hashlib
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

SOURCE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
PINNED_SHA256 = "c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510"
DEFAULT_TARGET_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "yellow_tripdata_2024-01.parquet",
)
METADATA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data",
    "metadata.json",
)


def compute_sha256(filepath: str) -> str:
    hasher = hashlib.sha256()
    with open(filepath, "rb") as f:
        while chunk := f.read(1024 * 1024):
            hasher.update(chunk)
    return hasher.hexdigest()


def fetch_dataset(target_path: str = DEFAULT_TARGET_PATH, force: bool = False) -> str:
    os.makedirs(os.path.dirname(target_path), exist_ok=True)

    if os.path.exists(target_path) and not force:
        print(f"Checking existing dataset: {target_path}")
        current_hash = compute_sha256(target_path)
        if current_hash != PINNED_SHA256:
            raise ValueError(
                f"SHA-256 mismatch for existing file.\n"
                f"Expected: {PINNED_SHA256}\n"
                f"Got:      {current_hash}"
            )
        print(f"SHA-256 matches pinned hash: {current_hash}")
        if not os.path.exists(METADATA_PATH):
            metadata = {
                "source_url": SOURCE_URL,
                "pinned_sha256": PINNED_SHA256,
                "actual_sha256": current_hash,
                "file_size_bytes": os.path.getsize(target_path),
                "verified_at_utc": datetime.now(timezone.utc).isoformat(),
                "target_path": os.path.relpath(target_path, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            }
            with open(METADATA_PATH, "w", encoding="utf-8") as f:
                json.dump(metadata, f, indent=2)
        return target_path

    print(f"Downloading {SOURCE_URL}...")
    temp_target = target_path + ".tmp"
    try:
        urllib.request.urlretrieve(SOURCE_URL, temp_target)
        downloaded_hash = compute_sha256(temp_target)
        if downloaded_hash != PINNED_SHA256:
            os.remove(temp_target)
            raise ValueError(
                f"Downloaded file hash differs from expected pinned hash.\n"
                f"Expected: {PINNED_SHA256}\n"
                f"Got:      {downloaded_hash}"
            )
        os.replace(temp_target, target_path)
        file_size = os.path.getsize(target_path)
        print(f"Downloaded {file_size / (1024 * 1024):.2f} MB. Hash verified.")

        metadata = {
            "source_url": SOURCE_URL,
            "pinned_sha256": PINNED_SHA256,
            "actual_sha256": downloaded_hash,
            "file_size_bytes": file_size,
            "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            "target_path": os.path.relpath(target_path, os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        }
        with open(METADATA_PATH, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2)

        return target_path
    finally:
        if os.path.exists(temp_target):
            try:
                os.remove(temp_target)
            except OSError:
                pass


def main():
    parser = argparse.ArgumentParser(description="Download and verify NYC TLC Yellow Taxi parquet dataset.")
    parser.add_argument("--output", type=str, default=DEFAULT_TARGET_PATH, help="Target download destination")
    parser.add_argument("--force", action="store_true", help="Force redownload even if file exists")
    args = parser.parse_args()

    fetch_dataset(target_path=args.output, force=args.force)


if __name__ == "__main__":
    main()
