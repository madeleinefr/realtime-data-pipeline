#!/usr/bin/env python3
"""Reproducible verification of the 1,000,000+ data-point requirement.

The assignment requires the data source to contain at least 1,000,000 time-
referenced data points. The ingestion service streams an *unbounded* synthetic
event stream, so "1M+ records" is a property of sustained operation rather than
a fixed file. This script makes that property directly verifiable and
reproducible without a multi-hour live run: it drives the real record generator
(``ingestion_service.generator.generate_device_record``) exactly as the running
producer does and asserts the volume, uniqueness, canton coverage, and that
every record is timestamped and monotonically non-decreasing.

Usage:
    python scripts/verify_volume.py [--count 1000000]

Requirements covered: data volume (>= 1,000,000) and time-referencing of the
chosen sample data.
"""

from __future__ import annotations

import argparse
import json
import sys
import time

# Ensure the repository root is importable when run from anywhere.
sys.path.insert(0, ".")

from ingestion_service.generator import generate_device_record  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--count",
        type=int,
        default=1_000_000,
        help="Number of records to generate and verify (default: 1,000,000).",
    )
    args = parser.parse_args()
    target = args.count

    start = time.perf_counter()
    count = 0
    unique_ids: set[str] = set()
    cantons: set[str] = set()
    byte_total = 0
    previous_ts: str | None = None
    timestamps_ok = True

    for _ in range(target):
        record = generate_device_record()
        count += 1
        unique_ids.add(record["record_id"])
        cantons.add(record["canton"])
        byte_total += len(json.dumps(record).encode("utf-8"))

        ts = record.get("timestamp")
        if not ts:
            timestamps_ok = False
        elif previous_ts is not None and ts < previous_ts:
            timestamps_ok = False
        previous_ts = ts

    elapsed = time.perf_counter() - start

    print("=" * 56)
    print("Data-volume verification")
    print("=" * 56)
    print(f"Records generated         : {count:,}")
    print(f"Unique record_id values   : {len(unique_ids):,}")
    print(f"Distinct cantons covered  : {len(cantons)} / 26")
    print(f"All records timestamped   : {timestamps_ok}")
    print(f"Elapsed                   : {elapsed:.2f} s")
    print(f"Sustained generation rate : {count / elapsed:,.0f} rec/s")
    print(f"Total payload             : {byte_total / 1_000_000:.1f} MB")
    print("=" * 56)

    integrity_ok = (
        len(unique_ids) == count and len(cantons) == 26 and timestamps_ok
    )
    volume_ok = count >= 1_000_000

    if not integrity_ok:
        print("FAIL: record integrity check failed "
              "(uniqueness / canton coverage / timestamps).")
        return 1

    if not volume_ok:
        # Integrity held, but the run was smaller than the 1M requirement.
        # This is expected for a reduced --count smoke check.
        print(f"OK (smoke): {count:,} unique, timestamped records across all "
              f"cantons. Run with the default count to verify the 1,000,000 "
              f"requirement.")
        return 0

    print("PASS: >= 1,000,000 unique, timestamped records across all cantons.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
