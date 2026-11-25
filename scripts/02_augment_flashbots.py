#!/usr/bin/env python3
"""
Step 2 - Augment df_blocks_builder (Xatu) with Flashbots relay API data.

Problem: Xatu relay data has severe coverage gaps in the early post-Merge period.
  - Xatu starts Nov 25 2022 (block 16,050,384); Sep-Nov 2022 has zero coverage.
  - Even after Xatu starts, only about 2% of blocks are classified PBS in Nov
    2022, growing slowly, because Xatu was adding relays to its collection
    over time.
  - Flashbots (the dominant relay in 2022-2023, roughly 75% PBS market share)
    is almost entirely missing from Xatu's early data.

Solution: query the Flashbots relay API directly.
  - The Flashbots API has complete delivered-payload history from roughly
    slot 4,800,000 (Sep 29, 2022) through the present.
  - Anti-join against Xatu to find blocks Xatu missed.
  - Union: df_blocks_builder (Xatu) union Flashbots-only blocks.

Validation: relayscan.io /stats/day/YYYY-MM-DD/json provides known relay
  payload counts per day since the Merge; used here to spot-check coverage.

Runtime: roughly 1-2 hours (15,000-20,000 API calls at a 0.15s/call rate limit).
Checkpointed: safe to kill and resume; batch files accumulate on disk.

Output:
  PBS_DATA_DIR/raw/flashbots_batches/      staging batch parquets (temporary)
  PBS_DATA_DIR/raw/flashbots/df_blocks_flashbots.parquet   all Flashbots data
  PBS_DATA_DIR/raw/df_blocks_builder_augmented.parquet     Xatu + Flashbots, deduped

Run:
    export PBS_DATA_DIR=/path/to/crypto_data/PBS/data
    python 02_augment_flashbots.py --step 1   # download
    python 02_augment_flashbots.py --step 2   # consolidate
    python 02_augment_flashbots.py --step 3   # merge with Xatu + validate
"""
import os
import sys
import time
from pathlib import Path

import polars as pl
import requests

DATA_DIR = Path(os.environ.get("PBS_DATA_DIR", "./data"))
RAW_DIR = DATA_DIR / "raw"

FLASHBOTS_URL = "https://boost-relay.flashbots.net/relay/v1/data/bidtraces/proposer_payload_delivered"
START_SLOT = 4_800_000  # roughly Sep 29, 2022, earliest Flashbots data confirmed
REQUEST_DELAY = 0.15  # seconds between API calls (roughly 6-7 req/s)
SAVE_EVERY = 200  # API calls between checkpoint saves (roughly 20,000 records)

BATCH_DIR = RAW_DIR / "flashbots_batches"
FLASHBOTS_OUT = RAW_DIR / "flashbots" / "df_blocks_flashbots.parquet"
AUGMENTED_OUT = RAW_DIR / "df_blocks_builder_augmented.parquet"
XATU_FILE = RAW_DIR / "df_blocks_builder.parquet"


def fetch_batch(cursor: int, limit: int = 100) -> list[dict]:
    """Fetch one page of delivered payloads (slot <= cursor), descending."""
    for attempt in range(5):
        try:
            r = requests.get(FLASHBOTS_URL, params={"cursor": str(cursor), "limit": limit}, timeout=30)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            wait = 2 ** attempt
            print(f"  Retry {attempt + 1}/5 at cursor={cursor:,}: {e}. Waiting {wait}s", flush=True)
            time.sleep(wait)
    print(f"  All retries failed at cursor={cursor:,}. Skipping.", flush=True)
    return []


def records_to_df(records: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(records)
    return df.with_columns([
        pl.col("slot").cast(pl.Int64),
        pl.col("block_number").cast(pl.Int64),
        pl.col("gas_limit").cast(pl.Int64),
        pl.col("gas_used").cast(pl.Int64),
        pl.col("num_tx").cast(pl.Int32),
        # Float64, not Int64: value can exceed Int64 max (e.g. 27+ ETH blocks ~2.79e19 wei)
        pl.col("value").cast(pl.Float64).alias("value_wei"),
    ]).drop("value").with_columns([
        (pl.col("value_wei") / 1e18).alias("value_eth"),
    ])


def download_flashbots() -> None:
    """
    Paginate the Flashbots relay API backwards (newest to oldest) down to
    START_SLOT. Checkpoints every SAVE_EVERY calls into numbered batch
    parquets in BATCH_DIR. Safe to kill and re-run; resumes from the minimum
    slot already saved.
    """
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    existing_batches = sorted(BATCH_DIR.glob("batch_*.parquet"))

    if existing_batches:
        min_slot = min(pl.scan_parquet(f).select(pl.col("slot").min()).collect().item() for f in existing_batches)
        cursor = min_slot - 1
        n_batches = len(existing_batches)
        n_total = sum(pl.scan_parquet(f).select(pl.len()).collect().item() for f in existing_batches)
        print(f"Resuming: {n_batches} batches, {n_total:,} records, next cursor={cursor:,}", flush=True)
    else:
        latest = fetch_batch(cursor=99_999_999, limit=1)
        if not latest:
            print("ERROR: could not fetch latest slot from Flashbots API.", flush=True)
            sys.exit(1)
        cursor = int(latest[0]["slot"])
        n_batches = 0
        n_total = 0
        print(f"Starting fresh from slot {cursor:,}", flush=True)

    pending: list[dict] = []
    n_calls = 0
    consecutive_empty = 0
    MAX_EMPTY_SKIP = 10
    EMPTY_SLOT_SKIP = 500

    while cursor >= START_SLOT:
        batch = fetch_batch(cursor)

        if not batch:
            consecutive_empty += 1
            print(f"  Empty at cursor={cursor:,}, skip #{consecutive_empty}/{MAX_EMPTY_SKIP} (skipping back {EMPTY_SLOT_SKIP} slots)", flush=True)
            if consecutive_empty >= MAX_EMPTY_SKIP:
                print(f"  {MAX_EMPTY_SKIP} consecutive empty responses, stopping download.", flush=True)
                break
            cursor -= EMPTY_SLOT_SKIP
            time.sleep(1.0)
            continue

        consecutive_empty = 0

        batch = [r for r in batch if int(r["slot"]) >= START_SLOT]
        if not batch:
            print(f"  Reached START_SLOT ({START_SLOT:,}), download complete.", flush=True)
            break

        pending.extend(batch)
        n_calls += 1
        n_total += len(batch)
        cursor = min(int(r["slot"]) for r in batch) - 1

        if n_calls % 100 == 0:
            print(f"  [{n_calls:,} calls] cursor={cursor:,} | records={n_total:,} | batches={n_batches}", flush=True)

        if n_calls % SAVE_EVERY == 0:
            _flush_batch(pending, n_batches)
            n_batches += 1
            pending = []

        time.sleep(REQUEST_DELAY)

    if pending:
        _flush_batch(pending, n_batches)
        n_batches += 1

    print(f"\nDownload complete: {n_total:,} records in {n_batches} batch files.", flush=True)


def _flush_batch(records: list[dict], batch_idx: int) -> None:
    path = BATCH_DIR / f"batch_{batch_idx:05d}.parquet"
    records_to_df(records).write_parquet(path, compression="zstd")
    print(f"  -> Saved batch {batch_idx:05d} ({len(records):,} records) to {path.name}", flush=True)


def consolidate_flashbots() -> pl.DataFrame:
    """Concat all batch files, deduplicate on block_number, save to FLASHBOTS_OUT."""
    FLASHBOTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    batch_files = sorted(BATCH_DIR.glob("batch_*.parquet"))
    if not batch_files:
        print("No batch files found. Run step 1 first.", flush=True)
        sys.exit(1)

    print(f"Consolidating {len(batch_files)} batch files ...", flush=True)
    df = pl.concat([pl.read_parquet(f) for f in batch_files])
    df = df.sort("slot").unique(subset=["block_number"], keep="first")

    print(f"  Total Flashbots blocks: {len(df):,}", flush=True)
    print(f"  Slot range: {df['slot'].min():,} - {df['slot'].max():,}", flush=True)
    print(f"  Block range: {df['block_number'].min():,} - {df['block_number'].max():,}", flush=True)

    df.write_parquet(FLASHBOTS_OUT, compression="zstd")
    print(f"  Saved -> {FLASHBOTS_OUT}", flush=True)
    return df


def augment_xatu(df_flashbots: pl.DataFrame) -> None:
    """
    Union df_blocks_builder (Xatu) with Flashbots blocks not already in Xatu.
    Deduplicates on block_number; Xatu rows take priority (richer metadata).
    """
    print(f"\nLoading Xatu: {XATU_FILE}", flush=True)
    df_xatu = pl.read_parquet(XATU_FILE)
    print(f"  Xatu rows:              {len(df_xatu):,}", flush=True)

    xatu_blocks = set(df_xatu["block_number"].to_list())
    df_new = df_flashbots.filter(~pl.col("block_number").is_in(xatu_blocks))

    print(f"  Flashbots total:        {len(df_flashbots):,}", flush=True)
    print(f"  Already in Xatu:        {len(df_flashbots) - len(df_new):,}", flush=True)
    print(f"  New blocks to add:      {len(df_new):,}", flush=True)

    type_casts = [
        pl.col(c).cast(df_xatu[c].dtype)
        for c in df_xatu.columns
        if c in df_new.columns and df_xatu[c].dtype != df_new[c].dtype
    ]
    if type_casts:
        df_new = df_new.with_columns(type_casts)

    relay_count_dtype = df_xatu["relay_count"].dtype if "relay_count" in df_xatu.columns else pl.Int64

    df_new = (
        df_new
        .with_columns([
            pl.lit(1).cast(relay_count_dtype).alias("relay_count"),
            pl.lit("Flashbots").alias("relay_1"),
            pl.lit(None).cast(pl.Utf8).alias("builder_label"),
            pl.lit("flashbots_api").alias("builder_name_source"),
        ])
        .drop([c for c in ["gas_limit", "parent_hash"] if c in df_new.columns])
    )

    df_augmented = pl.concat([df_xatu, df_new], how="diagonal").sort("block_number")

    print(f"\n  Augmented total:        {len(df_augmented):,} PBS blocks", flush=True)
    print(f"  Block range: {df_augmented['block_number'].min():,} - {df_augmented['block_number'].max():,}", flush=True)

    df_augmented.write_parquet(AUGMENTED_OUT, compression="zstd")
    print(f"  Saved -> {AUGMENTED_OUT}", flush=True)


def validate_against_relayscan(df_augmented: pl.DataFrame, day: str = "2022-10-01") -> None:
    """Spot-check one day against relayscan.io daily stats. day: 'YYYY-MM-DD'."""
    import datetime
    d = datetime.date.fromisoformat(day)
    MERGE_BLOCK = 15_537_393
    merge_date = datetime.date(2022, 9, 15)
    days_since = (d - merge_date).days
    block_lo = MERGE_BLOCK + days_since * 7200
    block_hi = block_lo + 7200

    n_pbs = df_augmented.filter(pl.col("block_number").is_between(block_lo, block_hi)).height

    url = f"https://relayscan.io/stats/day/{day}/json"
    try:
        data = requests.get(url, timeout=10).json()
        known_total = sum(r["num_payloads"] for r in data.get("relays", []))
        print(f"\nValidation for {day}:", flush=True)
        print(f"  relayscan known PBS blocks: {known_total:,}", flush=True)
        print(f"  Our augmented count:        {n_pbs:,}", flush=True)
        print(f"  Coverage:                   {n_pbs / known_total:.1%}" if known_total else "  (no relayscan data)", flush=True)
    except Exception as e:
        print(f"  Relayscan validation failed: {e}", flush=True)


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["1", "2", "3", "all"], default="all")
    args = parser.parse_args()

    run_all = args.step == "all"
    run_dl = args.step == "1" or run_all
    run_cons = args.step == "2" or run_all
    run_aug = args.step == "3" or run_all

    if run_dl:
        print("=== Step 1: Download Flashbots relay data ===", flush=True)
        download_flashbots()

    if run_cons:
        print("\n=== Step 2: Consolidate batch files ===", flush=True)
        df_flashbots = consolidate_flashbots()
    elif run_aug:
        if not FLASHBOTS_OUT.exists():
            print(f"ERROR: {FLASHBOTS_OUT} not found. Run step 2 first.", flush=True)
            sys.exit(1)
        df_flashbots = pl.read_parquet(FLASHBOTS_OUT)

    if run_aug:
        print("\n=== Step 3: Augment Xatu with Flashbots ===", flush=True)
        augment_xatu(df_flashbots)

        print("\n=== Validation spot-checks ===", flush=True)
        df_aug = pl.read_parquet(AUGMENTED_OUT)
        for d in ["2022-10-01", "2022-11-01", "2023-01-01", "2023-06-01"]:
            validate_against_relayscan(df_aug, d)

    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
