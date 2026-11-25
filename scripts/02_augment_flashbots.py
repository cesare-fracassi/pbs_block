#!/usr/bin/env python3
"""
Step 2 - Add Flashbots relay API data to the Xatu relay table.

Why: Xatu's relay data has coverage gaps, mostly in the months after the Merge.
  - Xatu starts Nov 25 2022 (block 16,050,384); Sep to Nov 2022 has no coverage.
  - After it starts, only about 2% of blocks are classified PBS in Nov 2022, growing slowly, because
    Xatu was adding relays to its collection over time.
  - Flashbots (roughly 75% of PBS blocks in 2022 and 2023) is almost missing from Xatu's early data.
The Flashbots relay API has the complete delivered-payload history from roughly slot 4,800,000
(Sep 29, 2022) to the present.

What it does:
  1. Sync: fetch the delivered payloads the API has and the local file lacks. Pages are read from the newest
     slot backwards, one request every --delay seconds (default 1.0), until the newest slot already saved
     (or START_SLOT on the first run). Pages are staged in data/raw/flashbots_batches/ so an interrupted run
     continues where it stopped; the staging files are merged into data/raw/flashbots/df_blocks_flashbots.parquet
     and removed.
  2. Augment: add the Flashbots blocks that Xatu does not have to the Xatu table from step 1. Xatu rows win
     when a block is in both. Output: data/interim/df_blocks_builder_augmented.parquet.
  Both tables stay in data/interim/: df_blocks_builder.parquet (Xatu only, from step 1) and
  df_blocks_builder_augmented.parquet (Xatu plus Flashbots, read by step 5).

Runtime: a first full download is about 15,000 to 20,000 requests (estimate from the first run), so several
hours at one request per second. A monthly update is about a thousand requests.

Run:
    python scripts/02_augment_flashbots.py
    python scripts/02_augment_flashbots.py --delay 2     # slower
    python scripts/02_augment_flashbots.py --no-download # only rebuild the augmented table
"""
import argparse
import sys

import polars as pl

from _common import DEFAULT_DELAY, INTERIM_DIR, RAW_DIR, polite_get

FLASHBOTS_URL = "https://boost-relay.flashbots.net/relay/v1/data/bidtraces/proposer_payload_delivered"
START_SLOT = 4_800_000  # roughly Sep 29, 2022, earliest Flashbots data confirmed
SAVE_EVERY = 200  # requests between staging saves (roughly 20,000 records)

BATCH_DIR = RAW_DIR / "flashbots_batches"
FLASHBOTS_OUT = RAW_DIR / "flashbots" / "df_blocks_flashbots.parquet"
XATU_FILE = INTERIM_DIR / "df_blocks_builder.parquet"
AUGMENTED_OUT = INTERIM_DIR / "df_blocks_builder_augmented.parquet"


def fetch_batch(cursor: int, limit: int = 100, delay: float = DEFAULT_DELAY) -> list[dict]:
    """One page of delivered payloads with slot <= cursor, newest first. Raises on a failed request."""
    r = polite_get(FLASHBOTS_URL, delay=delay, timeout=30, params={"cursor": str(cursor), "limit": limit})
    r.raise_for_status()
    return r.json()


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


def _flush_batch(records: list[dict], batch_idx: int) -> None:
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    path = BATCH_DIR / f"batch_{batch_idx:05d}.parquet"
    tmp = path.with_suffix(".parquet.tmp")
    records_to_df(records).write_parquet(tmp, compression="zstd")
    tmp.replace(path)
    print(f"  -> staged batch {batch_idx:05d} ({len(records):,} records)", flush=True)


def sync_flashbots(delay: float) -> None:
    """
    Fetch every delivered payload newer than what FLASHBOTS_OUT holds (all of them if it does not exist yet),
    walking back from the newest slot. Staged in BATCH_DIR; call consolidate_flashbots() afterwards.
    """
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    if FLASHBOTS_OUT.exists():
        stop_slot = pl.scan_parquet(FLASHBOTS_OUT).select(pl.col("slot").max()).collect().item()
        print(f"Local Flashbots file goes up to slot {stop_slot:,}; fetching newer slots.", flush=True)
    else:
        stop_slot = START_SLOT - 1
        print(f"No local Flashbots file; fetching everything from slot {START_SLOT:,}.", flush=True)

    batches = sorted(BATCH_DIR.glob("batch_*.parquet"))
    if batches:
        min_slot = min(pl.scan_parquet(f).select(pl.col("slot").min()).collect().item() for f in batches)
        n_batches = len(batches)
        n_total = sum(pl.scan_parquet(f).select(pl.len()).collect().item() for f in batches)
        cursor = min_slot - 1
        print(f"Resuming: {n_batches} staged batches, {n_total:,} records, next cursor {cursor:,}", flush=True)
    else:
        latest = fetch_batch(cursor=99_999_999, limit=1, delay=delay)
        if not latest:
            sys.exit("ERROR: could not fetch the latest slot from the Flashbots API.")
        cursor = int(latest[0]["slot"])
        n_batches = n_total = 0
        print(f"Newest slot on the relay: {cursor:,}", flush=True)
        if cursor <= stop_slot:
            print("Already up to date.", flush=True)
            return

    pending: list[dict] = []
    n_calls = 0
    while cursor > stop_slot:
        page = fetch_batch(cursor, delay=delay)
        if not page:
            # Nothing older than the cursor: this is the start of the history the API serves.
            print(f"  No records at or below slot {cursor:,}; treating this as the start of the available history.", flush=True)
            break
        page = [r for r in page if int(r["slot"]) > stop_slot]
        if not page:
            print(f"  Reached the slot already saved ({stop_slot:,}).", flush=True)
            break
        pending.extend(page)
        n_calls += 1
        n_total += len(page)
        cursor = min(int(r["slot"]) for r in page) - 1
        if n_calls % 100 == 0:
            print(f"  [{n_calls:,} requests] cursor {cursor:,} | records {n_total:,} | staged batches {n_batches}", flush=True)
        if n_calls % SAVE_EVERY == 0:
            _flush_batch(pending, n_batches)
            n_batches += 1
            pending = []

    if pending:
        _flush_batch(pending, n_batches)
        n_batches += 1
    print(f"Sync complete: {n_total:,} new records in {n_batches} staged batches.", flush=True)


def consolidate_flashbots() -> pl.DataFrame:
    """
    Merge the staged batches into FLASHBOTS_OUT (existing rows kept), one row per block_number, then remove the
    staging files this run produced.
    """
    FLASHBOTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    batch_files = sorted(BATCH_DIR.glob("batch_*.parquet"))
    parts = [pl.read_parquet(f) for f in batch_files]
    if FLASHBOTS_OUT.exists():
        parts.insert(0, pl.read_parquet(FLASHBOTS_OUT))
    if not parts:
        sys.exit("No Flashbots data: nothing staged and no local file. Run the sync first.")

    df = pl.concat(parts, how="diagonal").sort("slot").unique(subset=["block_number"], keep="first").sort("slot")
    print(f"Flashbots blocks: {len(df):,}", flush=True)
    print(f"  slot range:  {df['slot'].min():,} - {df['slot'].max():,}", flush=True)
    print(f"  block range: {df['block_number'].min():,} - {df['block_number'].max():,}", flush=True)

    tmp = FLASHBOTS_OUT.with_suffix(".parquet.tmp")
    df.write_parquet(tmp, compression="zstd")
    tmp.replace(FLASHBOTS_OUT)
    print(f"  saved -> {FLASHBOTS_OUT}", flush=True)

    for f in batch_files:
        f.unlink()
    if BATCH_DIR.exists() and not any(BATCH_DIR.iterdir()):
        BATCH_DIR.rmdir()
    return df


def augment_xatu(df_flashbots: pl.DataFrame) -> None:
    """
    Union the Xatu table with the Flashbots blocks it does not have. One row per block_number; Xatu rows win
    because they carry more metadata.
    """
    if not XATU_FILE.exists():
        sys.exit(f"{XATU_FILE} not found. Run step 1 (01_download_xatu.py) first.")
    print(f"\nLoading Xatu table: {XATU_FILE}", flush=True)
    df_xatu = pl.read_parquet(XATU_FILE)
    print(f"  Xatu rows:           {len(df_xatu):,}", flush=True)

    xatu_blocks = set(df_xatu["block_number"].to_list())
    df_new = df_flashbots.filter(~pl.col("block_number").is_in(xatu_blocks))

    print(f"  Flashbots total:     {len(df_flashbots):,}", flush=True)
    print(f"  Already in Xatu:     {len(df_flashbots) - len(df_new):,}", flush=True)
    print(f"  New blocks to add:   {len(df_new):,}", flush=True)

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

    print(f"\n  Augmented total:     {len(df_augmented):,} PBS blocks", flush=True)
    print(f"  Block range: {df_augmented['block_number'].min():,} - {df_augmented['block_number'].max():,}", flush=True)

    AUGMENTED_OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = AUGMENTED_OUT.with_suffix(".parquet.tmp")
    df_augmented.write_parquet(tmp, compression="zstd")
    tmp.replace(AUGMENTED_OUT)
    print(f"  Saved -> {AUGMENTED_OUT}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Flashbots relay data and add it to the Xatu relay table.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="seconds between two requests")
    parser.add_argument("--no-download", action="store_true", help="skip the sync; use the local Flashbots file")
    args = parser.parse_args()

    if not args.no_download:
        print("=== Sync Flashbots relay data ===", flush=True)
        sync_flashbots(args.delay)
    print("\n=== Consolidate ===", flush=True)
    df_flashbots = consolidate_flashbots()
    print("\n=== Augment the Xatu table with Flashbots ===", flush=True)
    augment_xatu(df_flashbots)
    print("\nDone.", flush=True)


if __name__ == "__main__":
    main()
