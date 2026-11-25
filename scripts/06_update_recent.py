#!/usr/bin/env python3
"""
Step 6 - Update existing PBS data with new blocks produced since the last run.

Only touches Xatu and Flashbots (the two free, no-auth sources). It does NOT
re-run the BigQuery steps (03/04) - those are a separate, costed update left
for the user to run deliberately (see the main README).

Why this script exists, not just "rerun 01 and 02":

  01_download_xatu.py is genuinely incremental: it checks each day's file
  individually and only fetches missing days. Just rerunning it correctly
  picks up new days. No fix needed - this script calls it unchanged.

  02_augment_flashbots.py's download_flashbots() walks the Flashbots API
  BACKWARD from the newest slot down to a fixed START_SLOT (Sep 2022), and
  its resume logic picks up from the MINIMUM slot already saved in an
  interrupted batch. That is correct for resuming a run that was killed
  partway through, but WRONG for extending an already-COMPLETE run with new
  data: a complete run's minimum slot is already at START_SLOT, so resuming
  from there walks backward over ground already covered and finds nothing
  new at the recent end. This script adds the missing capability: find the
  MAXIMUM slot already saved, and walk backward from today's newest slot
  down to that maximum, saving the new batches separately.

Run:
    export PBS_DATA_DIR=/path/to/crypto_data/PBS/data
    python 06_update_recent.py
"""
import os
import sys
import time
from pathlib import Path

import polars as pl
import requests

def _load_module(filename: str, name: str):
    """Load a script whose filename starts with a digit (not a valid module
    name for import_module) directly from its file path instead."""
    import importlib.util
    path = Path(__file__).parent / filename
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


xatu_mod = _load_module("01_download_xatu.py", "xatu_mod")
flashbots_mod = _load_module("02_augment_flashbots.py", "flashbots_mod")

DATA_DIR = Path(os.environ.get("PBS_DATA_DIR", "./data"))
RAW_DIR = DATA_DIR / "raw"

FLASHBOTS_URL = flashbots_mod.FLASHBOTS_URL
REQUEST_DELAY = flashbots_mod.REQUEST_DELAY
SAVE_EVERY = flashbots_mod.SAVE_EVERY
BATCH_DIR = flashbots_mod.BATCH_DIR
FLASHBOTS_OUT = flashbots_mod.FLASHBOTS_OUT
XATU_FILE = flashbots_mod.XATU_FILE

NEW_BATCH_PREFIX = "newbatch_"  # kept separate from the original batch_* files


def extend_flashbots_forward() -> int:
    """
    Fetch Flashbots delivered payloads newer than whatever is already saved.

    Returns the number of new records fetched. If there are no existing
    batches at all (a fresh build, not an update), this does nothing and
    tells the caller to run 02_augment_flashbots.py's full download instead.
    """
    existing_batches = sorted(BATCH_DIR.glob("batch_*.parquet")) + sorted(BATCH_DIR.glob(f"{NEW_BATCH_PREFIX}*.parquet"))
    if existing_batches:
        max_slot_saved = max(
            pl.scan_parquet(f).select(pl.col("slot").max()).collect().item()
            for f in existing_batches
        )
        print(f"Existing Flashbots batch files go up to slot {max_slot_saved:,}.")
    elif FLASHBOTS_OUT.exists():
        # Only the already-consolidated output exists (e.g. this repo was seeded
        # from a copy of someone else's finished run, with no raw batch files) -
        # use its max slot as the baseline instead of requiring batch files.
        max_slot_saved = pl.scan_parquet(FLASHBOTS_OUT).select(pl.col("slot").max()).collect().item()
        print(f"No batch files, but found the consolidated output: goes up to slot {max_slot_saved:,}.")
    else:
        print("No existing Flashbots data found at all - this is a fresh build, not an update.")
        print("Run 02_augment_flashbots.py (full download) instead of this script.")
        return 0

    latest = flashbots_mod.fetch_batch(cursor=99_999_999, limit=1)
    if not latest:
        print("ERROR: could not fetch the latest slot from the Flashbots API.")
        sys.exit(1)
    newest_slot = int(latest[0]["slot"])
    print(f"Newest slot currently on the relay: {newest_slot:,}.")

    if newest_slot <= max_slot_saved:
        print("Already up to date - nothing new to fetch.")
        return 0

    cursor = newest_slot
    pending: list[dict] = []
    n_calls = 0
    n_total = 0
    n_batches = 0
    consecutive_empty = 0
    MAX_EMPTY_SKIP = 10
    EMPTY_SLOT_SKIP = 500

    while cursor > max_slot_saved:
        batch = flashbots_mod.fetch_batch(cursor)

        if not batch:
            consecutive_empty += 1
            print(f"  Empty at cursor={cursor:,}, skip #{consecutive_empty}/{MAX_EMPTY_SKIP}")
            if consecutive_empty >= MAX_EMPTY_SKIP:
                print(f"  {MAX_EMPTY_SKIP} consecutive empty responses, stopping.")
                break
            cursor -= EMPTY_SLOT_SKIP
            time.sleep(1.0)
            continue
        consecutive_empty = 0

        batch = [r for r in batch if int(r["slot"]) > max_slot_saved]
        if not batch:
            print(f"  Reached already-covered slot {max_slot_saved:,}, update complete.")
            break

        pending.extend(batch)
        n_calls += 1
        n_total += len(batch)
        cursor = min(int(r["slot"]) for r in batch) - 1

        if n_calls % 100 == 0:
            print(f"  [{n_calls:,} calls] cursor={cursor:,} | new records={n_total:,}")

        if n_calls % SAVE_EVERY == 0:
            _flush_new_batch(pending, n_batches)
            n_batches += 1
            pending = []

        time.sleep(REQUEST_DELAY)

    if pending:
        _flush_new_batch(pending, n_batches)
        n_batches += 1

    print(f"\nUpdate download complete: {n_total:,} new records in {n_batches} new batch files.")
    return n_total


def _flush_new_batch(records: list[dict], batch_idx: int) -> None:
    BATCH_DIR.mkdir(parents=True, exist_ok=True)
    path = BATCH_DIR / f"{NEW_BATCH_PREFIX}{batch_idx:05d}.parquet"
    flashbots_mod.records_to_df(records).write_parquet(path, compression="zstd")
    print(f"  -> Saved {path.name} ({len(records):,} records)")


def merge_new_flashbots_batches() -> pl.DataFrame:
    """
    Safely merge newly-fetched batches into the existing consolidated
    FLASHBOTS_OUT, instead of using consolidate_flashbots() directly - that
    function only concatenates whatever batch_*.parquet files its own glob
    finds and writes the result over FLASHBOTS_OUT. If the existing
    FLASHBOTS_OUT was seeded from a copy with no raw batch files sitting
    alongside it (as in this repo), calling it after only a small forward
    extension would overwrite the full, already-consolidated dataset with
    just the newly-fetched slice. This function always starts from whatever
    is already in FLASHBOTS_OUT and adds only the genuinely new batches.
    """
    new_batches = sorted(BATCH_DIR.glob(f"{NEW_BATCH_PREFIX}*.parquet"))
    if not new_batches:
        print("No new Flashbots batches to merge.")
        if FLASHBOTS_OUT.exists():
            return pl.read_parquet(FLASHBOTS_OUT)
        print("ERROR: no existing Flashbots data and nothing new to merge.")
        sys.exit(1)

    new_df = pl.concat([pl.read_parquet(f) for f in new_batches])
    print(f"  {len(new_df):,} new records across {len(new_batches)} batch files")

    if FLASHBOTS_OUT.exists():
        old_df = pl.read_parquet(FLASHBOTS_OUT)
        print(f"  {len(old_df):,} existing records in {FLASHBOTS_OUT}")
        combined = pl.concat([old_df, new_df], how="diagonal")
    else:
        combined = new_df

    combined = combined.sort("slot").unique(subset=["block_number"], keep="first")
    print(f"  {len(combined):,} total after merge + dedupe on block_number")
    FLASHBOTS_OUT.parent.mkdir(parents=True, exist_ok=True)
    combined.write_parquet(FLASHBOTS_OUT, compression="zstd")
    print(f"  Saved -> {FLASHBOTS_OUT}")

    for f in new_batches:
        f.unlink()
    return combined


def main():
    print("=== Step 1: Xatu (genuinely incremental, picks up new days automatically) ===")
    xatu_mod.download_xatu()

    print("\n=== Step 2: Flashbots forward extension (new slots since last save) ===")
    n_new = extend_flashbots_forward()

    print("\n=== Step 3: Merge new Flashbots batches into the existing consolidated file ===")
    df_flashbots = merge_new_flashbots_batches()

    print("\n=== Step 4: Re-run Xatu builder table (new Xatu days included) ===")
    xatu_mod.build_blocks_table()

    print("\n=== Step 5: Re-augment Xatu with Flashbots ===")
    flashbots_mod.augment_xatu(df_flashbots)

    print("\nRaw data updated. BigQuery steps (03, 04) and the final merge (05)")
    print("still need to be re-run separately - 03 and 04 are not incremental")
    print("(delete their output files first; each costs a small, approved amount),")
    print("then run 05_build_pbs_flag.py to rebuild the final df_blocks_pbs.parquet.")


if __name__ == "__main__":
    main()
