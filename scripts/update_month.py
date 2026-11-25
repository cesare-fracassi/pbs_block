#!/usr/bin/env python3
"""
Monthly update: extend a finished dataset from its last block to the end of the last complete month.

A month is complete once it has ended (UTC). Run on 15 January it adds everything up to 31 December; run on
2 February it adds January as well. --through YYYY-MM stops earlier.

What a run does, in order (each part continues where it stopped if the run is interrupted):
  1. Xatu: downloads the daily files for the new days (one request at a time). Stops here if Xatu has no file yet
     for a day of the month (its newest days appear after about two days); rerun later.
  2. Flashbots: fetches the delivered payloads newer than the local file and adds them to the relay table.
  3. Node: extracts extra_data, fee recipient and last transaction for the new blocks.
  4. relayscan: fetches the new daily counts (coverage check only).
  5. Rebuilds the classification (05_build_pbs_flag.py) into data/processed/df_blocks_pbs.parquet.new.
  6. Checks the new file against the old one (see below) and only then replaces df_blocks_pbs.parquet.

The classification of the whole range is recomputed from the stored inputs, so a block whose relay record arrived
late is classified correctly. The check compares every block of the old file with the new one and reports how
many changed (a few late relay records are expected; a large number is not). The file is replaced only if the new
one has: unique sorted block numbers, no gaps, the first block not later than before, and the last block equal to
the last block of the month.

Needs the Ethereum node (as in 03_extract_node_block_data.py; address from --rpc-url or ERIGON_RPC_URL) and a
finished dataset with node data (run_all_pbs.py first). Do not run two updates at once.

Usage (from the repository root):
    python scripts/update_month.py --status      # state of the dataset and what would be added; changes nothing
    python scripts/update_month.py --dry-run     # run every check that needs no download or write; changes nothing
    python scripts/update_month.py               # add every complete month after the last block
    python scripts/update_month.py --through 2026-11

Progress goes to the screen; to keep it, redirect it to a file outside this repository.
This updater has been checked on a small sample only, not yet end to end on a real month.
"""
import argparse
import datetime as dt
import os
import sys

import polars as pl

import run_all_pbs
from _common import PROCESSED_DIR, RAW_DIR, load_script, polite_get

FINAL = PROCESSED_DIR / "df_blocks_pbs.parquet"
NEW = FINAL.with_name(FINAL.name + ".new")

node = load_script("03_extract_node_block_data.py")
xatu = load_script("01_download_xatu.py")
flashbots = load_script("02_augment_flashbots.py")


def last_block_of_final() -> tuple[int, int, int]:
    if not FINAL.exists():
        sys.exit(f"{FINAL} does not exist. Build the dataset first with scripts/run_all_pbs.py.")
    lo, hi, n = pl.scan_parquet(FINAL).select(
        pl.col("block_number").min().alias("lo"), pl.col("block_number").max().alias("hi"), pl.len().alias("n")
    ).collect().row(0)
    return lo, hi, n


def block_date(url: str, block: int) -> dt.date:
    return dt.datetime.fromtimestamp(int(node.get_block(url, block)["timestamp"], 16), dt.timezone.utc).date()


def xatu_missing_days(first_day: dt.date, last_day: dt.date) -> list[dt.date]:
    d, out = first_day, []
    while d <= last_day:
        if not (RAW_DIR / "xatu" / f"xatu_{d.isoformat()}.parquet").exists():
            out.append(d)
        d += dt.timedelta(days=1)
    return out


def check_xatu_days(first_day: dt.date, last_day: dt.date) -> None:
    missing = xatu_missing_days(first_day, last_day)
    if missing:
        sys.exit(f"Xatu has no downloaded file for {len(missing)} day(s), {missing[0]} to {missing[-1]}. "
                 "Its newest days are published after about two days; rerun later.")


def verify(old: pl.DataFrame, new: pl.DataFrame, target_block: int) -> list[str]:
    problems = []
    nb = new["block_number"]
    if nb.n_unique() != len(new):
        problems.append("duplicate block numbers")
    if not nb.is_sorted():
        problems.append("block numbers are not sorted")
    lo, hi = nb.min(), nb.max()
    if hi != target_block:
        problems.append(f"last block is {hi:,}, expected {target_block:,}")
    if hi - lo + 1 != len(new):
        problems.append(f"gaps: {hi - lo + 1 - len(new):,} blocks missing between {lo:,} and {hi:,}")
    if lo > old["block_number"].min():
        problems.append(f"first block moved from {old['block_number'].min():,} to {lo:,}")

    both = old.join(new, on="block_number", how="inner", suffix="_new")
    changed = both.filter(
        ~pl.col("is_pbs").eq_missing(pl.col("is_pbs_new"))
        | ~pl.col("builder_name").eq_missing(pl.col("builder_name_new"))
        | ~pl.col("builder_address").eq_missing(pl.col("builder_address_new"))
    ).height
    print(f"  old blocks compared:      {len(both):,}", flush=True)
    print(f"  old blocks that changed:  {changed:,} ({changed / max(len(both), 1) * 100:.3f}%)", flush=True)
    print(f"  old blocks missing now:   {len(old) - len(both):,}", flush=True)
    if len(old) - len(both):
        problems.append(f"{len(old) - len(both):,} blocks of the old file are missing from the new one")
    added = new.filter(pl.col("block_number") > old["block_number"].max())
    if len(added):
        print(f"  new blocks:               {len(added):,} ({added['block_number'].min():,} - {added['block_number'].max():,})", flush=True)
        print(f"    is_pbs share:           {added['is_pbs'].mean() * 100:.1f}%", flush=True)
        print(f"    with a builder name:    {added['builder_name'].is_not_null().mean() * 100:.1f}%", flush=True)
    return problems


def main() -> None:
    ap = argparse.ArgumentParser(description="Extend the PBS dataset to the end of the last complete month.")
    ap.add_argument("--rpc-url", default=os.environ.get("ERIGON_RPC_URL", "http://127.0.0.1:8545"))
    ap.add_argument("--through", default=run_all_pbs.last_complete_month(), metavar="YYYY-MM",
                    help="last month to add (default: the last complete month)")
    ap.add_argument("--status", action="store_true", help="show the state and what would be added; change nothing")
    ap.add_argument("--dry-run", action="store_true", help="run the checks that need no download or write; change nothing")
    a = ap.parse_args()

    os.environ["ERIGON_RPC_URL"] = a.rpc_url
    lo, last_block, n = last_block_of_final()
    nr = run_all_pbs.node_data_range()
    print(f"dataset: {n:,} blocks, {lo:,} to {last_block:,}")
    print(f"node data: {'blocks ' + format(nr[0], ',') + ' to ' + format(nr[1], ',') if nr else 'none'}")
    print(f"target: end of {a.through}")

    reachable = run_all_pbs.node_reachable(a.rpc_url)
    if not reachable:
        if a.status:
            print(f"The node does not answer at {a.rpc_url}; cannot work out the target block.")
            return
        sys.exit(f"The node does not answer at {a.rpc_url}. Start it, or set ERIGON_RPC_URL.")
    target_block = node.month_last_block(a.rpc_url, a.through)
    last_day = run_all_pbs.month_last_day(a.through)
    first_day = block_date(a.rpc_url, last_block)
    print(f"last block of the dataset: {last_block:,} ({first_day}); last block of {a.through}: {target_block:,}")

    if target_block <= last_block:
        print("Nothing to add: the dataset already reaches the end of that month.")
        return
    print(f"To add: {target_block - last_block:,} blocks, {first_day} to {last_day}")
    if nr is None:
        sys.exit("No node data in data/raw/node. Run `python scripts/run_all_pbs.py --only node` once for the full "
                 "range (a few hours) before using the monthly update.")
    if a.status:
        print(f"Xatu days still to download: {len(xatu_missing_days(first_day, last_day))}")
        return

    fin = int(node.get_block(a.rpc_url, "finalized")["number"], 16)
    if target_block > fin:
        sys.exit(f"Block {target_block:,} is not finalized yet (finalized: {fin:,}).")

    if a.dry_run:
        todo = xatu_missing_days(first_day, last_day)
        print(f"Xatu: {len(todo)} day(s) to download; checking that the server has them ...")
        absent = []
        for d in todo:
            r = polite_get(f"{xatu.XATU_BASE}/{d.year}/{d.month}/{d.day}.parquet", method="HEAD", timeout=30)
            if r.status_code != 200:
                absent.append(d)
        if absent:
            print(f"  NOT available yet: {', '.join(str(d) for d in absent)}")
        else:
            print("  all available")
        r = polite_get(flashbots.FLASHBOTS_URL, params={"cursor": "99999999", "limit": 1}, timeout=30)
        newest = int(r.json()[0]["slot"]) if r.ok and r.json() else None
        print(f"Flashbots: newest slot on the relay {newest:,}" if newest else "Flashbots: could not read the newest slot")
        print("Dry run complete; nothing was downloaded or written.")
        return

    old = pl.read_parquet(FINAL)
    run_all_pbs.run_stage("xatu", a.through)
    check_xatu_days(first_day, last_day)
    run_all_pbs.run_stage("flashbots", a.through)
    run_all_pbs.run_stage("node", a.through)
    run_all_pbs.run_stage("relayscan", a.through)
    run_all_pbs.run_stage("build", a.through, ["--out", str(NEW)])

    print("\nChecking the new file against the old one ...", flush=True)
    new = pl.read_parquet(NEW)
    problems = verify(old, new, target_block)
    if problems:
        print("\nCHECK FAILED, the dataset was NOT replaced:", flush=True)
        for p in problems:
            print(f"  - {p}", flush=True)
        print(f"The new file is kept for inspection: {NEW}", flush=True)
        sys.exit(1)
    os.replace(NEW, FINAL)
    print(f"\nUPDATE DONE: {FINAL} now holds {len(new):,} blocks, {new['block_number'].min():,} to {new['block_number'].max():,}.", flush=True)


if __name__ == "__main__":
    main()
