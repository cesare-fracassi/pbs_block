#!/usr/bin/env python3
"""
Build the PBS dataset from scratch: runs the five stages in order and stops at the first one that fails.

  xatu        01_download_xatu.py            Xatu relay payload days -> relay table
  flashbots   02_augment_flashbots.py        Flashbots relay API -> relay table with Flashbots blocks added
  node        03_extract_node_block_data.py  extra_data, fee recipient and last transaction per block, from a node
  relayscan   04_download_relayscan.py       relayscan.io daily counts (coverage check only)
  build       05_build_pbs_flag.py           classification -> data/processed/df_blocks_pbs.parquet

Every stage can be stopped and restarted: finished downloads are skipped and extraction continues where it ended.
The data runs up to the end of a month, --through YYYY-MM (default: the last month that has ended, UTC).

Usage (from the repository root):
    python scripts/run_all_pbs.py                       # everything, through the last complete month
    python scripts/run_all_pbs.py --through 2026-10     # up to the end of October 2026
    python scripts/run_all_pbs.py --status              # what exists now; runs nothing
    python scripts/run_all_pbs.py --from-stage node     # restart from a stage
    python scripts/run_all_pbs.py --only relayscan,build

To add new months to a finished dataset use update_month.py instead.
Progress goes to the screen; to keep it, redirect it to a file outside this repository.
"""
import argparse
import calendar
import datetime as dt
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

from _common import INTERIM_DIR, MERGE_DATE, PROCESSED_DIR, RAW_DIR, REPO_DIR

SCRIPTS = Path(__file__).resolve().parent
STAGE_ORDER = ["xatu", "flashbots", "node", "relayscan", "build"]
STAGE_SCRIPT = {
    "xatu": "01_download_xatu.py",
    "flashbots": "02_augment_flashbots.py",
    "node": "03_extract_node_block_data.py",
    "relayscan": "04_download_relayscan.py",
    "build": "05_build_pbs_flag.py",
}


def last_complete_month(today: dt.date | None = None) -> str:
    """The most recent month that has ended (UTC), as YYYY-MM."""
    today = today or dt.datetime.now(dt.timezone.utc).date()
    first = today.replace(day=1)
    prev = first - dt.timedelta(days=1)
    return f"{prev.year}-{prev.month:02d}"


def month_last_day(month: str) -> dt.date:
    y, m = (int(x) for x in month.split("-"))
    return dt.date(y, m, calendar.monthrange(y, m)[1])


def stage_args(stage: str, through: str, extra: list[str]) -> list[str]:
    end = month_last_day(through).isoformat()
    if stage == "xatu":
        return ["--end", end]
    if stage == "node":
        return ["--through-month", through]
    if stage == "relayscan":
        return ["--end", end]
    if stage == "build":
        return extra
    return []


def run_stage(stage: str, through: str, extra: list[str] | None = None) -> None:
    cmd = [sys.executable, "-u", str(SCRIPTS / STAGE_SCRIPT[stage]), *stage_args(stage, through, extra or [])]
    print(f"\n{'=' * 70}\nSTAGE {stage}: {' '.join(Path(c).name if c == cmd[1] else c for c in cmd[1:])}\n{'=' * 70}", flush=True)
    rc = subprocess.run(cmd, cwd=REPO_DIR).returncode
    if rc != 0:
        sys.exit(f"STAGE {stage} FAILED with exit code {rc}. Fix the cause and rerun; finished work is kept.")
    print(f"STAGE {stage} OK", flush=True)


def node_reachable(url: str) -> bool:
    try:
        req = urllib.request.Request(url, data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "eth_blockNumber", "params": []}).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=10) as r:
            return "result" in json.loads(r.read())
    except Exception:  # noqa: BLE001
        return False


def node_data_range() -> tuple[int, int] | None:
    files = sorted((RAW_DIR / "node").glob("block_inputs_*.parquet"))
    spans = [tuple(int(x) for x in m.groups()) for p in files if (m := re.match(r"block_inputs_(\d+)_(\d+)\.parquet$", p.name))]
    return (spans[0][0], spans[-1][1]) if spans else None


def status() -> None:
    import polars as pl
    print(f"Repository: {REPO_DIR}")
    xatu = sorted((RAW_DIR / "xatu").glob("xatu_*.parquet"))
    print(f"xatu:       {len(xatu):,} daily files" + (f", {xatu[0].stem[5:]} to {xatu[-1].stem[5:]}" if xatu else ""))
    fb = RAW_DIR / "flashbots" / "df_blocks_flashbots.parquet"
    print(f"flashbots:  {'file present, ' + format(fb.stat().st_size / 1e6, '.0f') + ' MB' if fb.exists() else 'no file'}")
    aug = INTERIM_DIR / "df_blocks_builder_augmented.parquet"
    print(f"            relay table with Flashbots: {'present' if aug.exists() else 'missing'}")
    nr = node_data_range()
    print(f"node:       {'blocks ' + format(nr[0], ',') + ' to ' + format(nr[1], ',') if nr else 'no data'}")
    rs = RAW_DIR / "relayscan_daily.csv"
    print(f"relayscan:  {len(pl.read_csv(rs)):,} days" if rs.exists() else "relayscan:  no file")
    out = PROCESSED_DIR / "df_blocks_pbs.parquet"
    if out.exists():
        r = pl.scan_parquet(out).select(pl.col("block_number").min().alias("lo"), pl.col("block_number").max().alias("hi"),
                                        pl.len().alias("n")).collect().row(0)
        print(f"build:      {r[2]:,} blocks, {r[0]:,} to {r[1]:,}")
    else:
        print("build:      no file")
    print(f"Default end of a run: end of {last_complete_month()}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Build the PBS dataset from scratch.")
    ap.add_argument("--through", default=last_complete_month(), metavar="YYYY-MM", help="last month to include")
    ap.add_argument("--from-stage", choices=STAGE_ORDER, default=None, help="start at this stage")
    ap.add_argument("--only", default=None, help="comma-separated stages to run, in the standard order")
    ap.add_argument("--status", action="store_true", help="show what exists and run nothing")
    ap.add_argument("--keep-full", action="store_true", help="pass --keep-full to the build stage")
    a = ap.parse_args()

    if a.status:
        status()
        return
    if not re.match(r"^\d{4}-\d{2}$", a.through):
        sys.exit("--through must look like 2026-10")
    if a.only:
        wanted = [s.strip() for s in a.only.split(",")]
        bad = [s for s in wanted if s not in STAGE_ORDER]
        if bad:
            sys.exit(f"unknown stage(s) {bad}; choose from {STAGE_ORDER}")
        stages = [s for s in STAGE_ORDER if s in wanted]
    else:
        stages = STAGE_ORDER[STAGE_ORDER.index(a.from_stage):] if a.from_stage else list(STAGE_ORDER)

    if "node" in stages:
        url = os.environ.get("ERIGON_RPC_URL", "http://127.0.0.1:8545")
        if not node_reachable(url):
            sys.exit(f"The node does not answer at {url}. Start it, or set ERIGON_RPC_URL.")
    print(f"Stages: {', '.join(stages)}; data through the end of {a.through} (Merge day {MERGE_DATE} onward)", flush=True)
    for s in stages:
        run_stage(s, a.through, ["--keep-full"] if (s == "build" and a.keep_full) else [])
    print("\nALL STAGES DONE", flush=True)


if __name__ == "__main__":
    main()
