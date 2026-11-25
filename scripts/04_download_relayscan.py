#!/usr/bin/env python3
"""
Step 4 - Download relayscan.io daily counts of delivered MEV-Boost blocks. Used only to check coverage (step 5
compares our PBS blocks per month with these counts); nothing from relayscan goes into the dataset.

Source: https://relayscan.io/stats/day/YYYY-MM-DD/json (public). The count of a day is the sum of `num_blocks` over
the builders listed for that day.

Output: data/raw/relayscan_daily.csv with columns date (YYYY-MM-DD) and relayscan_pbs (number of PBS blocks that day).

Pacing: one request at a time, --delay seconds apart (default 1.0). The site may block an address that sends many
requests quickly; the script stops at the first 403 or 429 answer instead of retrying. Days already in the file are
not fetched again; a day that failed is not written, so the next run retries it. Today and yesterday are skipped
because their counts are still changing.

Run:
    python scripts/04_download_relayscan.py
    python scripts/04_download_relayscan.py --end 2026-10-31
"""
import argparse
import sys
from datetime import date, datetime, timedelta

import polars as pl

from _common import DEFAULT_DELAY, MERGE_DATE, RAW_DIR, HostBlocked, polite_get

OUT_FILE = RAW_DIR / "relayscan_daily.csv"
SAVE_EVERY = 100  # days between saves


def load_cache() -> pl.DataFrame:
    if OUT_FILE.exists():
        return pl.read_csv(OUT_FILE, schema_overrides={"date": pl.Utf8, "relayscan_pbs": pl.Int64})
    return pl.DataFrame({"date": pl.Series([], dtype=pl.Utf8), "relayscan_pbs": pl.Series([], dtype=pl.Int64)})


def save_cache(df: pl.DataFrame) -> None:
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT_FILE.with_suffix(".csv.tmp")
    df.sort("date").write_csv(tmp)
    tmp.replace(OUT_FILE)


def fetch_day(day: date, delay: float) -> int | None:
    """Number of PBS blocks relayscan reports for `day`, or None if the request failed or returned nothing usable."""
    r = polite_get(f"https://relayscan.io/stats/day/{day}/json", delay=delay, timeout=30)
    if not r.ok:
        print(f"  {day}: HTTP {r.status_code}", flush=True)
        return None
    try:
        data = r.json()
    except ValueError:
        print(f"  {day}: the answer is not JSON", flush=True)
        return None
    builders = data.get("builders")
    if not isinstance(builders, list):
        print(f"  {day}: no builders list in the answer", flush=True)
        return None
    return int(sum(b.get("info", {}).get("num_blocks", 0) for b in builders))


def main() -> None:
    ap = argparse.ArgumentParser(description="Download relayscan.io daily PBS block counts (coverage check only).")
    ap.add_argument("--end", default=None, metavar="YYYY-MM-DD", help="last day (default: two days ago)")
    ap.add_argument("--delay", type=float, default=DEFAULT_DELAY, help="seconds between two requests")
    args = ap.parse_args()

    end = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else date.today() - timedelta(days=2)
    cache = load_cache()
    have = set(cache["date"].to_list())
    days, d = [], MERGE_DATE
    while d <= end:
        if str(d) not in have:
            days.append(d)
        d += timedelta(days=1)
    print(f"relayscan: {len(have)} days on file, {len(days)} to fetch ({MERGE_DATE} to {end}), one request every {args.delay:g} s", flush=True)

    new_rows, failed = [], 0
    try:
        for i, day in enumerate(days, start=1):
            n = fetch_day(day, args.delay)
            if n is None:
                failed += 1
            else:
                new_rows.append({"date": str(day), "relayscan_pbs": n})
            if i % SAVE_EVERY == 0 and new_rows:
                cache = pl.concat([cache, pl.DataFrame(new_rows, schema=cache.schema)]).unique("date")
                save_cache(cache)
                new_rows = []
                print(f"  {i}/{len(days)} days fetched, saved", flush=True)
    except HostBlocked as e:
        print(f"STOPPED: {e}", flush=True)
        if new_rows:
            save_cache(pl.concat([cache, pl.DataFrame(new_rows, schema=cache.schema)]).unique("date"))
        sys.exit(1)

    if new_rows:
        cache = pl.concat([cache, pl.DataFrame(new_rows, schema=cache.schema)]).unique("date")
        save_cache(cache)
    print(f"Done: {len(cache):,} days in {OUT_FILE} ({failed} failed requests will be retried next run).", flush=True)


if __name__ == "__main__":
    main()
