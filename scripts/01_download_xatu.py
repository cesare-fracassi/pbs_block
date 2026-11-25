#!/usr/bin/env python3
"""
Step 1 — Download block-builder and relay data from the Xatu public dataset
and build the block-level builder assignment table.

Source:   https://data.ethpandaops.io/xatu/mainnet/databases/default/
            mev_relay_proposer_payload_delivered/{YEAR}/{MONTH}/{DAY}.parquet
Coverage: Sep 15 2022 (the Merge) through roughly 2 days ago, updated daily.
Access:   Fully public, no authentication required.

Raw schema (per-sentry observations, not pre-deduplicated):
    slot, block_number, relay_name, block_hash, proposer_pubkey,
    builder_pubkey, proposer_fee_recipient, gas_used, value (32-byte uint256),
    num_tx, plus sentry metadata columns.

Deduplication:
    Multiple Xatu sentry nodes independently observe each relay API call,
    producing duplicate rows for the same (slot, relay_name) pair.
    Step 1: deduplicate to one row per (slot, relay_name).
    Step 2: aggregate all relays per slot; keep the highest-value bid as winner.

Builder label sources, in priority order:
    1. dataalways/mevboost-data mapping_pubkey_builder_name.csv (CC0, ~427 entries)
       builder_name_source = "curated"
    2. Pubkey prefix fallback (first 18 characters of the pubkey)
       builder_name_source = "pubkey_prefix"

Relay names are already human-readable strings in the Xatu data (e.g. "flashbots",
"bloxroute_maxprofit", "ultrasound", "agnostic", "titan", "aestus").

Output: PBS_DATA_DIR/raw/xatu/xatu_YYYY-MM-DD.parquet (one file per day)
        PBS_DATA_DIR/raw/df_blocks_builder.parquet (merged, one row per block_number)

Run:
    export PBS_DATA_DIR=/path/to/crypto_data/PBS/data
    python 01_download_xatu.py              # download + build in one pass
    python 01_download_xatu.py --step download
    python 01_download_xatu.py --step build
"""
import argparse
import io
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DATA_DIR = Path(os.environ.get("PBS_DATA_DIR", "./data"))
RAW_DIR = DATA_DIR / "raw"

MERGE_DATE = date(2022, 9, 15)

XATU_BASE = (
    "https://data.ethpandaops.io/xatu/mainnet/databases/default"
    "/mev_relay_proposer_payload_delivered"
)

BUILDER_LABELS_URL = (
    "https://raw.githubusercontent.com/dataalways/mevboost-data/"
    "main/mapping_pubkey_builder_name.csv"
)

XATU_KEEP = [
    "slot", "block_number", "relay_name", "block_hash",
    "proposer_pubkey", "builder_pubkey", "proposer_fee_recipient",
    "gas_used", "value", "num_tx",
]

# beaverbuild.org merged with buildernet; treat as one entity.
LABEL_MERGES = {
    "buildernet": "beaverbuild/buildernet",
    "beaverbuild.org": "beaverbuild/buildernet",
}


def _decode_bytes_col(expr: pl.Expr) -> pl.Expr:
    # Xatu's binary columns come back as Polars Binary when read from parquet
    # written with bytes dtype; decode to Utf8, leaving existing strings as-is.
    return (
        pl.when(expr.cast(pl.Utf8, strict=False).is_not_null())
        .then(expr.cast(pl.Utf8, strict=False))
        .otherwise(None)
    )


def _decode_xatu_df(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize one day's raw Xatu parquet:
      - Keep only XATU_KEEP columns
      - Decode byte columns (relay_name, hashes, pubkeys) to string
      - Convert the binary `value` (32-byte big-endian uint256) to float wei
      - Deduplicate to one row per (slot, relay_name), dropping sentry duplicates
    """
    cols = [c for c in XATU_KEEP if c in df.columns]
    df = df.select(cols)

    str_cols = [
        c for c in ["relay_name", "block_hash", "proposer_pubkey",
                     "builder_pubkey", "proposer_fee_recipient"]
        if c in df.columns
    ]
    for c in str_cols:
        if df[c].dtype == pl.Binary:
            df = df.with_columns(
                pl.col(c).map_elements(
                    lambda v: v.decode("utf-8", errors="replace") if v is not None else None,
                    return_dtype=pl.Utf8,
                ).alias(c)
            )

    if "value" in df.columns:
        if df["value"].dtype == pl.Binary:
            df = df.with_columns(
                pl.col("value").map_elements(
                    lambda v: float(int.from_bytes(v, "big")) if v is not None else 0.0,
                    return_dtype=pl.Float64,
                ).alias("value_wei")
            ).drop("value")
        else:
            df = df.with_columns(pl.col("value").cast(pl.Float64).alias("value_wei")).drop("value")

    df = df.unique(subset=["slot", "relay_name"], keep="first")
    return df


def _fetch_day(d: date, xatu_dir: Path) -> int:
    """Download and save one day's Xatu parquet. Returns rows saved (0 = skipped)."""
    out = xatu_dir / f"xatu_{d.isoformat()}.parquet"
    if out.exists():
        return 0  # already done, resumable

    url = f"{XATU_BASE}/{d.year}/{d.month}/{d.day}.parquet"
    try:
        resp = requests.get(url, timeout=60)
        if resp.status_code == 404:
            return 0  # no data for this date (future or gap)
        if resp.status_code != 200:
            log.warning(f"Xatu {d}: HTTP {resp.status_code}")
            return 0
        df = pl.read_parquet(io.BytesIO(resp.content))
        if df.is_empty():
            return 0
        df = _decode_xatu_df(df)
        df = df.with_columns(pl.lit(str(d)).alias("date"))
        df.write_parquet(out, compression="zstd")
        return len(df)
    except Exception as e:
        log.warning(f"Xatu {d}: {e}")
        return 0


def download_xatu(start_date: str = "2022-09-15", end_date: str | None = None, n_workers: int = 16) -> int:
    """
    Download the full Xatu mev_relay_proposer_payload_delivered dataset.
    Daily files are fetched in parallel and saved to raw/xatu/.
    Already-downloaded files are skipped (safe to re-run / resume).
    Returns total rows saved.
    """
    xatu_dir = RAW_DIR / "xatu"
    xatu_dir.mkdir(parents=True, exist_ok=True)

    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = (date.today() - timedelta(days=2)) if end_date is None else datetime.strptime(end_date, "%Y-%m-%d").date()

    all_dates = []
    d = start
    while d <= end:
        all_dates.append(d)
        d += timedelta(days=1)

    log.info(f"Xatu download: {len(all_dates)} days ({start} - {end}), {n_workers} parallel workers")

    total_rows = 0
    done_count = 0
    with ThreadPoolExecutor(max_workers=n_workers) as pool:
        futures = {pool.submit(_fetch_day, d, xatu_dir): d for d in all_dates}
        for future in as_completed(futures):
            n = future.result()
            total_rows += n
            done_count += 1
            if done_count % 100 == 0:
                log.info(f"Xatu: {done_count}/{len(all_dates)} days done, {total_rows:,} rows so far")

    log.info(f"Xatu download complete: {total_rows:,} rows, {done_count} days processed")
    return total_rows


def _load_builder_labels() -> dict[str, str]:
    """
    Fetch builder pubkey -> name mapping from dataalways/mevboost-data (CC0).

    This file is NOT append-only - confirmed directly against its GitHub
    commit history, which includes at least one explicit correction commit
    ("Fixed mislabeled beaverbuild pubkeys and regenerated historic data
    that was affected", 2025-11-13). Since we fetch it fresh on every run
    with no caching, two runs of this pipeline at different times could
    label the SAME historical block differently - not because the block
    changed, but because the upstream mapping was corrected. To keep every
    run traceable to the exact mapping version that produced it, we save a
    dated snapshot locally every time we fetch, before using it.
    """
    try:
        resp = requests.get(BUILDER_LABELS_URL, timeout=20)
        resp.raise_for_status()

        snapshot_dir = DATA_DIR / "raw" / "builder_label_snapshots"
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / f"builder_labels_{date.today().isoformat()}.csv"
        if not snapshot_path.exists():
            snapshot_path.write_bytes(resp.content)
            log.info(f"Saved builder-label snapshot -> {snapshot_path}")
        else:
            log.info(f"Snapshot for today already exists, not overwriting: {snapshot_path}")

        df = pl.read_csv(io.BytesIO(resp.content))
        mapping = {
            str(row["pubkey"]).lower(): str(row["builder_name"])
            for row in df.iter_rows(named=True)
            if row.get("pubkey") is not None and row.get("builder_name") is not None
        }
        log.info(f"Loaded {len(mapping):,} builder labels from dataalways/mevboost-data")
        return mapping
    except Exception as e:
        log.warning(f"Builder labels unavailable ({e}). Falling back to truncated pubkey labels.")
        return {}


def build_blocks_table() -> pl.DataFrame:
    """
    Union all xatu_*.parquet files, deduplicate to one row per block_number,
    enrich with builder_label, and save df_blocks_builder.parquet to raw/.

    Deduplication logic:
      Step 1 - keep only rows with the maximum bid per block_number (the block
        that was actually delivered on-chain).
      Step 2 - reshape relays wide: if multiple relays share the same max bid,
        record each relay name in its own column (relay_1, relay_2, ...),
        sorted alphabetically for a deterministic order.
    """
    xatu_dir = RAW_DIR / "xatu"
    xatu_files = sorted(xatu_dir.glob("xatu_*.parquet"))
    if not xatu_files:
        raise FileNotFoundError(f"No xatu_*.parquet found in {xatu_dir}")

    log.info(f"Merging {len(xatu_files)} Xatu daily files ...")
    all_data = pl.concat([pl.read_parquet(f) for f in xatu_files], how="diagonal_relaxed")
    log.info(f"Total rows (pre-dedup): {len(all_data):,}")

    if "relay_name" in all_data.columns:
        all_data = all_data.rename({"relay_name": "relay"})

    # Step 1: keep only the maximum-bid rows per block_number
    all_data = all_data.with_columns(pl.col("value_wei").max().over("block_number").alias("_max_val"))
    winning = all_data.filter(pl.col("value_wei") == pl.col("_max_val")).drop("_max_val")
    log.info(f"After keeping max-bid rows: {len(winning):,} ({len(all_data) - len(winning):,} lower-bid rows dropped)")

    winning = winning.sort(["block_number", "relay"])

    canonical = winning.group_by("block_number", maintain_order=False).agg(
        slot=pl.col("slot").first(),
        block_hash=pl.col("block_hash").first(),
        builder_pubkey=pl.col("builder_pubkey").first(),
        proposer_pubkey=pl.col("proposer_pubkey").first(),
        proposer_fee_recipient=pl.col("proposer_fee_recipient").first(),
        gas_used=pl.col("gas_used").first(),
        value_wei=pl.col("value_wei").first(),
        num_tx=pl.col("num_tx").first(),
        relay_count=pl.col("relay").n_unique(),
    )

    winning = winning.with_columns(
        (pl.col("relay").cum_count().over("block_number")).alias("relay_rank")
    )
    relay_wide = winning.pivot(
        columns="relay_rank", index="block_number", values="relay", aggregate_function="first"
    ).rename(lambda c: f"relay_{c}" if c != "block_number" else c)

    agg = canonical.join(relay_wide, on="block_number", how="left").sort("block_number")

    relay_cols = sorted([c for c in agg.columns if c.startswith("relay_") and c != "relay_count"])
    log.info(
        f"Unique blocks: {len(agg):,}  |  max relays per block: {len(relay_cols)}  |  "
        f"multi-relay blocks: {(agg['relay_count'] > 1).sum():,}"
    )

    labels = _load_builder_labels()
    pubkey_lower = agg["builder_pubkey"].str.to_lowercase()
    if labels:
        label_df = pl.DataFrame({"pubkey_lower": list(labels.keys()), "builder_name": list(labels.values())})
        agg = agg.with_columns(pubkey_lower.alias("_pubkey_lower")).join(
            label_df, left_on="_pubkey_lower", right_on="pubkey_lower", how="left"
        ).drop("_pubkey_lower")
        agg = agg.with_columns([
            pl.coalesce([pl.col("builder_name"), pl.col("builder_pubkey").str.slice(0, 18)]).alias("builder_label"),
            pl.when(pl.col("builder_name").is_not_null()).then(pl.lit("curated")).otherwise(pl.lit("pubkey_prefix")).alias("builder_name_source"),
        ]).drop("builder_name")
        n_curated = agg.filter(pl.col("builder_name_source") == "curated").height
        log.info(f"Builder labels: {n_curated:,} curated ({n_curated / len(agg) * 100:.1f}%), {len(agg) - n_curated:,} pubkey_prefix")
    else:
        agg = agg.with_columns([
            pl.col("builder_pubkey").str.slice(0, 18).alias("builder_label"),
            pl.lit("pubkey_prefix").alias("builder_name_source"),
        ])

    agg = agg.with_columns((pl.col("value_wei").cast(pl.Float64) / 1e18).alias("value_eth"))
    agg = agg.with_columns(
        pl.col("builder_label").replace(LABEL_MERGES).alias("builder_label")
    )

    core_order = [
        "block_number", "slot", "block_hash",
        "builder_pubkey", "builder_label", "builder_name_source",
        "proposer_pubkey", "proposer_fee_recipient",
        "gas_used", "value_wei", "value_eth", "num_tx", "relay_count",
    ]
    core_cols = [c for c in core_order if c in agg.columns]
    agg = agg.select(core_cols + relay_cols)

    out_path = RAW_DIR / "df_blocks_builder.parquet"
    agg.write_parquet(out_path, compression="zstd")
    log.info(f"Saved df_blocks_builder.parquet: {len(agg):,} rows -> {out_path}")

    print(f"\n{'=' * 60}")
    print(f"Blocks with MEV-boost data:  {len(agg):,}")
    print(f"Block range:                 {agg['block_number'].min():,} - {agg['block_number'].max():,}")
    print(f"Multi-relay blocks:          {(agg['relay_count'] > 1).sum():,} ({(agg['relay_count'] > 1).mean() * 100:.1f}%)")
    src_counts = agg["builder_name_source"].value_counts()
    print("\nBuilder label coverage:")
    for row in src_counts.iter_rows(named=True):
        print(f"  {row['builder_name_source']:20s}: {row['count']:>8,}  ({row['count'] / len(agg) * 100:.1f}%)")
    print(f"\nTop 15 builders (by block count):")
    print(agg["builder_label"].value_counts().sort("count", descending=True).head(15))
    print(f"{'=' * 60}\n")

    return agg


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["download", "build", "all"], default="all")
    args = parser.parse_args()
    if args.step in ("download", "all"):
        download_xatu()
    if args.step in ("build", "all"):
        build_blocks_table()


if __name__ == "__main__":
    main()
