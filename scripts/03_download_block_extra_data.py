#!/usr/bin/env python3
"""
Step 3 - Download extra_data for all post-Merge Ethereum blocks from BigQuery.

extra_data is set by the block builder, not the validator, so it contains
builder identity strings (e.g. "beaverbuild.org", "Titan (titanbuilder.xyz)")
that reliably identify PBS blocks independent of relay data.

Output: PBS_DATA_DIR/raw/bq/block_extra_data.parquet
  Columns:
    block_number    (int64)
    block_timestamp (timestamp, UTC)
    extra_data_hex  (str)   raw bytes as hex, preserves full content
    extra_data_utf8 (str)   decoded UTF-8, null when bytes are not valid UTF-8
    fee_recipient   (str)   block.miner, equals the validator's fee_recipient under PBS

Cost: roughly $0.005 (scans only header columns of ethereum.blocks for about 4 years)

Run:
    export PBS_BQ_DIR=/path/to/crypto_data/PBS/data/raw/bq
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp-key.json
    python 03_download_block_extra_data.py
"""
import os
from pathlib import Path

from google.cloud import bigquery

BQ_DIR = Path(os.environ.get("PBS_BQ_DIR", "./data/raw/bq"))
OUT_FILE = BQ_DIR / "block_extra_data.parquet"

MERGE_DATE = "2022-09-15"

# Uses the legacy bigquery-public-data.crypto_ethereum dataset, matching the
# dataset this pipeline was originally built and validated against. A future
# update could migrate to bigquery-public-data.goog_blockchain_ethereum_mainnet_us,
# but that dataset's schema differs (STRUCT-heavy) and needs its own row-level
# validation against a sample month before being trusted for this table; not
# done here.
QUERY = f"""
SELECT
    number     AS block_number,
    timestamp  AS block_timestamp,
    extra_data AS extra_data_hex,
    miner      AS fee_recipient
FROM `bigquery-public-data.crypto_ethereum.blocks`
WHERE timestamp >= TIMESTAMP('{MERGE_DATE}')
ORDER BY number
"""


def main() -> None:
    if OUT_FILE.exists():
        import polars as pl
        n = pl.scan_parquet(OUT_FILE).select("block_number").collect().height
        print(f"Already exists: {OUT_FILE}  ({n:,} rows). Delete to re-download.")
        return

    client = bigquery.Client()

    dry_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    dry_job = client.query(QUERY, job_config=dry_config)
    gb_scanned = dry_job.total_bytes_processed / 1e9
    cost_usd = gb_scanned / 1000 * 5
    print(f"Dry run: {gb_scanned:.1f} GB scanned -> estimated cost ${cost_usd:.3f}", flush=True)
    input("Press Enter to proceed with the real query, or Ctrl-C to cancel: ")

    job_config = bigquery.QueryJobConfig(use_query_cache=True, priority=bigquery.QueryPriority.INTERACTIVE)
    print("Submitting query ...", flush=True)
    job = client.query(QUERY, job_config=job_config)
    print(f"Job ID: {job.job_id}", flush=True)

    print("Fetching results (this may take a few minutes) ...", flush=True)
    import polars as pl
    table = job.to_arrow()
    print(f"  {table.num_rows:,} blocks fetched", flush=True)

    def hex_to_utf8(h: str | None) -> str | None:
        if not h or h in ("0x", ""):
            return None
        raw = h[2:] if h.startswith("0x") else h
        try:
            return bytes.fromhex(raw).decode("utf-8")
        except (UnicodeDecodeError, ValueError):
            return None

    df = pl.from_arrow(table)
    utf8_vals = [hex_to_utf8(v) for v in df["extra_data_hex"].to_list()]
    df = df.with_columns(pl.Series("extra_data_utf8", utf8_vals))

    BQ_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_FILE, compression="zstd")
    print(f"  Saved -> {OUT_FILE}  ({OUT_FILE.stat().st_size / 1e6:.0f} MB)", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
