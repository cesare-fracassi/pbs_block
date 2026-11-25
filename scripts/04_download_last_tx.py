#!/usr/bin/env python3
"""
Step 4 - Download the last transaction per block for all post-Merge Ethereum blocks.

Under PBS, the builder pays the validator via a plain ETH transfer as the last
transaction in the block (highest transaction_index). This signal identifies
PBS blocks independent of relay data and extra_data builder strings, covering
blocks where extra_data is empty or contains an unrecognized string.

The key check (done in 05_build_pbs_flag.py):
    to_address == fee_recipient   and   value > 0
A receipt_gas_used of exactly 21,000 further confirms a simple ETH transfer
with no calldata; some builders include a short identity string in calldata
(roughly 100-200 bytes), raising gas to roughly 22,000-23,000. Both count as
valid PBS signals.

Output: PBS_DATA_DIR/raw/bq/block_last_tx.parquet
  Columns:
    block_number      (int64)
    block_timestamp   (timestamp, UTC)
    transaction_index (int64)   always the maximum in that block
    from_address      (str)     builder's EOA or contract
    to_address        (str)     payment recipient, matches fee_recipient under PBS
    value             (numeric) ETH transferred, in wei
    receipt_gas_used  (int64)   21,000 for a bare transfer, roughly 22-23K with calldata

Cost: roughly $0.40-0.60 (reads 7 columns; deliberately excludes the large `input` column)

Run:
    export PBS_BQ_DIR=/path/to/crypto_data/PBS/data/raw/bq
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/gcp-key.json
    python 04_download_last_tx.py
"""
import os
from pathlib import Path

from google.cloud import bigquery

BQ_DIR = Path(os.environ.get("PBS_BQ_DIR", "./data/raw/bq"))
OUT_FILE = BQ_DIR / "block_last_tx.parquet"

MERGE_DATE = "2022-09-15"

# QUALIFY returns exactly one row per block_number: the transaction with the
# highest transaction_index (the last one executed). The `input` column
# (calldata) is deliberately excluded to avoid scanning a large variable-length
# column. Uses the legacy bigquery-public-data.crypto_ethereum dataset, same
# note on a possible future goog_blockchain_ethereum_mainnet_us migration as
# in 03_download_block_extra_data.py.
QUERY = f"""
SELECT
    block_number,
    block_timestamp,
    transaction_index,
    from_address,
    to_address,
    value,
    receipt_gas_used
FROM `bigquery-public-data.crypto_ethereum.transactions`
WHERE block_timestamp >= TIMESTAMP('{MERGE_DATE}')
QUALIFY ROW_NUMBER() OVER (PARTITION BY block_number ORDER BY transaction_index DESC) = 1
ORDER BY block_number
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

    print("Fetching results (this may take several minutes) ...", flush=True)
    table = job.to_arrow()
    print(f"  {table.num_rows:,} rows fetched", flush=True)

    import polars as pl
    df = pl.from_arrow(table)

    BQ_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_FILE, compression="zstd")
    print(f"  Saved -> {OUT_FILE}  ({OUT_FILE.stat().st_size / 1e6:.0f} MB)", flush=True)
    print("Done.", flush=True)


if __name__ == "__main__":
    main()
