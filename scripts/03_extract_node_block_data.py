#!/usr/bin/env python3
"""
Step 3 - Extract from an Ethereum node the block fields the PBS classification needs. This replaces the two
BigQuery downloads of earlier versions (block extra_data and last transaction per block), so the whole pipeline
is free and needs no cloud account.

Per block, from the first block of 2022-09-15 UTC (the Merge day) on:
    block_number         Int64
    block_timestamp      Int64   unix seconds
    extra_data_hex       Utf8    the block header's extraData, '0x' + hex (builders often write their name here)
    fee_recipient        Utf8    the block header's miner field, lower case (the validator's payout address under PBS)
    n_tx                 Int64   number of transactions in the block
    last_tx_hash         Utf8    hash of the block's last transaction (null for an empty block)
    transaction_index    Int64   its position in the block (n_tx - 1; null for an empty block)
    from_address         Utf8    its sender
    to_address           Utf8    its receiver (null for a contract creation)
    value                Utf8    wei sent, as a decimal string (exact)
    receipt_gas_used     Int64   gas the last transaction used (from its receipt)

The last transaction is the signal of step 5: under PBS a builder often pays the validator with a small plain ETH
transfer as the last transaction of the block.

Output: data/raw/node/block_inputs_<first>_<last>.parquet, one file per block range (default 20,000 blocks), zstd.
A range is complete once its file exists (written to a temporary name first). Re-running continues where the data
ends; the script refuses to leave a gap.

How: three JSON-RPC calls per block, sent in batches: eth_getBlockByNumber (hashes only), then
eth_getTransactionByHash and eth_getTransactionReceipt for the block's last transaction. Any Ethereum execution
client answers them; designed and tested with Erigon on a local machine. A hosted endpoint works but is slower and
rate-limited (use fewer --workers).

Block range: by default from the block after the last one stored (or the Merge-day block on an empty folder) to the
latest FINALIZED block, so a reorganisation cannot change stored data. Other ends: --last-block N, or
--through-month YYYY-MM (the last block of that month, UTC; stops with an error if that block is not finalized).

Usage (from the repository root):
    python scripts/03_extract_node_block_data.py --benchmark 2000                  # speed test, writes nothing
    python scripts/03_extract_node_block_data.py                                   # up to the finalized block
    python scripts/03_extract_node_block_data.py --through-month 2026-10           # up to the end of October 2026
    python scripts/03_extract_node_block_data.py --first-block 20000000 --last-block 20000099 \
        --chunk-blocks 100 --out-dir /tmp/node_test                                # small test

The node URL comes from --rpc-url or the environment variable ERIGON_RPC_URL (default http://127.0.0.1:8545).
"""
import argparse
import calendar
import datetime as dt
import http.client
import json
import os
import re
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import polars as pl

from _common import MERGE_DATE, RAW_DIR

sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.stderr.reconfigure(encoding="utf-8", errors="replace")

HEX_RE = re.compile(r"^0x[0-9a-f]*$")
ADDR_RE = re.compile(r"^0x[0-9a-f]{40}$")
_local = threading.local()

SCHEMA = {
    "block_number": pl.Int64, "block_timestamp": pl.Int64, "extra_data_hex": pl.Utf8, "fee_recipient": pl.Utf8,
    "n_tx": pl.Int64, "last_tx_hash": pl.Utf8, "transaction_index": pl.Int64, "from_address": pl.Utf8,
    "to_address": pl.Utf8, "value": pl.Utf8, "receipt_gas_used": pl.Int64,
}


class Rpc:
    """Minimal JSON-RPC client over one persistent HTTP connection, one instance per thread."""

    def __init__(self, url: str, timeout: int = 180):
        u = urllib.parse.urlparse(url)
        self.https = u.scheme == "https"
        self.host = u.hostname
        self.port = u.port or (443 if self.https else 80)
        self.path = u.path or "/"
        self.timeout = timeout
        self.conn = None

    def call(self, payload):
        last = None
        for attempt in range(1, 6):
            try:
                if self.conn is None:
                    cls = http.client.HTTPSConnection if self.https else http.client.HTTPConnection
                    self.conn = cls(self.host, self.port, timeout=self.timeout)
                self.conn.request("POST", self.path, body=json.dumps(payload),
                                  headers={"Content-Type": "application/json"})
                resp = self.conn.getresponse()
                data = resp.read()
                if resp.status != 200:
                    raise RuntimeError(f"HTTP {resp.status}: {data[:200]!r}")
                return json.loads(data)
            except Exception as e:  # noqa: BLE001
                last = e
                self.conn = None
                time.sleep(min(2 ** attempt, 30))
        raise RuntimeError(f"RPC call failed after 5 attempts: {last}")


def rpc(url: str) -> Rpc:
    if getattr(_local, "rpc", None) is None:
        _local.rpc = Rpc(url)
    return _local.rpc


def get_block(url: str, tag) -> dict:
    """One block header (no transactions) by number or tag ('finalized', 'latest')."""
    p = hex(tag) if isinstance(tag, int) else tag
    r = rpc(url).call({"jsonrpc": "2.0", "id": 1, "method": "eth_getBlockByNumber", "params": [p, False]})
    if r.get("error") or not r.get("result"):
        sys.exit(f"node returned no block for {p}: {r.get('error')}")
    return r["result"]


def first_block_at_or_after(url: str, ts: int) -> int:
    """First block whose timestamp is >= ts (unix seconds)."""
    lo, hi = 0, int(get_block(url, "latest")["number"], 16)
    if int(get_block(url, hi)["timestamp"], 16) < ts:
        sys.exit("the chain has not reached that time yet")
    while lo < hi:
        mid = (lo + hi) // 2
        if int(get_block(url, mid)["timestamp"], 16) >= ts:
            hi = mid
        else:
            lo = mid + 1
    return lo


def month_last_block(url: str, month: str) -> int:
    """Last block whose timestamp is before the first second of the month after `month` (YYYY-MM, UTC)."""
    y, m = (int(x) for x in month.split("-"))
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    limit = calendar.timegm(dt.datetime(ny, nm, 1).timetuple())
    if int(get_block(url, "latest")["timestamp"], 16) < limit:
        sys.exit(f"the chain has not reached the end of {month} (latest block is earlier than {ny}-{nm:02d}-01)")
    return first_block_at_or_after(url, limit) - 1


def _result(reply: dict, what: str):
    if reply.get("error") or reply.get("result") is None:
        raise RuntimeError(f"{what}: {reply.get('error') or 'null result'}")
    return reply["result"]


def fetch_batch(url: str, first: int, last: int) -> list[dict]:
    """Rows for blocks first..last (inclusive), in block order."""
    r = rpc(url)
    payload = [{"jsonrpc": "2.0", "id": n, "method": "eth_getBlockByNumber", "params": [hex(n), False]}
               for n in range(first, last + 1)]
    replies = r.call(payload)
    if not isinstance(replies, list):
        raise RuntimeError(f"batch reply is not a list: {str(replies)[:200]}")
    by_id = {x["id"]: x for x in replies}

    rows, need = {}, []
    for n in range(first, last + 1):
        if n not in by_id:
            raise RuntimeError(f"block {n} missing from the batch reply")
        b = _result(by_id[n], f"block {n}")
        if int(b["number"], 16) != n:
            raise RuntimeError(f"block {n}: reply carries number {b['number']}")
        txs = b["transactions"]
        if txs and not isinstance(txs[0], str):
            raise RuntimeError(f"block {n}: transactions are objects, expected hashes")
        extra = (b["extraData"] or "0x").lower()
        miner = b["miner"].lower()
        if not HEX_RE.match(extra) or len(extra) % 2:
            raise RuntimeError(f"block {n}: malformed extraData {extra!r}")
        if not ADDR_RE.match(miner):
            raise RuntimeError(f"block {n}: malformed miner {miner!r}")
        rows[n] = {"block_number": n, "block_timestamp": int(b["timestamp"], 16), "extra_data_hex": extra,
                   "fee_recipient": miner, "n_tx": len(txs), "last_tx_hash": None, "transaction_index": None,
                   "from_address": None, "to_address": None, "value": None, "receipt_gas_used": None}
        if txs:
            rows[n]["last_tx_hash"] = txs[-1].lower()
            need.append(n)

    if need:
        payload2 = []
        for n in need:
            h = rows[n]["last_tx_hash"]
            payload2.append({"jsonrpc": "2.0", "id": f"t{n}", "method": "eth_getTransactionByHash", "params": [h]})
            payload2.append({"jsonrpc": "2.0", "id": f"r{n}", "method": "eth_getTransactionReceipt", "params": [h]})
        replies2 = r.call(payload2)
        if not isinstance(replies2, list):
            raise RuntimeError(f"transaction batch reply is not a list: {str(replies2)[:200]}")
        by_id2 = {x["id"]: x for x in replies2}
        for n in need:
            row = rows[n]
            tx = _result(by_id2[f"t{n}"], f"last transaction of block {n}")
            rc = _result(by_id2[f"r{n}"], f"receipt of the last transaction of block {n}")
            if tx["hash"].lower() != row["last_tx_hash"] or int(tx["blockNumber"], 16) != n:
                raise RuntimeError(f"block {n}: transaction reply does not match the block's last transaction")
            if int(tx["transactionIndex"], 16) != row["n_tx"] - 1:
                raise RuntimeError(f"block {n}: last transaction has index {int(tx['transactionIndex'], 16)}, expected {row['n_tx'] - 1}")
            if rc["transactionHash"].lower() != row["last_tx_hash"] or int(rc["blockNumber"], 16) != n:
                raise RuntimeError(f"block {n}: receipt does not match the block's last transaction")
            row["transaction_index"] = row["n_tx"] - 1
            row["from_address"] = tx["from"].lower()
            row["to_address"] = tx["to"].lower() if tx.get("to") else None
            row["value"] = str(int(tx["value"], 16))
            row["receipt_gas_used"] = int(rc["gasUsed"], 16)
    return [rows[n] for n in range(first, last + 1)]


def fetch_range(url: str, first: int, last: int, batch: int, pool: ThreadPoolExecutor) -> list[dict]:
    spans = [(s, min(s + batch - 1, last)) for s in range(first, last + 1, batch)]
    parts = list(pool.map(lambda sp: fetch_batch(url, sp[0], sp[1]), spans))
    return [row for part in parts for row in part]


def existing_ranges(out_dir: Path):
    """Complete ranges in out_dir: sorted list of (first, last). Exits on a gap or overlap."""
    rng = sorted((int(m.group(1)), int(m.group(2))) for p in out_dir.glob("block_inputs_*.parquet")
                 if (m := re.match(r"block_inputs_(\d+)_(\d+)\.parquet$", p.name)))
    for (a0, b0), (a1, b1) in zip(rng, rng[1:]):
        if a1 != b0 + 1:
            sys.exit(f"gap or overlap in {out_dir}: range {a0}-{b0} is followed by {a1}-{b1}")
    return rng


def write_chunk(out_dir: Path, first: int, last: int, rows: list[dict]) -> int:
    if len(rows) != last - first + 1:
        sys.exit(f"range {first}-{last}: got {len(rows)} blocks")
    stamps = [r["block_timestamp"] for r in rows]
    if any(stamps[i + 1] < stamps[i] for i in range(len(stamps) - 1)):
        sys.exit(f"range {first}-{last}: timestamps are not non-decreasing")
    out_dir.mkdir(parents=True, exist_ok=True)
    final = out_dir / f"block_inputs_{first:010d}_{last:010d}.parquet"
    tmp = final.with_suffix(".parquet.tmp")
    pl.DataFrame(rows, schema=SCHEMA).write_parquet(tmp, compression="zstd")
    os.replace(tmp, final)
    return sum(1 for r in rows if r["n_tx"] == 0)


def main():
    ap = argparse.ArgumentParser(description="Extract extra_data, fee recipient and last transaction per block from a node.")
    ap.add_argument("--rpc-url", default=os.environ.get("ERIGON_RPC_URL", "http://127.0.0.1:8545"))
    ap.add_argument("--out-dir", default=RAW_DIR / "node", type=Path)
    ap.add_argument("--first-block", type=int, default=None,
                    help="default: the block after the last one in --out-dir, or the first block of the Merge day")
    end = ap.add_mutually_exclusive_group()
    end.add_argument("--last-block", type=int, default=None, help="default: the latest finalized block")
    end.add_argument("--through-month", default=None, metavar="YYYY-MM", help="last block of this month (UTC)")
    ap.add_argument("--allow-unfinalized", action="store_true", help="allow an end beyond the finalized block")
    ap.add_argument("--chunk-blocks", type=int, default=20000, help="blocks per output file")
    ap.add_argument("--batch", type=int, default=50, help="blocks per JSON-RPC batch request")
    ap.add_argument("--workers", type=int, default=8, help="parallel connections")
    ap.add_argument("--benchmark", type=int, default=0, metavar="N", help="fetch N blocks, print the speed, write nothing")
    a = ap.parse_args()

    url = a.rpc_url
    fin = int(get_block(url, "finalized")["number"], 16)
    print(f"node {url}: finalized block {fin:,}", flush=True)

    if a.benchmark:
        first = a.first_block if a.first_block is not None else max(0, fin - a.benchmark - 1)
        t0 = time.time()
        with ThreadPoolExecutor(a.workers) as pool:
            rows = fetch_range(url, first, first + a.benchmark - 1, a.batch, pool)
        dtime = time.time() - t0
        print(f"benchmark: {len(rows):,} blocks in {dtime:.1f}s = {len(rows) / dtime:,.0f} blocks/s "
              f"with {a.workers} workers, batch {a.batch}")
        return

    out = a.out_dir
    rng = existing_ranges(out)
    if a.first_block is not None:
        first = a.first_block
    elif rng:
        first = rng[-1][1] + 1
    else:
        merge_ts = calendar.timegm(dt.datetime(MERGE_DATE.year, MERGE_DATE.month, MERGE_DATE.day).timetuple())
        first = first_block_at_or_after(url, merge_ts)
        print(f"first block of {MERGE_DATE} (UTC): {first:,}", flush=True)
    if a.through_month:
        last = month_last_block(url, a.through_month)
        print(f"last block of {a.through_month}: {last:,}", flush=True)
    elif a.last_block is not None:
        last = a.last_block
    else:
        last = fin
    if last > fin and not a.allow_unfinalized:
        sys.exit(f"block {last:,} is not finalized yet (finalized: {fin:,}); wait, or pass --allow-unfinalized")
    if rng and first != rng[-1][1] + 1:
        sys.exit(f"--first-block {first} does not continue the stored data, which ends at block {rng[-1][1]}")
    if first > last:
        print(f"nothing to do: stored up to block {first - 1:,}, requested end {last:,}")
        return

    total = last - first + 1
    print(f"extracting blocks {first:,}..{last:,} ({total:,}) into {out}, chunks of {a.chunk_blocks:,}", flush=True)
    t_start = time.time()
    done = 0
    with ThreadPoolExecutor(a.workers) as pool:
        s = first
        while s <= last:
            e = min(s + a.chunk_blocks - 1, last)
            rows = fetch_range(url, s, e, a.batch, pool)
            empty = write_chunk(out, s, e, rows)
            done += e - s + 1
            el = time.time() - t_start
            eta = el / done * (total - done)
            print(f"blocks {s:,}..{e:,}: {empty:,} empty | {done:,}/{total:,} blocks, "
                  f"{el / 60:.1f} min elapsed, ETA {eta / 60:.0f} min", flush=True)
            s = e + 1
    print(f"NODE EXTRACT DONE: blocks {first:,}..{last:,}, {(time.time() - t_start) / 60:.1f} min", flush=True)


if __name__ == "__main__":
    main()
