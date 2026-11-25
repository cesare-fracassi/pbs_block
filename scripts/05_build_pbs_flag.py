#!/usr/bin/env python3
"""
Step 5 - Build the definitive PBS flag for all post-Merge Ethereum blocks.

Combines four data sources using a priority hierarchy and runs cross-source
QC. No input is deleted or modified; this script only reads its inputs and
writes a new output file.

Input files (must exist before running):
    data/interim/df_blocks_builder_augmented.parquet   (step 2: Xatu + Flashbots relay blocks)
    data/raw/node/block_inputs_*.parquet               (step 3: extra_data, fee recipient, last transaction)

Optional, for the coverage comparison in the QC report:
    data/raw/relayscan_daily.csv                       (step 4)

The output covers exactly the blocks in the step 3 files: relay blocks outside that range are ignored, so the
dataset never has a tail of blocks that only the relay data knows about.

Output:
    data/processed/df_blocks_pbs.parquet       final, shared schema:
        block_number, is_pbs, builder_address, builder_name
        (block_timestamp and the fee recipient are deliberately NOT included in the shared file; they are
        available from any node)
    data/processed/df_blocks_pbs_full.parquet  only with --keep-full: the full intermediate table (every
        source column kept) for internal checks, not meant for sharing
    data/processed/pbs_flag_qc_sources.csv     blocks per classification source and extra_data class
    data/processed/pbs_flag_qc_monthly.csv     blocks per month and classification source
    data/processed/pbs_flag_qc_relayscan.csv   only when step 4 has run: monthly coverage against relayscan.io

PBS source priority
--------------------
  1. xatu        - block in Xatu relay data (builder_name_source != 'flashbots_api')
  2. flashbots   - block added from the Flashbots API (builder_name_source == 'flashbots_api')
  3. extra_data  - not in relay data, but extra_data contains a known builder string
  4. last_tx     - not in relay/extra_data; last tx is a value transfer to fee_recipient
  5. non_pbs     - not in relay data, extra_data contains an execution-client string
  6. unknown     - not in relay data, extra_data is empty or unrecognized, no last_tx signal

is_pbs = True  for sources: xatu, flashbots, extra_data, last_tx
is_pbs = False for sources: non_pbs, unknown

Known coverage caveat: this pipeline covers Sep 29, 2022 (Flashbots API start)
onward; Sep 15-28, 2022 is unclassifiable. A residual ~1.3% gap remains,
concentrated in late-2022 BloXroute blocks whose relay API data was purged
before any pipeline could capture it.

Run:
    python scripts/05_build_pbs_flag.py
    python scripts/05_build_pbs_flag.py --keep-full          # also write the full intermediate table
    python scripts/05_build_pbs_flag.py --out some.parquet   # write the final file elsewhere (used by update_month.py)
"""
import argparse
import os
import re
from pathlib import Path

import polars as pl

from _common import INTERIM_DIR, PROCESSED_DIR, RAW_DIR, pivot_compat

NODE_GLOB = str(RAW_DIR / "node" / "block_inputs_*.parquet")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

OUT_FILE = PROCESSED_DIR / "df_blocks_pbs.parquet"

# Known PBS builder strings (case-insensitive substring of decoded UTF-8)
BUILDER_PATTERNS: list[str] = [
    "beaverbuild", "titanbuilder", "titan",
    "rsync-builder", "rsync",
    "flashbots",
    "builder0x69",
    "bobthebuilder",
    "bloxroute", "bloXroute",
    "f1b.io",
    "penguinbuild",
    "eigenphi",
    "jet-builder",
    "eth-builder",
    "manifold",
    "buildai",
    "payy",
    "I am inevitable",
    "lilbuilder",
    "blocksmith",
    "gambit-builder",
    "pandabuilder",
    "nethermind-builder",
    "agnostic",
    "Eden",
    "manta",
    "loki builder",
    "blockbeelder",
    "vaultbuilder",
    "mevdefender",
    "ultrasound",
    "aestus",
    "relayooor",
    "illuminate",
    "0x4e70",
]
# Execution-client strings implying a validator self-built (non-PBS) block
CLIENT_PATTERNS: list[str] = [
    r"Geth/v", r"Nethermind/v", r"Besu/v", r"Erigon/v", r"reth/v", r"OpenEthereum/",
]

BUILDER_RE = re.compile("|".join(re.escape(p) for p in BUILDER_PATTERNS), re.IGNORECASE)
CLIENT_RE = re.compile("|".join(CLIENT_PATTERNS))

# Maps any raw builder name (relay registry or extra_data match) to a
# canonical form. Order matters: more specific patterns must come first
# (e.g. 'rsync-builder' before 'rsync').
CANONICAL_MAP: list[tuple[str, str]] = [
    ("beaverbuild", "beaverbuild"), ("beaver", "beaverbuild"),
    ("titanbuilder", "titan"), ("titan builder", "titan"), ("titan", "titan"),
    ("rsync-builder", "rsync-builder"), ("rsync", "rsync-builder"),
    ("flashbots", "flashbots"),
    ("builder0x69", "builder0x69"), ("0x69", "builder0x69"),
    ("bobthebuilder", "bob-the-builder"), ("bob the builder", "bob-the-builder"), ("bob-the-builder", "bob-the-builder"),
    ("bloxroute", "bloxroute"), ("blox route", "bloxroute"),
    ("f1b.io", "f1b"), ("f1b", "f1b"),
    ("penguinbuild", "penguinbuild"), ("penguin", "penguinbuild"),
    ("eigenphi", "eigenphi"),
    ("jet-builder", "jet-builder"), ("jet builder", "jet-builder"),
    ("eth-builder", "eth-builder"), ("eth builder", "eth-builder"),
    ("manifold", "manifold"),
    ("buildai", "buildai"), ("build ai", "buildai"),
    ("payy", "payy"),
    ("i am inevitable", "i-am-inevitable"), ("inevitable", "i-am-inevitable"),
    ("lilbuilder", "lilbuilder"), ("lil builder", "lilbuilder"),
    ("blocksmith", "blocksmith"),
    ("gambit-builder", "gambit-builder"), ("gambit builder", "gambit-builder"), ("gambit", "gambit-builder"),
    ("pandabuilder", "pandabuilder"), ("panda builder", "pandabuilder"),
    ("nethermind-builder", "nethermind-builder"), ("nethermind builder", "nethermind-builder"),
    ("agnostic", "agnostic"),
    ("eden", "eden"),
    ("manta", "manta"),
    ("loki builder", "loki-builder"), ("loki-builder", "loki-builder"), ("loki", "loki-builder"),
    ("blockbeelder", "blockbeelder"),
    ("vaultbuilder", "vaultbuilder"), ("vault builder", "vaultbuilder"),
    ("mevdefender", "mevdefender"), ("mev defender", "mevdefender"), ("mev-defender", "mevdefender"),
    ("ultrasound", "ultrasound"), ("ultra sound", "ultrasound"), ("ultra-sound", "ultrasound"),
    ("illuminate", "illuminate"),
    ("0x4e70", "0x4e70"),
    ("aestus", "aestus"),
    ("relayooor", "relayooor"),
]


def canonicalise_builder(name: str | None) -> str | None:
    if name is None or name.strip() == "":
        return None
    lowered = name.lower().strip()
    for pattern, canonical in CANONICAL_MAP:
        if pattern in lowered:
            return canonical
    return lowered  # unknown builder, keep lowercased


def classify_extra_data(utf8: str | None) -> str:
    if utf8 is None or utf8.strip("\x00") == "":
        return "EMPTY"
    if CLIENT_RE.search(utf8):
        return "NON_PBS"
    if BUILDER_RE.search(utf8):
        return "PBS_BUILDER"
    return "PBS_UNKNOWN"


def first_builder_match(utf8: str | None) -> str | None:
    if utf8 is None:
        return None
    m = BUILDER_RE.search(utf8)
    return m.group(0).lower() if m else None


def hex_to_utf8(h: str | None) -> str | None:
    if not h or h in ("0x", ""):
        return None
    raw = h[2:] if h.startswith("0x") else h
    try:
        return bytes.fromhex(raw).decode("utf-8")
    except (UnicodeDecodeError, ValueError):
        return None


def load_relay_blocks() -> pl.DataFrame:
    aug_file = INTERIM_DIR / "df_blocks_builder_augmented.parquet"
    if not aug_file.exists():
        raise FileNotFoundError(f"{aug_file} not found. Run 02_augment_flashbots.py first.")

    print("Loading df_blocks_builder_augmented.parquet ...", flush=True)
    relay_cols = ["block_number", "slot", "relay_count", "builder_pubkey", "builder_label", "builder_name_source"]
    schema = pl.read_parquet(aug_file, n_rows=0).columns
    extra_relay = [c for c in schema if re.match(r"^relay_\d+$", c)]
    aug = pl.read_parquet(aug_file, columns=relay_cols + extra_relay)

    # block_number dtype can come in as UInt64 or Int64 depending on which
    # upstream script last touched this file (Xatu-only vs Flashbots-augmented
    # paths use different aggregation builtins) - normalize here so every join
    # against this frame downstream has a consistent type to match against.
    aug = aug.with_columns(pl.col("block_number").cast(pl.Int64))

    aug = aug.with_columns(
        pl.when(pl.col("builder_name_source") == "flashbots_api")
        .then(pl.lit("flashbots"))
        .otherwise(pl.lit("xatu"))
        .alias("pbs_source")
    )
    print(
        f"  {len(aug):,} relay blocks  "
        f"({(aug['pbs_source'] == 'xatu').sum():,} Xatu  |  "
        f"{(aug['pbs_source'] == 'flashbots').sum():,} Flashbots-only)",
        flush=True,
    )
    return aug


def load_extra_data() -> pl.DataFrame:
    """Block header fields (timestamp, extra_data, fee recipient) from the step 3 node files."""
    if not list(Path(NODE_GLOB).parent.glob(Path(NODE_GLOB).name)):
        raise FileNotFoundError(f"No {NODE_GLOB} files found. Run 03_extract_node_block_data.py first.")

    print("Loading block header fields from the node files ...", flush=True)
    ed = (
        pl.scan_parquet(NODE_GLOB)
        .select(["block_number", "block_timestamp", "extra_data_hex", "fee_recipient"])
        .collect()
        .with_columns(pl.from_epoch(pl.col("block_timestamp"), time_unit="s").alias("block_timestamp"))
        .sort("block_number")
    )
    n_dup = len(ed) - ed["block_number"].n_unique()
    if n_dup:
        raise ValueError(f"{n_dup:,} duplicated block numbers in the node files")

    utf8_vals = [hex_to_utf8(v) for v in ed["extra_data_hex"].to_list()]
    ed = ed.with_columns(pl.Series("extra_data_utf8", utf8_vals))

    classes = [classify_extra_data(v) for v in ed["extra_data_utf8"].to_list()]
    builder_ids = [first_builder_match(v) for v in ed["extra_data_utf8"].to_list()]
    ed = ed.with_columns([
        pl.Series("extra_data_class", classes),
        pl.Series("extra_data_builder_id", builder_ids),
    ])

    dist = ed["extra_data_class"].value_counts().sort("count", descending=True)
    print(f"  {len(ed):,} blocks  |  extra_data classes:")
    for row in dist.iter_rows(named=True):
        print(f"    {row['extra_data_class']:20s}: {row['count']:>9,}  ({row['count'] / len(ed) * 100:.1f}%)")
    return ed


def load_last_tx(ed: pl.DataFrame) -> pl.DataFrame:
    """Last transaction of each non-empty block, from the step 3 node files."""
    print("Loading last-transaction fields from the node files ...", flush=True)
    lt = (
        pl.scan_parquet(NODE_GLOB)
        .filter(pl.col("n_tx") > 0)
        .select(["block_number", "transaction_index", "from_address", "to_address", "value", "receipt_gas_used"])
        .collect()
        # value is stored as an exact decimal string of wei; the signal only needs it as a number
        .with_columns(pl.col("value").cast(pl.Float64))
    )
    print(f"  {len(lt):,} blocks with a last transaction", flush=True)

    lt = lt.with_columns(pl.col("block_number").cast(pl.Int64)).join(
        ed.select(["block_number", "fee_recipient"]).with_columns(pl.col("block_number").cast(pl.Int64)),
        on="block_number", how="left",
    )

    # Builder-payment signal: last tx sends ETH to the validator's fee_recipient.
    # Gas <= 30,000 excludes complex contract calls while allowing builders who
    # include a short identity string in calldata (roughly 22-23K gas).
    lt = lt.with_columns([
        (
            (pl.col("to_address").str.to_lowercase() == pl.col("fee_recipient").str.to_lowercase())
            & (pl.col("value") > 0)
            & (pl.col("receipt_gas_used") <= 30_000)
        ).fill_null(False).alias("last_tx_signal"),
    ])

    n_signal = lt["last_tx_signal"].sum()
    print(f"  Last-tx builder-payment signal fires on {n_signal:,} blocks ({n_signal / len(lt) * 100:.1f}%)", flush=True)

    return lt.with_columns(
        pl.col("value").cast(pl.Float64).alias("last_tx_value"),
    ).rename({
        "from_address": "last_tx_from",
        "to_address": "last_tx_to",
        "receipt_gas_used": "last_tx_gas",
        "transaction_index": "last_tx_index",
    }).select([
        "block_number", "last_tx_from", "last_tx_to", "last_tx_value", "last_tx_gas", "last_tx_index", "last_tx_signal",
    ])


def build_pbs_flag(relay: pl.DataFrame, ed: pl.DataFrame, lt: pl.DataFrame | None = None) -> pl.DataFrame:
    print("\nMerging sources ...", flush=True)

    relay_set = set(relay["block_number"].to_list())

    relay_cols_keep = ["block_number", "slot", "relay_count", "builder_pubkey", "builder_label", "builder_name_source", "pbs_source"]
    relay_cols_keep += [c for c in relay.columns if re.match(r"^relay_\d+$", c)]

    relay_ed = relay.select(relay_cols_keep).join(
        ed.select(["block_number", "block_timestamp", "fee_recipient", "extra_data_hex", "extra_data_class", "extra_data_builder_id"]),
        on="block_number", how="left",
    )

    non_relay_ed = (
        ed.filter(~pl.col("block_number").is_in(relay_set))
        .drop("extra_data_utf8")
        .with_columns([
            pl.lit(None).cast(pl.Int64).alias("slot"),
            pl.lit(None).cast(pl.Int32).alias("relay_count"),
            pl.col("extra_data_builder_id").alias("builder_label"),
            pl.when(pl.col("extra_data_class") == "PBS_BUILDER").then(pl.lit("extra_data"))
            .when(pl.col("extra_data_class") == "NON_PBS").then(pl.lit("non_pbs"))
            .otherwise(pl.lit("unknown"))
            .alias("pbs_source"),
        ])
    )
    relay_cols_in_relay = [c for c in relay.columns if re.match(r"^relay_\d+$", c)]
    for c in relay_cols_in_relay:
        non_relay_ed = non_relay_ed.with_columns(pl.lit(None).cast(pl.Utf8).alias(c))

    lt_null = {
        "last_tx_from": pl.Utf8, "last_tx_to": pl.Utf8, "last_tx_value": pl.Float64,
        "last_tx_gas": pl.Int64, "last_tx_index": pl.Int64,
    }
    if lt is not None:
        lt_nr = lt.filter(~pl.col("block_number").is_in(relay_set))
        non_relay_ed = (
            non_relay_ed
            .with_columns(pl.col("block_number").cast(pl.Int64))
            .join(lt_nr.with_columns(pl.col("block_number").cast(pl.Int64)), on="block_number", how="left")
            .with_columns(
                pl.when((pl.col("pbs_source") == "unknown") & (pl.col("last_tx_signal").fill_null(False)))
                .then(pl.lit("last_tx"))
                .otherwise(pl.col("pbs_source"))
                .alias("pbs_source")
            )
            .drop("last_tx_signal")
        )
        del lt_nr
        n_last_tx = (non_relay_ed["pbs_source"] == "last_tx").sum()
        print(f"  last_tx signal: {n_last_tx:,} blocks upgraded to PBS", flush=True)
    else:
        for c, dtype in lt_null.items():
            non_relay_ed = non_relay_ed.with_columns(pl.lit(None).cast(dtype).alias(c))

    for c, dtype in lt_null.items():
        relay_ed = relay_ed.with_columns(pl.lit(None).cast(dtype).alias(c))

    relay_ed = relay_ed.with_columns(pl.col("block_number").cast(pl.Int64))
    non_relay_ed = non_relay_ed.with_columns(pl.col("block_number").cast(pl.Int64))

    combined = pl.concat([relay_ed, non_relay_ed], how="diagonal_relaxed")
    combined = combined.with_columns(
        pl.col("pbs_source").is_in(["xatu", "flashbots", "extra_data", "last_tx"]).alias("is_pbs")
    )
    combined = combined.sort("block_number")

    src_dist = combined.group_by("pbs_source").agg(pl.len().alias("n")).sort("n", descending=True)
    print("\nPBS source distribution:")
    for row in src_dist.iter_rows(named=True):
        pbs = "PBS" if row["pbs_source"] in ("xatu", "flashbots", "extra_data", "last_tx") else "non-PBS"
        print(f"  {row['pbs_source']:20s}: {row['n']:>9,}  ({pbs})")

    n_pbs = combined["is_pbs"].sum()
    print(f"\n  Total PBS blocks:      {n_pbs:,}  ({n_pbs / len(combined) * 100:.1f}%)")
    print(f"  Total non-PBS/unknown: {len(combined) - n_pbs:,}  ({(len(combined) - n_pbs) / len(combined) * 100:.1f}%)")
    return combined


def run_qc(combined: pl.DataFrame) -> None:
    print("\n" + "=" * 60)
    print("QC REPORT")
    print("=" * 60)

    print("\n--- Cross-validation: relay signal vs extra_data signal ---")
    relay_pbs = combined.filter(pl.col("pbs_source").is_in(["xatu", "flashbots"]))
    non_relay = combined.filter(~pl.col("pbs_source").is_in(["xatu", "flashbots"]))

    for label, subset in [("Relay PBS blocks", relay_pbs), ("Non-relay blocks", non_relay)]:
        dist = subset.group_by("extra_data_class").agg(pl.len().alias("n")).sort("n", descending=True)
        print(f"\n  {label} (n={len(subset):,}), extra_data breakdown:")
        for row in dist.iter_rows(named=True):
            # A null extra_data_class here means this block has no matching
            # row in block_extra_data.parquet at all - expected for any relay
            # block newer than that file's own last pull (see the "routine
            # update" policy in the README: extra_data/last_tx are not
            # refreshed automatically).
            cls_label = row["extra_data_class"] if row["extra_data_class"] is not None else "NO_EXTRA_DATA_MATCH"
            n = row["n"] or 0
            print(f"    {cls_label:20s}: {n:>9,}  ({n / len(subset) * 100:.1f}%)")

    false_pos = ((combined["pbs_source"].is_in(["xatu", "flashbots"])) & (combined["extra_data_class"] == "NON_PBS")).sum()
    false_neg = ((~combined["pbs_source"].is_in(["xatu", "flashbots", "extra_data"])) & (combined["extra_data_class"] == "PBS_BUILDER")).sum()
    print(f"\n  FALSE_POS (relay says PBS, extra_data says client-built): {false_pos:,}")
    print(f"  FALSE_NEG recovered (extra_data PBS, not in relay data):   {false_neg:,}")

    if "last_tx_value" in combined.columns:
        n_last_tx = (combined["pbs_source"] == "last_tx").sum()
        print(f"\n--- Last-tx builder payment signal ---")
        print(f"  Blocks upgraded to PBS via last_tx: {n_last_tx:,}")
        if n_last_tx > 0:
            n_bare = ((combined["pbs_source"] == "last_tx") & (combined["last_tx_gas"] == 21_000)).sum()
            print(f"  Bare ETH transfer (gas=21000): {n_bare:,} ({n_bare / n_last_tx * 100:.1f}%)")
            print(f"  With calldata (gas>21000):     {n_last_tx - n_bare:,} ({(n_last_tx - n_bare) / n_last_tx * 100:.1f}%)")

    print("\n--- Top builders in extra_data (PBS_BUILDER blocks only) ---")
    top_builders = (
        combined.filter(pl.col("extra_data_class") == "PBS_BUILDER")
        .group_by("extra_data_builder_id")
        .agg(pl.len().alias("n_blocks"))
        .sort("n_blocks", descending=True)
        .head(20)
    )
    for row in top_builders.iter_rows(named=True):
        print(f"  {str(row['extra_data_builder_id']):25s}: {row['n_blocks']:>9,}")

    # These two tables do not depend on relayscan, so they are written whether or not step 4 has run.
    src_qc = combined.group_by(["pbs_source", "extra_data_class"]).agg(pl.len().alias("n"))
    src_qc.write_csv(PROCESSED_DIR / "pbs_flag_qc_sources.csv")

    monthly_src = pivot_compat(
        combined.filter(pl.col("block_timestamp").is_not_null())
        .with_columns(pl.col("block_timestamp").dt.strftime("%Y-%m").alias("month"))
        .group_by(["month", "pbs_source"])
        .agg(pl.len().alias("n"))
        .sort(["month", "pbs_source"]),
        on="pbs_source", index="month", values="n", aggregate_function="sum",
    ).fill_null(0).sort("month")
    monthly_src.write_csv(PROCESSED_DIR / "pbs_flag_qc_monthly.csv")
    print(f"\nQC tables saved to {PROCESSED_DIR}")

    rs_cache = RAW_DIR / "relayscan_daily.csv"
    if not rs_cache.exists():
        print("\n  (relayscan_daily.csv not found, skipping coverage QC. Run 04_download_relayscan.py to create it.)")
        return

    print("\n--- Monthly coverage vs relayscan.io (PBS blocks only) ---")
    rs = (
        pl.read_csv(rs_cache)
        .with_columns(pl.col("date").str.slice(0, 7).alias("month"))
        .filter(pl.col("relayscan_pbs").is_not_null())
        .group_by("month")
        .agg(pl.col("relayscan_pbs").sum().alias("relayscan_blocks"))
    )

    monthly_ours = (
        combined.filter(pl.col("is_pbs") & pl.col("block_timestamp").is_not_null())
        .with_columns(pl.col("block_timestamp").dt.strftime("%Y-%m").alias("month"))
        .group_by("month")
        .agg(pl.col("block_number").n_unique().alias("our_blocks"))
    )

    cov = (
        monthly_ours.join(rs, on="month", how="inner")
        .with_columns([
            (pl.col("relayscan_blocks") - pl.col("our_blocks")).alias("gap"),
            (pl.col("our_blocks") / pl.col("relayscan_blocks") * 100).round(1).alias("coverage_pct"),
        ])
        .sort("month")
    )
    with pl.Config(tbl_rows=60):
        print(cov.select(["month", "relayscan_blocks", "our_blocks", "gap", "coverage_pct"]))

    total_rs = cov["relayscan_blocks"].sum()
    total_our = cov["our_blocks"].sum()
    print(f"\n  Overall coverage: {total_our:,} / {total_rs:,} = {total_our / total_rs * 100:.1f}%")

    cov.write_csv(PROCESSED_DIR / "pbs_flag_qc_relayscan.csv")
    print(f"Coverage table saved to {PROCESSED_DIR / 'pbs_flag_qc_relayscan.csv'}")


def main(keep_full: bool = False, out_file: Path = OUT_FILE) -> None:
    relay = load_relay_blocks()
    ed = load_extra_data()
    lt = load_last_tx(ed)

    # The dataset covers exactly the blocks the node files cover. Relay data can run ahead of them (Xatu and
    # Flashbots are fetched up to the newest day), and a block that only the relay data knows about would
    # leave a tail of PBS blocks with no matching non-PBS blocks.
    first_block, last_block = ed["block_number"].min(), ed["block_number"].max()
    n_before = len(relay)
    relay = relay.filter(pl.col("block_number").is_between(first_block, last_block))
    print(f"\nBlock range of the node files: {first_block:,} - {last_block:,}; "
          f"{n_before - len(relay):,} relay blocks outside it ignored", flush=True)
    combined = build_pbs_flag(relay, ed, lt)

    run_qc(combined)

    print("\nResolving builder_name (curated relay label > extra_data match > raw pubkey prefix) ...", flush=True)
    # A relay-registered builder_label can itself be a "curated" name (from the
    # dataalways/mevboost-data lookup) or just a truncated pubkey-prefix
    # fallback (builder_name_source == "pubkey_prefix") when the pubkey isn't
    # in that mapping yet. A pubkey-prefix is not a real name, so it should
    # defer to extra_data_builder_id if that resolves to something - only
    # falling back to the raw prefix itself as a last resort.
    lbl_col = combined["builder_label"].to_list() if "builder_label" in combined.columns else [None] * len(combined)
    src_col = combined["builder_name_source"].to_list() if "builder_name_source" in combined.columns else [None] * len(combined)
    eid_col = combined["extra_data_builder_id"].to_list() if "extra_data_builder_id" in combined.columns else [None] * len(combined)

    def resolve_name(label, name_source, extra_data_id):
        if label is not None and name_source == "curated":
            c = canonicalise_builder(label)
            if c is not None:
                return c
        ed_c = canonicalise_builder(extra_data_id)
        if ed_c is not None:
            return ed_c
        return canonicalise_builder(label)  # last resort: raw pubkey-prefix, if any

    builder_name = [resolve_name(lbl, src, eid) for lbl, src, eid in zip(lbl_col, src_col, eid_col)]
    combined = combined.with_columns(pl.Series("builder_name", builder_name, dtype=pl.Utf8))
    n_named = sum(1 for c in builder_name if c is not None)
    print(f"  {n_named:,} blocks with a builder name ({n_named / len(combined) * 100:.1f}%)", flush=True)

    # Full intermediate table (all source columns), only on request: nothing in the pipeline reads it.
    if keep_full:
        out_cols = [c for c in combined.columns if c != "extra_data_utf8"]
        qc_file = PROCESSED_DIR / "df_blocks_pbs_full.parquet"
        combined.select(out_cols).write_parquet(qc_file, compression="zstd")
        print(f"Saved full intermediate table (--keep-full) -> {qc_file}")

    # The shared file keeps only the classification and the builder. The block timestamp and the fee
    # recipient are left out on purpose: any node has them, and they would be one more thing to keep
    # in sync with each update.
    print("\nBuilding the final, shared schema: block_number, is_pbs, "
          "builder_address, builder_name ...", flush=True)
    final = (
        combined
        .rename({"builder_pubkey": "builder_address"})
        .select(["block_number", "is_pbs", "builder_address", "builder_name"])
        .sort("block_number")
    )
    out_file = Path(out_file)
    out_file.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_file.with_name(out_file.name + ".tmp")
    final.write_parquet(tmp, compression="zstd")
    os.replace(tmp, out_file)
    size_mb = out_file.stat().st_size / 1e6
    print(f"\nSaved -> {out_file}  ({size_mb:.0f} MB  |  {len(final):,} blocks, "
          f"{final['block_number'].min():,} - {final['block_number'].max():,})")
    print("Done.", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build the definitive PBS flag per block.")
    parser.add_argument(
        "--keep-full", action="store_true",
        help="also write processed/df_blocks_pbs_full.parquet (every source column; not needed by anything)",
    )
    parser.add_argument(
        "--out", type=Path, default=OUT_FILE,
        help="where to write the final file (default: data/processed/df_blocks_pbs.parquet)",
    )
    args = parser.parse_args()
    main(keep_full=args.keep_full, out_file=args.out)
