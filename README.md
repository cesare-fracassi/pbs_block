# PBS classification data

Per-block classification of whether an Ethereum block was built through the MEV-Boost auction (proposer-builder separation, PBS), or by the proposer's own client. It is chain-wide data: every block from the Merge day (2022-09-15) onward gets a row, whatever project's transactions you are interested in. Join it to your own data by `block_number`.

## The data

`data/processed/df_blocks_pbs.parquet`, one row per block.

| Column | Type | Coverage | Meaning |
|---|---|---|---|
| `block_number` | Int64 | 100% | Primary key |
| `is_pbs` | Bool | 100% | `True` if the block was built through the PBS auction, `False` if the proposer built it or if no signal could classify it (see the limitations) |
| `builder_address` | Utf8 | 82.1% | The builder's relay-registered public key. Only available for blocks found in relay data (signals 1 and 2 below). One builder often runs many keys at once (one major builder uses more than 70), so this is not an identifier of the company; use `builder_name` for that |
| `builder_name` | Utf8 | 89.6% | The builder's normalized name (for example `beaverbuild`, `titan`, `flashbots`). Naming variants and mergers of the same entity are consolidated |

The coverage figures and the counts below are from the 2026-10-29 build: 10,648,035 blocks, 15,535,777 to 26,300,997, 90.2% with `is_pbs = True`.

The block timestamp and the fee recipient are not in the file (any node has them).

## How blocks are classified

Four independent signals are checked for each block, in priority order; the first one that fires decides.

1. **Relay data (Xatu).** MEV-Boost relays publish a record whenever they deliver a winning block to a validator. [Xatu](https://ethpandaops.io) aggregates these records across many relays. A block found there was built through the auction.
2. **Relay data (Flashbots, direct).** Xatu has gaps for blocks delivered through the Flashbots relay, especially in the months after the Merge (Flashbots had roughly 75% of the market in late 2022 and is barely visible in Xatu then). The Flashbots relay API returns what Xatu misses.
3. **The block's `extra_data` field.** Builders often write their name into this header field (for example `beaverbuild.org`). This recovers PBS blocks that no relay source captured.
4. **The block's last transaction.** If the last transaction is a plain ETH payment to the block's own fee recipient (the validator), that is a common way for a builder to pay the validator.

A block is non-PBS if none of these fire and its `extra_data` contains an execution-client signature (Geth, Nethermind, Besu, Erigon, reth): the proposer built it. If nothing fires at all the block is `unknown`, which the shared file also shows as `is_pbs = False`.

Signals 3 and 4 and the block range come from an Ethereum execution node (script 03). Earlier versions took them from Google BigQuery; on a sample of four ranges of 2,000 blocks (blocks 15,600,000, 19,000,000, 24,000,000 and 25,280,000 onward) the node values equal the BigQuery files for `extra_data`, fee recipient, timestamp, last-transaction index, sender, receiver, gas used and value (`value` was stored as a float in the BigQuery file, so it matches up to float rounding). A full rebuild from the node and its comparison with the BigQuery-based file have not been run yet.

## Sources

| Source | License | Role |
|---|---|---|
| [Xatu](https://ethpandaops.io) `mev_relay_proposer_payload_delivered`, from 2022-11-25 | CC BY 4.0 (attribution required) | Relay delivery signal |
| [Flashbots relay API](https://boost-relay.flashbots.net), from about 2022-09-29 | Public; no redistribution terms stated for query results | Relay delivery signal |
| [`dataalways/mevboost-data`](https://github.com/dataalways/mevboost-data) builder pubkey to name mapping | CC0 | Resolves a relay-registered key to a builder name |
| An Ethereum execution node (tested with Erigon) | Chain data | Block `extra_data`, fee recipient, last transaction |
| [relayscan.io](https://relayscan.io) daily counts | Used for the coverage check only; nothing is stored in the dataset | Independent coverage check |

The builder-name mapping is not append-only (its maintainers have corrected historical entries), so each run saves a dated snapshot of the mapping it used in `data/raw/builder_label_snapshots/`.

## How to rebuild

Needs Python 3.11 with `pip install -r requirements.txt`, and an Ethereum execution node reachable by JSON-RPC (address in `--rpc-url` or the environment variable `ERIGON_RPC_URL`, default `http://127.0.0.1:8545`). Run every command from this folder.

```
python scripts/run_all_pbs.py                    # everything, through the last complete month
python scripts/run_all_pbs.py --through 2026-10  # through the end of a given month
python scripts/run_all_pbs.py --status           # what exists now; runs nothing
```

The stages run in this order and the run stops at the first failure. Every stage can be stopped and restarted: finished downloads are skipped and the extraction continues where it ended. `--from-stage` and `--only` select stages.

| Stage | Script | What it does | Rough time (estimates) |
|---|---|---|---|
| xatu | `01_download_xatu.py` | Downloads the daily Xatu files, one request every second, and builds the relay table | about 30 minutes for the 1,400 days |
| flashbots | `02_augment_flashbots.py` | Fetches Flashbots delivered payloads (one request every second), adds the blocks Xatu lacks to the relay table, removes the Xatu-only table | several hours the first time (15,000 to 20,000 requests), then minutes |
| node | `03_extract_node_block_data.py` | Extracts `extra_data`, fee recipient and the last transaction of every block from the node | about 3 hours for the full range (about 1,000 blocks per second measured on 1,000 blocks) |
| relayscan | `04_download_relayscan.py` | Downloads the daily relayscan counts, one request every second | about 30 minutes the first time |
| build | `05_build_pbs_flag.py` | Combines the signals, writes `df_blocks_pbs.parquet` and the QC tables | minutes |

Downloads from public hosts are sequential and paced on purpose (`--delay` changes the gap); a host that answers 403 or 429 stops the script instead of being retried. Scripts print their progress to the screen and write no log files into this folder; redirect the output to a file elsewhere if you want one.

The QC tables (`data/processed/pbs_flag_qc_*.csv`) give blocks per classification source and month, and, once relayscan data exists, the monthly coverage against relayscan.io.

## Monthly update

```
python scripts/update_month.py --status     # state of the dataset and what would be added
python scripts/update_month.py --dry-run    # checks that need no download or write
python scripts/update_month.py              # add every complete month after the last block
```

A month is complete once it has ended (UTC): run on 15 January it adds everything up to 31 December. The update downloads the new Xatu days (and stops if Xatu does not have the last days yet; its newest days appear after about two days), the new Flashbots payloads, the new node blocks and relayscan days, then rebuilds the classification. It compares the new file with the old one block by block (and reports how many old blocks changed, which happens when a relay record arrives late) and replaces `df_blocks_pbs.parquet` only if the new file has unique sorted block numbers, no gaps and ends at the last block of the month.

The monthly update needs a finished build with node data first. It has been tested on small samples, not yet end to end on a real month.

## Folder layout

```
data/raw/xatu/                    daily Xatu files
data/raw/flashbots/               Flashbots delivered payloads (df_blocks_flashbots.parquet)
data/raw/node/                    block_inputs_<first>_<last>.parquet, 20,000 blocks each
data/raw/builder_label_snapshots/ dated copies of the builder-name mapping
data/raw/relayscan_daily.csv      relayscan daily counts
data/interim/                     relay table with Flashbots blocks (df_blocks_builder_augmented.parquet)
data/processed/                   df_blocks_pbs.parquet and the QC tables
scripts/                          the build and the updater; _common.py holds shared paths and the paced downloader
```

`data/` is not versioned. Set `PBS_DATA_DIR` to keep it somewhere else.

## Known limitations

- **Residual gap.** About 1.3% of the PBS blocks relayscan.io reports are missing from the dataset, concentrated in late 2022 blocks delivered through the BloXroute relay, whose historical API was taken down before it could be captured. There is no way to recover them. Blocks from 2022-09-15 to about 2022-09-28 (the Merge to the start of Flashbots API coverage) cannot be classified by relay data and rely on `extra_data` and the last-transaction signal.
- **`False` mixes two cases.** `is_pbs = False` covers both proposer-built blocks and `unknown` blocks. The distinction exists inside the build (`pbs_source`) and can be written with `05_build_pbs_flag.py --keep-full`.
- **Builder names come from three places** (the curated relay mapping, `extra_data`, a truncated key as a last resort), so a name can change if the upstream mapping is corrected; the dated snapshots make a run reproducible.
- **Xatu and Flashbots data are the servers' data as they are now.** A rebuild at another date can differ slightly from a published file.
- **The Flashbots API** has no stated redistribution terms for its query results; the raw Flashbots file is not part of any release.

## Attribution

Contains data from ethPandaOps Xatu (CC BY 4.0, https://ethpandaops.io) and the `dataalways/mevboost-data` builder mapping (CC0). Block data comes from an Ethereum execution node.
