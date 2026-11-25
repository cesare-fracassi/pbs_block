# PBS classification data

Per-block classification of whether an Ethereum block was built through the
MEV-Boost / proposer-builder separation (PBS) auction, or by the proposer's
own local client ("vanilla", non-PBS). This is chain-wide data: every
post-Merge block gets a determination here, independent of any specific
project's transactions.

## How this dataset was built

Four independent signals are checked for each block, in priority order, and
the first one that fires decides the classification:

1. **Relay data (Xatu).** MEV-Boost relays publish a record every time they
   deliver a winning block to a validator. [Xatu](https://ethpandaops.io),
   a public Ethereum research dataset, aggregates these records across many
   relays. If a block shows up here, it was built via the PBS auction.

2. **Relay data (Flashbots, direct).** Xatu's own coverage of blocks
   delivered specifically through the Flashbots relay has real gaps,
   especially in the months right after the Merge (Flashbots held roughly
   75% of the market in late 2022 but is barely visible in Xatu's data for
   that period). Querying Flashbots' own relay API directly recovers blocks
   Xatu alone misses, both historically and in general ongoing use.

3. **The block's own `extra_data` field.** Every block header carries a
   small, free-form byte field. Builders commonly self-identify here (e.g.
   literally writing "beaverbuild.org" into the block), which recovers
   blocks that were PBS-built but that neither relay source captured.

4. **The block's last transaction.** As a final fallback, if the block's
   final transaction is a plain ETH payment to the block's own fee
   recipient (the validator), that's a common way a builder pays a
   validator when no other signal is available.

A block is classified non-PBS if none of the above fire and its `extra_data`
instead contains a known execution-client signature (Geth, Nethermind,
Besu, Erigon, reth), meaning the proposer built the block itself. If nothing
fires at all, the block is left unclassified.

**Coverage**: the pipeline covers September 29, 2022 (the earliest confirmed
Flashbots relay data) onward; September 15-28, 2022 (the Merge itself
through the start of Flashbots coverage) cannot be classified with this
method. A residual gap of roughly 1.3% remains against relayscan.io's own
independent ground truth, concentrated in late-2022 blocks delivered via the
BloXroute relay, whose own historical API was taken down before this or any
other pipeline could capture that period - there is no way to recover it.

**Keeping this data current**: signals 1 and 2 above (Xatu, Flashbots) are
free, public APIs and are refreshed regularly. Signals 3 and 4 (`extra_data`,
last-transaction) come from a Google BigQuery query, which has a small cost
and is refreshed only periodically, not continuously. In practice this means
the most recent stretch of blocks at any given time relies on relay data
alone, with a somewhat lower resolution rate than the historical average
until the BigQuery-derived signals are next refreshed - a deliberate
trade-off to keep routine updates free. **As of the last update (2026-10-29)**:
10,648,035 total blocks, through block 26,300,997, 90.2% classified
`is_pbs = True`.

## Data sources and licenses

| Source | License | Role |
|---|---|---|
| [Xatu](https://ethpandaops.io) (`mev_relay_proposer_payload_delivered`) | CC BY 4.0 (attribution required) | Primary relay-delivery signal |
| [Flashbots relay API](https://boost-relay.flashbots.net) | Public, no stated redistribution terms for live query results | Supplementary relay-delivery signal |
| [`dataalways/mevboost-data`](https://github.com/dataalways/mevboost-data) (builder pubkey-to-name mapping) | CC0 (public domain) | Resolves a builder's relay-registered key to a human-readable name |
| Google BigQuery, `bigquery-public-data.crypto_ethereum` | Public dataset | Block `extra_data` field and last-transaction data |
| [relayscan.io](https://relayscan.io) daily stats | Used for validation only, not stored in the output | Independent coverage check |

The builder-name mapping is not append-only - its maintainers have at least
once gone back and corrected historical entries, not just added new ones -
so a dated snapshot of the exact mapping version used is saved alongside
each update, making every run's builder names traceable to the mapping that
produced them.

## Data dictionary: `data/processed/df_blocks_pbs.parquet`

One row per block.

| Column | Type | Coverage | Meaning |
|---|---|---|---|
| `block_number` | Int64 | 100% | Primary key |
| `is_pbs` | Bool | 100% | `True` if the block was built via the PBS/MEV-Boost auction, `False` if the proposer built it itself |
| `builder_address` | Utf8 | 82.1% | The builder's relay-registered public key. Only available for blocks resolved via relay data (signals 1-2 above); a block resolved only through `extra_data` has no registered key to show. A single builder commonly operates many different keys at once (e.g. one major builder uses 70+), so this is not a 1:1 identifier for "which company" - use `builder_name` for that. |
| `builder_name` | Utf8 | 89.6% | The builder's normalized, human-readable name (e.g. `beaverbuild`, `titan`, `flashbots`). Known naming variants and mergers for the same real-world entity are consolidated into one name. |

`block_timestamp` and `validator_address` (the block's own fee recipient -
the validator's payout address, not the builder's) are deliberately not
included: both come only from the BigQuery-derived signals above, which are
not refreshed on every update, so including them would mean both fields
silently went blank for every block added since the last BigQuery refresh.
