# Implementation notes

## Current GitHub storage model

GitHub Actions artifact storage is the constrained short-lived layer. Standard GitHub-hosted runners in public repositories are free and unlimited, while the GitHub Free plan lists 500 MB of included artifact storage. The collector therefore rotates raw artifacts into release assets before the artifact budget is exhausted. Storage housekeeping is a hard-fail operation: archival failures must not be swallowed, because a green workflow with no archival would silently accumulate artifacts.

## Why release assets are used for promoted archives

GitHub documents up to 1000 assets per release and a per-file limit of 2 GiB, with no total-size limit on a release. This makes a release asset a better long-term archive destination than keeping every raw batch as an Actions artifact. The archive manager splits a promotion into multiple assets if required.

## Local backup limitation

A GitHub-hosted workflow cannot directly place an archive into a user's local phone/PC filesystem. The included `tools/sync_archives.py` script bridges that gap by running on a machine the user controls.

## Scheduler reliability

The self-chain uses `repository_dispatch`, which GitHub documents as an event that creates a new workflow run even when the dispatch is made using `GITHUB_TOKEN`. The watchdog exists because any cloud workflow can fail or become stale.


## Longer-horizon research labels

The current flow-window definitions remain 5/15/30/60/180 seconds. Longer horizons are represented as forward-response labels at 5/10/15/30 minutes rather than expanding the rolling-flow feature set. The compact merge step recalculates those labels from the full accumulated compact history to preserve future bars that fall in the next collector batch.


## Futures capture design (additive)

Spot and Futures use separate Socket.IO connections because CoinDCX documents
different Futures and Spot stream endpoints. The main Futures socket captures
`new-trade`, `price-change` and the configured slice of `currentPrices@futures@rt`.
Futures trade/price payloads are accepted only when the payload product marker
identifies Futures (`pr=f`/`futures`).

Futures orderbooks use the documented `{instrument}@orderbook@50-futures`
channel and `depth-snapshot` event. The collector uses one dedicated Futures
orderbook Socket.IO connection per configured instrument so each snapshot has
a deterministic symbol attribution even when a multiplexed payload does not
carry a reliable symbol field. The raw snapshots are preserved in
`futures_depth_snapshot.jsonl.gz`; research features are computed directly from
the full snapshots rather than treating them as incremental deltas.

The configured universe is frozen in `config.json`: BTC/ETH are controls and
SOL/SUI/XRP/DOGE are exploratory higher-volatility markets. Futures capture is
intentionally an additive research input. The production Spot definitions are
unchanged, and absence of public Open Interest is not filled with a proxy.
