# AdvisorX — CoinDCX Order-Flow Research Collector v3

This package is the production-oriented next stage for the CoinDCX Delta/order-flow research system.

## Scope locked for this version

- Markets: **BTC/USDT + ETH/USDT controls**, plus **SOL/USDT, SUI/USDT, XRP/USDT and DOGE/USDT exploratory markets**. The universe is intentionally small and frozen in `config.json` so comparisons remain auditable rather than drifting every batch.
- Live Spot websocket streams: raw `new-trade`, `depth-update`, `depth-snapshot`, `price-change`.
- Live Futures websocket streams: `new-trade`, `price-change`, `currentPrices@futures#update`, plus per-instrument `depth-snapshot` orderbooks.
- Spot and Futures use **separate Socket.IO connections** because CoinDCX documents separate Spot and Futures websocket endpoints.
- No live 15m/1h/1d candle streams.
- Our own 1m/3m research bars are derived from the live raw trades/features. The 3m layer is built from three complete UTC-aligned 1m buckets; incomplete boundary buckets are excluded.
- Short order-flow windows: 5s, 15s, 30s, 60s, 180s.
- Microstructure forward labels: +5s, +15s, +30s, +60s, +180s.
- Longer response labels: +5m, +10m, +15m, +30m; these are labels only, not additional rolling-flow windows.
- MFE/MAE hooks can be added after the first stable feature pass.
- Spot depth-derived imbalance/microprice features use the empirically validated absolute-update + version-gap guard. Futures orderbooks are snapshot-only in the research layer and are not reconstructed as incremental books.
- Futures Open Interest is **not** synthesized or inferred from public fields.

## Why depth is treated conservatively

CoinDCX currently documents both `depth-snapshot` and `depth-update`. The collector preserves both raw payload types. A version number alone is not accepted as proof that an update is incremental. `src/depth_validator.py` reports version progression, repeated levels, payload sizes and other evidence without prematurely certifying a persistent book.

## Continuous collection architecture

The collector runs as independent batches. The production default is 230 minutes (3h50m), under the GitHub-hosted job execution ceiling. A finished batch triggers the next batch with `repository_dispatch`. A scheduled watchdog (`watchdog.yml`) checks for a stale collector and starts recovery if necessary. Concurrency prevents two collector jobs from running simultaneously.

This intentionally avoids overlapping batches as the normal mode. Overlap is a fallback idea, not the default, because depth deduplication is materially harder than trade deduplication.

## Storage architecture

Raw batches are uploaded as Actions artifacts for a short active window. `src/storage_manager.py` checks the size of raw artifacts and also checks raw-artifact age. When the configured threshold is reached (or the oldest raw batch becomes old enough), selected unprotected raw artifacts are bundled into one or more compressed release assets and **only then** deleted from Actions artifact storage.

The release assets provide a durable, downloadable GitHub archive. GitHub cannot directly write that archive into your phone/PC filesystem. `tools/sync_archives.py` is provided for a machine you control; it can periodically download new release archives automatically.

The initial 48-hour archive-age / 70% trigger settings are deliberately configurable. They are not a claim that 7 days is the correct raw-retention period. Actual compressed sizes should be observed during the first production batches.

## Long-lived data

`data/research/compact_1m/YYYY-MM.csv` is the compact research history. Month partitions are intentionally kept as plain text so Git can delta-compress incremental updates efficiently. It is intentionally much smaller than raw websocket data and is retained for 90 days by default. Strategy summaries, experiments and validated candidates can be kept indefinitely.

`data/state/protected_batches.json` is the manual protection list. A protected batch is excluded from automatic raw-artifact archival/deletion.

## Historical baseline

`tools/historical_backfill.py` is the validated backward-paginated 1-minute candle downloader. It downloads 1–3 days for BTC/ETH, derives 3-minute bars and records the limitation that public spot trade history is not a 3-day tick archive.

## First-time setup on GitHub

1. Copy this package into the root of `guptaapar21/coindcx-delta-research`.
2. Commit `.github/workflows`, `src`, `tools`, `config.json`, `requirements.txt`, `data/state/protected_batches.json`, and the README.
3. In **Actions**, run `AdvisorX CoinDCX micro transport diagnostic` once. It must pass. The diagnostic requires trades, prices, `depth-update`, and `depth-snapshot`; CoinDCX live candlesticks are optional because the production pipeline builds its own 1m/3m bars from trades.
4. Run `AdvisorX historical baseline backfill` for 3 days once.
5. Start `AdvisorX CoinDCX continuous collector` manually with the default 230-minute duration.
6. After the first real batch, inspect the uploaded raw artifact, `depth_validation.json`, `data_quality.json`, `research_summary.json`, and `storage_report.json`.

## Local archive synchronization

On a PC/server:

```bash
python tools/sync_archives.py --repo guptaapar21/coindcx-delta-research --dest ./advisorx_archives
```

Schedule that command using the operating system scheduler. Because release assets are public downloads, the script does not require a token for a public repository.

## Cross-market research layer

Each exploratory market is collected on both Spot and Futures. The compact research layer keeps Spot Delta, Futures Delta, Spot orderbook state and Futures orderbook state separate so we can test lead/lag and divergence without changing the original definitions. It also records Futures orderbook changes between snapshots (top-level quantity/imbalance changes, spread change and microprice return).

The first priority is evidence, not a pre-selected signal: Futures Delta -> future price, Futures vs Spot Delta divergence, Futures/Spot price basis, Delta + orderbook reaction, and volatility-normalized effects. BTC/ETH remain controls while SOL/SUI/XRP/DOGE provide higher-volatility test cases.

## Research horizon layers

The collector and core order-flow windows remain unchanged. The research layer now measures how long any signal response persists by adding forward-return labels at +5m, +10m, +15m and +30m. The long-horizon labels are recalculated from the merged compact history, which preserves labels that cross independent 230-minute collector-batch boundaries. The intended workflow is to use 5s–3m as microstructure diagnostics and the 5m–30m labels as the trading-horizon discovery layer. No trading strategy is implied by these labels.

## Live 3-minute layer

`src/research_batch.py` writes `features_3m.csv.gz` from the live-derived 1m layer. The 3m layer uses UTC-aligned 180-second buckets and only emits a bar when all three constituent 1m buckets are present. This keeps the live definition consistent with the validated historical 3m construction.

## Important research rule

A small number of apparently successful signals is not a strategy. All candidate signals must show sample size, baseline comparison, forward returns, and eventually out-of-sample/paper-test performance before being considered viable.
