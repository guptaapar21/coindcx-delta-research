# CoinDCX Delta / Order-Flow Research Collector

This repository is a **research-data collection system** for CoinDCX public market data. It places **no orders** and requires no API credentials for the public spot market-data channels used here.

The goal is to collect the complete set of market observations we should need before deciding whether a Delta / order-flow strategy has a statistically useful edge.

## What is collected from the start

For every configured pair:

1. **Tick-by-tick trades** — price, quantity, exchange timestamp, maker/taker flag, symbol, raw event, local receive timestamp.
2. **50-level order-book snapshots** — bids, asks, exchange timestamp, version, best bid/ask, spread, depth at 1/5/10/20/50 levels, imbalance, microprice and raw event.
3. **Price-change events** — a second LTP stream used mainly to validate event timing and last-price changes.
4. **1m / 15m / 1h / 1d candle streams** — low-bandwidth context for regime, volatility and longer-horizon filters.
5. **REST bootstrap market metadata** — market details, initial 50-level book, and recent candles before live collection starts.
6. **Data-quality events** — connect/disconnect/reconnect/error, local receive times, event counters, and run manifest.

This prevents the common failure mode of collecting only trades, discovering later that we need order-book state or regime context, and having to start the clock again.

## Important CoinDCX feed facts

- `m=true` on trade events means the buyer is the market maker, so the seller was the aggressive/taker side; for delta research that is classified as **aggressive SELL**.
- `m=false` means the buyer was the aggressive/taker side; classified as **aggressive BUY**.
- CoinDCX documents spot order-book websocket updates as **snapshot updates**. The collector therefore stores the full snapshot and calculates depth features; it does not claim to have a true order-add/cancel/order-delete event stream.
- CoinDCX documents up to 50 levels/orders for the order-book websocket, so the collector uses depth 50 by default.

## Recommended first run

Use Python 3.10+.

```bash
python -m venv .venv
# Linux/macOS:
source .venv/bin/activate
# Windows PowerShell:
# .venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
python scripts/run_collector.py --config config/config.json --minutes 60
```

For a continuous run:

```bash
python scripts/run_collector.py --config config/config.json
```

Stop with Ctrl+C. The collector writes a run folder under `data/raw/<UTC-run-id>/` and a manifest under `data/manifests/`.

## Suggested collection horizon

Do not judge the strategy from a 10-minute sample. Collect at least **several days**, ideally across quiet, trending, volatile and reversal conditions. For the first pass, keep the exact same collector and settings running so the dataset is homogeneous.

## Folder structure

```text
coindcx_delta_research/
├── config/
│   └── config.json
├── docs/
│   └── data_spec.md
├── scripts/
│   ├── run_collector.py
│   ├── analyze_runs.py
│   └── validate_run.py
├── src/
│   ├── __init__.py
│   ├── collector.py
│   ├── coindcx.py
│   └── features.py
├── data/
│   ├── raw/
│   │   └── <run-id>/
│   ├── processed/
│   └── manifests/
├── tests/
│   └── test_features.py
├── .gitignore
├── requirements.txt
└── README.md
```

## Output files inside a run

For each pair, expect files such as:

- `<pair>_trades.jsonl`
- `<pair>_depth.jsonl`
- `<pair>_prices.jsonl`
- `<pair>_candles_1m.jsonl`
- `<pair>_candles_15m.jsonl`
- `<pair>_candles_1h.jsonl`
- `<pair>_candles_1d.jsonl`
- `<pair>_bootstrap.jsonl`
- `data_quality.jsonl`
- `run_manifest.json`

Raw fields are preserved alongside normalized fields. Do not delete the raw files; future research may need the original feed representation.

## Run from GitHub Actions

The repository includes a manual GitHub Actions collector workflow. In GitHub, open **Actions → Collect CoinDCX research data → Run workflow**, choose a duration, and the collected files are uploaded as a workflow artifact. This is useful for initial collection/testing. For multi-day continuous collection, a VPS or another persistent runner is preferable because GitHub Actions jobs are time-limited.

## What we will analyze later

The collector deliberately does **not** decide that positive Delta means buy or negative Delta means sell. The research stage will measure:

- aggressive buy/sell volume
- volume and notional Delta
- Delta ratio
- cumulative Delta
- trade intensity
- order-book imbalance at several depths
- spread and microprice
- liquidity additions/removals between snapshots
- Delta-price divergence
- absorption / failed movement after aggressive flow
- breakout confirmation/failure
- forward returns at 1s/5s/15s/30s/60s
- MFE/MAE
- transaction costs, spread, slippage and measured latency

A strategy is only considered promising if the effect survives out-of-sample testing and realistic costs.
