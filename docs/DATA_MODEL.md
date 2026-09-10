# Data model and retention

## Raw event record

Each raw websocket record contains:

- local receive timestamp in UTC
- local receive time in milliseconds/nanoseconds
- event name
- exchange timestamp (`T` or `ts`)
- pair/symbol where available
- original raw payload

The original payload is retained so later feature logic can be changed without recollecting data.

## Derived features

Trade-side interpretation follows the CoinDCX maker flag already validated in the research work:

- `m=false` → buyer is aggressor → aggressive BUY
- `m=true` → buyer is maker → aggressive SELL

Delta is:

`aggressive_buy_qty - aggressive_sell_qty`

and is also represented in notional terms.

## Spot depth status

Spot depth-derived features are now populated conservatively from the validated
absolute-update interpretation with version-gap guarding. The production raw
streams remain preserved in full so the validation can be replayed. The derived
Spot fields include best bid/ask, mid, spread, microprice, multi-level bid/ask
quantities and imbalance at levels 1/5/10/20, plus book-valid status.

The collector also preserves both `depth-update` and `depth-snapshot` payloads;
reconstruction is re-anchored at snapshots and invalidated across version
discontinuities rather than silently carrying a stale book forward.

## 1-second research layer

Contains rolling 5/15/30/60/180-second trade-flow measurements plus forward-price labels. This remains a rolling/archived research layer.

## 1-minute compact layer

Contains compact 1-minute price/flow features suitable for longer retention and repository use. The canonical deduplication key is `(symbol, minute_epoch)`.

## 3-minute live research layer

The live 3-minute layer is derived from the live 1-minute layer, not from CoinDCX's higher-timeframe candle feed. UTC-aligned 180-second buckets are used, and a 3-minute bar is emitted only when all three constituent 1-minute buckets are present. The canonical key is `(symbol, three_minute_epoch)`.


## Forward-response horizon labels

The 1-second research layer retains the five rolling order-flow windows (5/15/30/60/180s). It additionally records forward price-return labels at 300s, 600s, 900s and 1800s so the same microstructure observation can be evaluated over longer response horizons.

The canonical 1-minute and 3-minute compact layers add longer forward close-to-close labels after merge. Recalculation occurs over the merged compact history rather than an individual collector batch, so labels are not systematically lost at 230-minute batch boundaries. These longer labels are research diagnostics, not trading rules.


## Futures executed-flow and orderbook layers

Production batches capture public Futures data for the configured universe (BTC/ETH controls plus SOL/SUI/XRP/DOGE exploratory markets). Futures trades are stored in `futures_trades.jsonl.gz`; futures price changes in `futures_price_change.jsonl.gz`; the configured slice of `currentPrices@futures@rt` in `futures_current_prices.jsonl.gz`; and per-instrument Futures orderbook snapshots in `futures_depth_snapshot.jsonl.gz`.

Futures fields remain separate from Spot Delta and Spot book state. The 1s/1m/3m research layers include Futures trade count, aggressive buy/sell quantity, Futures Delta, Futures Delta ratio, Futures-vs-Spot Delta divergence, and Futures orderbook state/change features across the existing 5/15/30/60/180-second windows. Futures orderbooks are treated as full snapshots in the research layer rather than reconstructed incrementally. No Open Interest is synthesized.
