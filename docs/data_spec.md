# Data specification

## 1. Trades

Raw CoinDCX trade fields are retained. Normalized fields include:

- `exchange_ts_ms`
- `recv_ts_ms`
- `price`
- `quantity`
- `notional`
- `maker_buyer`
- `aggressor_side`
- `event_id` when supplied by the feed

Derived per-trade signs:

- aggressive BUY: `m=false`
- aggressive SELL: `m=true`

For each pair and run we maintain running totals but do not use those totals as the final research feature. They are diagnostic. Final analysis should aggregate from the raw event stream into event-time windows.

## 2. Order book

The complete snapshot received is stored. Normalized features include:

- exchange version `vs`
- exchange timestamp `ts`
- best bid / ask
- spread
- mid price
- microprice
- bid/ask quantity at top 1, 5, 10, 20 and 50 levels
- proportional imbalance `(bid-ask)/(bid+ask)` at the same depths

Because CoinDCX documents websocket order-book updates as snapshots rather than order-by-order deltas, changes between snapshots are interpreted as **net depth change**, not as individual adds/cancels.

## 3. Candles

Capture 1m, 15m, 1h and 1d live updates. Also bootstrap recent history with the REST candles endpoint at the start of a run.

These are context, not the primary signal source.

## 4. Price channel

The pair-specific price-change channel is captured independently of trades. This lets us compare the price event timestamp against trade events and helps detect whether any downstream logic accidentally relies on stale data.

## 5. Bootstrap metadata

At the start of each run, record:

- market details
- initial 50-level order book
- recent candle history
- collector config
- collector start time
- host/runtime information

## 6. Data quality

Record:

- connection attempts
- successful connects
- disconnects
- exceptions
- reconnect timing
- handler event counts
- first/last exchange timestamp per stream
- local receive timestamps
- duplicate event IDs where supplied
- non-monotonic timestamps where detected

This lets us reject bad periods before building a backtest.

## 7. What is intentionally not claimed

We do not claim to reconstruct a complete order-level message book because the documented CoinDCX websocket book feed is snapshot-based. A future research question that specifically requires individual order IDs, cancellations, queue position or matching-engine messages would require a different data source.
