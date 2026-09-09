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

## Depth status

The following are placeholders until depth semantics are certified:

- order-book imbalance
- microprice
- multi-level pressure
- persistent reconstructed book state

The raw depth stream is still captured in full.

## 1-second research layer

Contains rolling 5/15/30/60/180-second trade-flow measurements plus forward-price labels. This remains a rolling/archived research layer.

## 1-minute compact layer

Contains compact 1-minute price/flow features suitable for longer retention and repository use. The canonical deduplication key is `(symbol, minute_epoch)`.

## 3-minute live research layer

The live 3-minute layer is derived from the live 1-minute layer, not from CoinDCX's higher-timeframe candle feed. UTC-aligned 180-second buckets are used, and a 3-minute bar is emitted only when all three constituent 1-minute buckets are present. The canonical key is `(symbol, three_minute_epoch)`.
