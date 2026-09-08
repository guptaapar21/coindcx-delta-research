# Data completeness check before collection

This is the checklist for the first research dataset. The purpose is to avoid stopping after the first analysis and then discovering that a required input was never collected.

## Captured now

**Primary signal data**

- every public trade event received from CoinDCX for each configured pair
- trade price, quantity, exchange timestamp and maker/taker flag
- normalized aggressor side and buy/sell notional
- trade IDs when supplied
- local receive timestamp and exchange-to-local latency estimate

**Liquidity data**

- complete order-book snapshot payload received from the documented websocket channel
- depth 50
- version (`vs`), exchange timestamp, best bid/ask, spread, mid, microprice
- bid/ask depth and proportional imbalance at 1/5/10/20/50 levels
- changes between successive snapshots, explicitly treated as net depth changes rather than order-level add/cancel events

**Price/context data**

- pair price-change events
- 1m, 15m, 1h and 1d candle streams
- recent candle history at startup
- startup REST order-book snapshot
- market metadata / precision / minimum trade constraints

**Integrity data**

- connection lifecycle
- reconnect attempts and errors
- event counts
- duplicate trade IDs where available
- non-monotonic exchange timestamp flags
- local receive timestamps
- run manifest and software/runtime information

## Known limitations that are already accounted for

CoinDCX documents the public spot order-book websocket as snapshot updates. Therefore this dataset cannot reconstruct exact order-level queue dynamics, individual order cancellations, queue position, or trader identity. That is a property of the available feed, not a missing collector field.

A second limitation is venue scope: CoinDCX order flow describes CoinDCX's market, not the full global crypto market. External exchange order flow could be useful later as an additional predictive feature, but it should be treated as a separate multi-venue research extension rather than silently mixing venues into the primary CoinDCX signal.

## Why we are not collecting API-key/private data

The first phase is designed to be reproducible and credential-free. Private account orders, fills, balances and fees are not market data and would introduce account-specific information. Execution costs will instead be modeled using documented fee assumptions and measured bid/ask spread until live execution testing is appropriate.

## Strategy-specific forward labels are not stored during collection

We deliberately do not compute future returns in the live collector. Doing so risks leakage and makes the raw dataset harder to audit. Once collection is complete, a separate offline feature/label pipeline will align each event with future 1s/5s/15s/30s/60s returns and MFE/MAE using event-time data only.

## Conclusion

For the first CoinDCX spot Delta study, no additional live field should be necessary before the first serious analysis. The remaining limitations are known feed limitations or deliberate research-stage separation, not accidental omissions.
