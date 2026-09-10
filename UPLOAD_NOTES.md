# AdvisorX Futures-flow integration

Replace these existing production files in the repository:

- `src/collector.py`
- `src/research_batch.py`
- `config.json`

What changes:
- Existing BTC/ETH Spot capture remains unchanged.
- Public CoinDCX Futures BTC/ETH trade and price-change events are captured separately.
- The target BTC/ETH slice of the Futures current-prices stream is captured; the full multi-asset payload is not archived.
- Futures trades are added to the existing 1s/1m/3m research layers as separate Futures Delta/flow fields.
- Futures-vs-Spot Delta divergence is recorded for 5/15/30/60/180s windows.
- No Open Interest is synthesized.
- No production signal/trading rule is changed.
