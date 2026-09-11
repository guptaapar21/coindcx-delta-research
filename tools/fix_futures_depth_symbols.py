#!/usr/bin/env python3
"""Repair futures-depth feature joins when CoinDCX emits compact symbols.

The futures orderbook stream can report symbols such as SOLUSDT while the
research universe uses B-SOL_USDT. research_batch already normalizes most
streams, but the historical implementation only mapped BTC/ETH. This small
post-processing guard canonicalizes the six configured futures symbols, fills
the matching B-* row at the same second, and removes duplicate compact-symbol
rows before validation/compaction.
"""
from __future__ import annotations

import argparse
import csv
import gzip
from pathlib import Path

ALIASES = {
    "BTCUSDT": "B-BTC_USDT",
    "B-BTCUSDT": "B-BTC_USDT",
    "ETHUSDT": "B-ETH_USDT",
    "B-ETHUSDT": "B-ETH_USDT",
    "SOLUSDT": "B-SOL_USDT",
    "B-SOLUSDT": "B-SOL_USDT",
    "SUIUSDT": "B-SUI_USDT",
    "B-SUIUSDT": "B-SUI_USDT",
    "XRPUSDT": "B-XRP_USDT",
    "B-XRPUSDT": "B-XRP_USDT",
    "DOGEUSDT": "B-DOGE_USDT",
    "B-DOGEUSDT": "B-DOGE_USDT",
}

FUTURES_BOOK_FIELDS = [
    "futures_best_bid", "futures_best_ask", "futures_mid_price",
    "futures_spread_abs", "futures_spread_bps", "futures_microprice",
    "futures_book_features_status", "futures_book_valid",
    "futures_depth_snapshot_events", "futures_depth_version",
]
for n in (1, 5, 10, 20):
    FUTURES_BOOK_FIELDS.extend([
        f"futures_book_bid_qty_{n}", f"futures_book_ask_qty_{n}",
        f"futures_book_imbalance_{n}",
        f"futures_book_bid_qty_{n}_change", f"futures_book_ask_qty_{n}_change",
        f"futures_book_imbalance_{n}_change",
    ])
FUTURES_BOOK_FIELDS.extend(["futures_spread_bps_change", "futures_microprice_return"])


def canonical(value: str) -> str:
    s = str(value).upper().strip()
    return ALIASES.get(s, s)


def repair(path: Path) -> tuple[int, int]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    key_to_row = {(r.get("symbol", ""), r.get("epoch_second", "")): r for r in rows}
    compact_rows = []
    repaired = 0
    removed = 0

    for row in rows:
        symbol = row.get("symbol", "")
        canonical_symbol = canonical(symbol)
        if canonical_symbol == symbol:
            compact_rows.append(row)
            continue

        target_key = (canonical_symbol, row.get("epoch_second", ""))
        target = key_to_row.get(target_key)
        if target is not None:
            for field in FUTURES_BOOK_FIELDS:
                value = row.get(field)
                if value not in (None, "") and target.get(field, "") in (None, ""):
                    target[field] = value
            repaired += 1
        removed += 1

    if removed:
        fields = list(rows[0]) if rows else []
        with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(compact_rows)

    print(f"futures_depth_symbol_repair: repaired_rows={repaired} removed_duplicate_rows={removed}")
    return repaired, removed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()
    path = Path(args.batch) / "features_1s.csv.gz"
    if not path.exists():
        raise SystemExit(f"missing feature file: {path}")
    repair(path)


if __name__ == "__main__":
    main()
