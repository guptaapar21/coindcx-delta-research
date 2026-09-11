#!/usr/bin/env python3
"""Repair futures-depth feature joins when CoinDCX emits compact symbols.

CoinDCX futures orderbook snapshots can report symbols such as SOLUSDT while
our research universe uses B-SOL_USDT. The collector is correctly capturing
the raw snapshots, but the feature join historically only normalized BTC/ETH.
This guard canonicalizes the six configured symbols across every generated
research layer, fills the matching canonical row at the same time bucket, and
removes duplicate compact-symbol rows before depth validation/compaction.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
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
    "futures_mid_price", "futures_spread_abs", "futures_spread_bps",
    "futures_microprice", "futures_book_features_status", "futures_book_valid",
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
FUTURES_BOOK_FIELDS = list(dict.fromkeys(FUTURES_BOOK_FIELDS))


def canonical(value: str) -> str:
    s = str(value).upper().strip()
    return ALIASES.get(s, s)


def repair_csv(path: Path, epoch_field: str) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))
    if not rows:
        return 0, 0

    key_to_row = {(r.get("symbol", ""), r.get(epoch_field, "")): r for r in rows}
    compact_rows = []
    repaired = 0
    removed = 0

    for row in rows:
        symbol = row.get("symbol", "")
        canonical_symbol = canonical(symbol)
        if canonical_symbol == symbol:
            compact_rows.append(row)
            continue

        target = key_to_row.get((canonical_symbol, row.get(epoch_field, "")))
        if target is not None:
            for field in FUTURES_BOOK_FIELDS:
                value = row.get(field)
                if value not in (None, "") and target.get(field, "") in (None, ""):
                    target[field] = value
            repaired += 1
        removed += 1

    fields = list(rows[0])
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(compact_rows)
    return repaired, removed


def repair_summary(path: Path) -> None:
    if not path.exists():
        return
    data = json.loads(path.read_text(encoding="utf-8"))
    symbols = data.get("symbols")
    if isinstance(symbols, list):
        data["symbols"] = sorted({canonical(x) for x in symbols})
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")


def repair(batch: Path) -> tuple[int, int]:
    repaired = removed = 0
    for filename, epoch_field in (
        ("features_1s.csv.gz", "epoch_second"),
        ("features_1m.csv.gz", "epoch_minute"),
        ("features_3m.csv.gz", "epoch_3m"),
    ):
        r, d = repair_csv(batch / filename, epoch_field)
        repaired += r
        removed += d
    repair_summary(batch / "research_summary.json")
    print(f"futures_depth_symbol_repair: repaired_rows={repaired} removed_duplicate_rows={removed}")
    return repaired, removed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()
    batch = Path(args.batch)
    if not (batch / "features_1s.csv.gz").exists():
        raise SystemExit(f"missing feature file: {batch / 'features_1s.csv.gz'}")
    repair(batch)


if __name__ == "__main__":
    main()
