import csv
import gzip
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tools"))
import fix_futures_depth_symbols


def test_compact_future_symbol_is_merged_into_canonical_row(tmp_path):
    path = tmp_path / "features_1s.csv.gz"
    fields = [
        "symbol", "epoch_second", "futures_book_valid",
        "futures_best_bid", "futures_book_imbalance_1",
    ]
    rows = [
        {"symbol": "B-SOL_USDT", "epoch_second": "100", "futures_book_valid": "", "futures_best_bid": "", "futures_book_imbalance_1": ""},
        {"symbol": "SOLUSDT", "epoch_second": "100", "futures_book_valid": "True", "futures_best_bid": "99.9", "futures_book_imbalance_1": "0.2"},
        {"symbol": "B-SOL_USDT", "epoch_second": "101", "futures_book_valid": "", "futures_best_bid": "", "futures_book_imbalance_1": ""},
    ]
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    repaired, removed = fix_futures_depth_symbols.repair(path)
    assert repaired == 1
    assert removed == 1

    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        out = list(csv.DictReader(fh))
    assert [r["symbol"] for r in out] == ["B-SOL_USDT", "B-SOL_USDT"]
    assert out[0]["futures_book_valid"] == "True"
    assert out[0]["futures_best_bid"] == "99.9"
    assert out[0]["futures_book_imbalance_1"] == "0.2"
