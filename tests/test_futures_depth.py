from pathlib import Path
import gzip
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import research_batch


def test_futures_depth_snapshot_is_mapped_to_symbol_and_changes(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    rows = [
        {"raw": {"data": {"pr": "futures", "s": "B-SUI_USDT", "ts": 1000,
                             "vs": 1, "bids": {"1": "10", "0.9": "5"},
                             "asks": {"1.1": "8", "1.2": "4"}}}},
        {"raw": {"data": {"pr": "futures", "s": "B-SUI_USDT", "ts": 2000,
                             "vs": 2, "bids": {"1": "14", "0.9": "5"},
                             "asks": {"1.1": "6", "1.2": "4"}}}},
    ]
    with gzip.open(batch / 'futures_depth_snapshot.jsonl.gz', 'wt', encoding='utf-8') as fh:
        for r in rows:
            fh.write(json.dumps(r) + '\n')
    out = research_batch.build_seconds(batch)
    assert [r['symbol'] for r in out] == ['B-SUI_USDT', 'B-SUI_USDT']
    second = out[1]
    assert second['futures_book_bid_qty_1'] == 14.0
    assert second['futures_book_ask_qty_1'] == 6.0
    assert second['futures_book_imbalance_1_change'] > 0
    assert second['futures_depth_snapshot_events'] == 1
    assert second['futures_book_features_status'] == 'FUTURES_DEPTH_SNAPSHOT'
