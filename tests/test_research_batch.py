from pathlib import Path
import gzip
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import research_batch


def write_trade_file(batch: Path, rows: list[dict]):
    with gzip.open(batch / 'trades.jsonl.gz', 'wt', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r) + '\n')


def test_trade_sign_and_delta(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    rows = [
        {"raw": {"data": {"s": "B-BTC_USDT", "T": 1000, "p": "100", "q": "2", "m": 0}}},
        {"raw": {"data": {"s": "B-BTC_USDT", "T": 2000, "p": "101", "q": "1", "m": 1}}},
    ]
    write_trade_file(batch, rows)
    out = research_batch.build_seconds(batch)
    assert len(out) == 2
    assert sum(x['delta_qty'] for x in out) == 1
    assert out[0]['aggressive_buy_qty'] == 2
    assert out[1]['aggressive_sell_qty'] == 1


def minute_row(symbol, epoch, close, delta=1.0):
    return {
        'symbol': symbol, 'minute_epoch': epoch, 'minute_utc': research_batch.iso_sec(epoch),
        'open': close - 0.5, 'high': close + 1, 'low': close - 1, 'close': close,
        'trade_count': 2, 'aggressive_buy_qty': delta + 1, 'aggressive_sell_qty': 1,
        'delta_qty': delta, 'delta_notional': delta * close, 'total_qty': delta + 2,
        'delta_ratio': delta / (delta + 2), 'depth_update_events': 0,
        'depth_snapshot_events': 0, 'book_features_status': 'UNCERTIFIED',
        'source_schema': 'research_batch_v2'
    }


def test_3m_complete_bucket_only():
    rows = [minute_row('B-BTC_USDT', t, 100 + i, delta=1.0)
            for i, t in enumerate((0, 60, 120, 180, 240))]
    out = research_batch.aggregate_3m(rows)
    assert [r['three_minute_epoch'] for r in out] == [0]
    assert out[0]['open'] == 99.5
    assert out[0]['close'] == 102
    assert out[0]['trade_count'] == 6
    assert out[0]['delta_qty'] == 3
    assert out[0]['complete_minutes'] == 3


def test_3m_dedup_not_required_and_missing_minute_skips_bucket():
    rows = [minute_row('B-ETH_USDT', 0, 100), minute_row('B-ETH_USDT', 120, 102)]
    assert research_batch.aggregate_3m(rows) == []
