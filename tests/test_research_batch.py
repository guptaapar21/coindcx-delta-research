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


def test_string_encoded_raw_data_is_parsed(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    rec = {"raw": {"data": json.dumps({"s": "B-ETH_USDT", "T": 1000, "p": "2500", "q": "1.5", "m": 0})}}
    write_trade_file(batch, [rec])
    out = research_batch.build_seconds(batch)
    assert out[0]['symbol'] == 'B-ETH_USDT'
    assert out[0]['aggressive_buy_qty'] == 1.5


def test_actual_collector_string_payload_and_symbol_normalization(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    rec = {
        "received_at_ms": 2000,
        "raw": {"event": "new-trade", "data": json.dumps({
            "s": "BTCUSDT", "T": 1000, "p": "80000", "q": "0.01", "m": 1
        })}
    }
    write_trade_file(batch, [rec])
    out = research_batch.build_seconds(batch)
    assert out[0]['symbol'] == 'B-BTC_USDT'
    assert out[0]['aggressive_sell_qty'] == 0.01


def test_long_forward_labels_are_added_without_expanding_flow_windows(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    rows = []
    for sec, price in [(0, 100), (1, 101), (5, 105), (15, 115), (30, 130),
                       (60, 160), (180, 280), (300, 400), (600, 700),
                       (900, 1000), (1800, 1900)]:
        rows.append({"raw": {"data": {
            "s": "B-BTC_USDT", "T": sec * 1000, "p": str(price), "q": "1", "m": 0
        }}})
    write_trade_file(batch, rows)
    out = research_batch.build_seconds(batch)
    r0 = next(r for r in out if r['epoch_second'] == 0)
    assert list(research_batch.WINDOWS) == [5, 15, 30, 60, 180]
    assert abs(r0['forward_return_5s'] - 0.05) < 1e-12
    assert abs(r0['forward_return_300s'] - 3.0) < 1e-12
    assert abs(r0['forward_return_1800s'] - 18.0) < 1e-12


def write_futures_trade_file(batch: Path, rows: list[dict]):
    with gzip.open(batch / 'futures_trades.jsonl.gz', 'wt', encoding='utf-8') as f:
        for r in rows:
            f.write(json.dumps(r) + '\n')


def test_futures_delta_is_captured_separately_and_aligned(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    write_trade_file(batch, [
        {"raw": {"data": {"s": "B-BTC_USDT", "T": 1000, "p": "100", "q": "2", "m": 0}}},
        {"raw": {"data": {"s": "B-BTC_USDT", "T": 2000, "p": "101", "q": "1", "m": 1}}},
    ])
    write_futures_trade_file(batch, [
        {"raw": {"data": json.dumps({"s": "B-BTC_USDT", "T": 1000, "p": "100.2", "q": "3", "m": 0, "pr": "f"})}},
        {"raw": {"data": json.dumps({"s": "B-BTC_USDT", "T": 2000, "p": "100.1", "q": "1", "m": 1, "pr": "f"})}},
    ])
    out = research_batch.build_seconds(batch)
    assert out[0]['futures_aggressive_buy_qty'] == 3
    assert out[1]['futures_aggressive_sell_qty'] == 1
    assert out[0]['futures_delta_qty'] == 3
    assert out[1]['futures_delta_qty'] == -1
    assert out[0]['futures_delta_ratio_5s'] == 1.0
    assert out[1]['futures_delta_ratio_5s'] == 0.5
    assert out[1]['futures_spot_delta_divergence_5s'] is not None


def test_exploratory_futures_symbol_is_kept_separate_from_spot(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    write_trade_file(batch, [
        {"raw": {"data": {"s": "B-SUI_USDT", "T": 1000, "p": "3.00", "q": "5", "m": 0}}},
    ])
    write_futures_trade_file(batch, [
        {"raw": {"data": json.dumps({"s": "B-SUI_USDT", "T": 1000, "p": "3.01", "q": "7", "m": 0, "pr": "f"})}},
    ])
    out = research_batch.build_seconds(batch)
    assert len(out) == 1
    assert out[0]['symbol'] == 'B-SUI_USDT'
    assert out[0]['futures_trade_count'] == 1
    assert out[0]['futures_delta_qty'] == 7.0
