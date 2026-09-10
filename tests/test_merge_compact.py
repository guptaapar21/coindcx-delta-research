from pathlib import Path
import csv
import gzip
import importlib.util
import sys

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('merge_compact', ROOT / 'tools' / 'merge_compact.py')
merge_compact = importlib.util.module_from_spec(spec)
sys.modules['merge_compact'] = merge_compact
spec.loader.exec_module(merge_compact)


def write_gz_csv(path: Path, rows: list[dict[str, str]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, 'wt', encoding='utf-8', newline='') as fh:
        writer = csv.DictWriter(fh, fieldnames=sorted(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def test_forward_labels_cross_batch_boundary(tmp_path):
    root = tmp_path / 'compact_1m'
    incoming = tmp_path / 'batch' / 'features_1m.csv.gz'
    rows = []
    for minute, close in [(0, 100), (60, 101), (120, 102), (300, 105), (600, 110), (900, 115), (1800, 130)]:
        rows.append({
            'symbol': 'B-BTC_USDT',
            'minute_epoch': str(minute),
            'minute_utc': merge_compact.datetime.fromtimestamp(minute, merge_compact.timezone.utc).isoformat(),
            'close': str(close),
            'open': str(close), 'high': str(close), 'low': str(close),
            'trade_count': '1', 'aggressive_buy_qty': '1', 'aggressive_sell_qty': '0',
            'delta_qty': '1', 'delta_notional': str(close), 'total_qty': '1', 'delta_ratio': '1',
            'depth_update_events': '0', 'depth_snapshot_events': '0',
            'book_features_status': 'UNCERTIFIED', 'source_schema': 'research_batch_v3'
        })
    write_gz_csv(incoming, rows)
    added, files = merge_compact.merge_layer(list(merge_compact.read_gz_csv(incoming)), root, 'minute_epoch')
    assert added == len(rows)
    assert str(root / '1970-01.csv') in files
    with (root / '1970-01.csv').open() as fh:
        out = list(csv.DictReader(fh))
    by_epoch = {int(r['minute_epoch']): r for r in out}
    assert abs(float(by_epoch[0]['forward_return_60s']) - 0.01) < 1e-12
    assert abs(float(by_epoch[0]['forward_return_300s']) - 0.05) < 1e-12
    assert abs(float(by_epoch[0]['forward_return_600s']) - 0.10) < 1e-12
    assert abs(float(by_epoch[0]['forward_return_900s']) - 0.15) < 1e-12
    assert abs(float(by_epoch[0]['forward_return_1800s']) - 0.30) < 1e-12
