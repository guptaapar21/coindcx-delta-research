from pathlib import Path
import gzip, json
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import depth_validator


def test_depth_validator_is_conservative(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    rec = {"raw": {"data": {"s": "BTCUSDT", "ts": 1000, "vs": 1, "bids": {"100":"1"}, "asks": {"101":"2"}}}}
    with gzip.open(batch/'depth_snapshot.jsonl.gz','wt') as f: f.write(json.dumps(rec)+'\n')
    with gzip.open(batch/'depth_update.jsonl.gz','wt') as f:
        for v in range(2, 22):
            x = dict(rec); x['raw'] = dict(rec['raw']); x['raw']['data'] = dict(rec['raw']['data']); x['raw']['data']['vs'] = v
            f.write(json.dumps(x)+'\n')
    result = depth_validator.analyze(batch)
    assert result['overall_classification'].startswith('UNCERTIFIED')
    assert result['classification']['BTCUSDT'] != 'INCREMENTAL'


def test_depth_validator_parses_string_encoded_payload(tmp_path):
    batch = tmp_path / 'b'; batch.mkdir()
    payload = {"s": "BTCUSDT", "ts": 1000, "vs": 1, "bids": {"100": "1"}, "asks": {"101": "2"}}
    rec = {"raw": {"data": json.dumps(payload)}}
    with gzip.open(batch/'depth_snapshot.jsonl.gz','wt') as f: f.write(json.dumps(rec)+'\n')
    with gzip.open(batch/'depth_update.jsonl.gz','wt') as f:
        for v in range(2, 22):
            d = dict(payload); d['vs'] = v; d['ts'] = 1000 + v
            f.write(json.dumps({"raw": {"data": json.dumps(d)}})+'\n')
    result = depth_validator.analyze(batch)
    assert 'BTCUSDT' in result['symbols']
    assert result['symbols']['BTCUSDT']['updates'] == 20

