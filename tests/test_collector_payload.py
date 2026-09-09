from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import collector


def test_collector_decodes_string_data_for_metadata():
    payload = {"event": "new-trade", "data": json.dumps({"s": "B-BTC_USDT", "T": 1234, "p": "80000", "q": "1", "m": 0})}
    decoded = collector.Collector._extract_data(payload)
    assert decoded["s"] == "B-BTC_USDT"
    assert decoded["T"] == 1234
