#!/usr/bin/env python3
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "https://api.coindcx.com"
PAIRS = ["B-BTC_USDT", "B-ETH_USDT"]
VALID_INTERVALS = {"1m", "15m", "1h", "1d"}

def utc_now_ms():
    return int(time.time() * 1000)

def get_json(session, url, params=None):
    r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def fetch_candles(session, pair, start_ms, end_ms, out_dir):
    rows = []
    cursor = start_ms
    step_ms = 1000 * 60 * 1000 * 1000  # 1,000 one-minute bars
    while cursor < end_ms:
        window_end = min(end_ms, cursor + step_ms)
        data = get_json(session, f"{BASE}/market_data/candles", {
            "pair": pair,
            "interval": "1m",
            "startTime": cursor,
            "endTime": window_end,
            "limit": 1000,
        })
        if not isinstance(data, list):
            raise RuntimeError(f"Unexpected candle response for {pair}: {data!r}")
        rows.extend(data)
        if not data:
            break
        times = [int(x["time"]) for x in data if isinstance(x, dict) and "time" in x]
        if not times:
            break
        max_time = max(times)
        cursor = max(cursor + 60_000, max_time + 60_000)
        time.sleep(0.15)

    dedup = {}
    for x in rows:
        if isinstance(x, dict) and "time" in x:
            dedup[int(x["time"])] = x
    ordered = [dedup[k] for k in sorted(dedup)]

    # Keep only requested window.
    ordered = [x for x in ordered if start_ms <= int(x["time"]) < end_ms]
    path = out_dir / f"{pair.replace('-', '_')}_1m_candles.json"
    path.write_text(json.dumps(ordered, indent=2), encoding="utf-8")
    return ordered

def fetch_recent_trades(session, pair, out_dir):
    data = get_json(session, f"{BASE}/market_data/trade_history", {
        "pair": pair,
        "limit": 500,
    })
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected trade response for {pair}: {data!r}")
    path = out_dir / f"{pair.replace('-', '_')}_recent_500_trades.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data

def fetch_orderbook(session, pair, out_dir):
    data = get_json(session, f"{BASE}/market_data/orderbook", {
        "pair": pair,
        "depth": 50,
    })
    path = out_dir / f"{pair.replace('-', '_')}_orderbook_snapshot.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data

def make_3m(candles):
    by_t = {int(x["time"]): x for x in candles}
    times = sorted(by_t)
    if not times:
        return []
    buckets = {}
    for t in times:
        b = t - (t % 180_000)
        buckets.setdefault(b, []).append(by_t[t])

    out = []
    for b in sorted(buckets):
        xs = sorted(buckets[b], key=lambda x: int(x["time"]))
        # Only accept complete three-minute bars.
        expected = [b, b + 60_000, b + 120_000]
        if [int(x["time"]) for x in xs] != expected:
            continue
        out.append({
            "time": b,
            "open": float(xs[0]["open"]),
            "high": max(float(x["high"]) for x in xs),
            "low": min(float(x["low"]) for x in xs),
            "close": float(xs[-1]["close"]),
            "volume": sum(float(x["volume"]) for x in xs),
        })
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3, choices=(1, 2, 3))
    ap.add_argument("--out", default="historical_output")
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    end_ms = utc_now_ms()
    start_ms = end_ms - args.days * 24 * 60 * 60 * 1000
    session = requests.Session()
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "days_requested": args.days,
        "start_ms": start_ms,
        "end_ms": end_ms,
        "pairs": PAIRS,
        "historical_candle_interval": "1m",
        "historical_orderflow_note": (
            "Public documented spot trade_history is limited to the most recent "
            "500 trades; no documented timestamp-based public 3-day spot trade archive "
            "was used. Live websocket data remains required for historical Delta/orderflow."
        ),
        "files": {},
    }

    for pair in PAIRS:
        candles = fetch_candles(session, pair, start_ms, end_ms, out)
        bars3 = make_3m(candles)
        p3 = out / f"{pair.replace('-', '_')}_3m_derived.json"
        p3.write_text(json.dumps(bars3, indent=2), encoding="utf-8")

        recent = fetch_recent_trades(session, pair, out)
        book = fetch_orderbook(session, pair, out)

        manifest["files"][pair] = {
            "1m_candles": len(candles),
            "3m_derived": len(bars3),
            "recent_trades": len(recent),
            "orderbook_snapshot": bool(book),
        }

    (out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))

if __name__ == "__main__":
    main()
