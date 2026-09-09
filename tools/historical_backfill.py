#!/usr/bin/env python3
"""
Robust CoinDCX historical candle backfill for the strategy-research baseline.

Scope: BTC/USDT + ETH/USDT, 1-minute candles, last 1-3 days.
The CoinDCX candle API returns candles descending by time and has a max limit of 1000.
We therefore page BACKWARD from the requested end time, using the oldest returned
candle as the next cursor. Every response is deduplicated and the final coverage
is checked before the run is considered successful.
"""
import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE = "https://api.coindcx.com"
PAIRS = ["B-BTC_USDT", "B-ETH_USDT"]
MINUTE_MS = 60_000
PAGE_SIZE = 1000


def utc_now_ms():
    return int(time.time() * 1000)


def iso_ms(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def get_candles(session, pair, start_ms, end_ms):
    r = session.get(
        f"{BASE}/market_data/candles",
        params={
            "pair": pair,
            "interval": "1m",
            "startTime": start_ms,
            "endTime": end_ms,
            "limit": PAGE_SIZE,
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        raise RuntimeError(f"Unexpected candle response for {pair}: {data!r}")
    return [x for x in data if isinstance(x, dict) and "time" in x]


def fetch_full_window(session, pair, start_ms, end_ms, out_dir):
    candles = {}
    requests_made = 0
    cursor_end = end_ms
    pages = []

    while cursor_end > start_ms:
        # Ask for at most 1000 minutes ending at the current cursor.
        cursor_start = max(start_ms, cursor_end - PAGE_SIZE * MINUTE_MS)

        data = get_candles(session, pair, cursor_start, cursor_end)
        requests_made += 1

        if not data:
            pages.append({
                "request_start_ms": cursor_start,
                "request_end_ms": cursor_end,
                "returned": 0,
            })
            break

        page_times = [int(x["time"]) for x in data]
        oldest = min(page_times)
        newest = max(page_times)

        for x in data:
            t = int(x["time"])
            if start_ms <= t < end_ms:
                candles[t] = x

        pages.append({
            "request_start_ms": cursor_start,
            "request_end_ms": cursor_end,
            "returned": len(data),
            "oldest_returned_ms": oldest,
            "newest_returned_ms": newest,
        })

        # Always move backward. If the endpoint ignores our requested range and
        # returns a fixed recent page, this still terminates safely.
        next_cursor = oldest
        if next_cursor >= cursor_end:
            # Try a one-minute step back rather than getting stuck.
            next_cursor = cursor_end - MINUTE_MS

        cursor_end = next_cursor
        time.sleep(0.15)

        # Safety against pathological responses.
        if requests_made > 20:
            raise RuntimeError("More than 20 candle pages were needed; stopping safely.")

    ordered = [candles[t] for t in sorted(candles)]

    if not ordered:
        raise RuntimeError(f"No candles returned for {pair}")

    # Expected 1m candle count: allow for the current/incomplete minute at the end,
    # but require essentially complete requested historical coverage.
    expected = max(1, (end_ms - start_ms + MINUTE_MS - 1) // MINUTE_MS)
    first_t = int(ordered[0]["time"])
    last_t = int(ordered[-1]["time"])
    missing_count = max(0, expected - len(ordered))

    coverage = {
        "pair": pair,
        "expected_1m_bars_approx": expected,
        "returned_unique_bars": len(ordered),
        "first_candle_ms": first_t,
        "last_candle_ms": last_t,
        "first_candle_utc": iso_ms(first_t),
        "last_candle_utc": iso_ms(last_t),
        "requested_start_utc": iso_ms(start_ms),
        "requested_end_utc": iso_ms(end_ms),
        "missing_bar_count_estimate": missing_count,
        "coverage_fraction": round(len(ordered) / expected, 6),
        "requests_made": requests_made,
        "pages": pages,
    }

    path = out_dir / f"{pair.replace('-', '_')}_1m_candles.json"
    path.write_text(json.dumps(ordered, indent=2), encoding="utf-8")

    return ordered, coverage


def derive_3m(candles, start_ms, end_ms):
    by_t = {int(x["time"]): x for x in candles}
    buckets = {}

    for t, x in by_t.items():
        b = t - (t % (3 * MINUTE_MS))
        buckets.setdefault(b, []).append(x)

    result = []
    for b, xs in sorted(buckets.items()):
        xs = sorted(xs, key=lambda x: int(x["time"]))
        expected = [b, b + MINUTE_MS, b + 2 * MINUTE_MS]
        if [int(x["time"]) for x in xs] != expected:
            continue
        if not (start_ms <= b < end_ms):
            continue
        result.append({
            "time": b,
            "open": float(xs[0]["open"]),
            "high": max(float(x["high"]) for x in xs),
            "low": min(float(x["low"]) for x in xs),
            "close": float(xs[-1]["close"]),
            "volume": sum(float(x["volume"]) for x in xs),
        })
    return result


def fetch_recent_trades(session, pair, out_dir):
    r = session.get(
        f"{BASE}/market_data/trade_history",
        params={"pair": pair, "limit": 500},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    path = out_dir / f"{pair.replace('-', '_')}_recent_500_trades.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data


def fetch_orderbook(session, pair, out_dir):
    r = session.get(
        f"{BASE}/market_data/orderbook",
        params={"pair": pair, "depth": 50},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    path = out_dir / f"{pair.replace('-', '_')}_orderbook_snapshot.json"
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    return data


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, choices=(1, 2, 3), default=3)
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
        "requested_start_utc": iso_ms(start_ms),
        "requested_end_utc": iso_ms(end_ms),
        "pairs": PAIRS,
        "interval": "1m",
        "files": {},
        "historical_orderflow_warning": (
            "This backfill provides historical 1m/3m price-volume data. "
            "CoinDCX's documented public spot trade_history endpoint is limited "
            "to the most recent 500 trades, so it is not used as a 1-3 day Delta archive."
        ),
    }

    failures = []

    for pair in PAIRS:
        candles, coverage = fetch_full_window(
            session, pair, start_ms, end_ms, out
        )
        bars3 = derive_3m(candles, start_ms, end_ms)

        p3 = out / f"{pair.replace('-', '_')}_3m_derived.json"
        p3.write_text(json.dumps(bars3, indent=2), encoding="utf-8")

        recent = fetch_recent_trades(session, pair, out)
        book = fetch_orderbook(session, pair, out)

        manifest["files"][pair] = {
            "1m_candles": len(candles),
            "3m_derived": len(bars3),
            "recent_trades": len(recent) if isinstance(recent, list) else None,
            "orderbook_snapshot_present": bool(book),
            "coverage": coverage,
        }

        # Require >=99% of expected 1m bars for a 3-day research dataset.
        if coverage["coverage_fraction"] < 0.99:
            failures.append(
                f"{pair}: coverage only {coverage['coverage_fraction']:.2%}"
            )

    manifest["status"] = "PASS" if not failures else "FAIL"
    manifest["failures"] = failures
    (out / "manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    print(json.dumps(manifest, indent=2))

    raise SystemExit(0 if not failures else 2)


if __name__ == "__main__":
    main()
