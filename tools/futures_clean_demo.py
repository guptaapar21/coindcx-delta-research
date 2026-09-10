\
#!/usr/bin/env python3
"""Clean public CoinDCX Futures market-data/OI discovery demo.

This probe uses only documented public Futures REST endpoints and documented
public Futures WebSocket channels. It does not use guessed OI channel names.

It samples the public Futures current-prices REST endpoint every 10 seconds
for the requested duration, while simultaneously listening to:
  - currentPrices@futures@rt
  - B-BTC_USDT@trades-futures
  - B-ETH_USDT@trades-futures
  - B-BTC_USDT@prices-futures
  - B-ETH_USDT@prices-futures

Every observed payload is saved. OI-like fields are detected recursively,
but a hit is only a lead for manual validation; this script does not label
anything as economic Open Interest without inspection.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests
import socketio

REST_BASE = "https://public.coindcx.com"
CURRENT_PRICES_URL = f"{REST_BASE}/market_data/v3/current_prices/futures/rt"
SOCKET_URL = "https://stream.coindcx.com"

INSTRUMENTS = ["B-BTC_USDT", "B-ETH_USDT"]
WS_CHANNELS = [
    "currentPrices@futures@rt",
    "B-BTC_USDT@trades-futures",
    "B-ETH_USDT@trades-futures",
    "B-BTC_USDT@prices-futures",
    "B-ETH_USDT@prices-futures",
]

OI_KEYS = {
    "oi", "openinterest", "open_interest",
    "openinterestvalue", "open_interest_value",
    "openinterestqty", "open_interest_qty",
}

def normalized_key(value: Any) -> str:
    return "".join(ch for ch in str(value).lower() if ch.isalnum() or ch == "_")

def find_oi_paths(value: Any, path: str = "$") -> list[dict[str, Any]]:
    hits: list[dict[str, Any]] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if normalized_key(key) in OI_KEYS:
                hits.append({"path": child_path, "value": child})
            hits.extend(find_oi_paths(child, child_path))
    elif isinstance(value, list):
        for i, child in enumerate(value):
            hits.extend(find_oi_paths(child, f"{path}[{i}]"))
    return hits

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    start = time.time()
    deadline = start + args.minutes * 60
    session = requests.Session()

    rest_records: list[dict[str, Any]] = []
    ws_records: list[dict[str, Any]] = []
    ws_counts = Counter()
    ws_errors: list[str] = []
    connected = False

    # REST: discover active futures instruments first.
    active_url = (
        f"{REST_BASE}/market_data/v3/current_prices/futures/rt"
    )

    # WebSocket: documented public futures market-data channels only.
    sio = socketio.Client(
        reconnection=True,
        reconnection_attempts=5,
        logger=False,
        engineio_logger=False,
    )

    @sio.event
    def connect() -> None:
        nonlocal connected
        connected = True
        sio.emit("join", {"channelName": "currentPrices@futures@rt"})
        for channel in WS_CHANNELS[1:]:
            sio.emit("join", {"channelName": channel})

    @sio.event
    def disconnect() -> None:
        pass

    @sio.on("currentPrices@futures#update")
    def on_current_prices(response: Any) -> None:
        item = {
            "received_at_ms": int(time.time() * 1000),
            "event": "currentPrices@futures#update",
            "data": response,
            "oi_hits": find_oi_paths(response),
        }
        ws_records.append(item)
        ws_counts[item["event"]] += 1

    @sio.on("new-trade")
    def on_trade(response: Any) -> None:
        item = {
            "received_at_ms": int(time.time() * 1000),
            "event": "new-trade",
            "data": response,
            "oi_hits": find_oi_paths(response),
        }
        ws_records.append(item)
        ws_counts[item["event"]] += 1

    @sio.on("price-change")
    def on_price(response: Any) -> None:
        item = {
            "received_at_ms": int(time.time() * 1000),
            "event": "price-change",
            "data": response,
            "oi_hits": find_oi_paths(response),
        }
        ws_records.append(item)
        ws_counts[item["event"]] += 1

    try:
        sio.connect(SOCKET_URL, transports=["websocket"], wait_timeout=30)
    except Exception as exc:
        ws_errors.append(f"connect: {exc}")

    # Sample documented public Futures current-prices REST endpoint every 10 s.
    next_poll = time.monotonic()
    while time.time() < deadline:
        now = time.monotonic()
        if now >= next_poll:
            ts_ms = int(time.time() * 1000)
            try:
                resp = session.get(CURRENT_PRICES_URL, timeout=20)
                try:
                    payload = resp.json()
                except ValueError:
                    payload = {"raw_text": resp.text}
                rest_records.append({
                    "received_at_ms": ts_ms,
                    "status_code": resp.status_code,
                    "data": payload,
                    "oi_hits": find_oi_paths(payload),
                })
            except Exception as exc:
                rest_records.append({
                    "received_at_ms": ts_ms,
                    "error": str(exc),
                })
            next_poll = now + 10
        sio.sleep(0.25)

    try:
        sio.disconnect()
    except Exception as exc:
        ws_errors.append(f"disconnect: {exc}")

    with gzip.open(out / "futures_ws_events.jsonl.gz", "wt", encoding="utf-8") as fh:
        for item in ws_records:
            fh.write(json.dumps(item, separators=(",", ":"), ensure_ascii=False) + "\n")

    with gzip.open(out / "futures_current_prices_rest.jsonl.gz", "wt", encoding="utf-8") as fh:
        for item in rest_records:
            fh.write(json.dumps(item, separators=(",", ":"), ensure_ascii=False) + "\n")

    rest_oi = [x for x in rest_records if x.get("oi_hits")]
    ws_oi = [x for x in ws_records if x.get("oi_hits")]

    summary = {
        "runtime_seconds": round(time.time() - start, 3),
        "rest_endpoint": CURRENT_PRICES_URL,
        "websocket_endpoint": SOCKET_URL,
        "instruments_checked": INSTRUMENTS,
        "documented_ws_channels_checked": WS_CHANNELS,
        "websocket_connected": connected,
        "rest_samples": len(rest_records),
        "websocket_event_counts": dict(ws_counts),
        "rest_oi_hit_samples": len(rest_oi),
        "websocket_oi_hit_events": len(ws_oi),
        "oi_paths_rest": sorted({
            hit["path"]
            for row in rest_oi
            for hit in row["oi_hits"]
        }),
        "oi_paths_websocket": sorted({
            hit["path"]
            for row in ws_oi
            for hit in row["oi_hits"]
        }),
        "websocket_errors": ws_errors,
        "notes": [
            "Only documented public Futures market-data channels were used.",
            "No guessed Open Interest channel names were used.",
            "OI-like field detection is lexical and requires manual economic validation.",
        ],
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
