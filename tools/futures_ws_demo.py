\
#!/usr/bin/env python3
"""Five-minute discovery probe for CoinDCX public Futures WebSocket channels.

This is intentionally isolated from the production collector.
It connects to the documented Futures Socket endpoint and records every
observed event/payload for BTC and ETH futures, plus a broad set of public
channel guesses that are derived from the documented naming scheme.

It does NOT assume that any event is Open Interest. The output is intended
to establish exactly what the exchange sends.
"""
from __future__ import annotations

import argparse
import gzip
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any

import socketio

URL = "https://stream.coindcx.com"
INSTRUMENTS = ["B-BTC_USDT", "B-ETH_USDT"]

# Documented public futures channels.
CHANNELS = []
for instrument in INSTRUMENTS:
    CHANNELS.extend([
        f"{instrument}@trades-futures",
        f"{instrument}@prices-futures",
        f"{instrument}@orderbook@50-futures",
        f"{instrument}_1m-futures",
        f"{instrument}_15m-futures",
        f"{instrument}_1h-futures",
    ])
CHANNELS.append("currentPrices@futures@rt")

# Extra discovery names. These are probes only; a response is never treated
# as OI unless its payload demonstrably contains an OI-like field.
CHANNELS.extend([
    "openInterest@futures",
    "openInterest@futures@rt",
    "open_interest@futures",
    "futuresOpenInterest",
])

OI_KEYS = {
    "openinterest", "open_interest", "oi",
    "openinterestvalue", "open_interest_value",
    "openinterestqty", "open_interest_qty",
}

def contains_oi_like(value: Any) -> bool:
    if isinstance(value, dict):
        for k, v in value.items():
            key = "".join(ch for ch in str(k).lower() if ch.isalnum() or ch == "_")
            if key in OI_KEYS:
                return True
            if contains_oi_like(v):
                return True
    elif isinstance(value, list):
        return any(contains_oi_like(x) for x in value)
    return False

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=5)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    counts = Counter()
    unknown = Counter()
    joins = []
    records = []
    errors = []
    connected = {"value": False}

    sio = socketio.Client(
        reconnection=True,
        reconnection_attempts=5,
        logger=False,
        engineio_logger=False,
    )

    def record(event: str, response: Any):
        item = {
            "received_at_ms": int(time.time() * 1000),
            "event": event,
            "data": response,
            "oi_like": contains_oi_like(response),
        }
        counts[event] += 1
        records.append(item)

    @sio.event
    def connect():
        connected["value"] = True
        for channel in CHANNELS:
            try:
                sio.emit("join", {"channelName": channel})
                joins.append({"channel": channel, "status": "emitted"})
            except Exception as exc:
                joins.append({"channel": channel, "status": "error", "error": str(exc)})

    @sio.event
    def disconnect():
        connected["value"] = False

    @sio.on("*")
    def catch_all(event, *args):
        if event in {"connect", "disconnect", "connect_error"}:
            return
        payload = args[0] if len(args) == 1 else list(args)
        record(str(event), payload)

    start = time.time()
    try:
        sio.connect(URL, transports=["websocket"], wait_timeout=30)
        deadline = time.monotonic() + args.minutes * 60
        while time.monotonic() < deadline:
            sio.sleep(0.25)
    except Exception as exc:
        errors.append({"context": "socket", "error": str(exc)})
    finally:
        try:
            sio.disconnect()
        except Exception as exc:
            errors.append({"context": "disconnect", "error": str(exc)})

    oi_hits = [r for r in records if r["oi_like"]]

    with gzip.open(out / "futures_ws_events.jsonl.gz", "wt", encoding="utf-8") as fh:
        for item in records:
            fh.write(json.dumps(item, separators=(",", ":"), ensure_ascii=False) + "\n")

    summary = {
        "url": URL,
        "runtime_seconds": round(time.time() - start, 3),
        "connected": connected["value"],
        "channels": CHANNELS,
        "join_count": len(joins),
        "joins": joins,
        "event_counts": dict(counts),
        "unknown_event_counts": dict(unknown),
        "oi_like_event_count": len(oi_hits),
        "oi_like_events": sorted({r["event"] for r in oi_hits}),
        "errors": errors,
        "note": (
            "A field is marked oi_like only when a payload key matches a small "
            "set of Open Interest naming variants. This is discovery output, "
            "not certification that the field is economically valid OI."
        ),
    }
    (out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if connected["value"] or records else 2

if __name__ == "__main__":
    raise SystemExit(main())
