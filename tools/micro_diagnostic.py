#!/usr/bin/env python3
"""Four-minute BTC/ETH transport diagnostic using the production event names."""
from __future__ import annotations

import argparse
import gzip
import importlib.metadata
import json
import time
from collections import Counter
from pathlib import Path

import socketio

PAIRS = ["B-BTC_USDT", "B-ETH_USDT"]
URL = "https://stream-spot.coindcx.com"
EVENTS = ("new-trade", "price-change", "candlestick", "depth-update", "depth-snapshot")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=4)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    out = Path(args.out); out.mkdir(parents=True, exist_ok=True)
    counts = Counter(); unknown = Counter(); start = time.time(); records = {e: [] for e in EVENTS}
    sio = socketio.Client(reconnection=True)

    def writer(event, response):
        counts[event] += 1
        records[event].append({"received_at_ms": int(time.time()*1000), "event": event, "data": response})

    for ev in EVENTS:
        sio.on(ev, lambda response, ev=ev: writer(ev, response))

    @sio.on("*")
    def catch(event, *args):
        if event not in EVENTS and event not in {"connect", "disconnect"}:
            unknown[str(event)] += 1

    @sio.event
    def connect():
        for pair in PAIRS:
            for channel in (f"{pair}@trades", f"{pair}@prices", f"{pair}@orderbook@50"):
                sio.emit("join", {"channelName": channel})

    sio.connect(URL, transports=["websocket"], wait_timeout=30)
    deadline = time.monotonic() + args.minutes*60
    while time.monotonic() < deadline:
        sio.sleep(0.25)
    sio.disconnect()

    for ev, items in records.items():
        with gzip.open(out / f"{ev.replace('-', '_')}.jsonl.gz", "wt", encoding="utf-8") as fh:
            for item in items:
                fh.write(json.dumps(item, separators=(",", ":")) + "\n")
    summary = {
        "runtime_seconds": round(time.time()-start,3),
        "counts": dict(counts),
        "unknown_events": dict(unknown),
        "socketio_version": importlib.metadata.version("python-socketio"),
        "required": list(EVENTS),
        "status": "PASS" if all(counts[e] > 0 for e in EVENTS) else "FAIL"
    }
    (out / "manifest.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0 if summary["status"] == "PASS" else 2


if __name__ == "__main__":
    raise SystemExit(main())
