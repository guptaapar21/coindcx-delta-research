#!/usr/bin/env python3
import argparse, json, time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import socketio

PAIRS = ["B-BTC_USDT", "B-ETH_USDT"]
SOCKET = "https://stream-spot.coindcx.com"

def iso():
    return datetime.now(timezone.utc).isoformat()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--minutes", type=float, default=4.0)
    ap.add_argument("--out", default="diagnostic_output")
    a = ap.parse_args()

    out = Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    events_file = out / "events.jsonl"
    summary_file = out / "summary.json"

    counts = Counter()
    errors = []
    sio = socketio.Client(reconnection=True, logger=False, engineio_logger=False)

    def save(kind, payload):
        counts[kind] += 1
        with events_file.open("a", encoding="utf-8") as f:
            f.write(json.dumps({
                "received_at": iso(),
                "kind": kind,
                "payload": payload
            }, separators=(",", ":")) + "\n")

    @sio.event
    def connect():
        print(f"{iso()} CONNECTED")
        for pair in PAIRS:
            for ch in (
                f"{pair}@trades",
                f"{pair}@prices",
                f"{pair}@orderbook@50",
                f"{pair}_1m",
            ):
                try:
                    sio.emit("join", {"channelName": ch})
                    print(f"{iso()} JOIN_EMIT {ch}")
                except Exception as e:
                    errors.append(f"join {ch}: {e!r}")

    @sio.event
    def connect_error(data):
        errors.append(f"connect_error: {data!r}")
        print(f"{iso()} CONNECT_ERROR {data!r}")

    @sio.event
    def disconnect():
        print(f"{iso()} DISCONNECTED")

    @sio.on("new-trade")
    def trade(data): save("trade", data)

    @sio.on("price-change")
    def price(data): save("price_change", data)

    @sio.on("candlestick")
    def candle(data): save("candlestick", data)

    @sio.on("depth-update")
    def depth_update(data): save("depth_update", data)

    @sio.on("depth-snapshot")
    def depth_snapshot(data): save("depth_snapshot", data)

    start = time.monotonic()
    try:
        sio.connect(SOCKET, transports=["websocket"], wait_timeout=20)
        while time.monotonic() - start < a.minutes * 60:
            time.sleep(1)
    except Exception as e:
        errors.append(f"runtime: {e!r}")
        print(f"{iso()} ERROR {e!r}")
    finally:
        if sio.connected:
            sio.disconnect()

    elapsed = time.monotonic() - start
    result = {
        "requested_minutes": a.minutes,
        "elapsed_seconds": round(elapsed, 2),
        "pairs": PAIRS,
        "counts": dict(counts),
        "errors": errors,
        "success": {
            "trade": counts["trade"] > 0,
            "price_change": counts["price_change"] > 0,
            "candlestick": counts["candlestick"] > 0,
            "depth": counts["depth_update"] + counts["depth_snapshot"] > 0
        }
    }
    summary_file.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))

    ok = all(counts[k] > 0 for k in ("trade", "price_change", "candlestick")) and (
        counts["depth_update"] + counts["depth_snapshot"] > 0
    )
    raise SystemExit(0 if ok else 2)

if __name__ == "__main__":
    main()
