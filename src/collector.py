#!/usr/bin/env python3
"""CoinDCX BTC/USDT + ETH/USDT live raw market-data collector.

Design goals:
- Capture raw websocket payloads first; do not make unverified depth assumptions.
- Keep exchange timestamp and local receive timestamp.
- Capture trades, depth-update, depth-snapshot and price-change.
- No 15m/1h/1d live candle streams are joined.
- Each run is an independent batch with an auditable manifest.
"""
from __future__ import annotations

import argparse
import gzip
import importlib.metadata
import json
import os
import signal
import sys
import threading
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import socketio

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "config.json"


def utc_now_ms() -> int:
    return int(time.time() * 1000)


def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_config() -> dict[str, Any]:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def socketio_version() -> str:
    try:
        return importlib.metadata.version("python-socketio")
    except Exception:
        return "unknown"


def resolve_pairs(session: requests.Session, configured: list[dict[str, str]], base_url: str) -> dict[str, str]:
    """Resolve/verify pair values from market metadata, with safe configured fallback."""
    r = session.get(f"{base_url}/exchange/v1/markets_details", timeout=30)
    r.raise_for_status()
    markets = r.json()
    if not isinstance(markets, list):
        raise RuntimeError("Unexpected markets_details response")

    by_pair = {str(x.get("pair")): x for x in markets if isinstance(x, dict) and x.get("pair")}
    out: dict[str, str] = {}
    for item in configured:
        pair = item["pair"]
        if pair not in by_pair:
            raise RuntimeError(f"Configured CoinDCX pair not present in markets_details: {pair}")
        out[item["name"]] = pair
    return out


class JsonlGzWriter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = gzip.open(path, "at", encoding="utf-8")
        self._lock = threading.Lock()
        self.count = 0

    def write(self, record: dict[str, Any]) -> None:
        line = json.dumps(record, separators=(",", ":"), ensure_ascii=False)
        with self._lock:
            self._fh.write(line + "\n")
            self.count += 1
            if self.count % 500 == 0:
                self._fh.flush()

    def close(self) -> None:
        with self._lock:
            self._fh.flush()
            self._fh.close()


class Collector:
    EVENT_FILES = {
        "new-trade": "trades.jsonl.gz",
        "price-change": "price_change.jsonl.gz",
        "depth-update": "depth_update.jsonl.gz",
        "depth-snapshot": "depth_snapshot.jsonl.gz",
    }

    def __init__(self, cfg: dict[str, Any], out_dir: Path, duration_minutes: float) -> None:
        self.cfg = cfg
        self.out_dir = out_dir
        self.duration_seconds = max(5.0, duration_minutes * 60)
        self.stop_event = threading.Event()
        self.sio = socketio.Client(
            reconnection=bool(cfg["collector"].get("reconnect", True)),
            reconnection_attempts=int(cfg["collector"].get("max_reconnect_attempts", 12)),
            logger=False,
            engineio_logger=False,
        )
        self.session = requests.Session()
        self.event_counts = Counter()
        self.raw_writers: dict[str, JsonlGzWriter] = {}
        self.unknown_events = Counter()
        self.errors: list[dict[str, Any]] = []
        self.connection_events: list[dict[str, Any]] = []
        self.join_channels: list[str] = []
        self.join_emissions = 0
        self.reconnect_count = 0
        self.start_epoch = time.time()
        self.last_event_monotonic = time.monotonic()
        self.last_event_exchange_ms: dict[str, int] = {}
        self.received_exchange_times: dict[str, list[int]] = defaultdict(list)
        self._register_handlers()

    def _record_error(self, context: str, exc: Exception | str) -> None:
        entry = {"time_utc": utc_iso(), "context": context, "error": str(exc)}
        self.errors.append(entry)
        print(json.dumps(entry), flush=True)

    def _writer_for(self, event_name: str) -> JsonlGzWriter:
        if event_name not in self.raw_writers:
            self.raw_writers[event_name] = JsonlGzWriter(self.out_dir / self.EVENT_FILES[event_name])
        return self.raw_writers[event_name]

    @staticmethod
    def _payload(event_name: str, response: Any) -> dict[str, Any]:
        if isinstance(response, dict):
            return response
        return {"_raw": response}

    @staticmethod
    def _extract_data(payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data")
        return data if isinstance(data, dict) else payload

    def _write_event(self, event_name: str, response: Any) -> None:
        received_ms = utc_now_ms()
        received_ns = time.time_ns()
        payload = self._payload(event_name, response)
        data = self._extract_data(payload)
        record = {
            "received_at_utc": datetime.fromtimestamp(received_ms / 1000, tz=timezone.utc).isoformat(),
            "received_at_ms": received_ms,
            "received_at_ns": received_ns,
            "event": event_name,
            "exchange_timestamp_ms": data.get("T", data.get("ts")),
            "pair": data.get("s"),
            "raw": payload,
        }
        self._writer_for(event_name).write(record)
        self.event_counts[event_name] += 1
        self.last_event_monotonic = time.monotonic()
        ex = record["exchange_timestamp_ms"]
        if ex is not None:
            try:
                ex_i = int(ex)
                self.received_exchange_times[event_name].append(ex_i)
                key = f"{event_name}:{record.get('pair')}"
                self.last_event_exchange_ms[key] = ex_i
            except (TypeError, ValueError):
                pass

    def _register_handlers(self) -> None:
        @self.sio.event
        def connect():
            self.connection_events.append({"time_utc": utc_iso(), "state": "connected"})
            print(f"Connected to {self.cfg['coindcx']['socket_url']}", flush=True)
            for channel in self.join_channels:
                try:
                    self.sio.emit("join", {"channelName": channel})
                    self.join_emissions += 1
                except Exception as exc:
                    self._record_error(f"join:{channel}", exc)

        @self.sio.event
        def disconnect():
            self.reconnect_count += 1
            self.connection_events.append({"time_utc": utc_iso(), "state": "disconnected", "reconnect_count": self.reconnect_count})
            print("Socket disconnected", flush=True)

        @self.sio.on("new-trade")
        def on_trade(response):
            self._write_event("new-trade", response)

        @self.sio.on("price-change")
        def on_price(response):
            self._write_event("price-change", response)

        @self.sio.on("depth-update")
        def on_depth_update(response):
            self._write_event("depth-update", response)

        @self.sio.on("depth-snapshot")
        def on_depth_snapshot(response):
            self._write_event("depth-snapshot", response)

        @self.sio.on("*")
        def on_catch_all(event, *args):
            if event not in self.EVENT_FILES and event not in {"connect", "disconnect", "connect_error"}:
                self.unknown_events[str(event)] += 1

    def stop(self, *_args: Any) -> None:
        self.stop_event.set()

    def run(self, pairs: dict[str, str]) -> int:
        self.out_dir.mkdir(parents=True, exist_ok=True)
        channels = []
        depth = int(self.cfg["coindcx"].get("orderbook_depth", 50))
        for pair in pairs.values():
            channels.extend([
                f"{pair}@trades",
                f"{pair}@orderbook@{depth}",
                f"{pair}@prices",
            ])
        self.join_channels = sorted(channels)
        self.connection_events.append({"time_utc": utc_iso(), "state": "starting", "channels": self.join_channels})

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        try:
            self.sio.connect(
                self.cfg["coindcx"]["socket_url"],
                transports=["websocket"],
                wait_timeout=int(self.cfg["collector"].get("connect_timeout_seconds", 30)),
            )
        except Exception as exc:
            self._record_error("socket_connect", exc)
            return 2

        deadline = time.monotonic() + self.duration_seconds
        heartbeat_every = int(self.cfg["collector"].get("heartbeat_seconds", 60))
        next_heartbeat = time.monotonic() + heartbeat_every
        while not self.stop_event.is_set() and time.monotonic() < deadline:
            self.sio.sleep(0.25)
            now = time.monotonic()
            if now >= next_heartbeat:
                print(json.dumps({
                    "event": "heartbeat",
                    "time_utc": utc_iso(),
                    "elapsed_seconds": round(time.time() - self.start_epoch, 3),
                    "event_counts": dict(self.event_counts),
                    "last_event_age_seconds": round(now - self.last_event_monotonic, 3),
                }), flush=True)
                next_heartbeat = now + heartbeat_every

        try:
            self.sio.disconnect()
        except Exception as exc:
            self._record_error("socket_disconnect", exc)

        for writer in self.raw_writers.values():
            writer.close()

        end_epoch = time.time()
        manifest = {
            "schema_version": 1,
            "batch_id": self.out_dir.name,
            "created_utc": utc_iso(),
            "collector_start_utc": datetime.fromtimestamp(self.start_epoch, tz=timezone.utc).isoformat(),
            "collector_end_utc": datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat(),
            "runtime_seconds": round(end_epoch - self.start_epoch, 3),
            "requested_duration_seconds": self.duration_seconds,
            "symbols": pairs,
            "streams": ["new-trade", "depth-update", "depth-snapshot", "price-change"],
            "orderbook_channel_depth": depth,
            "socket_url": self.cfg["coindcx"]["socket_url"],
            "python_socketio_version": socketio_version(),
            "join_channels": self.join_channels,
            "join_emissions": self.join_emissions,
            "connection_events": self.connection_events,
            "reconnect_count": self.reconnect_count,
            "event_counts": dict(self.event_counts),
            "unknown_events": dict(self.unknown_events),
            "errors": self.errors,
            "notes": [
                "Raw depth payloads are preserved without assuming incremental semantics.",
                "Depth-derived imbalance/microprice features remain uncertified until depth semantics validation passes.",
            ],
        }
        (self.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        quality = {
            "status": "PASS" if not self.errors else "PASS_WITH_ERRORS",
            "required_event_counts": {k: int(self.event_counts.get(k, 0)) for k in self.EVENT_FILES},
            "missing_required_streams": [k for k in self.EVENT_FILES if self.event_counts.get(k, 0) == 0],
            "exchange_timestamp_ranges": {
                k: {
                    "min_ms": min(v) if v else None,
                    "max_ms": max(v) if v else None,
                }
                for k, v in self.received_exchange_times.items()
            },
        }
        if quality["missing_required_streams"]:
            quality["status"] = "FAIL_MISSING_STREAM"
        (self.out_dir / "data_quality.json").write_text(json.dumps(quality, indent=2), encoding="utf-8")
        print(json.dumps(manifest, indent=2), flush=True)
        return 0 if quality["status"] != "FAIL_MISSING_STREAM" else 3


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration-minutes", type=float, default=None)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    cfg = load_config()
    duration = args.duration_minutes or float(cfg["collector"]["default_duration_minutes"])
    out = Path(args.out)
    collector = Collector(cfg, out, duration)
    session = collector.session
    pairs = resolve_pairs(session, cfg["coindcx"]["symbols"], cfg["coindcx"]["base_api"])
    return collector.run(pairs)


if __name__ == "__main__":
    raise SystemExit(main())
