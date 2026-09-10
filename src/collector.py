#!/usr/bin/env python3
"""CoinDCX multi-market live raw market-data collector.

Design goals:
- Capture raw websocket payloads first; do not make unverified depth assumptions.
- Keep exchange timestamp and local receive timestamp.
- Capture configured Spot trades, depth-update, depth-snapshot and price-change.
- Capture configured public Futures trades, price-change, current-prices and
  per-instrument orderbook depth snapshots.
- Keep the configured universe small and frozen in config.json for auditable
  BTC/ETH controls plus exploratory higher-volatility markets.
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
    FUTURES_EVENT_FILES = {
        "new-trade": "futures_trades.jsonl.gz",
        "price-change": "futures_price_change.jsonl.gz",
        "current-prices": "futures_current_prices.jsonl.gz",
        "depth-snapshot": "futures_depth_snapshot.jsonl.gz",
    }

    def __init__(self, cfg: dict[str, Any], out_dir: Path, duration_minutes: float) -> None:
        self.cfg = cfg
        self.out_dir = out_dir
        self.duration_seconds = max(5.0, duration_minutes * 60)
        self.stop_event = threading.Event()
        socket_kwargs = {
            "reconnection": bool(cfg["collector"].get("reconnect", True)),
            "reconnection_attempts": int(cfg["collector"].get("max_reconnect_attempts", 12)),
            "logger": False,
            "engineio_logger": False,
        }
        self.sio = socketio.Client(**socket_kwargs)
        self.futures_sio = socketio.Client(**socket_kwargs)
        self.session = requests.Session()
        self.event_counts = Counter()
        self.futures_event_counts = Counter()
        self.raw_writers: dict[str, JsonlGzWriter] = {}
        self.unknown_events = Counter()
        self.errors: list[dict[str, Any]] = []
        self.connection_events: list[dict[str, Any]] = []
        self.join_channels: list[str] = []
        self.futures_join_channels: list[str] = []
        self.futures_pairs: set[str] = set()
        self.futures_book_sockets: dict[str, socketio.Client] = {}
        self.join_emissions = 0
        self.futures_join_emissions = 0
        self.reconnect_count = 0
        self.futures_reconnect_count = 0
        self.start_epoch = time.time()
        self.last_event_monotonic = time.monotonic()
        self.last_event_exchange_ms: dict[str, int] = {}
        self.received_exchange_times: dict[str, list[int]] = defaultdict(list)
        self._register_handlers()

    def _record_error(self, context: str, exc: Exception | str) -> None:
        entry = {"time_utc": utc_iso(), "context": context, "error": str(exc)}
        self.errors.append(entry)
        print(json.dumps(entry), flush=True)

    def _writer_for(self, event_name: str, futures: bool = False) -> JsonlGzWriter:
        files = self.FUTURES_EVENT_FILES if futures else self.EVENT_FILES
        key = f"futures:{event_name}" if futures else event_name
        if key not in self.raw_writers:
            self.raw_writers[key] = JsonlGzWriter(self.out_dir / files[event_name])
        return self.raw_writers[key]

    @staticmethod
    def _payload(event_name: str, response: Any) -> dict[str, Any]:
        if isinstance(response, dict):
            return response
        return {"_raw": response}

    @staticmethod
    def _extract_data(payload: dict[str, Any]) -> dict[str, Any]:
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        if isinstance(data, str):
            try:
                decoded = json.loads(data)
                if isinstance(decoded, dict):
                    return decoded
            except (TypeError, ValueError):
                pass
        return payload

    @staticmethod
    def _is_futures_data(data: dict[str, Any]) -> bool:
        return str(data.get("pr", "")).lower() in {"f", "futures"}

    def _write_event(self, event_name: str, response: Any, futures: bool = False) -> None:
        received_ms = utc_now_ms()
        received_ns = time.time_ns()
        payload = self._payload(event_name, response)
        data = self._extract_data(payload)
        record = {
            "received_at_utc": datetime.fromtimestamp(received_ms / 1000, tz=timezone.utc).isoformat(),
            "received_at_ms": received_ms,
            "received_at_ns": received_ns,
            "event": event_name,
            "market": "futures" if futures else "spot",
            "exchange_timestamp_ms": data.get("T", data.get("ts")),
            "pair": data.get("s"),
            "raw": payload,
        }
        self._writer_for(event_name, futures=futures).write(record)
        counter = self.futures_event_counts if futures else self.event_counts
        counter[event_name] += 1
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

    def _write_futures_current_prices(self, response: Any, target_pairs: set[str], event_name: str) -> None:
        received_ms = utc_now_ms()
        received_ns = time.time_ns()
        payload = self._payload("current-prices", response)
        data = self._extract_data(payload)
        prices = data.get("prices") if isinstance(data, dict) else None
        if not isinstance(prices, dict):
            return
        selected = {str(pair): value for pair, value in prices.items() if str(pair) in target_pairs}
        if not selected:
            return
        record = {
            "received_at_utc": datetime.fromtimestamp(received_ms / 1000, tz=timezone.utc).isoformat(),
            "received_at_ms": received_ms,
            "received_at_ns": received_ns,
            "event": event_name,
            "market": "futures",
            "exchange_timestamp_ms": data.get("ts"),
            "stream_timestamp_ms": data.get("pST"),
            "version": data.get("vs"),
            "pairs": selected,
        }
        self._writer_for("current-prices", futures=True).write(record)
        self.futures_event_counts["current-prices"] += 1
        self.last_event_monotonic = time.monotonic()

    def _register_handlers(self) -> None:
        @self.sio.event
        def connect():
            self.connection_events.append({"time_utc": utc_iso(), "market": "spot", "state": "connected"})
            print(f"Connected to {self.cfg['coindcx']['socket_url']}", flush=True)
            for channel in self.join_channels:
                try:
                    self.sio.emit("join", {"channelName": channel})
                    self.join_emissions += 1
                except Exception as exc:
                    self._record_error(f"spot:join:{channel}", exc)

        @self.sio.event
        def disconnect():
            self.reconnect_count += 1
            self.connection_events.append({"time_utc": utc_iso(), "market": "spot", "state": "disconnected", "reconnect_count": self.reconnect_count})
            print("Spot socket disconnected", flush=True)

        @self.sio.on("new-trade")
        def on_trade(response):
            self._write_event("new-trade", response, futures=False)

        @self.sio.on("price-change")
        def on_price(response):
            self._write_event("price-change", response, futures=False)

        @self.sio.on("depth-update")
        def on_depth_update(response):
            self._write_event("depth-update", response)

        @self.sio.on("depth-snapshot")
        def on_depth_snapshot(response):
            self._write_event("depth-snapshot", response)

        @self.sio.on("*")
        def on_spot_catch_all(event, *args):
            if event not in self.EVENT_FILES and event not in {"connect", "disconnect", "connect_error"}:
                self.unknown_events[f"spot:{event}"] += 1

        @self.futures_sio.on("connect")
        def futures_connect():
            futures_url = self.cfg["coindcx"].get("futures_socket_url")
            self.connection_events.append({"time_utc": utc_iso(), "market": "futures", "state": "connected"})
            print(f"Connected to {futures_url}", flush=True)
            for channel in self.futures_join_channels:
                try:
                    self.futures_sio.emit("join", {"channelName": channel})
                    self.futures_join_emissions += 1
                except Exception as exc:
                    self._record_error(f"futures:join:{channel}", exc)

        @self.futures_sio.on("disconnect")
        def futures_disconnect():
            self.futures_reconnect_count += 1
            self.connection_events.append({"time_utc": utc_iso(), "market": "futures", "state": "disconnected", "reconnect_count": self.futures_reconnect_count})
            print("Futures socket disconnected", flush=True)

        @self.futures_sio.on("new-trade")
        def on_futures_trade(response):
            payload = self._payload("new-trade", response)
            data = self._extract_data(payload)
            if self._is_futures_data(data):
                self._write_event("new-trade", response, futures=True)

        @self.futures_sio.on("price-change")
        def on_futures_price(response):
            payload = self._payload("price-change", response)
            data = self._extract_data(payload)
            if self._is_futures_data(data):
                self._write_event("price-change", response, futures=True)

        @self.futures_sio.on("currentPrices@futures#update")
        def on_futures_current_prices(response):
            self._write_futures_current_prices(response, self.futures_pairs, "currentPrices@futures#update")

        @self.futures_sio.on("currentPrices@futures#snapshot")
        def on_futures_current_prices_snapshot(response):
            self._write_futures_current_prices(response, self.futures_pairs, "currentPrices@futures#snapshot")

        @self.futures_sio.on("*")
        def on_futures_catch_all(event, *args):
            if event not in {"new-trade", "price-change", "currentPrices@futures#update", "currentPrices@futures#snapshot", "connect", "disconnect", "connect_error"}:
                self.unknown_events[f"futures:{event}"] += 1

    def _start_futures_book_sockets(self) -> int:
        if not self.futures_pairs:
            return 0
        url = self.cfg["coindcx"].get("futures_socket_url")
        if not url:
            self._record_error("futures:book_socket_config", "futures_socket_url is required for futures orderbook capture")
            return 0
        depth = int(self.cfg["coindcx"].get("futures_orderbook_depth", 50))
        started = 0
        socket_kwargs = {
            "reconnection": bool(self.cfg["collector"].get("reconnect", True)),
            "reconnection_attempts": int(self.cfg["collector"].get("max_reconnect_attempts", 12)),
            "logger": False,
            "engineio_logger": False,
        }
        for pair in sorted(self.futures_pairs):
            sio = socketio.Client(**socket_kwargs)

            @sio.event
            def connect(sio=sio, pair=pair):
                self.connection_events.append({"time_utc": utc_iso(), "market": "futures_orderbook", "pair": pair, "state": "connected"})
                channel = f"{pair}@orderbook@{depth}-futures"
                try:
                    sio.emit("join", {"channelName": channel})
                    self.futures_join_emissions += 1
                except Exception as exc:
                    self._record_error(f"futures:book_join:{pair}", exc)

            @sio.event
            def disconnect(pair=pair):
                self.connection_events.append({"time_utc": utc_iso(), "market": "futures_orderbook", "pair": pair, "state": "disconnected"})

            @sio.on("depth-snapshot")
            def on_depth_snapshot(response, pair=pair):
                payload = self._payload("depth-snapshot", response)
                data = self._extract_data(payload)
                if str(data.get("pr", "")).lower() not in {"f", "futures"}:
                    return
                received_ms = utc_now_ms()
                record = {
                    "received_at_utc": datetime.fromtimestamp(received_ms / 1000, tz=timezone.utc).isoformat(),
                    "received_at_ms": received_ms,
                    "received_at_ns": time.time_ns(),
                    "event": "depth-snapshot",
                    "market": "futures",
                    "exchange_timestamp_ms": data.get("ts", data.get("T")),
                    "pair": data.get("s") or pair,
                    "raw": payload,
                }
                self._writer_for("depth-snapshot", futures=True).write(record)
                self.futures_event_counts["depth-snapshot"] += 1
                self.last_event_monotonic = time.monotonic()

            self.futures_book_sockets[pair] = sio
            try:
                sio.connect(url, transports=["websocket"], wait_timeout=int(self.cfg["collector"].get("connect_timeout_seconds", 30)))
                started += 1
            except Exception as exc:
                self._record_error(f"futures:book_socket_connect:{pair}", exc)
                try:
                    sio.disconnect()
                except Exception:
                    pass
        return started

    def _stop_futures_book_sockets(self) -> None:
        for pair, sio in self.futures_book_sockets.items():
            try:
                if sio.connected:
                    sio.disconnect()
            except Exception as exc:
                self._record_error(f"futures:book_socket_disconnect:{pair}", exc)
        self.futures_book_sockets.clear()

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

        futures_enabled = bool(self.cfg["coindcx"].get("futures_enabled", True))
        futures_cfg = self.cfg["coindcx"].get("futures_symbols", self.cfg["coindcx"]["symbols"])
        self.futures_pairs = {str(x["pair"]) for x in futures_cfg}
        self.futures_join_channels = []
        if futures_enabled:
            self.futures_join_channels = ["currentPrices@futures@rt"]
            for pair in sorted(self.futures_pairs):
                self.futures_join_channels.extend([
                    f"{pair}@trades-futures",
                    f"{pair}@prices-futures",
                ])
        self.join_channels = sorted(channels)
        self.connection_events.append({"time_utc": utc_iso(), "market": "spot", "state": "starting", "channels": sorted(channels)})
        if futures_enabled:
            self.connection_events.append({"time_utc": utc_iso(), "market": "futures", "state": "starting", "channels": self.futures_join_channels})

        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

        spot_connected = False
        try:
            self.sio.connect(
                self.cfg["coindcx"]["socket_url"],
                transports=["websocket"],
                wait_timeout=int(self.cfg["collector"].get("connect_timeout_seconds", 30)),
            )
            spot_connected = True
        except Exception as exc:
            self._record_error("spot:socket_connect", exc)
            return 2

        futures_connected = False
        if futures_enabled:
            futures_url = self.cfg["coindcx"].get("futures_socket_url")
            if not futures_url:
                self._record_error("futures:socket_config", "futures_socket_url is required when futures_enabled=true")
            else:
                try:
                    self.futures_sio.connect(
                        futures_url,
                        transports=["websocket"],
                        wait_timeout=int(self.cfg["collector"].get("connect_timeout_seconds", 30)),
                    )
                    futures_connected = True
                    book_started = self._start_futures_book_sockets()
                except Exception as exc:
                    self._record_error("futures:socket_connect", exc)

        deadline = time.monotonic() + self.duration_seconds
        heartbeat_every = int(self.cfg["collector"].get("heartbeat_seconds", 60))
        next_heartbeat = time.monotonic() + heartbeat_every
        while not self.stop_event.is_set() and time.monotonic() < deadline:
            self.sio.sleep(0.25)
            if futures_connected:
                self.futures_sio.sleep(0.25)
            now = time.monotonic()
            if now >= next_heartbeat:
                print(json.dumps({
                    "event": "heartbeat",
                    "time_utc": utc_iso(),
                    "elapsed_seconds": round(time.time() - self.start_epoch, 3),
                    "event_counts": dict(self.event_counts),
                    "futures_event_counts": dict(self.futures_event_counts),
                    "last_event_age_seconds": round(now - self.last_event_monotonic, 3),
                }), flush=True)
                next_heartbeat = now + heartbeat_every

        if spot_connected:
            try:
                self.sio.disconnect()
            except Exception as exc:
                self._record_error("spot:socket_disconnect", exc)
        if futures_connected:
            self._stop_futures_book_sockets()
            try:
                self.futures_sio.disconnect()
            except Exception as exc:
                self._record_error("futures:socket_disconnect", exc)

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
            "futures_streams": ["new-trade", "price-change", "current-prices", "depth-snapshot"] if futures_enabled else [],
            "futures_orderbook_channel_depth": int(self.cfg["coindcx"].get("futures_orderbook_depth", 50)),
            "orderbook_channel_depth": depth,
            "socket_url": self.cfg["coindcx"]["socket_url"],
            "futures_socket_url": self.cfg["coindcx"].get("futures_socket_url"),
            "python_socketio_version": socketio_version(),
            "join_channels": sorted(channels),
            "futures_join_channels": self.futures_join_channels,
            "join_emissions": self.join_emissions,
            "futures_join_emissions": self.futures_join_emissions,
            "connection_events": self.connection_events,
            "reconnect_count": self.reconnect_count,
            "futures_reconnect_count": self.futures_reconnect_count,
            "event_counts": dict(self.event_counts),
            "futures_event_counts": dict(self.futures_event_counts),
            "unknown_events": dict(self.unknown_events),
            "errors": self.errors,
            "notes": [
                "Raw depth payloads are preserved without assuming incremental semantics.",
                "Public futures trades/price changes are captured separately; futures current-prices are filtered to configured futures pairs.",
                "Futures orderbooks are captured as per-instrument depth snapshots on dedicated futures socket connections so the pair identity is not inferred from a multiplexed payload.",
                "Futures OI is not assumed or synthesized; only fields actually present in public payloads are preserved.",
                "Depth-derived imbalance/microprice features remain uncertified until depth semantics validation passes.",
            ],
        }
        (self.out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        futures_required = ["new-trade", "price-change", "current-prices", "depth-snapshot"] if futures_enabled else []
        futures_missing = [k for k in futures_required if self.futures_event_counts.get(k, 0) == 0]
        quality = {
            "status": "PASS" if not self.errors else "PASS_WITH_ERRORS",
            "required_event_counts": {k: int(self.event_counts.get(k, 0)) for k in self.EVENT_FILES},
            "futures_event_counts": dict(self.futures_event_counts),
            "futures_status": ("PASS" if futures_enabled and not futures_missing else "WARN_MISSING_FUTURES") if futures_enabled else "DISABLED",
            "missing_futures_streams": futures_missing,
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
