#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import logging
import platform
import signal
from importlib.metadata import PackageNotFoundError, version as package_version
import sys
import time
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import socketio

from .coindcx import SPOT_SOCKET, fetch_candles, fetch_market_details, fetch_orderbook, write_bootstrap
from .features import aggressor_side, orderbook_metrics

STOP = False


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_ms(ms: int | float) -> str:
    return datetime.fromtimestamp(float(ms) / 1000, tz=timezone.utc).isoformat()


def unwrap(response: Any) -> Any:
    if isinstance(response, dict) and "data" in response:
        return response["data"]
    return response


def safe_name(value: str) -> str:
    return value.replace("/", "_").replace("-", "_")

def get_socketio_version() -> str:
    """Return the installed python-socketio package version.

    python-socketio does not reliably expose ``__version__`` across releases,
    so read the distribution metadata instead.
    """
    try:
        return package_version("python-socketio")
    except PackageNotFoundError:
        return "unknown"



def write_jsonl(path: Path, obj: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n")


def build_run(args: argparse.Namespace) -> int:
    global STOP
    STOP = False
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "_" + uuid.uuid4().hex[:6]
    root = Path(args.output_root)
    run_dir = root / "raw" / run_id
    manifest_dir = root / "manifests"
    run_dir.mkdir(parents=True, exist_ok=True)
    manifest_dir.mkdir(parents=True, exist_ok=True)

    log_path = run_dir / "collector.log"
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler(sys.stdout)])

    def stop_handler(signum: int, _frame: Any) -> None:
        global STOP
        STOP = True
        logging.info("Stop requested by signal %s", signum)

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    state = defaultdict(lambda: {"buy_qty": 0.0, "sell_qty": 0.0, "buy_notional": 0.0, "sell_notional": 0.0})
    counts = Counter()
    last_exchange_ts: dict[str, int] = {}
    seen_trade_ids: set[str] = set()
    prev_book: dict[str, dict[str, Any]] = {}

    manifest: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "started_utc": datetime.now(timezone.utc).isoformat(),
        "started_ms": now_ms(),
        "product": "spot",
        "socket_url": SPOT_SOCKET,
        "pairs_requested": args.pair,
        "depth": args.depth,
        "candle_intervals": args.candle_interval,
        "host": platform.node(),
        "python": sys.version,
        "platform": platform.platform(),
        "socket_library": f"python-socketio {get_socketio_version()}",
        "config_source": args.config or None,
        "notes": "Research-only public market-data collector. No trading actions.",
    }
    (manifest_dir / f"{run_id}_started.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # Bootstrap metadata and state before opening the live stream.
    try:
        md = fetch_market_details(timeout=args.rest_timeout)
        (run_dir / "market_details.json").write_text(json.dumps(md, ensure_ascii=False, indent=2), encoding="utf-8")
        # Resolve the exact websocket channel pair from Market Details. CoinDCX
        # explicitly requires the `pair` field from Market Details for socket channels.
        # Keep the configured value for output naming, but subscribe using the canonical
        # market-details pair whenever we can resolve it.
        def norm_market_key(value: Any) -> str:
            if value is None:
                return ""
            return str(value).upper().replace("-", "").replace("_", "").replace("/", "")

        rows = [x for x in md if isinstance(x, dict)] if isinstance(md, list) else []
        pair_by_exact: dict[str, str] = {}
        pair_by_norm: dict[str, str] = {}
        for row in rows:
            mpair = row.get("pair")
            symbol = row.get("symbol")
            if mpair:
                pair_by_exact[str(mpair).upper()] = str(mpair)
                pair_by_norm[norm_market_key(mpair)] = str(mpair)
            if symbol and mpair:
                pair_by_norm[norm_market_key(symbol)] = str(mpair)

        resolved_pairs: dict[str, str] = {}
        missing: list[str] = []
        for configured in args.pair:
            canonical = pair_by_exact.get(str(configured).upper())
            if canonical is None:
                canonical = pair_by_norm.get(norm_market_key(configured))
            if canonical is None:
                missing.append(configured)
            else:
                resolved_pairs[configured] = canonical

        if missing:
            write_jsonl(run_dir / "data_quality.jsonl", {"event": "pair_validation_warning", "recv_ts_ms": now_ms(), "missing_pairs": missing})
            logging.warning("Some requested pairs were not matched in market metadata: %s", missing)
        else:
            logging.info("Resolved websocket pairs: %s", resolved_pairs)
    except Exception as exc:
        logging.exception("Market details bootstrap failed: %s", exc)
        write_jsonl(run_dir / "data_quality.jsonl", {"event": "bootstrap_error", "kind": "market_details", "recv_ts_ms": now_ms(), "error": repr(exc)})

    for pair in args.pair:
        request_pair = resolved_pairs.get(pair, pair)
        payload: dict[str, Any] = {"pair": pair, "resolved_pair": request_pair, "captured_ms": now_ms(), "orderbook_50": None, "candles": {}}
        try:
            payload["orderbook_50"] = fetch_orderbook(request_pair, depth=50, timeout=args.rest_timeout)
        except Exception as exc:
            payload["orderbook_error"] = repr(exc)
        for interval in args.candle_interval:
            try:
                payload["candles"][interval] = fetch_candles(request_pair, interval, limit=args.bootstrap_candles, timeout=args.rest_timeout)
            except Exception as exc:
                payload["candles"][interval] = {"error": repr(exc)}
        write_bootstrap(run_dir / f"{safe_name(pair)}_bootstrap.json", payload)

    sio = socketio.Client(
        reconnection=True,
        reconnection_attempts=0,
        reconnection_delay=args.reconnect_delay,
        reconnection_delay_max=args.reconnect_max_delay,
        logger=False,
        engineio_logger=False,
    )

    def quality(event: str, **extra: Any) -> None:
        rec = {"event": event, "recv_ts_ms": now_ms(), "recv_utc": datetime.now(timezone.utc).isoformat(), **extra}
        write_jsonl(run_dir / "data_quality.jsonl", rec)

    @sio.event
    def connect() -> None:
        quality("connected")
        logging.info("Connected")
        for configured_pair in args.pair:
            pair = resolved_pairs.get(configured_pair, configured_pair)
            channels = [f"{pair}@trades", f"{pair}@orderbook@50"]
            if args.capture_price_channel:
                channels.append(f"{pair}@prices")
            for interval in args.candle_interval:
                channels.append(f"{pair}_{interval}")
            for channel in channels:
                try:
                    def _join_ack(*ack: Any, _channel: str = channel) -> None:
                        quality("join_ack", channel=_channel, ack=ack)
                        logging.info("Join ack %s: %s", _channel, ack)
                    sio.emit("join", {"channelName": channel}, callback=_join_ack)
                    quality("join_emitted", channel=channel)
                except Exception as exc:
                    quality("join_error", channel=channel, error=repr(exc))
                    logging.exception("Join failed for %s: %s", channel, exc)

    @sio.event
    def disconnect() -> None:
        quality("disconnected")
        logging.warning("Disconnected")

    @sio.on("new-trade")
    def on_trade(response: Any) -> None:
        data = unwrap(response)
        if not isinstance(data, dict):
            return
        pair = data.get("pair") or data.get("s")
        # The feed can return a symbol such as BTCUSDT; normalize to configured pair where possible.
        configured_pair = next((p for p in args.pair if p.replace("B-", "").replace("_", "") == str(pair).upper()), pair)
        ts_raw = data.get("T") or data.get("timestamp")
        try:
            ts = int(float(ts_raw))
            p = float(data.get("p") or data.get("price"))
            q = float(data.get("q") or data.get("quantity"))
        except (TypeError, ValueError):
            quality("malformed_trade", raw=data)
            return
        side = aggressor_side(data.get("m"))
        st = state[configured_pair]
        notional = p * q
        if side == "BUY":
            st["buy_qty"] += q; st["buy_notional"] += notional
        elif side == "SELL":
            st["sell_qty"] += q; st["sell_notional"] += notional
        total = st["buy_qty"] + st["sell_qty"]
        delta = st["buy_qty"] - st["sell_qty"]
        ratio = delta / total if total else 0.0
        trade_id = data.get("t") or data.get("trade_id") or data.get("id")
        duplicate = False
        if trade_id is not None:
            tid = f"{configured_pair}:{trade_id}"
            duplicate = tid in seen_trade_ids
            seen_trade_ids.add(tid)
        last = last_exchange_ts.get(f"trade:{configured_pair}")
        nonmono = last is not None and ts < last
        last_exchange_ts[f"trade:{configured_pair}"] = max(ts, last or ts)
        rec = {
            "event": "trade",
            "run_id": run_id,
            "recv_ts_ms": now_ms(),
            "exchange_ts_ms": ts,
            "exchange_utc": iso_ms(ts),
            "local_exchange_latency_ms": now_ms() - ts,
            "pair": configured_pair,
            "symbol_raw": data.get("s"),
            "price": p,
            "quantity": q,
            "notional": notional,
            "maker_buyer": data.get("m"),
            "aggressor_side": side,
            "trade_id": trade_id,
            "duplicate_trade_id": duplicate,
            "non_monotonic_exchange_timestamp": nonmono,
            "cum_buy_qty": st["buy_qty"],
            "cum_sell_qty": st["sell_qty"],
            "cum_delta_qty": delta,
            "cum_delta_ratio": ratio,
            "cum_buy_notional": st["buy_notional"],
            "cum_sell_notional": st["sell_notional"],
            "cum_delta_notional": st["buy_notional"] - st["sell_notional"],
            "raw": data,
        }
        write_jsonl(run_dir / f"{safe_name(configured_pair)}_trades.jsonl", rec)
        counts["trades"] += 1

    @sio.on("depth-snapshot")
    def on_depth(response: Any) -> None:
        data = unwrap(response)
        if not isinstance(data, dict):
            return
        pair = data.get("s") or data.get("symbol")
        configured_pair = next((p for p in args.pair if p.replace("B-", "").replace("_", "") == str(pair).upper()), pair)
        try:
            ts = int(float(data.get("ts") or data.get("T") or now_ms()))
        except (TypeError, ValueError):
            ts = now_ms()
        bids = data.get("bids") or {}
        asks = data.get("asks") or {}
        metrics = orderbook_metrics(bids, asks)
        old = prev_book.get(str(configured_pair))
        if old:
            metrics["mid_change"] = (metrics.get("mid") - old.get("mid")) if metrics.get("mid") is not None and old.get("mid") is not None else None
            metrics["spread_change"] = (metrics.get("spread") - old.get("spread")) if metrics.get("spread") is not None and old.get("spread") is not None else None
            for n in (1, 5, 10, 20, 50):
                metrics[f"bid_qty_change_{n}"] = metrics[f"bid_qty_{n}"] - old.get(f"bid_qty_{n}", metrics[f"bid_qty_{n}"])
                metrics[f"ask_qty_change_{n}"] = metrics[f"ask_qty_{n}"] - old.get(f"ask_qty_{n}", metrics[f"ask_qty_{n}"])
        else:
            for key in ("mid_change", "spread_change", *[f"bid_qty_change_{n}" for n in (1,5,10,20,50)], *[f"ask_qty_change_{n}" for n in (1,5,10,20,50)]):
                metrics[key] = None
        prev_book[str(configured_pair)] = metrics.copy()
        version = data.get("vs")
        last = last_exchange_ts.get(f"depth:{configured_pair}")
        nonmono = last is not None and ts < last
        last_exchange_ts[f"depth:{configured_pair}"] = max(ts, last or ts)
        rec = {
            "event": "depth_snapshot",
            "run_id": run_id,
            "recv_ts_ms": now_ms(),
            "exchange_ts_ms": ts,
            "exchange_utc": iso_ms(ts),
            "local_exchange_latency_ms": now_ms() - ts,
            "pair": configured_pair,
            "version": version,
            "non_monotonic_exchange_timestamp": nonmono,
            **metrics,
            "bids": bids,
            "asks": asks,
            "raw": data,
        }
        write_jsonl(run_dir / f"{safe_name(str(configured_pair))}_depth.jsonl", rec)
        counts["depth"] += 1

    @sio.on("price-change")
    def on_price(response: Any) -> None:
        data = unwrap(response)
        if not isinstance(data, dict):
            return
        rec = {"event": "price_change", "run_id": run_id, "recv_ts_ms": now_ms(), "raw": data}
        ts = data.get("T")
        try:
            if ts is not None:
                rec["exchange_ts_ms"] = int(float(ts)); rec["exchange_utc"] = iso_ms(rec["exchange_ts_ms"])
                rec["local_exchange_latency_ms"] = now_ms() - rec["exchange_ts_ms"]
        except (TypeError, ValueError):
            pass
        symbol = data.get("s") or data.get("symbol")
        rec["symbol"] = symbol; rec["price"] = data.get("p") or data.get("price")
        pair = next((p for p in args.pair if p.replace("B-", "").replace("_", "") == str(symbol).upper()), symbol)
        rec["pair"] = pair
        write_jsonl(run_dir / f"{safe_name(str(pair))}_prices.jsonl", rec)
        counts["prices"] += 1

    @sio.on("candlestick")
    def on_candle(response: Any) -> None:
        data = unwrap(response)
        rows = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
        for row in rows:
            if not isinstance(row, dict):
                continue
            interval = row.get("i") or row.get("interval") or "unknown"
            symbol = row.get("s") or row.get("symbol")
            pair = next((p for p in args.pair if p.replace("B-", "").replace("_", "") == str(symbol).upper()), symbol)
            rec = {"event": "candle", "run_id": run_id, "recv_ts_ms": now_ms(), "pair": pair, "interval": interval, "raw": row}
            write_jsonl(run_dir / f"{safe_name(str(pair))}_candles_{interval}.jsonl", rec)
            counts["candles"] += 1

    @sio.on("*")
    def on_any_event(event: str, response: Any) -> None:
        # Diagnostic visibility for unexpected server events. Known data events are
        # already handled above; this is intentionally lightweight.
        if event not in {"new-trade", "depth-snapshot", "price-change", "candlestick", "connect", "disconnect"}:
            quality("socket_event", socket_event=event, raw=response)
            logging.info("Socket event %s: %s", event, response)

    start_monotonic = time.monotonic()
    quality("collector_started", run_id=run_id, pid=__import__("os").getpid())
    try:
        while not STOP:
            if args.minutes and time.monotonic() - start_monotonic >= args.minutes * 60:
                STOP = True
                break
            if not sio.connected:
                try:
                    quality("connect_attempt")
                    sio.connect(SPOT_SOCKET, transports=["websocket"], wait_timeout=20)
                except Exception as exc:
                    quality("connect_error", error=repr(exc))
                    logging.exception("Connect failed: %s", exc)
                    time.sleep(args.reconnect_delay)
                    continue
            time.sleep(args.heartbeat_seconds)
            quality("heartbeat", connected=sio.connected, event_counts=dict(counts))
    finally:
        if sio.connected:
            for configured_pair in args.pair:
                pair = resolved_pairs.get(configured_pair, configured_pair)
                channels = [f"{pair}@trades", f"{pair}@orderbook@50"]
                if args.capture_price_channel:
                    channels.append(f"{pair}@prices")
                for interval in args.candle_interval:
                    channels.append(f"{pair}_{interval}")
                for channel in channels:
                    try: sio.emit("leave", {"channelName": channel})
                    except Exception: pass
            try: sio.disconnect()
            except Exception: pass
        manifest.update({
            "ended_utc": datetime.now(timezone.utc).isoformat(),
            "ended_ms": now_ms(),
            "event_counts": dict(counts),
            "run_dir": str(run_dir),
        })
        (manifest_dir / f"{run_id}.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        quality("collector_stopped", event_counts=dict(counts))
        logging.info("Stopped. Data written to %s", run_dir.resolve())
    return 0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CoinDCX public spot Delta/order-flow collector")
    p.add_argument("--config")
    p.add_argument("--pair", action="append")
    p.add_argument("--depth", type=int, choices=[50], default=50)
    p.add_argument("--candle-interval", action="append", choices=["1m", "15m", "1h", "1d"])
    p.add_argument("--bootstrap-candles", type=int, default=1000)
    p.add_argument("--output-root", default="data")
    p.add_argument("--minutes", type=float, default=0)
    p.add_argument("--rest-timeout", type=int, default=15)
    p.add_argument("--reconnect-delay", type=float, default=3)
    p.add_argument("--reconnect-max-delay", type=float, default=30)
    p.add_argument("--heartbeat-seconds", type=float, default=10)
    p.add_argument("--capture-price-channel", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.config:
        cfg = json.loads(Path(args.config).read_text(encoding="utf-8"))
        if args.pair is None: args.pair = cfg["pairs"]
        if args.candle_interval is None: args.candle_interval = cfg.get("candle_intervals", ["1m", "15m", "1h", "1d"])
        args.depth = 50
        args.bootstrap_candles = cfg.get("bootstrap_candles", 1000)
        args.output_root = cfg.get("output_root", "data")
        args.rest_timeout = cfg.get("rest_timeout_seconds", 15)
        args.reconnect_delay = cfg.get("reconnect_delay_seconds", 3)
        args.reconnect_max_delay = cfg.get("reconnect_max_delay_seconds", 30)
        args.heartbeat_seconds = cfg.get("heartbeat_seconds", 10)
        if not getattr(args, "capture_price_channel", False):
            args.capture_price_channel = bool(cfg.get("capture_price_channel", True))
    if not args.pair:
        args.pair = ["B-BTC_USDT", "B-ETH_USDT", "B-SOL_USDT", "B-XRP_USDT"]
    if not args.candle_interval:
        args.candle_interval = ["1m", "15m", "1h", "1d"]
    return build_run(args)


if __name__ == "__main__":
    raise SystemExit(main())
