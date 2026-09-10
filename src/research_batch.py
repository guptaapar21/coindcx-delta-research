#!/usr/bin/env python3
"""Build compact CoinDCX research layers from one collector batch.

The Spot depth layer uses the empirically validated absolute price-level
replacement semantics with version-gap guarding. Futures orderbooks are full
depth snapshots and are not reconstructed incrementally.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

WINDOWS = (5, 15, 30, 60, 180)
FORWARD_HORIZONS = (5, 15, 30, 60, 180, 300, 600, 900, 1800)
BOOK_LEVELS = (1, 5, 10, 20)
BOOK_STATUS = "VALIDATED_ABSOLUTE_UPDATES_WITH_GAP_GUARD"
FUTURES_BOOK_STATUS = "FUTURES_DEPTH_SNAPSHOT"
SOURCE_SCHEMA = "research_batch_v6"


def read_gz_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def extract_payload_data(rec: dict[str, Any]) -> dict[str, Any]:
    raw = rec.get("raw", {})
    data = raw.get("data") if isinstance(raw, dict) else None
    if isinstance(data, dict):
        return data
    if isinstance(data, str):
        try:
            decoded = json.loads(data)
            if isinstance(decoded, dict):
                return decoded
        except (TypeError, ValueError):
            pass
    return raw if isinstance(raw, dict) else {}


def safe_float(x: Any) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def canonical_symbol(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).upper()
    return {
        "B-BTC_USDT": "B-BTC_USDT",
        "BTCUSDT": "B-BTC_USDT",
        "B-ETH_USDT": "B-ETH_USDT",
        "ETHUSDT": "B-ETH_USDT",
    }.get(s, s if s else None)


def sec_bucket(ms: int) -> int:
    return ms // 1000


def iso_sec(s: int) -> str:
    return datetime.fromtimestamp(s, tz=timezone.utc).isoformat()


def _levels(d: dict[str, Any], key: str) -> dict[str, float]:
    value = d.get(key)
    if not isinstance(value, dict):
        return {}
    out: dict[str, float] = {}
    for price, qty in value.items():
        p = safe_float(price)
        q = safe_float(qty)
        if p is not None and q is not None:
            out[str(price)] = q
    return out


def _depth_event_ts(d: dict[str, Any], rec: dict[str, Any]) -> int:
    value = d.get("ts", d.get("T", rec.get("exchange_timestamp_ms", rec.get("received_at_ms", 0))))
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(rec.get("received_at_ms", 0) or 0)


def _depth_version(d: dict[str, Any]) -> int | None:
    value = d.get("vs")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _apply_absolute(book: dict[str, dict[str, float]], d: dict[str, Any]) -> tuple[float, float, int]:
    added = 0.0
    removed = 0.0
    touched = 0
    for key in ("bids", "asks"):
        levels = _levels(d, key)
        side = book[key]
        for price, qty in levels.items():
            old = side.get(price)
            touched += 1
            if qty <= 0.0:
                if old is not None:
                    removed += abs(old)
                side.pop(price, None)
            else:
                if old is None:
                    added += qty
                elif qty > old:
                    added += qty - old
                elif qty < old:
                    removed += old - qty
                side[price] = qty
    return added, removed, touched


def _book_metrics(book: dict[str, dict[str, float]]) -> dict[str, Any]:
    bids = sorted(((float(p), q) for p, q in book["bids"].items()), key=lambda x: x[0], reverse=True)
    asks = sorted(((float(p), q) for p, q in book["asks"].items()), key=lambda x: x[0])
    out: dict[str, Any] = {
        "book_valid": False,
        "book_bid_qty_1": None,
        "book_ask_qty_1": None,
        "book_bid_qty_5": None,
        "book_ask_qty_5": None,
        "book_bid_qty_10": None,
        "book_ask_qty_10": None,
        "book_bid_qty_20": None,
        "book_ask_qty_20": None,
        "book_imbalance_1": None,
        "book_imbalance_5": None,
        "book_imbalance_10": None,
        "book_imbalance_20": None,
        "best_bid": None,
        "best_ask": None,
        "mid_price": None,
        "spread_abs": None,
        "spread_bps": None,
        "microprice": None,
    }
    if not bids or not asks:
        return out
    best_bid, bid1 = bids[0]
    best_ask, ask1 = asks[0]
    mid = (best_bid + best_ask) / 2.0
    out.update({
        "best_bid": best_bid,
        "best_ask": best_ask,
        "mid_price": mid,
        "spread_abs": best_ask - best_bid,
        "spread_bps": (best_ask - best_bid) / mid * 10000 if mid else None,
        "microprice": (best_ask * bid1 + best_bid * ask1) / (bid1 + ask1) if (bid1 + ask1) else mid,
    })
    for n in BOOK_LEVELS:
        bq = sum(q for _, q in bids[:n])
        aq = sum(q for _, q in asks[:n])
        out[f"book_bid_qty_{n}"] = bq
        out[f"book_ask_qty_{n}"] = aq
        out[f"book_imbalance_{n}"] = (bq - aq) / (bq + aq) if (bq + aq) else None
    out["book_valid"] = True
    return out


def _load_depth_seconds(batch: Path) -> dict[tuple[str, int], dict[str, Any]]:
    events: list[tuple[int, int, str, dict[str, Any], dict[str, Any]]] = []
    for filename, kind, order in (("depth_snapshot.jsonl.gz", "snapshot", 0), ("depth_update.jsonl.gz", "update", 1)):
        path = batch / filename
        if not path.exists():
            continue
        for rec in read_gz_jsonl(path):
            d = extract_payload_data(rec)
            ts = _depth_event_ts(d, rec)
            events.append((ts, order, kind, d, rec))
    events.sort(key=lambda x: (x[0], x[1]))

    states: dict[str, dict[str, dict[str, float]]] = {}
    last_version: dict[str, int] = {}
    valid: dict[str, bool] = {}
    out: dict[tuple[str, int], dict[str, Any]] = {}

    for ts, _, kind, d, rec in events:
        symbol = canonical_symbol(d.get("s") or rec.get("pair"))
        if symbol is None:
            continue
        sec = sec_bucket(ts)
        if symbol not in states:
            states[symbol] = {"bids": {}, "asks": {}}
            valid[symbol] = False
        if kind == "snapshot":
            states[symbol]["bids"] = _levels(d, "bids")
            states[symbol]["asks"] = _levels(d, "asks")
            valid[symbol] = True
        else:
            version = _depth_version(d)
            previous = last_version.get(symbol)
            if previous is not None and version is not None and version != previous + 1:
                valid[symbol] = False
            _apply_absolute(states[symbol], d)
        version = _depth_version(d)
        if version is not None:
            last_version[symbol] = version
        metrics = _book_metrics(states[symbol])
        if not valid[symbol]:
            metrics["book_valid"] = False
        metrics.update({
            "book_features_status": BOOK_STATUS,
            "depth_event_type": kind,
            "depth_version": version,
            "depth_update_touched_levels": len(_levels(d, "bids")) + len(_levels(d, "asks")) if kind == "update" else None,
        })
        out[(symbol, sec)] = metrics

    counts: dict[tuple[str, int], dict[str, int]] = defaultdict(lambda: {"updates": 0, "snapshots": 0})
    for filename, kind in (("depth_update.jsonl.gz", "updates"), ("depth_snapshot.jsonl.gz", "snapshots")):
        path = batch / filename
        if not path.exists():
            continue
        for rec in read_gz_jsonl(path):
            d = extract_payload_data(rec)
            ts = _depth_event_ts(d, rec)
            symbol = canonical_symbol(d.get("s") or rec.get("pair"))
            if symbol:
                counts[(symbol, sec_bucket(ts))][kind] += 1
    for key, c in counts.items():
        out.setdefault(key, {"book_valid": False, "book_features_status": BOOK_STATUS})
        out[key]["depth_update_events"] = c["updates"]
        out[key]["depth_snapshot_events"] = c["snapshots"]
    return out


def _load_futures_depth_seconds(batch: Path) -> dict[tuple[str, int], dict[str, Any]]:
    path = batch / "futures_depth_snapshot.jsonl.gz"
    if not path.exists():
        return {}
    events: list[tuple[int, str, dict[str, Any]]] = []
    for rec in read_gz_jsonl(path):
        d = extract_payload_data(rec)
        if str(d.get("pr", "")).lower() not in {"f", "futures"}:
            continue
        ts = _depth_event_ts(d, rec)
        symbol = canonical_symbol(d.get("s") or rec.get("pair"))
        if symbol is not None:
            events.append((ts, symbol, d))
    events.sort(key=lambda x: (x[1], x[0]))
    out: dict[tuple[str, int], dict[str, Any]] = {}
    previous: dict[str, dict[str, Any]] = {}
    for ts, symbol, d in events:
        book = {"bids": _levels(d, "bids"), "asks": _levels(d, "asks")}
        base = _book_metrics(book)
        metrics = {
            "futures_best_bid": base.get("best_bid"),
            "futures_best_ask": base.get("best_ask"),
            "futures_mid_price": base.get("mid_price"),
            "futures_spread_abs": base.get("spread_abs"),
            "futures_spread_bps": base.get("spread_bps"),
            "futures_microprice": base.get("microprice"),
            "futures_book_features_status": FUTURES_BOOK_STATUS,
            "futures_book_valid": base.get("book_valid"),
            "futures_depth_snapshot_events": 1,
            "futures_depth_version": _depth_version(d),
        }
        for level in BOOK_LEVELS:
            metrics[f"futures_book_bid_qty_{level}"] = base.get(f"book_bid_qty_{level}")
            metrics[f"futures_book_ask_qty_{level}"] = base.get(f"book_ask_qty_{level}")
            metrics[f"futures_book_imbalance_{level}"] = base.get(f"book_imbalance_{level}")
        prev = previous.get(symbol)
        if prev:
            for level in BOOK_LEVELS:
                metrics[f"futures_book_bid_qty_{level}_change"] = (metrics.get(f"futures_book_bid_qty_{level}") or 0.0) - (prev.get(f"futures_book_bid_qty_{level}") or 0.0)
                metrics[f"futures_book_ask_qty_{level}_change"] = (metrics.get(f"futures_book_ask_qty_{level}") or 0.0) - (prev.get(f"futures_book_ask_qty_{level}") or 0.0)
                metrics[f"futures_book_imbalance_{level}_change"] = (metrics.get(f"futures_book_imbalance_{level}") or 0.0) - (prev.get(f"futures_book_imbalance_{level}") or 0.0)
            if metrics.get("futures_spread_bps") is not None and prev.get("futures_spread_bps") is not None:
                metrics["futures_spread_bps_change"] = metrics["futures_spread_bps"] - prev["futures_spread_bps"]
            if metrics.get("futures_microprice") is not None and prev.get("futures_microprice"):
                metrics["futures_microprice_return"] = metrics["futures_microprice"] / prev["futures_microprice"] - 1.0
        previous[symbol] = metrics
        out[(symbol, sec_bucket(ts))] = metrics
    return out


def _load_trade_seconds(batch: Path, filename: str, futures: bool = False) -> dict[str, dict[int, dict[str, float]]]:
    out: dict[str, dict[int, dict[str, float]]] = defaultdict(lambda: defaultdict(dict))
    path = batch / filename
    if not path.exists():
        return out
    for rec in read_gz_jsonl(path):
        data = extract_payload_data(rec)
        if futures and str(data.get("pr", "")).lower() not in {"f", "futures"}:
            continue
        symbol = canonical_symbol(data.get("s") or rec.get("pair"))
        if symbol is None:
            continue
        ts = data.get("T", rec.get("exchange_timestamp_ms", rec.get("received_at_ms")))
        try:
            t_ms = int(ts)
        except (TypeError, ValueError):
            continue
        p = safe_float(data.get("p"))
        q = safe_float(data.get("q"))
        if p is None or q is None:
            continue
        bucket = out[symbol].setdefault(sec_bucket(t_ms), {})
        bucket["trade_count"] = bucket.get("trade_count", 0) + 1
        bucket["total_qty"] = bucket.get("total_qty", 0.0) + q
        bucket["total_notional"] = bucket.get("total_notional", 0.0) + p * q
        if bool(data.get("m")):
            bucket["aggressive_sell_qty"] = bucket.get("aggressive_sell_qty", 0.0) + q
            bucket["aggressive_sell_notional"] = bucket.get("aggressive_sell_notional", 0.0) + p * q
        else:
            bucket["aggressive_buy_qty"] = bucket.get("aggressive_buy_qty", 0.0) + q
            bucket["aggressive_buy_notional"] = bucket.get("aggressive_buy_notional", 0.0) + p * q
        bucket["last_trade_price"] = p
        bucket["last_trade_exchange_ms"] = t_ms
    return out


def _mean(values: list[Any]) -> float | None:
    nums = [float(v) for v in values if v is not None]
    return sum(nums) / len(nums) if nums else None


def build_seconds(batch: Path) -> list[dict[str, Any]]:
    spot = _load_trade_seconds(batch, "trades.jsonl.gz")
    futures = _load_trade_seconds(batch, "futures_trades.jsonl.gz", futures=True)
    spot_depth = _load_depth_seconds(batch)
    futures_depth = _load_futures_depth_seconds(batch)
    symbols = sorted(set(spot) | set(futures) | {k[0] for k in spot_depth} | {k[0] for k in futures_depth})
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        seconds = sorted(set(spot.get(symbol, {})) | set(futures.get(symbol, {})) | {k[1] for k in spot_depth if k[0] == symbol} | {k[1] for k in futures_depth if k[0] == symbol})
        if not seconds:
            continue
        spot_prices = [(sec, b.get("last_trade_price")) for sec, b in sorted(spot.get(symbol, {}).items()) if b.get("last_trade_price") is not None]
        futures_prices = [(sec, b.get("last_trade_price")) for sec, b in sorted(futures.get(symbol, {}).items()) if b.get("last_trade_price") is not None]
        for sec in seconds:
            sb = spot.get(symbol, {}).get(sec, {})
            fb = futures.get(symbol, {}).get(sec, {})
            price = sb.get("last_trade_price")
            if price is None:
                prior = [p for t, p in spot_prices if t <= sec]
                price = prior[-1] if prior else None
            fprice = fb.get("last_trade_price")
            if fprice is None:
                prior_f = [p for t, p in futures_prices if t <= sec]
                fprice = prior_f[-1] if prior_f else None
            delta = float(sb.get("aggressive_buy_qty", 0.0)) - float(sb.get("aggressive_sell_qty", 0.0))
            fdelta = float(fb.get("aggressive_buy_qty", 0.0)) - float(fb.get("aggressive_sell_qty", 0.0))
            row: dict[str, Any] = {
                "symbol": symbol,
                "epoch_second": sec,
                "utc_second": iso_sec(sec),
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "trade_count": int(sb.get("trade_count", 0)),
                "aggressive_buy_qty": float(sb.get("aggressive_buy_qty", 0.0)),
                "aggressive_sell_qty": float(sb.get("aggressive_sell_qty", 0.0)),
                "aggressive_buy_notional": float(sb.get("aggressive_buy_notional", 0.0)),
                "aggressive_sell_notional": float(sb.get("aggressive_sell_notional", 0.0)),
                "delta_qty": delta,
                "delta_notional": float(sb.get("aggressive_buy_notional", 0.0)) - float(sb.get("aggressive_sell_notional", 0.0)),
                "total_qty": float(sb.get("total_qty", 0.0)),
                "total_notional": float(sb.get("total_notional", 0.0)),
                "last_trade_price": price,
                "futures_trade_count": int(fb.get("trade_count", 0)),
                "futures_aggressive_buy_qty": float(fb.get("aggressive_buy_qty", 0.0)),
                "futures_aggressive_sell_qty": float(fb.get("aggressive_sell_qty", 0.0)),
                "futures_aggressive_buy_notional": float(fb.get("aggressive_buy_notional", 0.0)),
                "futures_aggressive_sell_notional": float(fb.get("aggressive_sell_notional", 0.0)),
                "futures_delta_qty": fdelta,
                "futures_delta_notional": float(fb.get("aggressive_buy_notional", 0.0)) - float(fb.get("aggressive_sell_notional", 0.0)),
                "futures_total_qty": float(fb.get("total_qty", 0.0)),
                "futures_last_trade_price": fprice,
                "depth_update_events": 0,
                "depth_snapshot_events": 0,
                "book_features_status": BOOK_STATUS,
                "futures_book_features_status": FUTURES_BOOK_STATUS,
                "source_schema": SOURCE_SCHEMA,
            }
            if price is not None:
                row["open"] = row["high"] = row["low"] = row["close"] = price
            row.update(spot_depth.get((symbol, sec), {}))
            row.update(futures_depth.get((symbol, sec), {}))
            for w in WINDOWS:
                cutoff = sec - w + 1
                srows = [b for t, b in spot.get(symbol, {}).items() if cutoff <= t <= sec]
                frows = [b for t, b in futures.get(symbol, {}).items() if cutoff <= t <= sec]
                buy = sum(float(b.get("aggressive_buy_qty", 0.0)) for b in srows)
                sell = sum(float(b.get("aggressive_sell_qty", 0.0)) for b in srows)
                total = sum(float(b.get("total_qty", 0.0)) for b in srows)
                fbuy = sum(float(b.get("aggressive_buy_qty", 0.0)) for b in frows)
                fsell = sum(float(b.get("aggressive_sell_qty", 0.0)) for b in frows)
                ftotal = sum(float(b.get("total_qty", 0.0)) for b in frows)
                row[f"buy_qty_{w}s"] = buy
                row[f"sell_qty_{w}s"] = sell
                row[f"delta_qty_{w}s"] = buy - sell
                row[f"delta_ratio_{w}s"] = (buy - sell) / total if total else None
                row[f"futures_buy_qty_{w}s"] = fbuy
                row[f"futures_sell_qty_{w}s"] = fsell
                row[f"futures_delta_qty_{w}s"] = fbuy - fsell
                row[f"futures_delta_ratio_{w}s"] = (fbuy - fsell) / ftotal if ftotal else None
                row[f"futures_spot_delta_divergence_{w}s"] = (fbuy - fsell) - (buy - sell) if (total or ftotal) else None
                start_prices = [p for t, p in spot_prices if cutoff <= t <= sec]
                start_fprices = [p for t, p in futures_prices if cutoff <= t <= sec]
                row[f"price_return_{w}s"] = (price / start_prices[0] - 1.0) if price and start_prices and start_prices[0] else None
                row[f"futures_price_return_{w}s"] = (fprice / start_fprices[0] - 1.0) if fprice and start_fprices and start_fprices[0] else None
            rows.append(row)

        symbol_rows = [r for r in rows if r["symbol"] == symbol]
        by_epoch = {r["epoch_second"]: r["close"] for r in symbol_rows if r.get("close") is not None}
        for row in symbol_rows:
            base = by_epoch.get(row["epoch_second"])
            for horizon in FORWARD_HORIZONS:
                future = by_epoch.get(row["epoch_second"] + horizon)
                row[f"forward_return_{horizon}s"] = (future / base - 1.0) if future is not None and base else None
    return rows


def aggregate_1m(seconds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for r in seconds:
        epoch = int(r["epoch_second"])
        grouped[(r["symbol"], epoch - epoch % 60)].append(r)
    out: list[dict[str, Any]] = []
    for (symbol, epoch), rs in sorted(grouped.items()):
        rs.sort(key=lambda x: x["epoch_second"])
        close_rows = [r for r in rs if r.get("close") is not None]
        if not close_rows:
            continue
        total_qty = sum(float(r.get("total_qty") or 0.0) for r in rs)
        delta_qty = sum(float(r.get("delta_qty") or 0.0) for r in rs)
        row: dict[str, Any] = {
            "symbol": symbol,
            "minute_epoch": epoch,
            "minute_utc": iso_sec(epoch),
            "open": close_rows[0].get("open"),
            "high": max(float(r["high"]) for r in close_rows if r.get("high") is not None),
            "low": min(float(r["low"]) for r in close_rows if r.get("low") is not None),
            "close": close_rows[-1].get("close"),
            "trade_count": sum(int(r.get("trade_count") or 0) for r in rs),
            "aggressive_buy_qty": sum(float(r.get("aggressive_buy_qty") or 0.0) for r in rs),
            "aggressive_sell_qty": sum(float(r.get("aggressive_sell_qty") or 0.0) for r in rs),
            "delta_qty": delta_qty,
            "delta_notional": sum(float(r.get("delta_notional") or 0.0) for r in rs),
            "total_qty": total_qty,
            "delta_ratio": delta_qty / total_qty if total_qty else None,
            "depth_update_events": sum(int(r.get("depth_update_events") or 0) for r in rs),
            "depth_snapshot_events": sum(int(r.get("depth_snapshot_events") or 0) for r in rs),
            "book_valid_seconds": sum(1 for r in rs if r.get("book_valid")),
            "futures_trade_count": sum(int(r.get("futures_trade_count") or 0) for r in rs),
            "futures_aggressive_buy_qty": sum(float(r.get("futures_aggressive_buy_qty") or 0.0) for r in rs),
            "futures_aggressive_sell_qty": sum(float(r.get("futures_aggressive_sell_qty") or 0.0) for r in rs),
            "futures_delta_qty": sum(float(r.get("futures_delta_qty") or 0.0) for r in rs),
            "futures_delta_notional": sum(float(r.get("futures_delta_notional") or 0.0) for r in rs),
            "futures_total_qty": sum(float(r.get("futures_total_qty") or 0.0) for r in rs),
            "futures_depth_snapshot_events": sum(int(r.get("futures_depth_snapshot_events") or 0) for r in rs),
            "futures_book_valid_seconds": sum(1 for r in rs if r.get("futures_book_valid")),
            "futures_last_trade_price": next((r.get("futures_last_trade_price") for r in reversed(rs) if r.get("futures_last_trade_price") is not None), None),
            "book_features_status": BOOK_STATUS,
            "source_schema": SOURCE_SCHEMA,
        }
        row["futures_delta_ratio"] = row["futures_delta_qty"] / row["futures_total_qty"] if row["futures_total_qty"] else None
        for key in ("book_imbalance_1", "book_imbalance_5", "book_imbalance_10", "book_imbalance_20", "spread_bps", "microprice", "futures_best_bid", "futures_best_ask", "futures_mid_price", "futures_spread_bps", "futures_microprice", "futures_book_imbalance_1", "futures_book_imbalance_5", "futures_book_imbalance_10", "futures_book_imbalance_20", "futures_book_imbalance_1_change", "futures_book_imbalance_5_change", "futures_spread_bps_change", "futures_microprice_return"):
            row[key + "_mean"] = _mean([r.get(key) for r in rs])
        out.append(row)
    return out


def aggregate_3m(minute_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], dict[int, dict[str, Any]]] = defaultdict(dict)
    for r in minute_rows:
        epoch = int(r["minute_epoch"])
        bucket = epoch - epoch % 180
        grouped[(r["symbol"], bucket)][epoch] = r
    out: list[dict[str, Any]] = []
    for (symbol, bucket), minute_map in sorted(grouped.items()):
        expected = [bucket, bucket + 60, bucket + 120]
        if any(epoch not in minute_map for epoch in expected):
            continue
        rs = [minute_map[e] for e in expected]
        total_qty = sum(float(r.get("total_qty") or 0.0) for r in rs)
        delta_qty = sum(float(r.get("delta_qty") or 0.0) for r in rs)
        row: dict[str, Any] = {
            "symbol": symbol,
            "three_minute_epoch": bucket,
            "three_minute_utc": iso_sec(bucket),
            "open": rs[0].get("open"),
            "high": max(float(r["high"]) for r in rs if r.get("high") is not None),
            "low": min(float(r["low"]) for r in rs if r.get("low") is not None),
            "close": rs[-1].get("close"),
            "trade_count": sum(int(r.get("trade_count") or 0) for r in rs),
            "aggressive_buy_qty": sum(float(r.get("aggressive_buy_qty") or 0.0) for r in rs),
            "aggressive_sell_qty": sum(float(r.get("aggressive_sell_qty") or 0.0) for r in rs),
            "delta_qty": delta_qty,
            "delta_notional": sum(float(r.get("delta_notional") or 0.0) for r in rs),
            "total_qty": total_qty,
            "delta_ratio": delta_qty / total_qty if total_qty else None,
            "depth_update_events": sum(int(r.get("depth_update_events") or 0) for r in rs),
            "depth_snapshot_events": sum(int(r.get("depth_snapshot_events") or 0) for r in rs),
            "book_valid_seconds": sum(int(r.get("book_valid_seconds") or 0) for r in rs),
            "futures_trade_count": sum(int(r.get("futures_trade_count") or 0) for r in rs),
            "futures_aggressive_buy_qty": sum(float(r.get("futures_aggressive_buy_qty") or 0.0) for r in rs),
            "futures_aggressive_sell_qty": sum(float(r.get("futures_aggressive_sell_qty") or 0.0) for r in rs),
            "futures_delta_qty": sum(float(r.get("futures_delta_qty") or 0.0) for r in rs),
            "futures_delta_notional": sum(float(r.get("futures_delta_notional") or 0.0) for r in rs),
            "futures_total_qty": sum(float(r.get("futures_total_qty") or 0.0) for r in rs),
            "futures_depth_snapshot_events": sum(int(r.get("futures_depth_snapshot_events") or 0) for r in rs),
            "futures_book_valid_seconds": sum(int(r.get("futures_book_valid_seconds") or 0) for r in rs),
            "futures_last_trade_price": next((r.get("futures_last_trade_price") for r in reversed(rs) if r.get("futures_last_trade_price") is not None), None),
            "book_features_status": BOOK_STATUS,
            "source_schema": SOURCE_SCHEMA,
            "complete_minutes": 3,
        }
        row["futures_delta_ratio"] = row["futures_delta_qty"] / row["futures_total_qty"] if row["futures_total_qty"] else None
        for key in ("book_imbalance_1_mean", "book_imbalance_5_mean", "book_imbalance_10_mean", "book_imbalance_20_mean", "spread_bps_mean", "microprice_mean", "futures_best_bid_mean", "futures_best_ask_mean", "futures_mid_price_mean", "futures_spread_bps_mean", "futures_microprice_mean", "futures_book_imbalance_1_mean", "futures_book_imbalance_5_mean", "futures_book_imbalance_10_mean", "futures_book_imbalance_20_mean", "futures_book_imbalance_1_change_mean", "futures_book_imbalance_5_change_mean", "futures_spread_bps_change_mean", "futures_microprice_return_mean"):
            row[key] = _mean([r.get(key) for r in rs])
        out.append(row)
    return out


def write_csv_gz(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
            fh.write("")
        return
    fields = sorted({k for row in rows for k in row})
    with gzip.open(path, "wt", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True)
    args = ap.parse_args()
    batch = Path(args.batch)
    rows = build_seconds(batch)
    write_csv_gz(rows, batch / "features_1s.csv.gz")
    mins = aggregate_1m(rows)
    write_csv_gz(mins, batch / "features_1m.csv.gz")
    bars3 = aggregate_3m(mins)
    write_csv_gz(bars3, batch / "features_3m.csv.gz")
    summary = {
        "schema_version": 3,
        "source_schema": SOURCE_SCHEMA,
        "rows_1s": len(rows),
        "rows_1m": len(mins),
        "rows_3m": len(bars3),
        "symbols": sorted({r["symbol"] for r in rows}),
        "windows_seconds": list(WINDOWS),
        "forward_labels": list(FORWARD_HORIZONS),
        "long_horizon_labels_seconds": [300, 600, 900, 1800],
        "book_features_status": BOOK_STATUS,
        "futures_book_features_status": FUTURES_BOOK_STATUS,
        "futures_data": "Public Futures trades, LTP/current-prices and per-instrument orderbook depth snapshots are captured as additive features; no Open Interest is synthesized.",
        "book_features": [
            "best_bid", "best_ask", "mid_price", "spread_abs", "spread_bps", "microprice",
            "book_bid_qty_1/5/10/20", "book_ask_qty_1/5/10/20", "book_imbalance_1/5/10/20",
            "futures_best_bid", "futures_best_ask", "futures_mid_price", "futures_spread_bps", "futures_microprice",
            "futures_book_imbalance_1/5/10/20", "futures_book_imbalance_1/5/10/20_change",
        ],
        "book_reconstruction": "Absolute price-level replacements from validated depth updates, re-anchored at snapshots and invalidated across version discontinuities.",
        "futures_research": "Futures Delta remains separate from Spot Delta and Futures orderbook state/change remains separate from Spot orderbook state, so lead/lag, agreement, disagreement, divergence, and book-response hypotheses can be tested without changing the original Spot definitions.",
        "bars_3m_definition": "UTC-aligned aggregation of three complete 1m buckets derived from live raw trades/features; incomplete boundary buckets excluded.",
    }
    (batch / "research_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
