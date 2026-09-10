#!/usr/bin/env python3
"""Build compact CoinDCX research layers from one collector batch.

Trade/Delta definitions and research windows are unchanged. This implementation
uses linear-time prefix sums for rolling flow/price features so multi-market,
long batches do not become quadratic in batch length.

Spot depth is reconstructed only from empirically validated absolute updates
with version-gap invalidation and snapshot re-anchoring. Futures orderbooks are
kept as full depth snapshots and are never reconstructed incrementally.
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
            if not line:
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                yield value


def extract_payload_data(rec: dict[str, Any]) -> dict[str, Any]:
    raw = rec.get("raw", rec)
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
        "B-BTCUSDT": "B-BTC_USDT",
        "B-ETH_USDT": "B-ETH_USDT",
        "ETHUSDT": "B-ETH_USDT",
        "B-ETHUSDT": "B-ETH_USDT",
    }.get(s, s if s else None)


def sec_bucket(ms: int) -> int:
    return ms // 1000


def iso_sec(s: int) -> str:
    return datetime.fromtimestamp(s, tz=timezone.utc).isoformat()


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


def _book_metrics(book: dict[str, dict[str, float]]) -> dict[str, Any]:
    bids = sorted(((float(p), q) for p, q in book["bids"].items() if q > 0), key=lambda x: x[0], reverse=True)
    asks = sorted(((float(p), q) for p, q in book["asks"].items() if q > 0), key=lambda x: x[0])
    out: dict[str, Any] = {
        "book_valid": False,
        "book_bid_qty_1": None, "book_ask_qty_1": None,
        "book_bid_qty_5": None, "book_ask_qty_5": None,
        "book_bid_qty_10": None, "book_ask_qty_10": None,
        "book_bid_qty_20": None, "book_ask_qty_20": None,
        "book_imbalance_1": None, "book_imbalance_5": None,
        "book_imbalance_10": None, "book_imbalance_20": None,
        "best_bid": None, "best_ask": None, "mid_price": None,
        "spread_abs": None, "spread_bps": None, "microprice": None,
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


def _load_trade_seconds(batch: Path, filename: str, futures: bool = False) -> dict[str, dict[int, dict[str, float]]]:
    out: dict[str, dict[int, dict[str, float]]] = defaultdict(dict)
    path = batch / filename
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
    valid: dict[str, bool] = {}
    last_version: dict[str, int] = {}
    out: dict[tuple[str, int], dict[str, Any]] = {}
    counts: dict[tuple[str, int], dict[str, int]] = defaultdict(lambda: {"updates": 0, "snapshots": 0})

    for ts, _, kind, d, rec in events:
        symbol = canonical_symbol(d.get("s") or rec.get("pair"))
        if symbol is None:
            continue
        sec = sec_bucket(ts)
        states.setdefault(symbol, {"bids": {}, "asks": {}})
        valid.setdefault(symbol, False)
        if kind == "snapshot":
            states[symbol]["bids"] = _levels(d, "bids")
            states[symbol]["asks"] = _levels(d, "asks")
            valid[symbol] = True
            counts[(symbol, sec)]["snapshots"] += 1
        else:
            version = _depth_version(d)
            previous = last_version.get(symbol)
            if previous is not None and version is not None and version != previous + 1:
                valid[symbol] = False
            for side in ("bids", "asks"):
                for price, qty in _levels(d, side).items():
                    if qty <= 0:
                        states[symbol][side].pop(price, None)
                    else:
                        states[symbol][side][price] = qty
            counts[(symbol, sec)]["updates"] += 1
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

    for key, c in counts.items():
        out.setdefault(key, {"book_valid": False, "book_features_status": BOOK_STATUS})
        out[key]["depth_update_events"] = c["updates"]
        out[key]["depth_snapshot_events"] = c["snapshots"]
    return out


def _load_futures_depth_seconds(batch: Path) -> dict[tuple[str, int], dict[str, Any]]:
    path = batch / "futures_depth_snapshot.jsonl.gz"
    if not path.exists():
        return {}
    events: list[tuple[int, str, dict[str, Any], dict[str, Any]]] = []
    for rec in read_gz_jsonl(path):
        d = extract_payload_data(rec)
        if str(d.get("pr", "")).lower() not in {"f", "futures"}:
            continue
        symbol = canonical_symbol(d.get("s") or rec.get("pair"))
        if symbol is not None:
            events.append((_depth_event_ts(d, rec), symbol, d, rec))
    events.sort(key=lambda x: (x[1], x[0]))
    out: dict[tuple[str, int], dict[str, Any]] = {}
    previous: dict[str, dict[str, Any]] = {}
    for ts, symbol, d, rec in events:
        base = _book_metrics({"bids": _levels(d, "bids"), "asks": _levels(d, "asks")})
        metrics: dict[str, Any] = {
            "futures_best_bid": base["best_bid"], "futures_best_ask": base["best_ask"],
            "futures_mid_price": base["mid_price"], "futures_spread_abs": base["spread_abs"],
            "futures_spread_bps": base["spread_bps"], "futures_microprice": base["microprice"],
            "futures_book_features_status": FUTURES_BOOK_STATUS, "futures_book_valid": base["book_valid"],
            "futures_depth_snapshot_events": 1, "futures_depth_version": _depth_version(d),
        }
        for level in BOOK_LEVELS:
            metrics[f"futures_book_bid_qty_{level}"] = base[f"book_bid_qty_{level}"]
            metrics[f"futures_book_ask_qty_{level}"] = base[f"book_ask_qty_{level}"]
            metrics[f"futures_book_imbalance_{level}"] = base[f"book_imbalance_{level}"]
        prev = previous.get(symbol)
        if prev:
            for level in BOOK_LEVELS:
                metrics[f"futures_book_bid_qty_{level}_change"] = metrics[f"futures_book_bid_qty_{level}"] - prev[f"futures_book_bid_qty_{level}"]
                metrics[f"futures_book_ask_qty_{level}_change"] = metrics[f"futures_book_ask_qty_{level}"] - prev[f"futures_book_ask_qty_{level}"]
                metrics[f"futures_book_imbalance_{level}_change"] = (metrics[f"futures_book_imbalance_{level}"] or 0.0) - (prev[f"futures_book_imbalance_{level}"] or 0.0)
            if metrics["futures_spread_bps"] is not None and prev.get("futures_spread_bps") is not None:
                metrics["futures_spread_bps_change"] = metrics["futures_spread_bps"] - prev["futures_spread_bps"]
            if metrics["futures_microprice"] is not None and prev.get("futures_microprice"):
                metrics["futures_microprice_return"] = metrics["futures_microprice"] / prev["futures_microprice"] - 1.0
        previous[symbol] = metrics
        out[(symbol, sec_bucket(ts))] = metrics
    return out


def _window_sums(values: list[float], epochs: list[int], w: int) -> list[float]:
    prefix = [0.0]
    for value in values:
        prefix.append(prefix[-1] + value)
    out: list[float] = [0.0] * len(values)
    left = 0
    for i, sec in enumerate(epochs):
        cutoff = sec - w + 1
        while left <= i and epochs[left] < cutoff:
            left += 1
        out[i] = prefix[i + 1] - prefix[left]
    return out


def _rolling_first(values: list[float | None], epochs: list[int], w: int) -> list[float | None]:
    out: list[float | None] = [None] * len(values)
    left = 0
    for i, sec in enumerate(epochs):
        cutoff = sec - w + 1
        while left <= i and epochs[left] < cutoff:
            left += 1
        for j in range(left, i + 1):
            if values[j] is not None:
                out[i] = values[j]
                break
    return out


def build_seconds(batch: Path) -> list[dict[str, Any]]:
    spot = _load_trade_seconds(batch, "trades.jsonl.gz")
    futures = _load_trade_seconds(batch, "futures_trades.jsonl.gz", futures=True)
    spot_depth = _load_depth_seconds(batch)
    futures_depth = _load_futures_depth_seconds(batch)
    symbols = sorted(set(spot) | set(futures) | {k[0] for k in spot_depth} | {k[0] for k in futures_depth})
    rows: list[dict[str, Any]] = []

    for symbol in symbols:
        sec_set = set(spot.get(symbol, {})) | set(futures.get(symbol, {})) | {k[1] for k in spot_depth if k[0] == symbol} | {k[1] for k in futures_depth if k[0] == symbol}
        epochs = sorted(sec_set)
        if not epochs:
            continue

        spot_last: list[float | None] = []
        futures_last: list[float | None] = []
        spot_buy: list[float] = []
        spot_sell: list[float] = []
        spot_total: list[float] = []
        spot_buy_notional: list[float] = []
        spot_sell_notional: list[float] = []
        spot_trade_count: list[float] = []
        fut_buy: list[float] = []
        fut_sell: list[float] = []
        fut_total: list[float] = []
        fut_trade_count: list[float] = []

        prior_spot = None
        prior_fut = None
        for sec in epochs:
            sb = spot.get(symbol, {}).get(sec, {})
            fb = futures.get(symbol, {}).get(sec, {})
            if sb.get("last_trade_price") is not None:
                prior_spot = float(sb["last_trade_price"])
            if fb.get("last_trade_price") is not None:
                prior_fut = float(fb["last_trade_price"])
            spot_last.append(prior_spot)
            futures_last.append(prior_fut)
            spot_buy.append(float(sb.get("aggressive_buy_qty", 0.0)))
            spot_sell.append(float(sb.get("aggressive_sell_qty", 0.0)))
            spot_total.append(float(sb.get("total_qty", 0.0)))
            spot_buy_notional.append(float(sb.get("aggressive_buy_notional", 0.0)))
            spot_sell_notional.append(float(sb.get("aggressive_sell_notional", 0.0)))
            spot_trade_count.append(float(sb.get("trade_count", 0)))
            fut_buy.append(float(fb.get("aggressive_buy_qty", 0.0)))
            fut_sell.append(float(fb.get("aggressive_sell_qty", 0.0)))
            fut_total.append(float(fb.get("total_qty", 0.0)))
            fut_trade_count.append(float(fb.get("trade_count", 0)))

        rolling: dict[int, dict[str, list[float | None]]] = {}
        for w in WINDOWS:
            rolling[w] = {
                "buy": _window_sums(spot_buy, epochs, w), "sell": _window_sums(spot_sell, epochs, w),
                "total": _window_sums(spot_total, epochs, w), "buy_notional": _window_sums(spot_buy_notional, epochs, w),
                "sell_notional": _window_sums(spot_sell_notional, epochs, w), "trade_count": _window_sums(spot_trade_count, epochs, w),
                "f_buy": _window_sums(fut_buy, epochs, w), "f_sell": _window_sums(fut_sell, epochs, w),
                "f_total": _window_sums(fut_total, epochs, w), "f_trade_count": _window_sums(fut_trade_count, epochs, w),
                "spot_first_price": _rolling_first(spot_last, epochs, w), "f_first_price": _rolling_first(futures_last, epochs, w),
            }

        symbol_rows: list[dict[str, Any]] = []
        for i, sec in enumerate(epochs):
            sprice = spot_last[i]
            fprice = futures_last[i]
            sb = spot.get(symbol, {}).get(sec, {})
            fb = futures.get(symbol, {}).get(sec, {})
            row: dict[str, Any] = {
                "symbol": symbol, "epoch_second": sec, "utc_second": iso_sec(sec),
                "open": sprice, "high": sprice, "low": sprice, "close": sprice,
                "trade_count": int(sb.get("trade_count", 0)),
                "aggressive_buy_qty": float(sb.get("aggressive_buy_qty", 0.0)),
                "aggressive_sell_qty": float(sb.get("aggressive_sell_qty", 0.0)),
                "aggressive_buy_notional": float(sb.get("aggressive_buy_notional", 0.0)),
                "aggressive_sell_notional": float(sb.get("aggressive_sell_notional", 0.0)),
                "delta_qty": float(sb.get("aggressive_buy_qty", 0.0)) - float(sb.get("aggressive_sell_qty", 0.0)),
                "delta_notional": float(sb.get("aggressive_buy_notional", 0.0)) - float(sb.get("aggressive_sell_notional", 0.0)),
                "total_qty": float(sb.get("total_qty", 0.0)), "total_notional": float(sb.get("total_notional", 0.0)),
                "last_trade_price": sprice,
                "futures_trade_count": int(fb.get("trade_count", 0)),
                "futures_aggressive_buy_qty": float(fb.get("aggressive_buy_qty", 0.0)),
                "futures_aggressive_sell_qty": float(fb.get("aggressive_sell_qty", 0.0)),
                "futures_aggressive_buy_notional": float(fb.get("aggressive_buy_notional", 0.0)),
                "futures_aggressive_sell_notional": float(fb.get("aggressive_sell_notional", 0.0)),
                "futures_delta_qty": float(fb.get("aggressive_buy_qty", 0.0)) - float(fb.get("aggressive_sell_qty", 0.0)),
                "futures_delta_notional": float(fb.get("aggressive_buy_notional", 0.0)) - float(fb.get("aggressive_sell_notional", 0.0)),
                "futures_total_qty": float(fb.get("total_qty", 0.0)), "futures_last_trade_price": fprice,
                "depth_update_events": 0, "depth_snapshot_events": 0,
                "book_features_status": BOOK_STATUS, "futures_book_features_status": FUTURES_BOOK_STATUS,
                "source_schema": SOURCE_SCHEMA,
            }
            row.update(spot_depth.get((symbol, sec), {}))
            row.update(futures_depth.get((symbol, sec), {}))
            for w in WINDOWS:
                vals = rolling[w]
                buy = float(vals["buy"][i]); sell = float(vals["sell"][i]); total = float(vals["total"][i])
                fbuy = float(vals["f_buy"][i]); fsell = float(vals["f_sell"][i]); ftotal = float(vals["f_total"][i])
                first_price = vals["spot_first_price"][i]; first_fprice = vals["f_first_price"][i]
                row[f"buy_qty_{w}s"] = buy; row[f"sell_qty_{w}s"] = sell; row[f"delta_qty_{w}s"] = buy - sell
                row[f"delta_ratio_{w}s"] = (buy - sell) / total if total else None
                row[f"futures_buy_qty_{w}s"] = fbuy; row[f"futures_sell_qty_{w}s"] = fsell; row[f"futures_delta_qty_{w}s"] = fbuy - fsell
                row[f"futures_delta_ratio_{w}s"] = (fbuy - fsell) / ftotal if ftotal else None
                row[f"futures_spot_delta_divergence_{w}s"] = (fbuy - fsell) - (buy - sell) if (total or ftotal) else None
                row[f"price_return_{w}s"] = (sprice / first_price - 1.0) if sprice is not None and first_price else None
                row[f"futures_price_return_{w}s"] = (fprice / first_fprice - 1.0) if fprice is not None and first_fprice else None
            rows.append(row)
            symbol_rows.append(row)

        close_by_epoch = {row["epoch_second"]: row["close"] for row in symbol_rows if row.get("close") is not None}
        for row in symbol_rows:
            base = row.get("close")
            if base is None:
                continue
            for horizon in FORWARD_HORIZONS:
                future = close_by_epoch.get(row["epoch_second"] + horizon)
                row[f"forward_return_{horizon}s"] = (future / base - 1.0) if future is not None and base else None

    return rows


def _mean(values: list[Any]) -> float | None:
    nums = [float(v) for v in values if v is not None]
    return sum(nums) / len(nums) if nums else None


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
            "symbol": symbol, "minute_epoch": epoch, "minute_utc": iso_sec(epoch),
            "open": close_rows[0].get("open"), "high": max(float(r["high"]) for r in close_rows if r.get("high") is not None),
            "low": min(float(r["low"]) for r in close_rows if r.get("low") is not None), "close": close_rows[-1].get("close"),
            "trade_count": sum(int(r.get("trade_count") or 0) for r in rs),
            "aggressive_buy_qty": sum(float(r.get("aggressive_buy_qty") or 0.0) for r in rs),
            "aggressive_sell_qty": sum(float(r.get("aggressive_sell_qty") or 0.0) for r in rs),
            "delta_qty": delta_qty, "delta_notional": sum(float(r.get("delta_notional") or 0.0) for r in rs),
            "total_qty": total_qty, "delta_ratio": delta_qty / total_qty if total_qty else None,
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
            "book_features_status": BOOK_STATUS, "source_schema": SOURCE_SCHEMA,
        }
        row["futures_delta_ratio"] = row["futures_delta_qty"] / row["futures_total_qty"] if row["futures_total_qty"] else None
        for key in ("book_imbalance_1", "book_imbalance_5", "book_imbalance_10", "book_imbalance_20", "spread_bps", "microprice", "futures_book_imbalance_1", "futures_book_imbalance_5", "futures_book_imbalance_10", "futures_book_imbalance_20", "futures_spread_bps", "futures_microprice"):
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
        if any(e not in minute_map for e in expected):
            continue
        rs = [minute_map[e] for e in expected]
        row: dict[str, Any] = {
            "symbol": symbol, "three_minute_epoch": bucket, "three_minute_utc": iso_sec(bucket),
            "open": rs[0].get("open"), "high": max(float(r["high"]) for r in rs if r.get("high") is not None),
            "low": min(float(r["low"]) for r in rs if r.get("low") is not None), "close": rs[-1].get("close"),
            "trade_count": sum(int(r.get("trade_count") or 0) for r in rs),
            "aggressive_buy_qty": sum(float(r.get("aggressive_buy_qty") or 0.0) for r in rs),
            "aggressive_sell_qty": sum(float(r.get("aggressive_sell_qty") or 0.0) for r in rs),
            "delta_qty": sum(float(r.get("delta_qty") or 0.0) for r in rs),
            "delta_notional": sum(float(r.get("delta_notional") or 0.0) for r in rs),
            "total_qty": sum(float(r.get("total_qty") or 0.0) for r in rs),
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
            "book_features_status": BOOK_STATUS, "source_schema": SOURCE_SCHEMA, "complete_minutes": 3,
        }
        row["delta_ratio"] = row["delta_qty"] / row["total_qty"] if row["total_qty"] else None
        row["futures_delta_ratio"] = row["futures_delta_qty"] / row["futures_total_qty"] if row["futures_total_qty"] else None
        for key in ("book_imbalance_1_mean", "book_imbalance_5_mean", "book_imbalance_10_mean", "book_imbalance_20_mean", "spread_bps_mean", "microprice_mean", "futures_book_imbalance_1_mean", "futures_book_imbalance_5_mean", "futures_book_imbalance_10_mean", "futures_book_imbalance_20_mean", "futures_spread_bps_mean", "futures_microprice_mean"):
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
        "schema_version": 6, "rows_1s": len(rows), "rows_1m": len(mins), "rows_3m": len(bars3),
        "symbols": sorted({r["symbol"] for r in rows}), "windows_seconds": list(WINDOWS),
        "forward_labels": list(FORWARD_HORIZONS), "book_features_status": BOOK_STATUS,
        "futures_book_features_status": FUTURES_BOOK_STATUS, "source_schema": SOURCE_SCHEMA,
        "bars_3m_definition": "UTC-aligned aggregation of three complete 1m buckets derived from live raw trades/features; incomplete boundary buckets excluded.",
        "note": "Rolling flow windows are computed with prefix sums; exact-horizon forward labels remain unchanged.",
    }
    (batch / "research_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
