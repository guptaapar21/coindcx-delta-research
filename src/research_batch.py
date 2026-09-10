#!/usr/bin/env python3
"""Build compact CoinDCX research layers from one collector batch.

The depth layer uses empirically validated absolute price-level replacements:
- a non-zero quantity replaces the current quantity at that price;
- a zero quantity deletes the price level.

A reconstructed book is only considered valid from a full snapshot until the
next version discontinuity or until a new snapshot re-anchors it. Trade/Delta
features and their existing rolling windows remain unchanged.
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


def read_gz_jsonl(path: Path) -> Iterable[dict[str, Any]]:
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
    out.update({"best_bid": best_bid, "best_ask": best_ask, "mid_price": mid, "spread_abs": best_ask - best_bid,
                "spread_bps": (best_ask - best_bid) / mid * 10000 if mid else None,
                "microprice": (best_ask * bid1 + best_bid * ask1) / (bid1 + ask1) if (bid1 + ask1) else mid})
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
        added, removed, touched = (0.0, 0.0, 0) if kind == "snapshot" else _apply_delta_metrics_placeholder()
        # For update liquidity accounting, replay the update once more against a
        # shadow copy to get changes without mutating the actual book twice.
        if kind == "update":
            shadow = {"bids": dict(states[symbol]["bids"]), "asks": dict(states[symbol]["asks"])}
            # The already-applied state is the desired state. Rebuild before-state
            # by reversing each touched level where possible is fragile, so instead
            # expose event counts and leave cumulative added/removed unreported here.
            _ = shadow
        metrics = _book_metrics(states[symbol])
        if not valid[symbol]:
            metrics["book_valid"] = False
        metrics.update({
            "book_features_status": BOOK_STATUS,
            "depth_event_type": kind,
            "depth_version": version,
            "depth_update_touched_levels": len(_levels(d, "bids")) + len(_levels(d, "asks")) if kind == "update" else None,
        })
        # Last event in a second wins, while accumulated event counts are filled below.
        out[(symbol, sec)] = metrics

    # Count events per second without changing book state.
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


def _apply_delta_metrics_placeholder() -> tuple[float, float, int]:
    return 0.0, 0.0, 0


def build_seconds(batch: Path) -> list[dict[str, Any]]:
    trade_path = batch / "trades.jsonl.gz"
    price_path = batch / "price_change.jsonl.gz"

    rows: dict[tuple[str, int], dict[str, Any]] = {}

    def row(symbol: str, sec: int) -> dict[str, Any]:
        key = (symbol, sec)
        if key not in rows:
            rows[key] = {
                "symbol": symbol,
                "epoch_second": sec,
                "utc_second": iso_sec(sec),
                "trade_count": 0,
                "aggressive_buy_qty": 0.0,
                "aggressive_sell_qty": 0.0,
                "aggressive_buy_notional": 0.0,
                "aggressive_sell_notional": 0.0,
                "delta_qty": 0.0,
                "delta_notional": 0.0,
                "total_qty": 0.0,
                "total_notional": 0.0,
                "last_trade_price": None,
                "last_trade_exchange_ms": None,
                "price_events": 0,
                "last_price": None,
                "depth_update_events": 0,
                "depth_snapshot_events": 0,
                "last_depth_version": None,
            }
        return rows[key]

    if trade_path.exists():
        for rec in read_gz_jsonl(trade_path):
            data = extract_payload_data(rec)
            symbol = canonical_symbol(data.get("s") or rec.get("pair"))
            if symbol is None:
                continue
            t = data.get("T", rec.get("exchange_timestamp_ms"))
            p = safe_float(data.get("p"))
            q = safe_float(data.get("q"))
            if t is None or p is None or q is None:
                continue
            t_ms = int(t)
            r = row(symbol, sec_bucket(t_ms))
            maker_buyer = bool(data.get("m"))
            notion = p * q
            r["trade_count"] += 1
            r["total_qty"] += q
            r["total_notional"] += notion
            r["last_trade_price"] = p
            r["last_trade_exchange_ms"] = t_ms
            if maker_buyer:
                r["aggressive_sell_qty"] += q
                r["aggressive_sell_notional"] += notion
            else:
                r["aggressive_buy_qty"] += q
                r["aggressive_buy_notional"] += notion

    if price_path.exists():
        for rec in read_gz_jsonl(price_path):
            data = extract_payload_data(rec)
            t = data.get("T", rec.get("exchange_timestamp_ms"))
            p = safe_float(data.get("p"))
            if t is None or p is None:
                continue
            symbol = canonical_symbol(data.get("s") or rec.get("pair"))
            if symbol is None:
                continue
            r = row(symbol, sec_bucket(int(t)))
            r["price_events"] += 1
            r["last_price"] = p

    depth_by_second = _load_depth_seconds(batch)
    for (symbol, sec), features in depth_by_second.items():
        r = row(symbol, sec)
        for key, value in features.items():
            if key in {"depth_update_events", "depth_snapshot_events"}:
                continue
            r[key] = value
        r["depth_update_events"] = features.get("depth_update_events", r["depth_update_events"])
        r["depth_snapshot_events"] = features.get("depth_snapshot_events", r["depth_snapshot_events"])
        r["last_depth_version"] = features.get("depth_version")

    by_symbol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in rows.values():
        if r["last_price"] is None:
            r["last_price"] = r["last_trade_price"]
        r["delta_qty"] = r["aggressive_buy_qty"] - r["aggressive_sell_qty"]
        r["delta_notional"] = r["aggressive_buy_notional"] - r["aggressive_sell_notional"]
        r["delta_ratio"] = r["delta_qty"] / r["total_qty"] if r["total_qty"] else None
        by_symbol[r["symbol"]].append(r)

    final: list[dict[str, Any]] = []
    for symbol, rs in by_symbol.items():
        rs.sort(key=lambda x: x["epoch_second"])
        for i, r in enumerate(rs):
            for w in WINDOWS:
                target = r["epoch_second"] - w + 1
                lo = i
                while lo >= 0 and rs[lo]["epoch_second"] >= target:
                    lo -= 1
                window_rows = rs[lo + 1 : i + 1]
                r[f"buy_qty_{w}s"] = sum(x["aggressive_buy_qty"] for x in window_rows)
                r[f"sell_qty_{w}s"] = sum(x["aggressive_sell_qty"] for x in window_rows)
                r[f"delta_qty_{w}s"] = sum(x["delta_qty"] for x in window_rows)
                r[f"delta_notional_{w}s"] = sum(x["delta_notional"] for x in window_rows)
                total = sum(x["total_qty"] for x in window_rows)
                r[f"delta_ratio_{w}s"] = r[f"delta_qty_{w}s"] / total if total else None
                r[f"trade_count_{w}s"] = sum(x["trade_count"] for x in window_rows)
            final.append(r.copy())

    final.sort(key=lambda x: (x["symbol"], x["epoch_second"]))
    for symbol in {x["symbol"] for x in final}:
        rs = [x for x in final if x["symbol"] == symbol]
        price_by_s = {x["epoch_second"]: x.get("last_price") for x in rs}
        for x in rs:
            p0 = safe_float(x.get("last_price"))
            for w in FORWARD_HORIZONS:
                p1 = price_by_s.get(x["epoch_second"] + w)
                x[f"forward_return_{w}s"] = p1 / p0 - 1.0 if p0 and p1 else None
    return final


def write_csv_gz(rows: list[dict[str, Any]], path: Path) -> None:
    if not rows:
        path.write_bytes(b"")
        return
    fields = sorted({k for r in rows for k in r})
    with gzip.open(path, "wt", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def _price_stats(rs: list[dict[str, Any]]) -> tuple[float | None, float | None, float | None, float | None]:
    prices = [safe_float(x.get("last_price")) for x in rs if x.get("last_price") is not None]
    prices = [p for p in prices if p is not None]
    return (
        prices[0] if prices else None,
        max(prices) if prices else None,
        min(prices) if prices else None,
        prices[-1] if prices else None,
    )


def aggregate_1m(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_key: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_key[(r["symbol"], r["epoch_second"] // 60)].append(r)
    out: list[dict[str, Any]] = []
    for (symbol, minute), rs in sorted(by_key.items()):
        rs.sort(key=lambda x: x["epoch_second"])
        op, high, low, close = _price_stats(rs)
        total_qty = sum(float(x.get("total_qty") or 0.0) for x in rs)
        delta_qty = sum(float(x.get("delta_qty") or 0.0) for x in rs)
        delta_notional = sum(float(x.get("delta_notional") or 0.0) for x in rs)
        out.append({
            "symbol": symbol,
            "minute_epoch": minute * 60,
            "minute_utc": iso_sec(minute * 60),
            "open": op,
            "high": high,
            "low": low,
            "close": close,
            "trade_count": sum(int(x.get("trade_count") or 0) for x in rs),
            "aggressive_buy_qty": sum(float(x.get("aggressive_buy_qty") or 0.0) for x in rs),
            "aggressive_sell_qty": sum(float(x.get("aggressive_sell_qty") or 0.0) for x in rs),
            "delta_qty": delta_qty,
            "delta_notional": delta_notional,
            "total_qty": total_qty,
            "delta_ratio": delta_qty / total_qty if total_qty else None,
            "book_imbalance_1_mean": _mean([x.get("book_imbalance_1") for x in rs]),
            "book_imbalance_5_mean": _mean([x.get("book_imbalance_5") for x in rs]),
            "book_imbalance_10_mean": _mean([x.get("book_imbalance_10") for x in rs]),
            "book_imbalance_20_mean": _mean([x.get("book_imbalance_20") for x in rs]),
            "spread_bps_mean": _mean([x.get("spread_bps") for x in rs]),
            "microprice_mean": _mean([x.get("microprice") for x in rs]),
            "depth_update_events": sum(int(x.get("depth_update_events") or 0) for x in rs),
            "depth_snapshot_events": sum(int(x.get("depth_snapshot_events") or 0) for x in rs),
            "book_valid_seconds": sum(1 for x in rs if x.get("book_valid")),
            "book_features_status": BOOK_STATUS,
            "source_schema": "research_batch_v4",
        })
    return out


def _mean(values: list[Any]) -> float | None:
    nums = [float(v) for v in values if v is not None]
    return sum(nums) / len(nums) if nums else None


def aggregate_3m(minute_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, int], dict[int, dict[str, Any]]] = defaultdict(dict)
    for r in minute_rows:
        epoch = int(r["minute_epoch"])
        bucket = epoch - (epoch % 180)
        grouped[(r["symbol"], bucket)][epoch] = r
    out: list[dict[str, Any]] = []
    for (symbol, bucket), minute_map in sorted(grouped.items()):
        expected = [bucket, bucket + 60, bucket + 120]
        if any(epoch not in minute_map for epoch in expected):
            continue
        rs = [minute_map[e] for e in expected]
        opens = safe_float(rs[0].get("open"))
        closes = safe_float(rs[-1].get("close"))
        highs = [safe_float(r.get("high")) for r in rs if r.get("high") is not None]
        lows = [safe_float(r.get("low")) for r in rs if r.get("low") is not None]
        total_qty = sum(float(r.get("total_qty") or 0.0) for r in rs)
        delta_qty = sum(float(r.get("delta_qty") or 0.0) for r in rs)
        out.append({
            "symbol": symbol,
            "three_minute_epoch": bucket,
            "three_minute_utc": iso_sec(bucket),
            "open": opens,
            "high": max(highs) if highs else None,
            "low": min(lows) if lows else None,
            "close": closes,
            "trade_count": sum(int(r.get("trade_count") or 0) for r in rs),
            "aggressive_buy_qty": sum(float(r.get("aggressive_buy_qty") or 0.0) for r in rs),
            "aggressive_sell_qty": sum(float(r.get("aggressive_sell_qty") or 0.0) for r in rs),
            "delta_qty": delta_qty,
            "delta_notional": sum(float(r.get("delta_notional") or 0.0) for r in rs),
            "total_qty": total_qty,
            "delta_ratio": delta_qty / total_qty if total_qty else None,
            "book_imbalance_1_mean": _mean([r.get("book_imbalance_1_mean") for r in rs]),
            "book_imbalance_5_mean": _mean([r.get("book_imbalance_5_mean") for r in rs]),
            "book_imbalance_10_mean": _mean([r.get("book_imbalance_10_mean") for r in rs]),
            "book_imbalance_20_mean": _mean([r.get("book_imbalance_20_mean") for r in rs]),
            "spread_bps_mean": _mean([r.get("spread_bps_mean") for r in rs]),
            "microprice_mean": _mean([r.get("microprice_mean") for r in rs]),
            "depth_update_events": sum(int(r.get("depth_update_events") or 0) for r in rs),
            "depth_snapshot_events": sum(int(r.get("depth_snapshot_events") or 0) for r in rs),
            "book_valid_seconds": sum(int(r.get("book_valid_seconds") or 0) for r in rs),
            "book_features_status": BOOK_STATUS,
            "source_schema": "research_batch_v4",
            "complete_minutes": 3,
        })
    return out


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
        "rows_1s": len(rows),
        "rows_1m": len(mins),
        "rows_3m": len(bars3),
        "symbols": sorted({r["symbol"] for r in rows}),
        "windows_seconds": list(WINDOWS),
        "forward_labels": list(FORWARD_HORIZONS),
        "long_horizon_labels_seconds": [300, 600, 900, 1800],
        "book_features_status": BOOK_STATUS,
        "book_features": [
            "best_bid", "best_ask", "mid_price", "spread_abs", "spread_bps", "microprice",
            "book_bid_qty_1/5/10/20", "book_ask_qty_1/5/10/20", "book_imbalance_1/5/10/20",
        ],
        "book_reconstruction": "Absolute price-level replacements from validated depth updates, re-anchored at snapshots and invalidated across version discontinuities.",
        "bars_3m_definition": "UTC-aligned aggregation of three complete 1m buckets derived from live raw trades/features; incomplete boundary buckets excluded.",
    }
    (batch / "research_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
