#!/usr/bin/env python3
"""Build compact live research layers from a CoinDCX collector batch.

Layers:
- 1-second trade-flow observations with rolling 5/15/30/60/180s measurements
- 1-minute bars/features
- 3-minute bars/features derived from the same live 1-minute layer

Depth-derived imbalance/microprice/pressure remain explicitly UNCERTIFIED until
independent depth-semantics validation proves that the websocket messages form a
coherent reconstructable book.
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


def read_gz_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def sec_bucket(ms: int) -> int:
    return ms // 1000


def iso_sec(s: int) -> str:
    return datetime.fromtimestamp(s, tz=timezone.utc).isoformat()


def safe_float(x: Any) -> float | None:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def build_seconds(batch: Path) -> list[dict[str, Any]]:
    trade_path = batch / "trades.jsonl.gz"
    price_path = batch / "price_change.jsonl.gz"
    depth_u = batch / "depth_update.jsonl.gz"
    depth_s = batch / "depth_snapshot.jsonl.gz"

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
            data = rec.get("raw", {}).get("data", rec.get("raw", {}))
            symbol = str(data.get("s") or rec.get("pair") or "UNKNOWN")
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
            data = rec.get("raw", {}).get("data", rec.get("raw", {}))
            t = data.get("T", rec.get("exchange_timestamp_ms"))
            p = safe_float(data.get("p"))
            if t is None or p is None:
                continue
            symbol = str(data.get("s") or rec.get("pair") or "UNKNOWN")
            r = row(symbol, sec_bucket(int(t)))
            r["price_events"] += 1
            r["last_price"] = p

    for path, key in ((depth_u, "depth_update_events"), (depth_s, "depth_snapshot_events")):
        if not path.exists():
            continue
        for rec in read_gz_jsonl(path):
            data = rec.get("raw", {}).get("data", rec.get("raw", {}))
            t = data.get("ts", rec.get("exchange_timestamp_ms"))
            if t is None:
                continue
            symbol = str(data.get("s") or rec.get("pair") or "UNKNOWN")
            r = row(symbol, sec_bucket(int(t)))
            r[key] += 1
            if data.get("vs") is not None:
                r["last_depth_version"] = data.get("vs")

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
            r["book_features_status"] = "UNCERTIFIED"
            final.append(r.copy())

    final.sort(key=lambda x: (x["symbol"], x["epoch_second"]))
    for symbol in {x["symbol"] for x in final}:
        rs = [x for x in final if x["symbol"] == symbol]
        price_by_s = {x["epoch_second"]: x.get("last_price") for x in rs}
        for x in rs:
            p0 = safe_float(x.get("last_price"))
            for w in WINDOWS:
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
            "depth_update_events": sum(int(x.get("depth_update_events") or 0) for x in rs),
            "depth_snapshot_events": sum(int(x.get("depth_snapshot_events") or 0) for x in rs),
            "book_features_status": "UNCERTIFIED",
            "source_schema": "research_batch_v2",
        })
    return out


def aggregate_3m(minute_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Aggregate live 1m rows into complete UTC 3-minute buckets only.

    A bucket is complete when all three constituent minute epochs are present.
    This prevents partial leading/trailing batches from becoming misleading 3m bars.
    """
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
        prices = [safe_float(r.get("close")) for r in rs]
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
            "depth_update_events": sum(int(r.get("depth_update_events") or 0) for r in rs),
            "depth_snapshot_events": sum(int(r.get("depth_snapshot_events") or 0) for r in rs),
            "book_features_status": "UNCERTIFIED",
            "source_schema": "research_batch_v2",
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
        "schema_version": 2,
        "rows_1s": len(rows),
        "rows_1m": len(mins),
        "rows_3m": len(bars3),
        "symbols": sorted({r["symbol"] for r in rows}),
        "windows_seconds": list(WINDOWS),
        "forward_labels": [5, 15, 30, 60, 180],
        "book_features_status": "UNCERTIFIED",
        "bars_3m_definition": "UTC-aligned aggregation of three complete 1m buckets derived from live raw trades/features; incomplete boundary buckets excluded.",
        "note": "Depth imbalance/microprice/pressure remain disabled until independent depth-semantics validation passes.",
    }
    (batch / "research_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
