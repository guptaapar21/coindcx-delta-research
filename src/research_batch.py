#!/usr/bin/env python3
"""Build auditable 1-second, 1-minute and 3-minute research layers from raw CoinDCX batches.

Spot depth uses the empirically validated absolute price-level replacement semantics
with version-gap guarding. Futures orderbooks are full depth snapshots and are not
reconstructed incrementally.
"""
from __future__ import annotations

import argparse
import gzip
import json
import math
import statistics
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

WINDOWS = (5, 15, 30, 60, 180)
FORWARD_HORIZONS = (5, 15, 30, 60, 180, 300, 600, 900, 1800)
BOOK_LEVELS = (1, 5, 10, 20)
BOOK_STATUS = "VALIDATED_ABSOLUTE_UPDATES_WITH_GAP_GUARD"
FUTURES_BOOK_STATUS = "FUTURES_DEPTH_SNAPSHOT"
SOURCE_SCHEMA = "research_batch_v6"


def iso_sec(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def canonical_symbol(s: Any) -> str | None:
    if s is None:
        return None
    value = str(s).upper()
    mapping = {
        "BTCUSDT": "B-BTC_USDT",
        "B-BTCUSDT": "B-BTC_USDT",
        "ETHUSDT": "B-ETH_USDT",
        "B-ETHUSDT": "B-ETH_USDT",
        "B-BTC_USDT": "B-BTC_USDT",
        "B-ETH_USDT": "B-ETH_USDT",
    }
    return mapping.get(value, value)


def _iter_jsonl_gz(path: Path):
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _unwrap_data(rec: dict[str, Any]) -> dict[str, Any]:
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


def _num(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _epoch_sec(data: dict[str, Any], rec: dict[str, Any]) -> int | None:
    for key in ("T", "ts", "timestamp"):
        if key in data:
            val = _num(data.get(key))
            if val is not None:
                return int(val / 1000)
    val = _num(rec.get("received_at_ms"))
    if val is not None:
        return int(val / 1000)
    return None


def _trade_fields(data: dict[str, Any]) -> tuple[str | None, float | None, float | None, float | None, int | None]:
    symbol = canonical_symbol(data.get("s"))
    price = _num(data.get("p"))
    qty = _num(data.get("q"))
    maker = data.get("m")
    sec = _epoch_sec(data, {})
    return symbol, price, qty, _num(maker), sec


def _load_trade_seconds(batch: Path, filename: str, futures: bool = False) -> dict[str, dict[int, dict[str, float]]]:
    out: dict[str, dict[int, dict[str, float]]] = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    path = batch / filename
    for rec in _iter_jsonl_gz(path):
        data = _unwrap_data(rec)
        symbol = canonical_symbol(data.get("s"))
        if not symbol:
            continue
        sec = _epoch_sec(data, rec)
        if sec is None:
            continue
        qty = _num(data.get("q")) or 0.0
        price = _num(data.get("p")) or 0.0
        maker = data.get("m")
        try:
            maker_bool = bool(int(maker)) if isinstance(maker, (int, str)) else bool(maker)
        except (TypeError, ValueError):
            maker_bool = bool(maker)
        bucket = out[symbol][sec]
        bucket["trade_count"] += 1
        bucket["total_qty"] += qty
        bucket["total_notional"] += qty * price
        if maker_bool:
            bucket["aggressive_sell_qty"] += qty
            bucket["aggressive_sell_notional"] += qty * price
        else:
            bucket["aggressive_buy_qty"] += qty
            bucket["aggressive_buy_notional"] += qty * price
        bucket["last_price"] = price
    return out


def _parse_side(side: Any) -> list[tuple[float, float]]:
    if not isinstance(side, dict):
        return []
    out = []
    for price, qty in side.items():
        p = _num(price)
        q = _num(qty)
        if p is not None and q is not None and q > 0:
            out.append((p, q))
    return out


def _book_metrics(data: dict[str, Any]) -> dict[str, float | int | str | None]:
    bids = sorted(_parse_side(data.get("bids")), key=lambda x: x[0], reverse=True)
    asks = sorted(_parse_side(data.get("asks")), key=lambda x: x[0])
    result: dict[str, float | int | str | None] = {
        "book_features_status": BOOK_STATUS,
        "book_valid": True,
    }
    if not bids or not asks:
        result["book_valid"] = False
        result["book_features_status"] = "INVALID_EMPTY_SIDE"
        return result
    bb, bq = bids[0]
    ba, aq = asks[0]
    mid = (bb + ba) / 2.0
    spread = max(0.0, ba - bb)
    result.update({
        "best_bid": bb,
        "best_ask": ba,
        "mid_price": mid,
        "spread_abs": spread,
        "spread_bps": (spread / mid * 10000.0) if mid else None,
        "microprice": ((ba * bq) + (bb * aq)) / (bq + aq) if (bq + aq) else mid,
    })
    for level in BOOK_LEVELS:
        bqty = sum(q for _, q in bids[:level])
        aqty = sum(q for _, q in asks[:level])
        denom = bqty + aqty
        result[f"book_bid_qty_{level}"] = bqty
        result[f"book_ask_qty_{level}"] = aqty
        result[f"book_imbalance_{level}"] = (bqty - aqty) / denom if denom else 0.0
    return result


def _load_depth_seconds(batch: Path) -> dict[str, dict[int, dict[str, Any]]]:
    out: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    previous: dict[str, dict[str, Any]] = {}
    for rec in _iter_jsonl_gz(batch / "depth_snapshot.jsonl.gz"):
        data = _unwrap_data(rec)
        symbol = canonical_symbol(data.get("s") or rec.get("pair"))
        sec = _epoch_sec(data, rec)
        if not symbol or sec is None:
            continue
        metrics = _book_metrics(data)
        prev = previous.get(symbol)
        if prev:
            for level in BOOK_LEVELS:
                metrics[f"book_bid_qty_{level}_change"] = float(metrics.get(f"book_bid_qty_{level}") or 0.0) - float(prev.get(f"book_bid_qty_{level}") or 0.0)
                metrics[f"book_ask_qty_{level}_change"] = float(metrics.get(f"book_ask_qty_{level}") or 0.0) - float(prev.get(f"book_ask_qty_{level}") or 0.0)
                metrics[f"book_imbalance_{level}_change"] = float(metrics.get(f"book_imbalance_{level}") or 0.0) - float(prev.get(f"book_imbalance_{level}") or 0.0)
            if metrics.get("spread_bps") is not None and prev.get("spread_bps") is not None:
                metrics["spread_bps_change"] = float(metrics["spread_bps"]) - float(prev["spread_bps"])
            if metrics.get("microprice") is not None and prev.get("microprice"):
                metrics["microprice_return"] = float(metrics["microprice"]) / float(prev["microprice"]) - 1.0
        metrics["depth_snapshot_events"] = 1
        metrics["depth_version"] = data.get("vs")
        out[symbol][sec] = metrics
        previous[symbol] = metrics
    return out


def _load_futures_depth_seconds(batch: Path) -> dict[str, dict[int, dict[str, Any]]]:
    out: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
    previous: dict[str, dict[str, Any]] = {}
    path = batch / "futures_depth_snapshot.jsonl.gz"
    for rec in _iter_jsonl_gz(path):
        data = _unwrap_data(rec)
        product = str(data.get("pr", "")).lower()
        if product not in {"f", "futures"}:
            continue
        symbol = canonical_symbol(data.get("s") or rec.get("pair"))
        sec = _epoch_sec(data, rec)
        if not symbol or sec is None:
            continue
        base = _book_metrics(data)
        metrics: dict[str, Any] = {}
        for key, value in base.items():
            if key == "book_features_status":
                metrics["futures_book_features_status"] = FUTURES_BOOK_STATUS
            elif key == "book_valid":
                metrics["futures_book_valid"] = value
            elif key.startswith("book_"):
                metrics["futures_" + key] = value
            else:
                metrics["futures_book_" + key] = value
        prev = previous.get(symbol)
        if prev:
            for level in BOOK_LEVELS:
                metrics[f"futures_book_bid_qty_{level}_change"] = float(metrics.get(f"futures_book_bid_qty_{level}") or 0.0) - float(prev.get(f"futures_book_bid_qty_{level}") or 0.0)
                metrics[f"futures_book_ask_qty_{level}_change"] = float(metrics.get(f"futures_book_ask_qty_{level}") or 0.0) - float(prev.get(f"futures_book_ask_qty_{level}") or 0.0)
                metrics[f"futures_book_imbalance_{level}_change"] = float(metrics.get(f"futures_book_imbalance_{level}") or 0.0) - float(prev.get(f"futures_book_imbalance_{level}") or 0.0)
            if metrics.get("futures_book_spread_bps") is not None and prev.get("futures_book_spread_bps") is not None:
                metrics["futures_book_spread_bps_change"] = float(metrics["futures_book_spread_bps"]) - float(prev["futures_book_spread_bps"])
            if metrics.get("futures_book_microprice") is not None and prev.get("futures_book_microprice"):
                metrics["futures_book_microprice_return"] = float(metrics["futures_book_microprice"]) / float(prev["futures_book_microprice"]) - 1.0
        metrics["futures_depth_snapshot_events"] = 1
        metrics["futures_depth_version"] = data.get("vs")
        out[symbol][sec] = metrics
        previous[symbol] = metrics
    return out


def _load_futures_current_prices(batch: Path) -> dict[str, dict[int, float]]:
    out: dict[str, dict[int, float]] = defaultdict(dict)
    for rec in _iter_jsonl_gz(batch / "futures_current_prices.jsonl.gz"):
        sec = _epoch_sec(rec.get("raw", {}) if isinstance(rec.get("raw"), dict) else {}, rec)
        if sec is None:
            continue
        pairs = rec.get("pairs")
        if not isinstance(pairs, dict):
            continue
        for symbol, value in pairs.items():
            price = _num(value if not isinstance(value, dict) else value.get("p", value.get("price")))
            if price is not None:
                out[canonical_symbol(symbol) or str(symbol)][sec] = price
    return out


def build_seconds(batch: Path) -> list[dict[str, Any]]:
    spot = _load_trade_seconds(batch, "trades.jsonl.gz", futures=False)
    futures = _load_trade_seconds(batch, "futures_trades.jsonl.gz", futures=True)
    spot_depth = _load_depth_seconds(batch)
    futures_depth = _load_futures_depth_seconds(batch)
    futures_prices = _load_futures_current_prices(batch)
    symbols = sorted(set(spot) | set(futures) | set(spot_depth) | set(futures_depth))
    rows: list[dict[str, Any]] = []

    for symbol in symbols:
        secs = sorted(set(spot.get(symbol, {})) | set(futures.get(symbol, {})) | set(spot_depth.get(symbol, {})) | set(futures_depth.get(symbol, {})))
        price_history: deque[tuple[int, float]] = deque()
        futures_price_history: deque[tuple[int, float]] = deque()
        all_prices = sorted((s, float(v.get("last_price", 0.0))) for s, v in spot.get(symbol, {}).items() if v.get("last_price"))
        futures_all_prices = sorted((s, float(v)) for s, v in futures_prices.get(symbol, {}).items() if v)
        spot_idx = 0
        fut_idx = 0
        for sec in secs:
            while spot_idx < len(all_prices) and all_prices[spot_idx][0] <= sec:
                price_history.append(all_prices[spot_idx])
                spot_idx += 1
            while futures_all_prices and fut_idx < len(futures_all_prices) and futures_all_prices[fut_idx][0] <= sec:
                futures_price_history.append(futures_all_prices[fut_idx])
                fut_idx += 1
            current = float(spot.get(symbol, {}).get(sec, {}).get("last_price") or (price_history[-1][1] if price_history else 0.0))
            fcurrent = float(futures.get(symbol, {}).get(sec, {}).get("last_price") or (futures_price_history[-1][1] if futures_price_history else 0.0))
            row: dict[str, Any] = {
                "epoch_second": sec,
                "second_utc": iso_sec(sec),
                "symbol": symbol,
                "open": current,
                "high": current,
                "low": current,
                "close": current,
                "trade_count": int(spot.get(symbol, {}).get(sec, {}).get("trade_count", 0)),
                "aggressive_buy_qty": float(spot.get(symbol, {}).get(sec, {}).get("aggressive_buy_qty", 0.0)),
                "aggressive_sell_qty": float(spot.get(symbol, {}).get(sec, {}).get("aggressive_sell_qty", 0.0)),
                "delta_qty": float(spot.get(symbol, {}).get(sec, {}).get("aggressive_buy_qty", 0.0)) - float(spot.get(symbol, {}).get(sec, {}).get("aggressive_sell_qty", 0.0)),
                "aggressive_buy_notional": float(spot.get(symbol, {}).get(sec, {}).get("aggressive_buy_notional", 0.0)),
                "aggressive_sell_notional": float(spot.get(symbol, {}).get(sec, {}).get("aggressive_sell_notional", 0.0)),
                "delta_notional": float(spot.get(symbol, {}).get(sec, {}).get("aggressive_buy_notional", 0.0)) - float(spot.get(symbol, {}).get(sec, {}).get("aggressive_sell_notional", 0.0)),
                "total_qty": float(spot.get(symbol, {}).get(sec, {}).get("total_qty", 0.0)),
                "futures_trade_count": int(futures.get(symbol, {}).get(sec, {}).get("trade_count", 0)),
                "futures_aggressive_buy_qty": float(futures.get(symbol, {}).get(sec, {}).get("aggressive_buy_qty", 0.0)),
                "futures_aggressive_sell_qty": float(futures.get(symbol, {}).get(sec, {}).get("aggressive_sell_qty", 0.0)),
                "futures_delta_qty": float(futures.get(symbol, {}).get(sec, {}).get("aggressive_buy_qty", 0.0)) - float(futures.get(symbol, {}).get(sec, {}).get("aggressive_sell_qty", 0.0)),
                "futures_aggressive_buy_notional": float(futures.get(symbol, {}).get(sec, {}).get("aggressive_buy_notional", 0.0)),
                "futures_aggressive_sell_notional": float(futures.get(symbol, {}).get(sec, {}).get("aggressive_sell_notional", 0.0)),
                "futures_delta_notional": float(futures.get(symbol, {}).get(sec, {}).get("aggressive_buy_notional", 0.0)) - float(futures.get(symbol, {}).get(sec, {}).get("aggressive_sell_notional", 0.0)),
                "futures_total_qty": float(futures.get(symbol, {}).get(sec, {}).get("total_qty", 0.0)),
                "futures_last_price": fcurrent or None,
                "depth_update_events": 0,
                "depth_snapshot_events": int(spot_depth.get(symbol, {}).get(sec, {}).get("depth_snapshot_events", 0)),
                "book_features_status": spot_depth.get(symbol, {}).get(sec, {}).get("book_features_status", "NO_DEPTH_SNAPSHOT"),
                "source_schema": SOURCE_SCHEMA,
            }
            spot_m = spot_depth.get(symbol, {}).get(sec, {})
            for key, value in spot_m.items():
                if key not in {"depth_snapshot_events", "book_features_status"}:
                    row[key] = value
            fut_m = futures_depth.get(symbol, {}).get(sec, {})
            row.update(fut_m)

            hist: list[tuple[int, float]] = list(price_history)
            for w in WINDOWS:
                cutoff = sec - w + 1
                vals = [x for t, x in hist if t >= cutoff]
                fvals = [x for t, x in list(futures_price_history) if t >= cutoff]
                spot_s = spot.get(symbol, {})
                buy = sell = total = 0.0
                fbuy = fsell = ftotal = 0.0
                for t, b in spot_s.items():
                    if cutoff <= t <= sec:
                        buy += float(b.get("aggressive_buy_qty", 0.0))
                        sell += float(b.get("aggressive_sell_qty", 0.0))
                        total += float(b.get("total_qty", 0.0))
                for t, b in futures.get(symbol, {}).items():
                    if cutoff <= t <= sec:
                        fbuy += float(b.get("aggressive_buy_qty", 0.0))
                        fsell += float(b.get("aggressive_sell_qty", 0.0))
                        ftotal += float(b.get("total_qty", 0.0))
                delta = buy - sell
                fdelta = fbuy - fsell
                row[f"buy_qty_{w}s"] = buy
                row[f"sell_qty_{w}s"] = sell
                row[f"delta_qty_{w}s"] = delta
                row[f"delta_ratio_{w}s"] = delta / total if total else None
                row[f"futures_buy_qty_{w}s"] = fbuy
                row[f"futures_sell_qty_{w}s"] = fsell
                row[f"futures_delta_qty_{w}s"] = fdelta
                row[f"futures_delta_ratio_{w}s"] = fdelta / ftotal if ftotal else None
                row[f"futures_spot_delta_divergence_{w}s"] = (fdelta - delta) if total or ftotal else None
                row[f"price_return_{w}s"] = (current / vals[0] - 1.0) if vals and vals[0] else None
                row[f"futures_price_return_{w}s"] = (fcurrent / fvals[0] - 1.0) if fvals and fvals[0] and fcurrent else None
            rows.append(row)

        if not rows:
            continue

        symbol_rows = [r for r in rows if r["symbol"] == symbol]
        future_prices = {r["epoch_second"]: r["close"] for r in symbol_rows if r.get("close")}
        for row in symbol_rows:
            base = future_prices.get(row["epoch_second"], row["close"])
            for horizon in FORWARD_HORIZONS:
                later = future_prices.get(row["epoch_second"] + horizon)
                row[f"forward_return_{horizon}s"] = (later / base - 1.0) if later is not None and base else None
    return rows


def aggregate_1m(seconds: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in seconds:
        groups[(row["symbol"], row["epoch_second"] // 60 * 60)].append(row)
    out = []
    for (symbol, epoch), rows in sorted(groups.items()):
        rows.sort(key=lambda x: x["epoch_second"])
        valid = [r for r in rows if r.get("close") is not None]
        if not valid:
            continue
        close_rows = [r for r in valid if r.get("close")]
        item = {
            "symbol": symbol,
            "minute_epoch": epoch,
            "minute_utc": iso_sec(epoch),
            "open": close_rows[0]["open"],
            "high": max(r["high"] for r in close_rows),
            "low": min(r["low"] for r in close_rows),
            "close": close_rows[-1]["close"],
            "trade_count": sum(int(r.get("trade_count", 0)) for r in rows),
            "aggressive_buy_qty": sum(float(r.get("aggressive_buy_qty", 0.0)) for r in rows),
            "aggressive_sell_qty": sum(float(r.get("aggressive_sell_qty", 0.0)) for r in rows),
            "delta_qty": sum(float(r.get("delta_qty", 0.0)) for r in rows),
            "delta_notional": sum(float(r.get("delta_notional", 0.0)) for r in rows),
            "total_qty": sum(float(r.get("total_qty", 0.0)) for r in rows),
            "depth_update_events": sum(int(r.get("depth_update_events", 0)) for r in rows),
            "depth_snapshot_events": sum(int(r.get("depth_snapshot_events", 0)) for r in rows),
            "book_features_status": next((r.get("book_features_status") for r in rows if r.get("book_features_status")), "NO_DEPTH_SNAPSHOT"),
            "futures_trade_count": sum(int(r.get("futures_trade_count", 0)) for r in rows),
            "futures_aggressive_buy_qty": sum(float(r.get("futures_aggressive_buy_qty", 0.0)) for r in rows),
            "futures_aggressive_sell_qty": sum(float(r.get("futures_aggressive_sell_qty", 0.0)) for r in rows),
            "futures_delta_qty": sum(float(r.get("futures_delta_qty", 0.0)) for r in rows),
            "futures_delta_notional": sum(float(r.get("futures_delta_notional", 0.0)) for r in rows),
            "futures_depth_snapshot_events": sum(int(r.get("futures_depth_snapshot_events", 0)) for r in rows),
            "futures_book_features_status": next((r.get("futures_book_features_status") for r in rows if r.get("futures_book_features_status")), "NO_FUTURES_DEPTH_SNAPSHOT"),
            "source_schema": SOURCE_SCHEMA,
        }
        denom = item["total_qty"]
        item["delta_ratio"] = item["delta_qty"] / denom if denom else None
        # Aggregate book state conservatively as means of observed second-level features.
        for prefix in ("book_", "futures_book_"):
            keys = sorted({k for r in rows for k in r if k.startswith(prefix) and isinstance(r.get(k), (int, float))})
            for key in keys:
                vals = [float(r[key]) for r in rows if isinstance(r.get(key), (int, float))]
                if vals:
                    item[key + "_mean"] = statistics.fmean(vals)
        out.append(item)
    return out


def aggregate_3m(minutes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for row in minutes:
        groups[(row["symbol"], row["minute_epoch"] // 180 * 180)].append(row)
    out = []
    for (symbol, epoch), rows in sorted(groups.items()):
        by_minute = {r["minute_epoch"]: r for r in rows}
        needed = [epoch, epoch + 60, epoch + 120]
        if not all(t in by_minute for t in needed):
            continue
        rows = [by_minute[t] for t in needed]
        item = {
            "symbol": symbol,
            "three_minute_epoch": epoch,
            "three_minute_utc": iso_sec(epoch),
            "open": rows[0]["open"],
            "high": max(r["high"] for r in rows),
            "low": min(r["low"] for r in rows),
            "close": rows[-1]["close"],
            "trade_count": sum(int(r.get("trade_count", 0)) for r in rows),
            "aggressive_buy_qty": sum(float(r.get("aggressive_buy_qty", 0.0)) for r in rows),
            "aggressive_sell_qty": sum(float(r.get("aggressive_sell_qty", 0.0)) for r in rows),
            "delta_qty": sum(float(r.get("delta_qty", 0.0)) for r in rows),
            "delta_notional": sum(float(r.get("delta_notional", 0.0)) for r in rows),
            "total_qty": sum(float(r.get("total_qty", 0.0)) for r in rows),
            "complete_minutes": 3,
            "futures_trade_count": sum(int(r.get("futures_trade_count", 0)) for r in rows),
            "futures_aggressive_buy_qty": sum(float(r.get("futures_aggressive_buy_qty", 0.0)) for r in rows),
            "futures_aggressive_sell_qty": sum(float(r.get("futures_aggressive_sell_qty", 0.0)) for r in rows),
            "futures_delta_qty": sum(float(r.get("futures_delta_qty", 0.0)) for r in rows),
            "futures_delta_notional": sum(float(r.get("futures_delta_notional", 0.0)) for r in rows),
            "futures_depth_snapshot_events": sum(int(r.get("futures_depth_snapshot_events", 0)) for r in rows),
            "source_schema": SOURCE_SCHEMA,
        }
        denom = item["total_qty"]
        item["delta_ratio"] = item["delta_qty"] / denom if denom else None
        for prefix in ("book_", "futures_book_"):
            keys = sorted({k for r in rows for k in r if k.startswith(prefix + "") and isinstance(r.get(k), (int, float))})
            for key in keys:
                vals = [float(r[key]) for r in rows if isinstance(r.get(key), (int, float))]
                if vals:
                    item[key + "_mean"] = statistics.fmean(vals)
        out.append(item)
    return out


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, separators=(",", ":"), ensure_ascii=False) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("batch")
    args = ap.parse_args()
    batch = Path(args.batch)
    seconds = build_seconds(batch)
    minutes = aggregate_1m(seconds)
    threes = aggregate_3m(minutes)
    write_jsonl(batch / "research_seconds.jsonl.gz", seconds)
    write_jsonl(batch / "research_1m.jsonl.gz", minutes)
    write_jsonl(batch / "research_3m.jsonl.gz", threes)
    summary = {
        "source_schema": SOURCE_SCHEMA,
        "seconds": len(seconds),
        "minutes": len(minutes),
        "three_minutes": len(threes),
        "symbols": sorted({r["symbol"] for r in seconds}),
        "futures_orderbook_status": FUTURES_BOOK_STATUS,
        "notes": [
            "Futures trades, price changes and public current prices are captured as separate research inputs.",
            "Futures orderbooks are consumed as full depth snapshots; no incremental book reconstruction is performed.",
            "No Open Interest is synthesized because no public OI field was validated in the captured Futures streams.",
        ],
    }
    (batch / "research_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
