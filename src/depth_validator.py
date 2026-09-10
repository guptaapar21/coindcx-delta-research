#!/usr/bin/env python3
"""Empirical CoinDCX order-book semantics validator.

Validates the observed CoinDCX depth stream by replaying depth updates from one
full snapshot until the next full snapshot. The primary hypothesis is that each
price-level quantity in a depth-update is an absolute replacement; zero removes
that level. Version discontinuities are reported separately and do not by
themselves invalidate the update-semantics conclusion.
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


def records(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with gzip.open(path, "rt", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def extract(rec: dict[str, Any]) -> dict[str, Any]:
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


def symbol_of(d: dict[str, Any], rec: dict[str, Any]) -> str:
    value = d.get("s") or rec.get("pair") or "UNKNOWN"
    s = str(value).upper()
    return {
        "BTCUSDT": "B-BTC_USDT",
        "B-BTC_USDT": "B-BTC_USDT",
        "ETHUSDT": "B-ETH_USDT",
        "B-ETH_USDT": "B-ETH_USDT",
    }.get(s, s)


def ts_of(d: dict[str, Any], rec: dict[str, Any]) -> int:
    value = d.get("ts", d.get("T", rec.get("exchange_timestamp_ms", rec.get("received_at_ms", 0))))
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(rec.get("received_at_ms", 0) or 0)


def version_of(d: dict[str, Any]) -> int | None:
    value = d.get("vs")
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def levels(d: dict[str, Any], key: str) -> dict[str, str]:
    value = d.get(key)
    if not isinstance(value, dict):
        return {}
    return {str(price): str(qty) for price, qty in value.items()}


def normal_book(d: dict[str, Any]) -> tuple[dict[str, str], dict[str, str]]:
    return levels(d, "bids"), levels(d, "asks")


def apply_update(book: dict[str, dict[str, str]], d: dict[str, Any]) -> int:
    """Apply absolute replacement semantics. Return number of touched levels."""
    touched = 0
    for side_key, book_key in (("bids", "bids"), ("asks", "asks")):
        payload = levels(d, side_key)
        side = book[book_key]
        for price, qty in payload.items():
            touched += 1
            try:
                is_zero = float(qty) == 0.0
            except (TypeError, ValueError):
                is_zero = qty in {"0", "0.0"}
            if is_zero:
                side.pop(price, None)
            else:
                side[price] = qty
    return touched


def snapshot_equal(book: dict[str, dict[str, str]], snapshot: dict[str, Any]) -> bool:
    return book["bids"] == normal_book(snapshot)[0] and book["asks"] == normal_book(snapshot)[1]


def make_events(batch: Path) -> list[tuple[int, int, str, dict[str, Any], dict[str, Any]]]:
    events: list[tuple[int, int, str, dict[str, Any], dict[str, Any]]] = []
    for filename, typ, order in (
        ("depth_snapshot.jsonl.gz", "snapshot", 0),
        ("depth_update.jsonl.gz", "update", 1),
    ):
        for rec in records(batch / filename):
            d = extract(rec)
            events.append((ts_of(d, rec), order, typ, d, rec))
    events.sort(key=lambda x: (x[0], x[1]))
    return events


def analyze(batch: Path) -> dict[str, Any]:
    events = make_events(batch)
    per_symbol: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "snapshots": 0,
        "updates": 0,
        "replay_intervals": 0,
        "replay_exact_matches": 0,
        "replay_mismatches": 0,
        "intervals_with_version_gaps": 0,
        "version_duplicates": 0,
        "version_backtracks": 0,
        "version_gaps": 0,
        "update_level_count_min": None,
        "update_level_count_max": None,
        "first_version": None,
        "last_version": None,
    })

    states: dict[str, dict[str, dict[str, str]]] = {}
    last_version: dict[str, int] = {}
    interval_active: dict[str, bool] = {}
    interval_gap: dict[str, bool] = {}
    interval_touched: dict[str, int] = {}

    for _, _, typ, d, rec in events:
        symbol = symbol_of(d, rec)
        stat = per_symbol[symbol]
        version = version_of(d)

        if version is not None:
            if stat["first_version"] is None:
                stat["first_version"] = version
            stat["last_version"] = version
            previous = last_version.get(symbol)
            if previous is not None:
                if version == previous:
                    stat["version_duplicates"] += 1
                elif version < previous:
                    stat["version_backtracks"] += 1
                    if interval_active.get(symbol):
                        interval_gap[symbol] = True
                elif version > previous + 1:
                    stat["version_gaps"] += 1
                    if interval_active.get(symbol):
                        interval_gap[symbol] = True
            last_version[symbol] = version

        if typ == "snapshot":
            stat["snapshots"] += 1
            if interval_active.get(symbol):
                stat["replay_intervals"] += 1
                if snapshot_equal(states[symbol], d):
                    stat["replay_exact_matches"] += 1
                else:
                    stat["replay_mismatches"] += 1
                if interval_gap.get(symbol):
                    stat["intervals_with_version_gaps"] += 1

            bids, asks = normal_book(d)
            states[symbol] = {"bids": dict(bids), "asks": dict(asks)}
            interval_active[symbol] = True
            interval_gap[symbol] = False
            interval_touched[symbol] = 0
            continue

        stat["updates"] += 1
        touched = apply_update(states.setdefault(symbol, {"bids": {}, "asks": {}}), d)
        interval_touched[symbol] = interval_touched.get(symbol, 0) + touched
        min_count = stat["update_level_count_min"]
        max_count = stat["update_level_count_max"]
        if min_count is None or touched < min_count:
            stat["update_level_count_min"] = touched
        if max_count is None or touched > max_count:
            stat["update_level_count_max"] = touched

    symbols: dict[str, Any] = {}
    for symbol, stat in sorted(per_symbol.items()):
        n = stat["replay_intervals"]
        exact = stat["replay_exact_matches"]
        rate = exact / n if n else None
        if n == 0:
            classification = "NO_REPLAYABLE_SNAPSHOT_INTERVALS"
        elif rate == 1.0:
            classification = "VALIDATED_ABSOLUTE_LEVEL_REPLACEMENT"
        elif rate >= 0.999:
            classification = "STRONGLY_SUPPORTED_ABSOLUTE_LEVEL_REPLACEMENT"
        else:
            classification = "SEMANTICS_NOT_CONFIRMED"
        serial = dict(stat)
        serial["replay_exact_match_rate"] = rate
        serial["classification"] = classification
        serial["zero_quantity_means_delete"] = True
        serial["validation_basis"] = "snapshot_to_next_snapshot_replay"
        symbols[symbol] = serial

    classes = [v["classification"] for v in symbols.values()]
    if classes and all(c in {"VALIDATED_ABSOLUTE_LEVEL_REPLACEMENT", "STRONGLY_SUPPORTED_ABSOLUTE_LEVEL_REPLACEMENT"} for c in classes):
        overall = "VALIDATED_ABSOLUTE_LEVEL_REPLACEMENT"
    elif classes:
        overall = "SEMANTICS_NOT_CONFIRMED"
    else:
        overall = "NO_DEPTH_DATA"

    return {
        "schema_version": 2,
        "overall_classification": overall,
        "update_semantics": "absolute_level_replacement",
        "zero_quantity_semantics": "delete_level",
        "validation_basis": "Replay each snapshot using intervening absolute updates and compare with the next snapshot.",
        "version_discontinuities": "reported separately; a version gap/reset causes that interval to be flagged but does not change the tested update semantics.",
        "symbols": symbols,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True)
    args = ap.parse_args()
    batch = Path(args.batch)
    result = analyze(batch)
    (batch / "depth_validation.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
