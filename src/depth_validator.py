#!/usr/bin/env python3
"""Empirical CoinDCX order-book stream semantics validator.

This intentionally DOES NOT declare incremental semantics from a version counter
alone. It reports evidence that helps decide whether a persistent book can safely
be reconstructed.
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


def records(path: Path):
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
    return data if isinstance(data, dict) else (raw if isinstance(raw, dict) else {})


def normalize_levels(x: Any) -> dict[str, str]:
    if not isinstance(x, dict):
        return {}
    return {str(k): str(v) for k, v in x.items()}


def analyze(batch: Path) -> dict[str, Any]:
    stats = defaultdict(lambda: {
        "snapshots": 0, "updates": 0, "versions": [], "timestamp_ms": [],
        "levels_per_update": [], "levels_per_snapshot": [],
        "update_price_repeats": 0, "snapshot_price_repeats": 0,
        "version_duplicates": 0, "version_backtracks": 0, "version_gaps": 0,
        "version_gap_examples": [],
        "symbols_seen": set(),
    })
    previous_versions: dict[str, int] = {}
    previous_update_levels: dict[str, set[tuple[str, str, str]]] = defaultdict(set)
    previous_snapshot_levels: dict[str, set[tuple[str, str, str]]] = defaultdict(set)

    for filename, typ in (("depth_snapshot.jsonl.gz", "snapshot"), ("depth_update.jsonl.gz", "update")):
        path = batch / filename
        for rec in records(path):
            d = extract(rec)
            symbol = str(d.get("s") or rec.get("pair") or "UNKNOWN")
            s = stats[symbol]
            s["symbols_seen"].add(symbol)
            levels = []
            asks = normalize_levels(d.get("asks"))
            bids = normalize_levels(d.get("bids"))
            for price, qty in asks.items():
                levels.append(("a", price, qty))
            for price, qty in bids.items():
                levels.append(("b", price, qty))
            s[f"{typ}s"] += 1
            s[f"levels_per_{typ}"].append(len(levels))
            current_set = set(levels)
            prev = previous_update_levels[symbol] if typ == "update" else previous_snapshot_levels[symbol]
            if prev:
                s[f"{typ}_price_repeats"] += len({(side, price) for side, price, _ in current_set} & {(side, price) for side, price, _ in prev})
            if typ == "update":
                previous_update_levels[symbol] = current_set
            else:
                previous_snapshot_levels[symbol] = current_set

            vs = d.get("vs")
            if vs is not None:
                try:
                    v = int(vs)
                    last = previous_versions.get(symbol)
                    if last is not None:
                        if v == last:
                            s["version_duplicates"] += 1
                        elif v < last:
                            s["version_backtracks"] += 1
                        elif v > last + 1:
                            s["version_gaps"] += 1
                            if len(s["version_gap_examples"]) < 20:
                                s["version_gap_examples"].append({"previous": last, "current": v, "gap": v - last - 1})
                    previous_versions[symbol] = v
                    s["versions"].append(v)
                except (TypeError, ValueError):
                    pass
            ts = d.get("ts", d.get("T"))
            try:
                if ts is not None:
                    s["timestamp_ms"].append(int(ts))
            except (TypeError, ValueError):
                pass

    output = {"schema_version": 1, "classification": {}, "symbols": {}}
    for symbol, s in stats.items():
        versions = s["versions"]
        level_counts_u = s["levels_per_update"]
        level_counts_s = s["levels_per_snapshot"]
        evidence = []
        if s["updates"] == 0:
            classification = "UNKNOWN_NO_UPDATES"
        else:
            if s["version_backtracks"]:
                evidence.append("version_backtracks_present")
            if s["version_gaps"]:
                evidence.append("version_jumps_present")
            if level_counts_s and level_counts_u:
                evidence.append("both_snapshot_and_update_payloads_present")
            if level_counts_u:
                evidence.append(f"update_level_count_range={min(level_counts_u)}..{max(level_counts_u)}")
            # We deliberately stay conservative: a monotonic version series is not enough.
            if not s["version_backtracks"] and s["updates"] > 20 and s["snapshots"] > 0:
                classification = "NEEDS_SEMANTICS_CONFIRMATION"
            else:
                classification = "UNKNOWN"
        serial = dict(s)
        serial["symbols_seen"] = sorted(serial["symbols_seen"])
        serial["version_min"] = min(versions) if versions else None
        serial["version_max"] = max(versions) if versions else None
        serial["evidence"] = evidence
        for k in ("versions", "timestamp_ms", "levels_per_update", "levels_per_snapshot"):
            serial.pop(k, None)
        output["symbols"][symbol] = serial
        output["classification"][symbol] = classification

    overall_values = list(output["classification"].values())
    if overall_values and all(v == "NEEDS_SEMANTICS_CONFIRMATION" for v in overall_values):
        overall = "UNCERTIFIED_NEEDS_DIRECT_PAYLOAD_SEMANTICS_TEST"
    elif overall_values:
        overall = "UNCERTIFIED"
    else:
        overall = "NO_DEPTH_DATA"
    output["overall_classification"] = overall
    return output


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True)
    args = ap.parse_args()
    result = analyze(Path(args.batch))
    Path(args.batch, "depth_validation.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
