#!/usr/bin/env python3
"""Lightweight inventory tool; this intentionally does not backtest signals yet."""
from __future__ import annotations
import argparse, json
from pathlib import Path
from collections import Counter

p = argparse.ArgumentParser()
p.add_argument("run_dir")
a = p.parse_args()
run = Path(a.run_dir)
counts = Counter()
min_ts = None
max_ts = None
for f in run.glob("*.jsonl"):
    for line in f.open(encoding="utf-8"):
        if not line.strip(): continue
        x = json.loads(line)
        counts[f.name] += 1
        ts = x.get("exchange_ts_ms")
        if isinstance(ts, (int, float)):
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)
print(json.dumps({"files": dict(counts), "first_exchange_ts_ms": min_ts, "last_exchange_ts_ms": max_ts}, indent=2))
