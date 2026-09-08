#!/usr/bin/env python3
from __future__ import annotations
import argparse, json
from pathlib import Path
from collections import Counter


def validate(run_dir: Path) -> int:
    errors = []
    counts = Counter()
    for path in sorted(run_dir.glob("*.jsonl")):
        for lineno, line in enumerate(path.open(encoding="utf-8"), 1):
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                errors.append(f"{path.name}:{lineno}: invalid JSON: {e}")
                continue
            counts[path.name] += 1
            if path.name.endswith("_trades.jsonl"):
                for key in ("recv_ts_ms", "exchange_ts_ms", "price", "quantity", "aggressor_side"):
                    if key not in obj:
                        errors.append(f"{path.name}:{lineno}: missing {key}")
            if path.name.endswith("_depth.jsonl"):
                for key in ("recv_ts_ms", "exchange_ts_ms", "best_bid", "best_ask", "mid"):
                    if key not in obj:
                        errors.append(f"{path.name}:{lineno}: missing {key}")
    print("Files:")
    for name, count in counts.items():
        print(f"  {name}: {count}")
    if errors:
        print("\nErrors:")
        print("\n".join(errors[:50]))
        return 1
    print("\nValidation passed.")
    return 0

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("run_dir")
    a = p.parse_args()
    raise SystemExit(validate(Path(a.run_dir)))
