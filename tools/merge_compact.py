#!/usr/bin/env python3
"""Merge current batch 1m features into immutable/month-partitioned Git data.

Monthly plain-text partitions are intentionally used instead of rewriting one
compressed binary file on every batch. This keeps Git's own delta compression
useful and avoids needlessly ballooning repository history.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


def read_gz_csv(path: Path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        yield from csv.DictReader(fh)


def read_csv(path: Path):
    with path.open("rt", encoding="utf-8", newline="") as fh:
        yield from csv.DictReader(fh)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", required=True)
    ap.add_argument("--dest-root", default="data/research/compact_1m")
    ap.add_argument("--dest-root-3m", default="data/research/compact_3m")
    args = ap.parse_args()

    source = Path(args.batch) / "features_1m.csv.gz"
    source3 = Path(args.batch) / "features_3m.csv.gz"
    rows = list(read_gz_csv(source)) if source.exists() else []
    rows3 = list(read_gz_csv(source3)) if source3.exists() else []
    if not rows and not rows3:
        print(json.dumps({"rows_added_1m": 0, "rows_added_3m": 0}))
        return 0

    def merge_layer(incoming: list[dict[str, str]], root: Path, epoch_key: str) -> tuple[int, list[str]]:
        if not incoming:
            return 0, []
        root.mkdir(parents=True, exist_ok=True)
        by_month: dict[str, list[dict[str, str]]] = defaultdict(list)
        for row in incoming:
            dt = datetime.fromtimestamp(int(row[epoch_key]), tz=timezone.utc)
            by_month[dt.strftime("%Y-%m")].append(row)
        changed_files: list[str] = []
        added = 0
        for month, batch_rows in sorted(by_month.items()):
            path = root / f"{month}.csv"
            existing = list(read_csv(path)) if path.exists() else []
            keyed = {(r["symbol"], r[epoch_key]): r for r in existing}
            before = len(keyed)
            for row in batch_rows:
                keyed[(row["symbol"], row[epoch_key])] = row
            merged = sorted(keyed.values(), key=lambda r: (r["symbol"], int(r[epoch_key])))
            fields = sorted({k for r in merged for k in r})
            with path.open("w", encoding="utf-8", newline="") as fh:
                writer = csv.DictWriter(fh, fieldnames=fields)
                writer.writeheader()
                writer.writerows(merged)
            added += max(0, len(merged) - before)
            changed_files.append(str(path))
        index_path = root / "index.json"
        months = sorted(p.stem for p in root.glob("20??-??.csv"))
        index_path.write_text(json.dumps({
            "schema_version": 2,
            "months": months,
            "epoch_key": epoch_key,
            "generated_utc": datetime.now(timezone.utc).isoformat(),
            "note": "Compact history is retained as long-lived research data; raw websocket data lives in Actions artifacts/release archives."
        }, indent=2), encoding="utf-8")
        return added, changed_files

    added1, files1 = merge_layer(rows, Path(args.dest_root), "minute_epoch")
    added3, files3 = merge_layer(rows3, Path(args.dest_root_3m), "three_minute_epoch")
    print(json.dumps({"rows_added_1m": added1, "changed_files_1m": files1, "rows_added_3m": added3, "changed_files_3m": files3}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
