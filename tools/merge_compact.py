#!/usr/bin/env python3
"""Merge current batch compact layers into month-partitioned Git history.

Longer forward-return labels are recalculated after each merge over the full
compact history. This preserves labels that cross independent collector-batch
boundaries instead of permanently losing the last few horizons of every batch.
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


LABEL_HORIZONS_SECONDS = (60, 180, 300, 600, 900, 1800)


def read_gz_csv(path: Path):
    with gzip.open(path, "rt", encoding="utf-8", newline="") as fh:
        yield from csv.DictReader(fh)


def read_csv(path: Path):
    with path.open("rt", encoding="utf-8", newline="") as fh:
        yield from csv.DictReader(fh)


def read_all_rows(root: Path) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not root.exists():
        return rows
    for path in sorted(root.glob("20??-??.csv")):
        rows.extend(read_csv(path))
    return rows


def add_forward_returns(
    rows: list[dict[str, str]],
    epoch_key: str,
    horizons: tuple[int, ...],
) -> list[dict[str, str]]:
    """Add close-to-close forward returns from merged compact history."""
    by_symbol: dict[str, dict[int, dict[str, str]]] = defaultdict(dict)
    for row in rows:
        try:
            epoch = int(row[epoch_key])
        except (KeyError, TypeError, ValueError):
            continue
        by_symbol[row["symbol"]][epoch] = row

    for by_epoch in by_symbol.values():
        for epoch, row in by_epoch.items():
            try:
                p0 = float(row.get("close"))
            except (TypeError, ValueError):
                p0 = None
            for horizon in horizons:
                future = by_epoch.get(epoch + horizon)
                p1 = None
                if future is not None:
                    try:
                        p1 = float(future.get("close"))
                    except (TypeError, ValueError):
                        p1 = None
                row[f"forward_return_{horizon}s"] = (p1 / p0 - 1.0) if p0 and p1 else ""
    return rows


def merge_layer(
    incoming: list[dict[str, str]], root: Path, epoch_key: str
) -> tuple[int, list[str]]:
    if not incoming:
        return 0, []
    root.mkdir(parents=True, exist_ok=True)

    existing_all = read_all_rows(root)
    keyed = {(r["symbol"], r[epoch_key]): r for r in existing_all}
    before = len(keyed)
    for row in incoming:
        keyed[(row["symbol"], row[epoch_key])] = row

    merged_all = sorted(keyed.values(), key=lambda r: (r["symbol"], int(r[epoch_key])))
    merged_all = add_forward_returns(merged_all, epoch_key, LABEL_HORIZONS_SECONDS)

    by_month: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in merged_all:
        dt = datetime.fromtimestamp(int(row[epoch_key]), tz=timezone.utc)
        by_month[dt.strftime("%Y-%m")].append(row)

    changed_files: list[str] = []
    for month, month_rows in sorted(by_month.items()):
        path = root / f"{month}.csv"
        fields = sorted({k for r in month_rows for k in r})
        with path.open("w", encoding="utf-8", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=fields)
            writer.writeheader()
            writer.writerows(month_rows)
        changed_files.append(str(path))

    index_path = root / "index.json"
    months = sorted(p.stem for p in root.glob("20??-??.csv"))
    index_path.write_text(
        json.dumps(
            {
                "schema_version": 3,
                "months": months,
                "epoch_key": epoch_key,
                "forward_label_horizons_seconds": list(LABEL_HORIZONS_SECONDS),
                "generated_utc": datetime.now(timezone.utc).isoformat(),
                "note": (
                    "Compact history is retained as long-lived research data; raw websocket data "
                    "lives in Actions artifacts/release archives. Longer forward-return labels are "
                    "recalculated after merge so collector batch-boundary labels are not lost."
                ),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return max(0, len(keyed) - before), changed_files


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

    added1, files1 = merge_layer(rows, Path(args.dest_root), "minute_epoch")
    added3, files3 = merge_layer(rows3, Path(args.dest_root_3m), "three_minute_epoch")
    print(
        json.dumps(
            {
                "rows_added_1m": added1,
                "changed_files_1m": files1,
                "rows_added_3m": added3,
                "changed_files_3m": files3,
                "forward_label_horizons_seconds": list(LABEL_HORIZONS_SECONDS),
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
