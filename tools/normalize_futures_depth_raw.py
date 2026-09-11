#!/usr/bin/env python3
"""Normalize CoinDCX Futures depth symbols in a processing copy of the raw file."""
from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path

ALIASES = {
    "BTCUSDT": "B-BTC_USDT", "B-BTCUSDT": "B-BTC_USDT",
    "ETHUSDT": "B-ETH_USDT", "B-ETHUSDT": "B-ETH_USDT",
    "SOLUSDT": "B-SOL_USDT", "B-SOLUSDT": "B-SOL_USDT",
    "SUIUSDT": "B-SUI_USDT", "B-SUIUSDT": "B-SUI_USDT",
    "XRPUSDT": "B-XRP_USDT", "B-XRPUSDT": "B-XRP_USDT",
    "DOGEUSDT": "B-DOGE_USDT", "B-DOGEUSDT": "B-DOGE_USDT",
}


def canonical(v: object) -> str:
    s = str(v).upper().strip()
    return ALIASES.get(s, s)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()
    path = Path(args.batch) / "futures_depth_snapshot.jsonl.gz"
    if not path.exists():
        raise SystemExit(f"missing {path}")

    tmp = path.with_suffix(path.suffix + ".tmp")
    changed = 0
    with gzip.open(path, "rt", encoding="utf-8") as src, gzip.open(tmp, "wt", encoding="utf-8") as dst:
        for line in src:
            if not line.strip():
                continue
            rec = json.loads(line)
            raw = rec.get("raw", rec)
            data = raw.get("data") if isinstance(raw, dict) else None
            if isinstance(data, dict) and data.get("s") is not None:
                old = str(data["s"])
                new = canonical(old)
                if new != old:
                    data["s"] = new
                    changed += 1
            dst.write(json.dumps(rec, separators=(",", ":"), ensure_ascii=False) + "\n")
    tmp.replace(path)
    print(f"normalized_futures_depth_symbols={changed}")


if __name__ == "__main__":
    main()
