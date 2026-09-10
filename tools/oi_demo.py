#!/usr/bin/env python3
"""Small isolated CoinDCX futures OI discovery probe.

This does not modify the production collector and does not require API keys.
It checks documented public futures endpoints plus a small set of plausible OI
route names, recording HTTP status, response shape, and whether an OI-like
field is actually exposed. A failed probe is data, not a workflow failure.
"""
from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

BASE = "https://api.coindcx.com"
PUBLIC_BASE = "https://public.coindcx.com"
PAIRS = ["B-BTC_USDT", "B-ETH_USDT"]


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_ist(ms: int) -> str:
    from datetime import timedelta, timezone
    ist = timezone(timedelta(hours=5, minutes=30))
    return datetime.fromtimestamp(ms / 1000, tz=ist).isoformat()


def summarize_json(obj: Any) -> dict[str, Any]:
    fields: list[str] = []
    sample = obj
    if isinstance(obj, dict):
        fields = sorted(str(k) for k in obj.keys())
        sample = {k: obj[k] for k in list(obj)[:12]}
    elif isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            fields = sorted(str(k) for k in obj[0].keys())
            sample = [{k: obj[0][k] for k in list(obj[0])[:12]}]
        else:
            sample = obj[:3]
    lower = {f.lower() for f in fields}
    oi_fields = sorted(f for f in fields if "open" in f.lower() or "interest" in f.lower())
    return {"type": type(obj).__name__, "fields": fields, "oi_like_fields": oi_fields, "sample": sample}


def probe(session: requests.Session, name: str, method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    started = time.time()
    rec: dict[str, Any] = {"name": name, "method": method, "url": url}
    try:
        r = session.request(method, url, timeout=15, **kwargs)
        rec.update({
            "status_code": r.status_code,
            "elapsed_ms": round((time.time() - started) * 1000, 1),
            "content_type": r.headers.get("content-type"),
        })
        try:
            obj = r.json()
            rec["json"] = summarize_json(obj)
            rec["contains_oi_field"] = bool(rec["json"]["oi_like_fields"])
        except ValueError:
            rec["text_prefix"] = r.text[:500]
            rec["contains_oi_field"] = False
    except requests.RequestException as exc:
        rec.update({"error": str(exc), "elapsed_ms": round((time.time() - started) * 1000, 1), "contains_oi_field": False})
    return rec


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    s = requests.Session()
    s.headers["User-Agent"] = "AdvisorX-OI-Demo/1.0"

    results: list[dict[str, Any]] = []

    # Public futures discovery / sanity checks documented by CoinDCX.
    results.append(probe(
        s, "active_instruments", "GET",
        BASE + "/exchange/v1/derivatives/futures/data/active_instruments",
        params={"margin_currency_short_name[]": "USDT"},
    ))
    for pair in PAIRS:
        results.append(probe(
            s, f"instrument:{pair}", "GET",
            BASE + "/exchange/v1/derivatives/futures/data/instrument",
            params={"pair": pair, "margin_currency_short_name": "USDT"},
        ))
        results.append(probe(
            s, f"futures_orderbook:{pair}", "GET",
            PUBLIC_BASE + f"/market_data/v3/orderbook/{pair}-futures/20",
        ))
        results.append(probe(
            s, f"futures_trades:{pair}", "GET",
            BASE + "/exchange/v1/derivatives/futures/data/trades",
            params={"pair": pair},
        ))
        results.append(probe(
            s, f"documented_stats:{pair}", "GET",
            BASE + "/api/v1/derivatives/futures/data/stats",
            params={"pair": pair},
        ))

        # Deliberately small discovery set. These are NOT treated as supported
        # endpoints; the point is to see whether CoinDCX currently exposes an
        # unauthenticated OI field under a plausible public route.
        for suffix in (
            "/exchange/v1/derivatives/futures/data/open_interest",
            "/api/v1/derivatives/futures/data/open_interest",
            "/exchange/v1/derivatives/futures/data/openInterest",
            "/api/v1/derivatives/futures/data/openInterest",
        ):
            results.append(probe(s, f"candidate_oi:{suffix}:{pair}", "GET", BASE + suffix, params={"pair": pair}))

    oi_hits = [x for x in results if x.get("contains_oi_field")]
    likely_public_oi = [x for x in oi_hits if x.get("status_code") == 200]
    summary = {
        "generated_at_ms": now_ms(),
        "generated_at_ist": iso_ist(now_ms()),
        "pairs": PAIRS,
        "probe_count": len(results),
        "oi_field_hits": len(oi_hits),
        "likely_public_oi_hits": len(likely_public_oi),
        "interpretation": (
            "Found a 200 response containing an OI-like field; inspect raw probe records before integrating."
            if likely_public_oi else
            "No unauthenticated 200 response in this small probe exposed an OI-like field."
        ),
        "results": results,
    }
    (out / "oi_demo_report.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    (out / "README.txt").write_text(
        "This is an isolated demo probe. It does not modify AdvisorX production collection.\n"
        "A 200 response is not enough for integration: response semantics and timestamp behavior must also be validated.\n",
        encoding="utf-8",
    )
    print(json.dumps({k: summary[k] for k in summary if k != "results"}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
