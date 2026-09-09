#!/usr/bin/env python3
"""Recover the self-chaining collector if it becomes stale.

The watchdog is deliberately conservative so it does not create overlapping
collectors at normal batch boundaries.
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import subprocess
import sys


def iso_now():
    return dt.datetime.now(dt.timezone.utc)


def run_gh(repo: str, args: list[str]) -> str:
    out = subprocess.check_output(["gh", "api", *args], text=True)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True)
    ap.add_argument("--stale-minutes", type=int, default=35)
    args = ap.parse_args()

    import json
    data = json.loads(run_gh(args.repo, [f"repos/{args.repo}/actions/runs", "--method", "GET", "-f", "per_page=30"]))
    runs = [r for r in data.get("workflow_runs", []) if r.get("name") == "AdvisorX CoinDCX continuous collector"]
    active = [r for r in runs if r.get("status") in {"queued", "in_progress"}]
    if active:
        print(f"Collector active/queued: {len(active)}")
        return 0

    if not runs:
        should_start = True
        reason = "no collector run found"
    else:
        latest = runs[0]
        stamp = latest.get("updated_at") or latest.get("created_at")
        when = dt.datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        age_min = (iso_now() - when).total_seconds() / 60
        should_start = age_min >= args.stale_minutes
        reason = f"latest run age={age_min:.1f}m"

    if should_start:
        subprocess.check_call([
            "gh", "api", "--method", "POST", f"repos/{args.repo}/dispatches",
            "-f", "event_type=coindcx-next-batch",
            "-f", "client_payload[reason]=watchdog_recovery",
        ])
        print(f"Triggered collector recovery: {reason}")
    else:
        print(f"Collector healthy enough: {reason}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
