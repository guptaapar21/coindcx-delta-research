#!/usr/bin/env python3
"""Download all CoinDCX Release archives to a local folder.

This can be run periodically on a PC/server or in Termux. It is intentionally
outside GitHub Actions so files land on a machine you control.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import requests

API = "https://api.github.com"


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, help="owner/repo")
    ap.add_argument("--dest", default="advisorx_archives")
    args = ap.parse_args()
    owner, repo = args.repo.split("/", 1)
    dest = Path(args.dest)
    dest.mkdir(parents=True, exist_ok=True)

    releases = requests.get(f"{API}/repos/{owner}/{repo}/releases", params={"per_page": 100}, timeout=30)
    releases.raise_for_status()

    index_path = dest / "archive_index.json"
    index = json.loads(index_path.read_text()) if index_path.exists() else {}
    downloaded = index.setdefault("downloaded_assets", {})

    for rel in releases.json():
        for asset in rel.get("assets", []):
            name = asset.get("name", "")
            if not name.startswith("coindcx_raw_archive_"):
                continue
            aid = str(asset["id"])
            if aid in downloaded and Path(downloaded[aid]["path"]).exists():
                continue
            r = requests.get(asset["browser_download_url"], timeout=300)
            r.raise_for_status()
            out = dest / name
            out.write_bytes(r.content)
            downloaded[aid] = {
                "name": name,
                "release_tag": rel.get("tag_name"),
                "path": str(out),
                "size": len(r.content),
                "sha256": sha256_bytes(r.content),
            }
            index_path.write_text(json.dumps(index, indent=2), encoding="utf-8")
            print(f"Downloaded {name}")

    print(f"Archive sync complete: {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
