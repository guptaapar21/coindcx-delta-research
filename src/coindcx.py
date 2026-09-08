from __future__ import annotations
import json
from pathlib import Path
from typing import Any
import requests

BASE_API = "https://api.coindcx.com"
SPOT_SOCKET = "https://stream-spot.coindcx.com"


def get_json(url: str, params: dict[str, Any] | None = None, timeout: int = 15) -> Any:
    r = requests.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def fetch_market_details(timeout: int = 15) -> Any:
    return get_json(f"{BASE_API}/exchange/v1/markets_details", timeout=timeout)


def fetch_orderbook(pair: str, depth: int = 50, timeout: int = 15) -> Any:
    return get_json(f"{BASE_API}/market_data/orderbook", params={"pair": pair, "depth": depth}, timeout=timeout)


def fetch_candles(pair: str, interval: str, limit: int = 1000, timeout: int = 15) -> Any:
    return get_json(
        f"{BASE_API}/market_data/candles",
        params={"pair": pair, "interval": interval, "limit": limit},
        timeout=timeout,
    )


def write_bootstrap(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
