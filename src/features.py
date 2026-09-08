from __future__ import annotations
from typing import Any, Iterable

DEPTHS = (1, 5, 10, 20, 50)


def maker_flag(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and value in (0, 1):
        return bool(value)
    if isinstance(value, str):
        v = value.strip().lower()
        if v in {"true", "1", "yes", "y"}:
            return True
        if v in {"false", "0", "no", "n"}:
            return False
    return None


def aggressor_side(m: Any) -> str | None:
    flag = maker_flag(m)
    if flag is None:
        return None
    return "SELL" if flag else "BUY"


def level_list(levels: dict[str, Any] | None, reverse: bool) -> list[tuple[float, float]]:
    out: list[tuple[float, float]] = []
    for price, qty in (levels or {}).items():
        try:
            p = float(price)
            q = float(qty)
            if p > 0 and q >= 0:
                out.append((p, q))
        except (TypeError, ValueError):
            continue
    return sorted(out, key=lambda x: x[0], reverse=reverse)


def orderbook_metrics(bids_raw: dict[str, Any], asks_raw: dict[str, Any]) -> dict[str, Any]:
    bids = level_list(bids_raw, True)
    asks = level_list(asks_raw, False)
    result: dict[str, Any] = {
        "best_bid": bids[0][0] if bids else None,
        "best_ask": asks[0][0] if asks else None,
    }
    bb, ba = result["best_bid"], result["best_ask"]
    if bb is not None and ba is not None and ba >= bb:
        result["spread"] = ba - bb
        result["mid"] = (ba + bb) / 2.0
        bid_qty_1 = bids[0][1] if bids else 0.0
        ask_qty_1 = asks[0][1] if asks else 0.0
        denom = bid_qty_1 + ask_qty_1
        result["microprice"] = ((ba * bid_qty_1) + (bb * ask_qty_1)) / denom if denom else result["mid"]
    else:
        result["spread"] = None
        result["mid"] = None
        result["microprice"] = None
    for n in DEPTHS:
        b = sum(q for _, q in bids[:n])
        a = sum(q for _, q in asks[:n])
        d = b + a
        result[f"bid_qty_{n}"] = b
        result[f"ask_qty_{n}"] = a
        result[f"imbalance_{n}"] = (b - a) / d if d else 0.0
    result["bid_levels_received"] = len(bids)
    result["ask_levels_received"] = len(asks)
    return result
