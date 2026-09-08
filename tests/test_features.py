from src.features import aggressor_side, orderbook_metrics


def test_aggressor_mapping():
    assert aggressor_side(True) == "SELL"
    assert aggressor_side(False) == "BUY"
    assert aggressor_side(1) == "SELL"
    assert aggressor_side(0) == "BUY"


def test_orderbook_metrics():
    r = orderbook_metrics({"100": "2", "99": "3"}, {"101": "1", "102": "4"})
    assert r["best_bid"] == 100
    assert r["best_ask"] == 101
    assert r["spread"] == 1
    assert r["bid_qty_5"] == 5
    assert r["ask_qty_5"] == 5
    assert r["imbalance_5"] == 0
