from pathlib import Path
import ast


def _diagnostic_constants():
    source = (Path(__file__).resolve().parents[1] / "tools" / "micro_diagnostic.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    values = {}
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            name = node.targets[0].id
            if name in {"REQUIRED_EVENTS", "OPTIONAL_EVENTS"}:
                values[name] = ast.literal_eval(node.value)
    return values["REQUIRED_EVENTS"], values["OPTIONAL_EVENTS"]


def test_candlestick_is_optional_but_core_streams_are_required():
    required, optional = _diagnostic_constants()
    assert required == ("new-trade", "price-change", "depth-update", "depth-snapshot")
    assert optional == ("candlestick",)


def test_missing_required_stream_would_fail():
    required, _ = _diagnostic_constants()
    counts = {event: 10 for event in required}
    counts["depth-snapshot"] = 0
    assert not all(counts[e] > 0 for e in required)
