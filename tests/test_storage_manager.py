from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
import storage_manager


def art(i, size, created):
    return {'id': i, 'name': f'coindcx-raw-{i}', 'size_in_bytes': size, 'created_at': created}


def test_select_uses_total_all_budget_not_undefined_variable():
    eligible = [art(1, 100, '2026-09-09T00:00:00Z'), art(2, 100, '2026-09-09T01:00:00Z')]
    selected = storage_manager.select_artifacts_for_target(eligible, 400, 250)
    assert [a['id'] for a in selected] == [1, 2]


def test_select_stops_once_total_hits_target():
    eligible = [art(1, 80, '2026-09-09T00:00:00Z'), art(2, 80, '2026-09-09T01:00:00Z')]
    selected = storage_manager.select_artifacts_for_target(eligible, 240, 160)
    assert [a['id'] for a in selected] == [1]


def test_select_force_marks_all_eligible():
    eligible = [art(1, 10, '2026-09-09T00:00:00Z'), art(2, 10, '2026-09-09T01:00:00Z')]
    selected = storage_manager.select_artifacts_for_target(eligible, 20, 20, force=True)
    assert len(selected) == 2
