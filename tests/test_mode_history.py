from spectre.mode_history import ModeHistory


def test_empty_history_has_no_last_transition():
    history = ModeHistory()
    assert history.last is None
    assert history.transitions == []


def test_record_returns_and_stores_transition():
    history = ModeHistory()
    transition = history.record("MONITOR", "TRACK", "task-1")

    assert transition.from_mode == "MONITOR"
    assert transition.to_mode == "TRACK"
    assert transition.task_id == "task-1"
    assert history.last is transition
    assert history.transitions == [transition]


def test_records_accumulate_in_order():
    history = ModeHistory()
    history.record("MONITOR", "TRACK", "task-1")
    history.record("TRACK", "MONITOR", "task-2")

    modes = [(t.from_mode, t.to_mode) for t in history.transitions]
    assert modes == [("MONITOR", "TRACK"), ("TRACK", "MONITOR")]
    assert history.last.to_mode == "MONITOR"


def test_history_is_capped_at_max_len():
    history = ModeHistory(max_len=3)
    for i in range(5):
        history.record(f"MODE_{i}", f"MODE_{i + 1}", f"task-{i}")

    assert len(history.transitions) == 3
    assert history.transitions[0].from_mode == "MODE_2"
    assert history.last.to_mode == "MODE_5"


def test_transitions_property_returns_a_copy():
    history = ModeHistory()
    history.record("MONITOR", "TRACK", "task-1")

    snapshot = history.transitions
    snapshot.clear()

    assert len(history.transitions) == 1
