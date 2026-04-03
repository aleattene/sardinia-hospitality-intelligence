"""Smoke tests — verify basic utilities work without requiring env vars."""

from src.utils.runtime import Timer, timestamp_now


def test_timestamp_format() -> None:
    """timestamp_now returns a string in YYYYMMDD_HHMMSS format."""
    ts = timestamp_now()
    assert len(ts) == 15
    assert ts[8] == "_"


def test_timer_measures_elapsed() -> None:
    """Timer context manager records a non-negative elapsed time."""
    with Timer("test") as t:
        pass
    assert t.elapsed >= 0.0
