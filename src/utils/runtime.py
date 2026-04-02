"""Runtime utilities: timers and timestamps."""

import time
from datetime import datetime, timezone


def timestamp_now() -> str:
    """Return current UTC timestamp as string (YYYYMMDD_HHMMSS)."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


class Timer:
    """Simple context-manager timer for measuring elapsed time."""

    def __init__(self, label: str = "") -> None:
        self.label: str = label
        self.elapsed: float = 0.0
        self._start: float = 0.0

    def __enter__(self) -> "Timer":
        self._start = time.perf_counter()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        self.elapsed = time.perf_counter() - self._start
