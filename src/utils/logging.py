"""Logging configuration for the project."""

import logging
import sys


def setup_logging(level: int = logging.INFO) -> None:
    """Configure root logger with a consistent format.

    Args:
        level: Logging level (default: INFO).
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
