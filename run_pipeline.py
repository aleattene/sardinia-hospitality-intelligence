"""Pipeline orchestrator: CSV → DuckDB staging → (transform) → (export).

Usage:
    python -m run_pipeline

Steps:
    01 — Ingest:    normalize raw CSV files → DuckDB staging tables
    02 — Transform: SQL views and aggregate queries  (not yet implemented)
    03 — Export:    DuckDB → CSV for notebook and dashboard  (not yet implemented)
"""

import logging

from src import config
from src.pipeline import step_01_ingest
from src.utils.db import get_connection
from src.utils.logging import setup_logging
from src.utils.runtime import Timer

logger: logging.Logger = logging.getLogger(__name__)


def main() -> None:
    """Run the full pipeline."""
    setup_logging()
    logger.info("Pipeline started.")
    logger.info("DB: %s", config.DB_PATH)
    logger.info("Raw data: %s", config.RAW_DATA_DIR)

    with Timer() as t:
        conn = get_connection(config.DB_PATH)
        step_01_ingest.run(conn)
        conn.close()

    logger.info("Pipeline completed in %.2fs.", t.elapsed)


if __name__ == "__main__":
    main()
