"""Step 02 — Transform: create SQL views and materialize analytical queries into DuckDB.

Execution order:
  1. sql/views/   — CREATE OR REPLACE VIEW statements (alphabetical order)
  2. sql/queries/ — CREATE OR REPLACE TABLE statements (alphabetical order)

Views depend on staging tables loaded in Step 01.
Queries depend on views, so views must be executed first.
"""

import logging

import duckdb

from src import config
from src.utils.db import execute_sql_directory

logger: logging.Logger = logging.getLogger(__name__)


def run(conn: duckdb.DuckDBPyConnection) -> None:
    """Execute transform step: create views and materialize analytical queries.

    Args:
        conn: Active DuckDB connection.
    """
    logger.info("=== Step 02: Transform ===")

    logger.info("Creating analytical views...")
    execute_sql_directory(conn, config.SQL_VIEWS_DIR)

    logger.info("Materializing analytical queries...")
    execute_sql_directory(conn, config.SQL_QUERIES_DIR)

    logger.info("=== Step 02 complete ===")


if __name__ == "__main__":
    from src.utils.db import get_connection
    from src.utils.logging import setup_logging

    setup_logging()
    with get_connection(config.DB_PATH) as conn:
        run(conn)
