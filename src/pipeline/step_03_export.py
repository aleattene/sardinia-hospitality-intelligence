"""Step 03 — Export: write analytical tables and views from DuckDB to CSV.

Exports two categories of objects:
  - Materialized query tables (q_*): ready-to-use analytical outputs
  - Analytical views (v_*): full grain data for flexible notebook exploration

Output files land in ANALYSIS_OUTPUT_DIR (configured via env var).
File names match the DuckDB object name (e.g. q_priority_score.csv).
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

from src import config

logger: logging.Logger = logging.getLogger(__name__)

# Materialized tables produced by step_02_transform (sql/queries/).
_QUERY_TABLES: list[str] = [
    "q_priority_score",
    "q_seasonality_extremes",
    "q_top_growth_segments",
]

# Views to export for notebook exploration (sql/views/).
# Excludes v_seasonality_profile (high cardinality — province × month × type × year)
# which the notebook can query directly from DuckDB if needed.
_VIEWS: list[str] = [
    "v_demand_by_province",
    "v_supply_by_province",
    "v_supply_demand_gap",
    "v_segment_origin",
    "v_segment_accommodation",
    "v_trend_yoy",
]


def _export_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    output_dir: Path,
) -> int:
    """Export a single DuckDB table or view to CSV.

    Args:
        conn: Active DuckDB connection.
        table: Name of the table or view to export.
        output_dir: Destination directory for the CSV file.

    Returns:
        Number of rows exported.
    """
    df: pd.DataFrame = conn.execute(f"SELECT * FROM {table}").df()  # noqa: S608
    output_path: Path = output_dir / f"{table}.csv"
    df.to_csv(output_path, index=False)
    logger.info("  %-40s %6d rows → %s", table, len(df), output_path.name)
    return len(df)


def run(conn: duckdb.DuckDBPyConnection) -> None:
    """Execute export step: write all analytical outputs to CSV files.

    Args:
        conn: Active DuckDB connection.
    """
    logger.info("=== Step 03: Export ===")

    output_dir: Path = config.ANALYSIS_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows: int = 0

    logger.info("Exporting materialized query tables...")
    for table in _QUERY_TABLES:
        total_rows += _export_table(conn, table, output_dir)

    logger.info("Exporting analytical views...")
    for view in _VIEWS:
        total_rows += _export_table(conn, view, output_dir)

    logger.info(
        "Export complete: %d objects, %d total rows → %s",
        len(_QUERY_TABLES) + len(_VIEWS),
        total_rows,
        output_dir,
    )
    logger.info("=== Step 03 complete ===")


if __name__ == "__main__":
    from src.utils.db import get_connection
    from src.utils.logging import setup_logging

    setup_logging()
    with get_connection(config.DB_PATH) as conn:
        run(conn)
