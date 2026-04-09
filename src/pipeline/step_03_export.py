"""Step 03 — Export: write analytical tables and views from DuckDB to CSV.

Exports two categories of objects:
  - Materialized query tables (q_*): ready-to-use analytical outputs
  - Analytical views (v_*): full grain data for flexible notebook exploration

Output files land in ANALYSIS_OUTPUT_DIR (configured via env var).
File names match the DuckDB object name (e.g. q_priority_score.csv).

If PUSH_TO_SHEETS=true, each exported table is also pushed to Google Sheets
after CSV export. Authentication is performed once before the export loop.
"""

import logging
from pathlib import Path

import duckdb
import gspread
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
# and v_segment_origin (~2M rows) which are queried directly from DuckDB when needed.
# v_segment_origin_summary replaces v_segment_origin for Looker Studio / Google Sheets.
_VIEWS: list[str] = [
    "v_demand_by_province",
    "v_supply_by_province",
    "v_supply_demand_gap",
    "v_segment_origin_summary",
    "v_segment_accommodation",
    "v_trend_yoy",
]

# Union of allowed export targets — used to validate identifiers before query execution.
_ALLOWED_EXPORT_TARGETS: frozenset[str] = frozenset(_QUERY_TABLES + _VIEWS)


def _export_table(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    output_dir: Path,
) -> tuple[pd.DataFrame, int]:
    """Export a single DuckDB table or view to CSV.

    Args:
        conn: Active DuckDB connection.
        table: Name of the table or view to export.
        output_dir: Destination directory for the CSV file.

    Returns:
        Tuple of (DataFrame, row count).

    Raises:
        ValueError: If table is not in the allowed export targets.
    """
    if table not in _ALLOWED_EXPORT_TARGETS:
        raise ValueError(f"Export target '{table}' is not an allowed table or view.")
    df: pd.DataFrame = conn.execute(f"SELECT * FROM {table}").df()  # noqa: S608
    output_path: Path = output_dir / f"{table}.csv"
    df.to_csv(output_path, index=False)
    logger.info("  %-40s %6d rows → %s", table, len(df), output_path.name)
    return df, len(df)


def run(conn: duckdb.DuckDBPyConnection) -> None:
    """Execute export step: write all analytical outputs to CSV files.

    If PUSH_TO_SHEETS=true, authenticates once via macOS Keychain and pushes
    each exported table to Google Sheets after CSV export (fail-fast on config
    or auth errors before any export begins).

    Args:
        conn: Active DuckDB connection.
    """
    logger.info("=== Step 03: Export ===")

    output_dir: Path = config.ANALYSIS_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Google Sheets: fail-fast validation before export loop ---
    sheets_client: gspread.Client | None = None
    spreadsheet_id: str | None = None

    if config.PUSH_TO_SHEETS:
        if not config.GOOGLE_SHEETS_SPREADSHEET_ID:
            raise RuntimeError(
                "GOOGLE_SHEETS_SPREADSHEET_ID is required when PUSH_TO_SHEETS=true."
            )
        spreadsheet_id = config.GOOGLE_SHEETS_SPREADSHEET_ID

        from src.sheets import _authorize

        try:
            sheets_client = _authorize()
        except RuntimeError:
            logger.error("Google Sheets authentication failed.")
            raise

        logger.info("Google Sheets authentication successful.")

    total_rows: int = 0

    logger.info("Exporting materialized query tables...")
    for table in _QUERY_TABLES:
        df, rows = _export_table(conn, table, output_dir)
        total_rows += rows
        if sheets_client is not None:
            from src.sheets import push_dataframe

            push_dataframe(sheets_client, df, table, spreadsheet_id)

    logger.info("Exporting analytical views...")
    for view in _VIEWS:
        df, rows = _export_table(conn, view, output_dir)
        total_rows += rows
        if sheets_client is not None:
            from src.sheets import push_dataframe

            push_dataframe(sheets_client, df, view, spreadsheet_id)

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
