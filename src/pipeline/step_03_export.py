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

# Views to export to CSV for notebook exploration (sql/views/).
# Excludes v_seasonality_profile (high cardinality — province × month × type × year).
# v_segment_origin is exported for the EDA notebook (reads data/analysis/v_segment_origin.csv).
# v_segment_origin_summary is the aggregated view for Looker Studio / Google Sheets.
_VIEWS: list[str] = [
    "v_demand_by_province",
    "v_supply_by_province",
    "v_supply_demand_gap",
    "v_segment_origin",
    "v_segment_origin_summary",
    "v_segment_accommodation",
    "v_trend_yoy",
]

# Subset of _VIEWS + _QUERY_TABLES safe to push to Google Sheets.
# Excludes v_segment_origin (high cardinality — use v_segment_origin_summary instead).
_SHEETS_TARGETS: frozenset[str] = frozenset(
    _QUERY_TABLES + [v for v in _VIEWS if v != "v_segment_origin"]
)

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

    # --- Google Sheets: fail-fast validation before any filesystem side effects ---
    # sheets_push is either None (disabled) or a (client, spreadsheet_id) tuple.
    # Using a tuple keeps spreadsheet_id non-optional inside the push branch,
    # avoiding assert/runtime guards that could be stripped by Python -O.
    sheets_push: tuple[gspread.Client, str] | None = None

    if config.PUSH_TO_SHEETS:
        if not config.GOOGLE_SHEETS_SPREADSHEET_ID:
            raise RuntimeError(
                "GOOGLE_SHEETS_SPREADSHEET_ID is required when PUSH_TO_SHEETS=true."
            )
        from src.sheets import _authorize

        try:
            sheets_push = (_authorize(), config.GOOGLE_SHEETS_SPREADSHEET_ID)
        except RuntimeError:
            logger.error("Google Sheets authentication failed.")
            raise

        logger.info("Google Sheets authentication successful.")

    # Directory creation happens after fail-fast checks to avoid filesystem
    # side effects when the function raises due to invalid Sheets config.
    output_dir: Path = config.ANALYSIS_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    total_rows: int = 0

    logger.info("Exporting materialized query tables...")
    for table in _QUERY_TABLES:
        df, rows = _export_table(conn, table, output_dir)
        total_rows += rows
        if sheets_push is not None and table in _SHEETS_TARGETS:
            from src.sheets import push_dataframe

            push_dataframe(sheets_push[0], df, table, sheets_push[1])

    logger.info("Exporting analytical views...")
    for view in _VIEWS:
        df, rows = _export_table(conn, view, output_dir)
        total_rows += rows
        if sheets_push is not None and view in _SHEETS_TARGETS:
            from src.sheets import push_dataframe

            push_dataframe(sheets_push[0], df, view, sheets_push[1])

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
