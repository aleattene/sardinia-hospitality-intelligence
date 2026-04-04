"""Step 01 — Ingest: normalize raw CSV files and load into DuckDB staging tables.

Schema normalization strategy (see docs/ADR.md — ADR-002):
- Column names are lowercased, stripped, and hyphens/spaces replaced with underscores.
- All years are mapped to the 2024 target schema via explicit column dictionaries.
- Columns absent in a given year are added as NULL.
- The string literal "NULL" from source data is replaced with pd.NA before loading.
"""

import logging
from pathlib import Path

import duckdb
import pandas as pd

from src import config
from src.utils.db import execute_sql_file, get_connection

logger: logging.Logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Column mappings: normalized source name → target schema name
# Applied after lowercasing and replacing spaces/hyphens with underscores.
# ---------------------------------------------------------------------------

_MOVEMENTS_COLUMN_MAP: dict[str, str] = {
    "anno": "year",
    "provincia": "province",
    "mese": "month",
    "macro_tipologia": "accommodation_type",  # covers both _ and - variants post-normalize
    "macro_provenienza": "origin_macro",
    "provenienza": "origin",
    "arrivi": "arrivals",
    "presenze": "nights",
}

_CAPACITY_COLUMN_MAP: dict[str, str] = {
    "anno": "year",
    "provincia": "province",
    "comune": "municipality",
    "mese": "month",
    "tipologia": "accommodation_type",
    "stelle": "category",  # 2018-2022
    "categoria": "category",  # 2023-2024
    "numero_strutture": "facilities",
    "letti": "beds",
    "camere": "rooms",
}

_MOVEMENTS_TARGET_COLUMNS: list[str] = [
    "year",
    "province",
    "month",
    "accommodation_type",
    "origin_macro",
    "origin",
    "arrivals",
    "nights",
]

_CAPACITY_TARGET_COLUMNS: list[str] = [
    "year",
    "province",
    "municipality",
    "month",
    "accommodation_type",
    "category",
    "facilities",
    "beds",
    "rooms",
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase, strip, and replace spaces/hyphens with underscores in column names."""
    df.columns = (
        df.columns.str.lower().str.strip().str.replace(r"[\s\-]+", "_", regex=True)
    )
    return df


def _replace_null_strings(df: pd.DataFrame) -> pd.DataFrame:
    """Replace the literal string 'NULL' (case-insensitive) with pd.NA."""
    return df.replace(r"(?i)^null$", pd.NA, regex=True)


def _to_target_schema(
    df: pd.DataFrame,
    column_map: dict[str, str],
    target_columns: list[str],
) -> pd.DataFrame:
    """Normalize, rename, and align a DataFrame to the target schema.

    Columns absent in the source are added as NULL.
    Only target columns are kept, in the declared order.
    """
    df = _normalize_column_names(df)
    df = _replace_null_strings(df)
    df = df.rename(columns=column_map)
    for col in target_columns:
        if col not in df.columns:
            df[col] = pd.NA
    return df[target_columns]


def _cast_numeric_columns(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    """Cast specified columns to numeric, coercing unparseable values to NaN."""
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _load_csv(path: Path) -> pd.DataFrame:
    """Read a CSV file, trying UTF-8 first then falling back to latin-1."""
    try:
        return pd.read_csv(path, dtype=str, encoding="utf-8")
    except UnicodeDecodeError:
        logger.warning(
            "UTF-8 decoding failed for %s — retrying with latin-1", path.name
        )
        return pd.read_csv(path, dtype=str, encoding="latin-1")


def _log_null_counts(df: pd.DataFrame, source: str) -> None:
    """Log a warning for each column that contains null values."""
    # TODO: Replace with explicit pre-insert validation that raises a clear error
    # (including filename and column name) when NOT NULL columns contain null values,
    # rather than relying on a generic DuckDB ConstraintException.
    # See: TODO.md — Technical Debt.
    null_counts = df.isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            logger.warning("  [%s] column '%s' has %d null values", source, col, count)


_ALLOWED_TABLES: frozenset[str] = frozenset(
    {"stg_tourism_flows", "stg_accommodation_capacity"}
)


def _insert_dataframe(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    df: pd.DataFrame,
) -> None:
    """Insert a DataFrame into a DuckDB staging table via a temporary view.

    Args:
        conn: Active DuckDB connection.
        table: Destination table name. Must be one of _ALLOWED_TABLES.
        df: DataFrame to insert. Column order must match the target table schema.

    Raises:
        ValueError: If table is not in the allowed set.
    """
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Insert target '{table}' is not an allowed staging table.")
    conn.register("_ingest_tmp", df)
    try:
        conn.execute(f"INSERT INTO {table} SELECT * FROM _ingest_tmp")  # noqa: S608
    finally:
        conn.unregister("_ingest_tmp")


# ---------------------------------------------------------------------------
# Dataset loaders
# ---------------------------------------------------------------------------


def _ingest_movements(conn: duckdb.DuckDBPyConnection, raw_dir: Path) -> int:
    """Load and normalize all customer movements CSV files into stg_tourism_flows.

    Returns:
        Total number of rows inserted.
    """
    files: list[Path] = sorted(raw_dir.glob("sardinia_customer_movements_*.csv"))
    if not files:
        raise FileNotFoundError(f"No customer movements CSV files found in: {raw_dir}")

    total_rows: int = 0
    for file in files:
        df: pd.DataFrame = _load_csv(file)
        df = _to_target_schema(df, _MOVEMENTS_COLUMN_MAP, _MOVEMENTS_TARGET_COLUMNS)
        df = _cast_numeric_columns(df, ["year", "month", "arrivals", "nights"])
        _log_null_counts(df, file.name)
        _insert_dataframe(conn, "stg_tourism_flows", df)
        logger.info("  %-55s %6d rows", file.name, len(df))
        total_rows += len(df)

    logger.info(
        "stg_tourism_flows loaded: %d rows from %d files", total_rows, len(files)
    )
    return total_rows


def _ingest_capacity(conn: duckdb.DuckDBPyConnection, raw_dir: Path) -> int:
    """Load and normalize all accommodation capacity CSV files into stg_accommodation_capacity.

    Returns:
        Total number of rows inserted.
    """
    files: list[Path] = sorted(raw_dir.glob("sardinia_accommodation_capacity_*.csv"))
    if not files:
        raise FileNotFoundError(
            f"No accommodation capacity CSV files found in: {raw_dir}"
        )

    total_rows: int = 0
    for file in files:
        df = _load_csv(file)
        df = _to_target_schema(df, _CAPACITY_COLUMN_MAP, _CAPACITY_TARGET_COLUMNS)
        df = _cast_numeric_columns(df, ["year", "month", "facilities", "beds", "rooms"])
        _log_null_counts(df, file.name)
        _insert_dataframe(conn, "stg_accommodation_capacity", df)
        logger.info("  %-55s %6d rows", file.name, len(df))
        total_rows += len(df)

    logger.info(
        "stg_accommodation_capacity loaded: %d rows from %d files",
        total_rows,
        len(files),
    )
    return total_rows


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run(conn: duckdb.DuckDBPyConnection) -> None:
    """Execute ingest step: create schema and load all raw CSV files into staging tables.

    Args:
        conn: Active DuckDB connection.
    """
    logger.info("=== Step 01: Ingest ===")
    execute_sql_file(conn, config.SQL_SCHEMA)
    logger.info("Schema created.")
    _ingest_movements(conn, config.RAW_DATA_DIR)
    _ingest_capacity(conn, config.RAW_DATA_DIR)
    logger.info("=== Step 01 complete ===")


if __name__ == "__main__":
    from src.utils.logging import setup_logging

    setup_logging()
    with get_connection(config.DB_PATH) as conn:
        run(conn)
