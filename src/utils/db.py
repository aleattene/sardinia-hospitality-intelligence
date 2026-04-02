"""DuckDB helper: connection management and SQL file execution."""

import logging
from pathlib import Path

import duckdb

logger: logging.Logger = logging.getLogger(__name__)


def get_connection(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    """Open a DuckDB connection, creating parent directories if needed.

    Args:
        db_path: Path to the DuckDB file. Use ":memory:" for in-memory database.

    Returns:
        DuckDB connection object.
    """
    if str(db_path) != ":memory:":
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn: duckdb.DuckDBPyConnection = duckdb.connect(str(db_path))
    logger.info("Connected to DuckDB: %s", db_path)
    return conn


def execute_sql_file(conn: duckdb.DuckDBPyConnection, sql_path: str | Path) -> None:
    """Read and execute a SQL file.

    Args:
        conn: Active DuckDB connection.
        sql_path: Path to the .sql file.
    """
    path: Path = Path(sql_path)
    sql: str = path.read_text(encoding="utf-8")
    conn.execute(sql)
    logger.info("Executed SQL: %s", path.name)


def execute_sql_directory(conn: duckdb.DuckDBPyConnection, sql_dir: str | Path) -> None:
    """Execute all .sql files in a directory, sorted alphabetically.

    Args:
        conn: Active DuckDB connection.
        sql_dir: Path to directory containing .sql files.
    """
    directory: Path = Path(sql_dir)
    sql_files: list[Path] = sorted(directory.glob("*.sql"))
    for sql_file in sql_files:
        execute_sql_file(conn, sql_file)
    logger.info("Executed %d SQL files from %s", len(sql_files), directory.name)
