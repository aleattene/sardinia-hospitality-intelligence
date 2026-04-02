"""Centralized configuration from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


def _require_env(name: str) -> str:
    """Read a required environment variable or raise an error.

    Args:
        name: Environment variable name.

    Returns:
        The variable value.

    Raises:
        RuntimeError: If the variable is not set or empty.
    """
    value: str | None = os.getenv(name)
    if not value:
        msg: str = f"Required environment variable '{name}' is not set."
        raise RuntimeError(msg)
    return value


# --- Project root ---
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent

# --- Pipeline control ---
FETCH_ISTAT_DATA: bool = _require_env("FETCH_ISTAT_DATA").lower() == "true"

# --- Data directories ---
DATA_DIR: Path = Path(_require_env("DATA_DIR"))
DB_DIR: Path = Path(_require_env("DB_DIR"))
DB_FILENAME: str = _require_env("DB_FILENAME")
DB_PATH: Path = DB_DIR / DB_FILENAME
RAW_DATA_DIR: Path = Path(_require_env("RAW_DATA_DIR"))
ANALYSIS_OUTPUT_DIR: Path = Path(_require_env("ANALYSIS_OUTPUT_DIR"))

# --- SQL (derived from project structure, not env vars) ---
SQL_DIR: Path = PROJECT_ROOT / "sql"
SQL_SCHEMA: Path = SQL_DIR / "schema.sql"
SQL_VIEWS_DIR: Path = SQL_DIR / "views"
SQL_QUERIES_DIR: Path = SQL_DIR / "queries"
