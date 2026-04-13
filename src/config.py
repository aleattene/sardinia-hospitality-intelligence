"""Centralized configuration from environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Compute PROJECT_ROOT before load_dotenv so the .env path is deterministic
# regardless of the working directory from which the code is invoked.
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")


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


def _resolve_path(raw: str, base: Path) -> Path:
    """Resolve a path against base if relative, leave absolute paths unchanged.

    Args:
        raw: Raw path string from environment variable.
        base: Base directory to resolve relative paths against.

    Returns:
        Resolved absolute Path.
    """
    path: Path = Path(raw)
    return path if path.is_absolute() else base / path


# --- Pipeline control ---
FETCH_ISTAT_DATA: bool = _require_env("FETCH_ISTAT_DATA").lower() == "true"

# --- Data directories ---
DATA_DIR: Path = _resolve_path(_require_env("DATA_DIR"), PROJECT_ROOT)
DB_DIR: Path = _resolve_path(_require_env("DB_DIR"), PROJECT_ROOT)
DB_FILENAME: str = _require_env("DB_FILENAME")
DB_PATH: Path = DB_DIR / DB_FILENAME
RAW_DATA_DIR: Path = _resolve_path(_require_env("RAW_DATA_DIR"), PROJECT_ROOT)
ANALYSIS_OUTPUT_DIR: Path = _resolve_path(
    _require_env("ANALYSIS_OUTPUT_DIR"), PROJECT_ROOT
)

# --- Google Sheets push (opt-in, default off) ---
# PUSH_TO_SHEETS: if "true", pipeline pushes CSVs to Google Sheets after export.
# Defaults to false so CI and contributors never touch Google APIs.
PUSH_TO_SHEETS: bool = os.getenv("PUSH_TO_SHEETS", "false").lower() == "true"

# GOOGLE_SHEETS_SPREADSHEET_ID: required at runtime only when PUSH_TO_SHEETS=true.
# Not a secret — it is the public spreadsheet ID from the Sheets URL.
GOOGLE_SHEETS_SPREADSHEET_ID: str | None = os.getenv("GOOGLE_SHEETS_SPREADSHEET_ID")

# Keyring identifiers: locate the macOS Keychain entry holding the service account JSON.
# These are identifiers, not secrets. Required at runtime only when PUSH_TO_SHEETS=true.
KEYRING_SERVICE: str | None = os.getenv("KEYRING_SERVICE")
KEYRING_KEY: str | None = os.getenv("KEYRING_KEY")

# --- SQL (derived from project structure, not env vars) ---
SQL_DIR: Path = PROJECT_ROOT / "sql"
SQL_SCHEMA: Path = SQL_DIR / "schema.sql"
SQL_VIEWS_DIR: Path = SQL_DIR / "views"
SQL_QUERIES_DIR: Path = SQL_DIR / "queries"
