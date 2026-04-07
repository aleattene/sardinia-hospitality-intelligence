"""Shared fixtures for the test suite.

Env vars are set at module level before any src import, because src/config.py
reads them at import time (module-level execution).
"""

import os

# Must be set before any `from src import config` is executed.
os.environ.setdefault("FETCH_ISTAT_DATA", "false")
os.environ.setdefault("DATA_DIR", "data")
os.environ.setdefault("DB_DIR", "data/db")
os.environ.setdefault("DB_FILENAME", "test.duckdb")
os.environ.setdefault("RAW_DATA_DIR", "data_sample")
os.environ.setdefault("ANALYSIS_OUTPUT_DIR", "data/analysis")

import duckdb
import pytest
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
DATA_SAMPLE_DIR = PROJECT_ROOT / "data_sample"


@pytest.fixture
def mem_conn():
    """Bare in-memory DuckDB connection."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()


@pytest.fixture
def schema_conn(mem_conn):
    """In-memory DuckDB with staging schema created."""
    from src.utils.db import execute_sql_file

    execute_sql_file(mem_conn, SQL_DIR / "schema.sql")
    return mem_conn


@pytest.fixture
def staged_conn(schema_conn):
    """Staging tables loaded from data_sample."""
    from src.pipeline.step_01_ingest import _ingest_capacity, _ingest_movements

    _ingest_movements(schema_conn, DATA_SAMPLE_DIR)
    _ingest_capacity(schema_conn, DATA_SAMPLE_DIR)
    return schema_conn


@pytest.fixture
def transformed_conn(staged_conn):
    """Views and materialized query tables created from data_sample."""
    from src.utils.db import execute_sql_directory

    execute_sql_directory(staged_conn, SQL_DIR / "views")
    execute_sql_directory(staged_conn, SQL_DIR / "queries")
    return staged_conn
