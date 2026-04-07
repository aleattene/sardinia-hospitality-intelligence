"""Tests for pipeline steps (ingest, transform, export) and src/utils."""

import logging
import time
from pathlib import Path
import math

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_SAMPLE_DIR = PROJECT_ROOT / "data_sample"
SQL_DIR = PROJECT_ROOT / "sql"


# ===========================================================================
# src/config.py
# ===========================================================================


class TestConfig:
    def test_require_env_raises_if_missing(self):
        from src.config import _require_env

        with pytest.raises(RuntimeError, match="Required environment variable"):
            _require_env("__DEFINITELY_NOT_SET_XYZ_12345__")

    def test_require_env_raises_if_empty(self, monkeypatch):
        import os

        from src.config import _require_env

        monkeypatch.setenv("__TEST_EMPTY_VAR__", "")
        with pytest.raises(RuntimeError):
            _require_env("__TEST_EMPTY_VAR__")

    def test_require_env_returns_value(self, monkeypatch):
        from src.config import _require_env

        monkeypatch.setenv("__TEST_VAR_XYZ__", "hello")
        assert _require_env("__TEST_VAR_XYZ__") == "hello"

    def test_resolve_path_relative(self):
        from src.config import _resolve_path

        base = Path("/base/dir")
        result = _resolve_path("subdir/data", base)
        assert result == Path("/base/dir/subdir/data")

    def test_resolve_path_absolute_unchanged(self):
        from src.config import _resolve_path

        result = _resolve_path("/absolute/path", Path("/base"))
        assert result == Path("/absolute/path")


# ===========================================================================
# src/utils/runtime.py
# ===========================================================================


class TestTimestampNow:
    def test_length(self):
        from src.utils.runtime import timestamp_now

        assert len(timestamp_now()) == 15

    def test_separator_at_index_8(self):
        from src.utils.runtime import timestamp_now

        assert timestamp_now()[8] == "_"

    def test_returns_string(self):
        from src.utils.runtime import timestamp_now

        assert isinstance(timestamp_now(), str)

    def test_date_part_is_digits(self):
        from src.utils.runtime import timestamp_now

        ts = timestamp_now()
        assert ts[:8].isdigit() and ts[9:].isdigit()

    def test_consecutive_calls_are_strings(self):
        from src.utils.runtime import timestamp_now

        ts1 = timestamp_now()
        ts2 = timestamp_now()
        assert isinstance(ts1, str) and isinstance(ts2, str)


class TestTimer:
    def test_elapsed_non_negative(self):
        from src.utils.runtime import Timer

        with Timer("t") as t:
            pass
        assert t.elapsed >= 0.0

    def test_elapsed_is_float(self):
        from src.utils.runtime import Timer

        with Timer() as t:
            pass
        assert isinstance(t.elapsed, float)

    def test_label_stored(self):
        from src.utils.runtime import Timer

        t = Timer("my-label")
        assert t.label == "my-label"

    def test_elapsed_reflects_real_time(self):
        from src.utils.runtime import Timer

        with Timer() as t:
            time.sleep(0.05)
        assert t.elapsed >= 0.04

    def test_elapsed_set_even_on_exception(self):
        from src.utils.runtime import Timer

        t = Timer()
        try:
            with t:
                raise ValueError("simulated error")
        except ValueError:
            pass
        assert t.elapsed >= 0.0

    def test_default_label_is_empty(self):
        from src.utils.runtime import Timer

        t = Timer()
        assert t.label == ""

    def test_initial_elapsed_is_zero(self):
        from src.utils.runtime import Timer

        t = Timer()
        assert math.isclose(t.elapsed, 0.0)


# ===========================================================================
# src/utils/logging.py
# ===========================================================================


class TestSetupLogging:
    def test_root_logger_has_handlers(self):
        from src.utils.logging import setup_logging

        setup_logging()
        assert len(logging.getLogger().handlers) > 0

    def test_default_level_is_info(self):
        from src.utils.logging import setup_logging

        setup_logging()
        assert logging.getLogger().level == logging.INFO

    def test_custom_level_debug(self):
        from src.utils.logging import setup_logging

        setup_logging(logging.DEBUG)
        assert logging.getLogger().level == logging.DEBUG
        setup_logging(logging.INFO)  # restore

    def test_idempotent_no_raise(self):
        from src.utils.logging import setup_logging

        setup_logging()
        setup_logging()  # second call must not raise


# ===========================================================================
# src/utils/db.py
# ===========================================================================


class TestGetConnection:
    def test_memory_connection_executes_query(self):
        from src.utils.db import get_connection

        conn = get_connection(":memory:")
        result = conn.execute("SELECT 42").fetchone()
        conn.close()
        assert result[0] == 42

    def test_file_connection_creates_parent_dir(self, tmp_path):
        from src.utils.db import get_connection

        db_path = tmp_path / "subdir" / "test.duckdb"
        conn = get_connection(db_path)
        conn.close()
        assert db_path.parent.exists()

    def test_memory_connection_does_not_create_disk_file(self, tmp_path, monkeypatch):
        from src.utils.db import get_connection

        monkeypatch.chdir(tmp_path)
        before = set(tmp_path.glob("*.duckdb"))
        conn = get_connection(":memory:")
        conn.close()
        after = set(tmp_path.glob("*.duckdb"))
        assert after == before


class TestExecuteSqlFile:
    def test_executes_ddl(self, tmp_path, mem_conn):
        from src.utils.db import execute_sql_file

        sql_file = tmp_path / "ddl.sql"
        sql_file.write_text("CREATE TABLE foo (x INTEGER);")
        execute_sql_file(mem_conn, sql_file)
        count = mem_conn.execute("SELECT count(*) FROM foo").fetchone()[0]
        assert count == 0

    def test_executes_dml(self, tmp_path, mem_conn):
        from src.utils.db import execute_sql_file

        (tmp_path / "create.sql").write_text("CREATE TABLE bar (v INTEGER);")
        (tmp_path / "insert.sql").write_text("INSERT INTO bar VALUES (99);")
        execute_sql_file(mem_conn, tmp_path / "create.sql")
        execute_sql_file(mem_conn, tmp_path / "insert.sql")
        assert mem_conn.execute("SELECT v FROM bar").fetchone()[0] == 99

    def test_missing_file_raises(self, mem_conn):
        from src.utils.db import execute_sql_file

        with pytest.raises(FileNotFoundError):
            execute_sql_file(mem_conn, Path("/nonexistent/query.sql"))

    def test_path_object_and_string_both_accepted(self, tmp_path, mem_conn):
        from src.utils.db import execute_sql_file

        sql_file = tmp_path / "t.sql"
        sql_file.write_text("CREATE TABLE t (n INTEGER);")
        execute_sql_file(mem_conn, str(sql_file))  # string path
        assert mem_conn.execute("SELECT count(*) FROM t").fetchone()[0] == 0


class TestExecuteSqlDirectory:
    def test_executes_files_alphabetically(self, tmp_path, mem_conn):
        from src.utils.db import execute_sql_directory

        (tmp_path / "a_create.sql").write_text("CREATE TABLE seq (n INTEGER);")
        (tmp_path / "b_insert.sql").write_text("INSERT INTO seq VALUES (7);")
        execute_sql_directory(mem_conn, tmp_path)
        assert mem_conn.execute("SELECT n FROM seq").fetchone()[0] == 7

    def test_missing_directory_raises_file_not_found(self, mem_conn):
        from src.utils.db import execute_sql_directory

        with pytest.raises(FileNotFoundError):
            execute_sql_directory(mem_conn, Path("/nonexistent/dir"))

    def test_empty_directory_raises_value_error(self, tmp_path, mem_conn):
        from src.utils.db import execute_sql_directory

        with pytest.raises(ValueError, match="No .sql files"):
            execute_sql_directory(mem_conn, tmp_path)

    def test_ignores_non_sql_files(self, tmp_path, mem_conn):
        from src.utils.db import execute_sql_directory

        (tmp_path / "notes.txt").write_text("not sql")
        (tmp_path / "readme.md").write_text("# readme")
        (tmp_path / "create.sql").write_text("CREATE TABLE t2 (x INTEGER);")
        execute_sql_directory(mem_conn, tmp_path)  # must not raise

    def test_multiple_files_all_executed(self, tmp_path, mem_conn):
        from src.utils.db import execute_sql_directory

        (tmp_path / "a.sql").write_text("CREATE TABLE a (v INTEGER);")
        (tmp_path / "b.sql").write_text("CREATE TABLE b (v INTEGER);")
        execute_sql_directory(mem_conn, tmp_path)
        tables = {t[0] for t in mem_conn.execute("SHOW TABLES").fetchall()}
        assert {"a", "b"}.issubset(tables)


# ===========================================================================
# src/pipeline/step_01_ingest.py — unit tests (pure helpers)
# ===========================================================================


class TestNormalizeColumnNames:
    def test_lowercase(self):
        from src.pipeline.step_01_ingest import _normalize_column_names

        df = pd.DataFrame(columns=["ANNO", "PROVINCIA"])
        result = _normalize_column_names(df)
        assert list(result.columns) == ["anno", "provincia"]

    def test_strip_leading_trailing_spaces(self):
        from src.pipeline.step_01_ingest import _normalize_column_names

        df = pd.DataFrame(columns=["  anno  "])
        result = _normalize_column_names(df)
        assert list(result.columns) == ["anno"]

    def test_replace_spaces_with_underscore(self):
        from src.pipeline.step_01_ingest import _normalize_column_names

        df = pd.DataFrame(columns=["Macro Tipologia"])
        result = _normalize_column_names(df)
        assert list(result.columns) == ["macro_tipologia"]

    def test_replace_hyphens_with_underscore(self):
        from src.pipeline.step_01_ingest import _normalize_column_names

        df = pd.DataFrame(columns=["my-column"])
        result = _normalize_column_names(df)
        assert list(result.columns) == ["my_column"]

    def test_combined_transformations(self):
        from src.pipeline.step_01_ingest import _normalize_column_names

        df = pd.DataFrame(columns=["  Macro-Tipologia  "])
        result = _normalize_column_names(df)
        assert list(result.columns) == ["macro_tipologia"]

    def test_preserves_already_normalized(self):
        from src.pipeline.step_01_ingest import _normalize_column_names

        df = pd.DataFrame(columns=["anno", "provincia"])
        result = _normalize_column_names(df)
        assert list(result.columns) == ["anno", "provincia"]


class TestReplaceNullStrings:
    def test_replaces_null_uppercase(self):
        from src.pipeline.step_01_ingest import _replace_null_strings

        df = pd.DataFrame({"a": ["NULL", "value"]})
        result = _replace_null_strings(df)
        assert pd.isna(result["a"][0])
        assert result["a"][1] == "value"

    def test_replaces_null_lowercase(self):
        from src.pipeline.step_01_ingest import _replace_null_strings

        df = pd.DataFrame({"a": ["null", "ok"]})
        result = _replace_null_strings(df)
        assert pd.isna(result["a"][0])

    def test_replaces_null_mixed_case(self):
        from src.pipeline.step_01_ingest import _replace_null_strings

        df = pd.DataFrame({"a": ["Null", "NuLl"]})
        result = _replace_null_strings(df)
        assert pd.isna(result["a"][0])
        assert pd.isna(result["a"][1])

    def test_preserves_null_substring(self):
        from src.pipeline.step_01_ingest import _replace_null_strings

        df = pd.DataFrame({"a": ["nullified", "not-null-here"]})
        result = _replace_null_strings(df)
        assert result["a"][0] == "nullified"
        assert result["a"][1] == "not-null-here"

    def test_preserves_numeric_strings(self):
        from src.pipeline.step_01_ingest import _replace_null_strings

        df = pd.DataFrame({"n": ["123", "0", ""]})
        result = _replace_null_strings(df)
        assert result["n"][0] == "123"

    def test_multi_column(self):
        from src.pipeline.step_01_ingest import _replace_null_strings

        df = pd.DataFrame({"a": ["NULL"], "b": ["NULL"], "c": ["ok"]})
        result = _replace_null_strings(df)
        assert pd.isna(result["a"][0])
        assert pd.isna(result["b"][0])
        assert result["c"][0] == "ok"


class TestToTargetSchema:
    def test_renames_columns(self):
        from src.pipeline.step_01_ingest import _to_target_schema

        df = pd.DataFrame({"Anno": ["2024"], "Provincia": ["CA"]})
        result = _to_target_schema(
            df, {"anno": "year", "provincia": "province"}, ["year", "province"]
        )
        assert list(result.columns) == ["year", "province"]

    def test_adds_missing_columns_as_null(self):
        from src.pipeline.step_01_ingest import _to_target_schema

        df = pd.DataFrame({"anno": ["2024"]})
        result = _to_target_schema(df, {"anno": "year"}, ["year", "extra_col"])
        assert "extra_col" in result.columns
        assert pd.isna(result["extra_col"][0])

    def test_drops_extra_columns(self):
        from src.pipeline.step_01_ingest import _to_target_schema

        df = pd.DataFrame({"anno": ["2024"], "irrelevant": ["drop me"]})
        result = _to_target_schema(df, {"anno": "year"}, ["year"])
        assert list(result.columns) == ["year"]
        assert "irrelevant" not in result.columns

    def test_column_order_matches_target(self):
        from src.pipeline.step_01_ingest import _to_target_schema

        df = pd.DataFrame({"b": [1], "a": [2]})
        result = _to_target_schema(df, {"a": "x", "b": "y"}, ["x", "y"])
        assert list(result.columns) == ["x", "y"]

    def test_null_strings_replaced(self):
        from src.pipeline.step_01_ingest import _to_target_schema

        df = pd.DataFrame({"categoria": ["NULL"]})
        result = _to_target_schema(df, {"categoria": "category"}, ["category"])
        assert pd.isna(result["category"][0])


class TestCastNumericColumns:
    def test_casts_string_to_float(self):
        from src.pipeline.step_01_ingest import _cast_numeric_columns

        df = pd.DataFrame({"n": ["42", "100"]})
        result = _cast_numeric_columns(df, ["n"])
        assert math.isclose(result["n"][0], 42.0)

    def test_coerces_invalid_to_nan(self):
        from src.pipeline.step_01_ingest import _cast_numeric_columns

        df = pd.DataFrame({"n": ["abc", "99"]})
        result = _cast_numeric_columns(df, ["n"])
        assert pd.isna(result["n"][0])
        assert math.isclose(result["n"][1], 99.0)

    def test_handles_multiple_columns(self):
        from src.pipeline.step_01_ingest import _cast_numeric_columns

        df = pd.DataFrame({"a": ["1"], "b": ["2"]})
        result = _cast_numeric_columns(df, ["a", "b"])
        assert math.isclose(result["a"][0], 1.0)
        assert math.isclose(result["b"][0], 2.0)

    def test_empty_string_becomes_nan(self):
        from src.pipeline.step_01_ingest import _cast_numeric_columns

        df = pd.DataFrame({"n": [""]})
        result = _cast_numeric_columns(df, ["n"])
        assert pd.isna(result["n"][0])

    def test_leaves_non_target_columns_unchanged(self):
        from src.pipeline.step_01_ingest import _cast_numeric_columns

        df = pd.DataFrame({"n": ["42"], "label": ["hello"]})
        result = _cast_numeric_columns(df, ["n"])
        assert result["label"][0] == "hello"


class TestLoadCsv:
    def test_reads_utf8_csv(self, tmp_path):
        from src.pipeline.step_01_ingest import _load_csv

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,hello\n", encoding="utf-8")
        df = _load_csv(csv_file)
        assert list(df.columns) == ["a", "b"]
        assert len(df) == 1

    def test_falls_back_to_latin1_on_decode_error(self, tmp_path):
        from src.pipeline.step_01_ingest import _load_csv

        csv_file = tmp_path / "latin1.csv"
        # \xe9 = é in latin-1, invalid in UTF-8
        csv_file.write_bytes(b"a,b\n1,caf\xe9\n")
        df = _load_csv(csv_file)
        assert len(df) == 1

    def test_returns_string_dtype(self, tmp_path):
        from src.pipeline.step_01_ingest import _load_csv

        csv_file = tmp_path / "nums.csv"
        csv_file.write_text("n\n42\n99\n", encoding="utf-8")
        df = _load_csv(csv_file)
        # dtype=str → object in pandas <2.0, StringDtype in pandas >=2.0
        assert pd.api.types.is_string_dtype(df["n"].dtype)

    def test_reads_multiple_rows(self, tmp_path):
        from src.pipeline.step_01_ingest import _load_csv

        csv_file = tmp_path / "multi.csv"
        csv_file.write_text("x\n1\n2\n3\n", encoding="utf-8")
        df = _load_csv(csv_file)
        assert len(df) == 3


class TestLogNullCounts:
    def test_logs_warning_for_null_column(self, caplog):
        from src.pipeline.step_01_ingest import _log_null_counts

        df = pd.DataFrame({"a": [None, "val"], "b": ["x", "y"]})
        with caplog.at_level(logging.WARNING):
            _log_null_counts(df, "test_file.csv")
        assert any("'a'" in msg for msg in caplog.messages)

    def test_no_warning_when_no_nulls(self, caplog):
        from src.pipeline.step_01_ingest import _log_null_counts

        df = pd.DataFrame({"a": ["1", "2"], "b": ["x", "y"]})
        with caplog.at_level(logging.WARNING):
            _log_null_counts(df, "clean.csv")
        assert len(caplog.messages) == 0

    def test_warns_for_all_null_columns(self, caplog):
        from src.pipeline.step_01_ingest import _log_null_counts

        df = pd.DataFrame({"a": [None], "b": [None]})
        with caplog.at_level(logging.WARNING):
            _log_null_counts(df, "nulls.csv")
        assert sum("'a'" in m or "'b'" in m for m in caplog.messages) == 2


class TestInsertDataframe:
    def test_inserts_into_stg_tourism_flows(self, schema_conn):
        from src.pipeline.step_01_ingest import _insert_dataframe

        df = pd.DataFrame(
            {
                "year": [2024],
                "province": ["Test"],
                "month": [1],
                "accommodation_type": ["Alberghiero"],
                "origin_macro": [None],
                "origin": ["Italia"],
                "arrivals": [100],
                "nights": [300],
            }
        )
        _insert_dataframe(schema_conn, "stg_tourism_flows", df)
        count = schema_conn.execute(
            "SELECT count(*) FROM stg_tourism_flows"
        ).fetchone()[0]
        assert count == 1

    def test_inserts_into_stg_accommodation_capacity(self, schema_conn):
        from src.pipeline.step_01_ingest import _insert_dataframe

        df = pd.DataFrame(
            {
                "year": [2024],
                "province": ["Test"],
                "municipality": ["TestCity"],
                "month": [None],
                "accommodation_type": ["Alberghiero"],
                "category": ["3 stelle"],
                "facilities": [10],
                "beds": [200],
                "rooms": [100],
            }
        )
        _insert_dataframe(schema_conn, "stg_accommodation_capacity", df)
        count = schema_conn.execute(
            "SELECT count(*) FROM stg_accommodation_capacity"
        ).fetchone()[0]
        assert count == 1

    def test_disallowed_table_raises_value_error(self, schema_conn):
        from src.pipeline.step_01_ingest import _insert_dataframe

        df = pd.DataFrame({"x": [1]})
        with pytest.raises(ValueError, match="not an allowed"):
            _insert_dataframe(schema_conn, "evil_table", df)

    def test_temp_view_unregistered_after_insert(self, schema_conn):
        from src.pipeline.step_01_ingest import _insert_dataframe

        df = pd.DataFrame(
            {
                "year": [2024],
                "province": ["T"],
                "month": [1],
                "accommodation_type": ["Alberghiero"],
                "origin_macro": [None],
                "origin": ["Italia"],
                "arrivals": [10],
                "nights": [30],
            }
        )
        _insert_dataframe(schema_conn, "stg_tourism_flows", df)
        tables = {t[0] for t in schema_conn.execute("SHOW TABLES").fetchall()}
        assert "_ingest_tmp" not in tables

    def test_multiple_rows_inserted(self, schema_conn):
        from src.pipeline.step_01_ingest import _insert_dataframe

        df = pd.DataFrame(
            {
                "year": [2023, 2024],
                "province": ["CA", "SS"],
                "month": [1, 2],
                "accommodation_type": ["Alberghiero", "Extralberghiero"],
                "origin_macro": [None, None],
                "origin": ["Italia", "Germania"],
                "arrivals": [100, 200],
                "nights": [300, 600],
            }
        )
        _insert_dataframe(schema_conn, "stg_tourism_flows", df)
        count = schema_conn.execute(
            "SELECT count(*) FROM stg_tourism_flows"
        ).fetchone()[0]
        assert count == 2


# ===========================================================================
# src/pipeline/step_01_ingest.py — integration tests
# ===========================================================================


class TestIngestMovements:
    def test_raises_if_no_matching_files(self, schema_conn, tmp_path):
        from src.pipeline.step_01_ingest import _ingest_movements

        with pytest.raises(FileNotFoundError, match="No customer movements"):
            _ingest_movements(schema_conn, tmp_path)

    def test_returns_total_row_count(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_movements

        total = _ingest_movements(schema_conn, DATA_SAMPLE_DIR)
        # 2 files (2023 + 2024) × 144 rows each = 288
        assert total == 288

    def test_row_count_in_staging_table(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_movements

        _ingest_movements(schema_conn, DATA_SAMPLE_DIR)
        count = schema_conn.execute(
            "SELECT count(*) FROM stg_tourism_flows"
        ).fetchone()[0]
        assert count == 288

    def test_both_years_loaded(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_movements

        _ingest_movements(schema_conn, DATA_SAMPLE_DIR)
        years = sorted(
            r[0]
            for r in schema_conn.execute(
                "SELECT DISTINCT year FROM stg_tourism_flows"
            ).fetchall()
        )
        assert years == [2023, 2024]

    def test_all_expected_provinces_loaded(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_movements

        _ingest_movements(schema_conn, DATA_SAMPLE_DIR)
        provinces = {
            r[0]
            for r in schema_conn.execute(
                "SELECT DISTINCT province FROM stg_tourism_flows"
            ).fetchall()
        }
        assert provinces == {"Cagliari", "Sassari", "Nuoro"}

    def test_numeric_columns_cast_correctly(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_movements

        _ingest_movements(schema_conn, DATA_SAMPLE_DIR)
        row = schema_conn.execute(
            "SELECT arrivals, nights FROM stg_tourism_flows "
            "WHERE year=2024 AND province='Cagliari' AND month=1 "
            "AND accommodation_type='Alberghiero' AND origin='Italia'"
        ).fetchone()
        assert row == (1000, 3000)


class TestIngestCapacity:
    def test_raises_if_no_matching_files(self, schema_conn, tmp_path):
        from src.pipeline.step_01_ingest import _ingest_capacity

        with pytest.raises(FileNotFoundError, match="No accommodation capacity"):
            _ingest_capacity(schema_conn, tmp_path)

    def test_returns_total_row_count(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_capacity

        total = _ingest_capacity(schema_conn, DATA_SAMPLE_DIR)
        # 2 files × 6 rows = 12
        assert total == 12

    def test_row_count_in_staging_table(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_capacity

        _ingest_capacity(schema_conn, DATA_SAMPLE_DIR)
        count = schema_conn.execute(
            "SELECT count(*) FROM stg_accommodation_capacity"
        ).fetchone()[0]
        assert count == 12

    def test_null_string_category_becomes_null(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_capacity

        _ingest_capacity(schema_conn, DATA_SAMPLE_DIR)
        # Extralberghiero rows have "NULL" in Categoria → should be NULL in DB
        # 2 files × 3 provinces × 1 Extralberghiero = 6 NULL category rows
        null_cats = schema_conn.execute(
            "SELECT count(*) FROM stg_accommodation_capacity WHERE category IS NULL"
        ).fetchone()[0]
        assert null_cats == 6

    def test_beds_loaded_correctly(self, schema_conn):
        from src.pipeline.step_01_ingest import _ingest_capacity

        _ingest_capacity(schema_conn, DATA_SAMPLE_DIR)
        row = schema_conn.execute(
            "SELECT beds FROM stg_accommodation_capacity "
            "WHERE year=2024 AND province='Cagliari' AND accommodation_type='Alberghiero'"
        ).fetchone()
        assert row[0] == 8000


class TestStep01Run:
    def test_run_populates_both_staging_tables(self, monkeypatch, mem_conn):
        from src import config
        from src.pipeline import step_01_ingest

        monkeypatch.setattr(config, "SQL_SCHEMA", SQL_DIR / "schema.sql")
        monkeypatch.setattr(config, "RAW_DATA_DIR", DATA_SAMPLE_DIR)
        step_01_ingest.run(mem_conn)

        flows = mem_conn.execute("SELECT count(*) FROM stg_tourism_flows").fetchone()[0]
        capacity = mem_conn.execute(
            "SELECT count(*) FROM stg_accommodation_capacity"
        ).fetchone()[0]
        assert flows == 288
        assert capacity == 12

    def test_run_is_idempotent_with_create_or_replace(self, monkeypatch, mem_conn):
        """Running ingest twice replaces the schema and reloads data."""
        from src import config
        from src.pipeline import step_01_ingest

        monkeypatch.setattr(config, "SQL_SCHEMA", SQL_DIR / "schema.sql")
        monkeypatch.setattr(config, "RAW_DATA_DIR", DATA_SAMPLE_DIR)
        step_01_ingest.run(mem_conn)
        step_01_ingest.run(mem_conn)  # second run must not raise or duplicate

        count = mem_conn.execute("SELECT count(*) FROM stg_tourism_flows").fetchone()[0]
        assert count == 288


# ===========================================================================
# src/pipeline/step_02_transform.py
# ===========================================================================


class TestStep02Run:
    def test_run_creates_views(self, monkeypatch, staged_conn):
        from src import config
        from src.pipeline import step_02_transform

        monkeypatch.setattr(config, "SQL_VIEWS_DIR", SQL_DIR / "views")
        monkeypatch.setattr(config, "SQL_QUERIES_DIR", SQL_DIR / "queries")
        step_02_transform.run(staged_conn)

        tables = {t[0] for t in staged_conn.execute("SHOW TABLES").fetchall()}
        assert "v_demand_by_province" in tables

    def test_run_creates_materialized_query_tables(self, monkeypatch, staged_conn):
        from src import config
        from src.pipeline import step_02_transform

        monkeypatch.setattr(config, "SQL_VIEWS_DIR", SQL_DIR / "views")
        monkeypatch.setattr(config, "SQL_QUERIES_DIR", SQL_DIR / "queries")
        step_02_transform.run(staged_conn)

        tables = {t[0] for t in staged_conn.execute("SHOW TABLES").fetchall()}
        assert "q_priority_score" in tables
        assert "q_seasonality_extremes" in tables
        assert "q_top_growth_segments" in tables

    def test_run_views_return_data(self, monkeypatch, staged_conn):
        from src import config
        from src.pipeline import step_02_transform

        monkeypatch.setattr(config, "SQL_VIEWS_DIR", SQL_DIR / "views")
        monkeypatch.setattr(config, "SQL_QUERIES_DIR", SQL_DIR / "queries")
        step_02_transform.run(staged_conn)

        rows = staged_conn.execute(
            "SELECT count(*) FROM v_demand_by_province"
        ).fetchone()[0]
        assert rows > 0


# ===========================================================================
# src/pipeline/step_03_export.py
# ===========================================================================


class TestExportTable:
    def test_disallowed_table_raises_value_error(self, transformed_conn, tmp_path):
        from src.pipeline.step_03_export import _export_table

        with pytest.raises(ValueError, match="not an allowed"):
            _export_table(transformed_conn, "malicious_table", tmp_path)

    def test_exports_view_to_csv(self, transformed_conn, tmp_path):
        from src.pipeline.step_03_export import _export_table

        rows = _export_table(transformed_conn, "v_demand_by_province", tmp_path)
        assert (tmp_path / "v_demand_by_province.csv").exists()
        assert rows > 0

    def test_exports_query_table_to_csv(self, transformed_conn, tmp_path):
        from src.pipeline.step_03_export import _export_table

        rows = _export_table(transformed_conn, "q_priority_score", tmp_path)
        assert (tmp_path / "q_priority_score.csv").exists()
        assert rows == 3  # 3 provinces

    def test_exported_csv_is_valid_pandas(self, transformed_conn, tmp_path):
        from src.pipeline.step_03_export import _export_table

        _export_table(transformed_conn, "q_priority_score", tmp_path)
        df = pd.read_csv(tmp_path / "q_priority_score.csv")
        assert len(df) > 0
        assert "priority_score" in df.columns

    def test_sql_injection_in_table_name_blocked(self, transformed_conn, tmp_path):
        from src.pipeline.step_03_export import _export_table

        with pytest.raises(ValueError):
            _export_table(
                transformed_conn,
                "v_demand_by_province; DROP TABLE stg_tourism_flows",
                tmp_path,
            )


class TestStep03Run:
    def test_run_creates_all_expected_csv_files(
        self, monkeypatch, transformed_conn, tmp_path
    ):
        from src import config
        from src.pipeline import step_03_export

        monkeypatch.setattr(config, "ANALYSIS_OUTPUT_DIR", tmp_path)
        step_03_export.run(transformed_conn)

        expected = {
            "q_priority_score.csv",
            "q_seasonality_extremes.csv",
            "q_top_growth_segments.csv",
            "v_demand_by_province.csv",
            "v_supply_by_province.csv",
            "v_supply_demand_gap.csv",
            "v_segment_origin.csv",
            "v_segment_accommodation.csv",
            "v_trend_yoy.csv",
        }
        created = {f.name for f in tmp_path.glob("*.csv")}
        assert expected == created

    def test_run_creates_output_dir_if_missing(
        self, monkeypatch, transformed_conn, tmp_path
    ):
        from src import config
        from src.pipeline import step_03_export

        output_dir = tmp_path / "new_subdir"
        monkeypatch.setattr(config, "ANALYSIS_OUTPUT_DIR", output_dir)
        step_03_export.run(transformed_conn)
        assert output_dir.exists()

    def test_run_total_rows_nonzero(self, monkeypatch, transformed_conn, tmp_path):
        from src import config
        from src.pipeline import step_03_export

        monkeypatch.setattr(config, "ANALYSIS_OUTPUT_DIR", tmp_path)
        step_03_export.run(transformed_conn)
        # Spot-check: v_trend_yoy.csv should have 6 rows (3 provinces × 2 years)
        df = pd.read_csv(tmp_path / "v_trend_yoy.csv")
        assert len(df) == 6


# ===========================================================================
# Stress tests
# ===========================================================================


class TestIngestStress:
    def test_large_dataframe_insert(self, schema_conn):
        """Insert 10 000 rows without error and verify count."""
        from src.pipeline.step_01_ingest import _insert_dataframe

        n = 10_000
        df = pd.DataFrame(
            {
                "year": [2024] * n,
                "province": [f"Province_{i % 5}" for i in range(n)],
                "month": [i % 12 + 1 for i in range(n)],
                "accommodation_type": ["Alberghiero"] * n,
                "origin_macro": [None] * n,
                "origin": ["Italia"] * n,
                "arrivals": [100] * n,
                "nights": [300] * n,
            }
        )
        _insert_dataframe(schema_conn, "stg_tourism_flows", df)
        count = schema_conn.execute(
            "SELECT count(*) FROM stg_tourism_flows"
        ).fetchone()[0]
        assert count == n

    def test_normalize_large_dataframe(self):
        """Normalize a 50 000-row DataFrame without error."""
        from src.pipeline.step_01_ingest import _normalize_column_names

        n = 50_000
        df = pd.DataFrame({f"Column {i}": range(n) for i in range(5)})
        result = _normalize_column_names(df)
        assert len(result) == n
        assert all(" " not in c for c in result.columns)

    def test_replace_null_strings_large(self):
        """Replace NULL strings in a 50 000-row DataFrame."""
        from src.pipeline.step_01_ingest import _replace_null_strings

        n = 50_000
        values = ["NULL" if i % 3 == 0 else f"value_{i}" for i in range(n)]
        df = pd.DataFrame({"col": values})
        result = _replace_null_strings(df)
        null_count = result["col"].isna().sum()
        expected_nulls = sum(1 for i in range(n) if i % 3 == 0)
        assert null_count == expected_nulls


class TestFullPipelinePerformance:
    def test_full_pipeline_completes_under_threshold(
        self, monkeypatch, mem_conn, tmp_path
    ):
        """Full pipeline on data_sample must complete in under 30 seconds."""
        from src import config
        from src.pipeline import step_01_ingest, step_02_transform, step_03_export

        monkeypatch.setattr(config, "SQL_SCHEMA", SQL_DIR / "schema.sql")
        monkeypatch.setattr(config, "RAW_DATA_DIR", DATA_SAMPLE_DIR)
        monkeypatch.setattr(config, "SQL_VIEWS_DIR", SQL_DIR / "views")
        monkeypatch.setattr(config, "SQL_QUERIES_DIR", SQL_DIR / "queries")
        monkeypatch.setattr(config, "ANALYSIS_OUTPUT_DIR", tmp_path)

        start = time.perf_counter()
        step_01_ingest.run(mem_conn)
        step_02_transform.run(mem_conn)
        step_03_export.run(mem_conn)
        elapsed = time.perf_counter() - start

        assert elapsed < 30.0, f"Pipeline took {elapsed:.2f}s (limit: 30s)"
