"""Tests for src/sheets (auth, push, validation) and step_03 Sheets integration.

All tests use complete mocks of keyring and gspread — zero real Google API calls.
Includes random and stress tests for worksheet name validation and push operations.
"""

import json
import random
import string
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_SA: dict = {
    "type": "service_account",
    "client_email": "sa@project.iam.gserviceaccount.com",
    "private_key": "-----BEGIN RSA PRIVATE KEY-----\nFAKE\n-----END RSA PRIVATE KEY-----\n",
    "token_uri": "https://oauth2.googleapis.com/token",
}


# ===========================================================================
# _get_credentials_from_keychain
# ===========================================================================


class TestGetCredentialsFromKeychain:
    def test_missing_entry_raises(self):
        from src.sheets import _get_credentials_from_keychain

        with patch("src.sheets.keyring.get_password", return_value=None):
            with pytest.raises(RuntimeError) as exc_info:
                _get_credentials_from_keychain()
        assert "not found" in str(exc_info.value).lower()

    def test_invalid_json_raises(self):
        from src.sheets import _get_credentials_from_keychain

        with patch("src.sheets.keyring.get_password", return_value="not-json{"):
            with pytest.raises(RuntimeError) as exc_info:
                _get_credentials_from_keychain()
        assert "not valid json" in str(exc_info.value).lower()

    def test_non_dict_json_raises(self):
        from src.sheets import _get_credentials_from_keychain

        for value in ["[]", '"a string"', "42", "true"]:
            with patch("src.sheets.keyring.get_password", return_value=value):
                with pytest.raises(RuntimeError) as exc_info:
                    _get_credentials_from_keychain()
            assert "not a valid service account" in str(exc_info.value).lower()

    def test_wrong_type_raises(self):
        from src.sheets import _get_credentials_from_keychain

        bad = {**_VALID_SA, "type": "authorized_user"}
        with patch("src.sheets.keyring.get_password", return_value=json.dumps(bad)):
            with pytest.raises(RuntimeError) as exc_info:
                _get_credentials_from_keychain()
        assert "not a valid service account" in str(exc_info.value).lower()

    def test_missing_fields_raises(self):
        from src.sheets import _get_credentials_from_keychain

        incomplete = {"type": "service_account", "client_email": "x@y.com"}
        with patch(
            "src.sheets.keyring.get_password", return_value=json.dumps(incomplete)
        ):
            with pytest.raises(RuntimeError) as exc_info:
                _get_credentials_from_keychain()
        assert "missing required fields" in str(exc_info.value).lower()

    def test_valid_credentials_returned(self):
        from src.sheets import _get_credentials_from_keychain

        with patch(
            "src.sheets.keyring.get_password", return_value=json.dumps(_VALID_SA)
        ):
            result = _get_credentials_from_keychain()
        assert result["type"] == "service_account"
        assert result["client_email"] == _VALID_SA["client_email"]

    def test_no_credential_values_in_error_messages(self):
        """Error messages must never expose private_key, token, or email."""
        from src.sheets import _get_credentials_from_keychain

        # Missing entry
        with patch("src.sheets.keyring.get_password", return_value=None):
            with pytest.raises(RuntimeError) as exc_info:
                _get_credentials_from_keychain()
        msg = str(exc_info.value)
        assert "private_key" not in msg
        assert "token_uri" not in msg
        assert "client_email" not in msg

        # Missing fields
        incomplete = {"type": "service_account"}
        with patch(
            "src.sheets.keyring.get_password", return_value=json.dumps(incomplete)
        ):
            with pytest.raises(RuntimeError) as exc_info:
                _get_credentials_from_keychain()
        msg = str(exc_info.value)
        assert "private_key" not in msg
        assert "token_uri" not in msg


# ===========================================================================
# _authorize
# ===========================================================================


class TestAuthorize:
    def test_returns_gspread_client(self):
        from src.sheets import _authorize

        mock_client = MagicMock()
        with (
            patch(
                "src.sheets.keyring.get_password", return_value=json.dumps(_VALID_SA)
            ),
            patch("src.sheets.Credentials.from_service_account_info") as _,
            patch("src.sheets.gspread.authorize", return_value=mock_client),
        ):
            client = _authorize()
        assert client is mock_client

    def test_authorize_wraps_exceptions_as_runtime_error(self):
        from src.sheets import _authorize

        with (
            patch(
                "src.sheets.keyring.get_password", return_value=json.dumps(_VALID_SA)
            ),
            patch(
                "src.sheets.Credentials.from_service_account_info",
                side_effect=ValueError("malformed key"),
            ),
        ):
            with pytest.raises(RuntimeError, match="Failed to authorize"):
                _authorize()

    def test_uses_sheets_scope_only(self):
        from src.sheets import _authorize, _SCOPES

        with (
            patch(
                "src.sheets.keyring.get_password", return_value=json.dumps(_VALID_SA)
            ),
            patch("src.sheets.Credentials.from_service_account_info") as mock_creds,
            patch("src.sheets.gspread.authorize"),
        ):
            _authorize()
            _, kwargs = mock_creds.call_args
            # scopes is always passed as keyword argument
            scopes = kwargs.get("scopes")
        assert scopes == _SCOPES
        assert "https://www.googleapis.com/auth/spreadsheets" in scopes
        assert not any("drive" in s for s in scopes)


# ===========================================================================
# _validate_worksheet_name
# ===========================================================================


class TestValidateWorksheetName:
    @pytest.mark.parametrize(
        "name",
        [
            "q_priority_score",
            "v_demand_by_province",
            "v_segment_origin_summary",
            "abc123",
            "a" * 100,
        ],
    )
    def test_valid_names(self, name: str):
        from src.sheets import _validate_worksheet_name

        assert _validate_worksheet_name(name) is True

    @pytest.mark.parametrize(
        "name",
        [
            "",
            "a" * 101,
            "Capital",
            "has space",
            "has-dash",
            "has.dot",
            "has/slash",
            "UPPER_CASE",
        ],
    )
    def test_invalid_names(self, name: str):
        from src.sheets import _validate_worksheet_name

        assert _validate_worksheet_name(name) is False


# ===========================================================================
# push_dataframe
# ===========================================================================


def _make_client(worksheet_exists: bool = True) -> tuple[MagicMock, MagicMock]:
    """Return (mock_client, mock_worksheet)."""
    import gspread

    mock_ws = MagicMock()
    mock_spreadsheet = MagicMock()
    if worksheet_exists:
        mock_spreadsheet.worksheet.return_value = mock_ws
    else:
        mock_spreadsheet.worksheet.side_effect = gspread.exceptions.WorksheetNotFound()
        mock_spreadsheet.add_worksheet.return_value = mock_ws
    mock_client = MagicMock()
    mock_client.open_by_key.return_value = mock_spreadsheet
    return mock_client, mock_ws


class TestPushDataframe:

    def test_resize_called_before_clear(self):
        """worksheet.resize() must be called before clear() and update()."""
        from src.sheets import push_dataframe

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        client, ws = _make_client()
        call_order = []
        ws.resize.side_effect = lambda **kw: call_order.append("resize")
        ws.clear.side_effect = lambda: call_order.append("clear")
        ws.update.side_effect = lambda *a, **kw: call_order.append("update")

        push_dataframe(client, df, "q_priority_score", "spreadsheet_id")
        assert call_order == ["resize", "clear", "update"]

    def test_resize_uses_correct_dimensions(self):
        from src.sheets import push_dataframe

        df = pd.DataFrame({"a": range(5), "b": range(5), "c": range(5)})
        client, ws = _make_client()
        push_dataframe(client, df, "q_priority_score", "spreadsheet_id")
        ws.resize.assert_called_once_with(rows=6, cols=3)  # len(df)+1, len(df.columns)

    def test_clear_called_before_update(self):
        from src.sheets import push_dataframe

        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        client, ws = _make_client()
        call_order = []
        ws.clear.side_effect = lambda: call_order.append("clear")
        ws.update.side_effect = lambda *a, **kw: call_order.append("update")

        push_dataframe(client, df, "q_priority_score", "spreadsheet_id")
        assert call_order == ["clear", "update"]

    def test_value_input_option_is_raw(self):
        from src.sheets import push_dataframe

        df = pd.DataFrame({"x": [1]})
        client, ws = _make_client()
        push_dataframe(client, df, "q_priority_score", "spreadsheet_id")
        _, kwargs = ws.update.call_args
        assert kwargs.get("value_input_option") == "RAW"

    def test_returns_row_count(self):
        from src.sheets import push_dataframe

        df = pd.DataFrame({"col": range(7)})
        client, _ = _make_client()
        rows = push_dataframe(client, df, "q_priority_score", "spreadsheet_id")
        assert rows == 7

    def test_open_by_key_used(self):
        from src.sheets import push_dataframe

        df = pd.DataFrame({"col": [1]})
        client, _ = _make_client()
        push_dataframe(client, df, "q_priority_score", "my_id")
        client.open_by_key.assert_called_once_with("my_id")

    def test_invalid_worksheet_name_raises_value_error(self):
        from src.sheets import push_dataframe

        df = pd.DataFrame({"col": [1]})
        client, _ = _make_client()
        with pytest.raises(ValueError, match="Invalid worksheet name"):
            push_dataframe(client, df, "Invalid Name!", "spreadsheet_id")

    def test_creates_worksheet_if_not_found(self):
        import gspread
        from src.sheets import push_dataframe

        df = pd.DataFrame({"col": [1, 2]})
        client, _ = _make_client(worksheet_exists=False)
        push_dataframe(client, df, "q_priority_score", "spreadsheet_id")
        mock_spreadsheet = client.open_by_key.return_value
        mock_spreadsheet.add_worksheet.assert_called_once()

    def test_api_error_is_reraised(self):
        import gspread
        from src.sheets import push_dataframe

        df = pd.DataFrame({"col": [1]})
        client, ws = _make_client()
        ws.update.side_effect = gspread.exceptions.APIError(MagicMock())
        with pytest.raises(gspread.exceptions.APIError):
            push_dataframe(client, df, "q_priority_score", "spreadsheet_id")

    def test_nan_values_become_none(self):
        """NaN in DataFrame must be written as empty cells (None), not 'nan'."""
        from src.sheets import push_dataframe

        df = pd.DataFrame({"a": [1.0, float("nan")], "b": ["x", None]})
        client, ws = _make_client()
        push_dataframe(client, df, "q_priority_score", "spreadsheet_id")
        values_passed = ws.update.call_args[0][0]
        # Flatten all data rows (skip header)
        flat = [cell for row in values_passed[1:] for cell in row]
        assert not any(
            pd.isna(v) for v in flat if isinstance(v, float)
        ), "NaN must not be passed to Sheets"


# ===========================================================================
# step_03_export — fail-fast with PUSH_TO_SHEETS=true
# ===========================================================================


class TestStep03PushFailFast:
    def test_missing_spreadsheet_id_raises_before_export(self, monkeypatch, tmp_path):
        import duckdb
        from src.pipeline import step_03_export

        monkeypatch.setattr("src.config.PUSH_TO_SHEETS", True)
        monkeypatch.setattr("src.config.GOOGLE_SHEETS_SPREADSHEET_ID", None)
        monkeypatch.setattr("src.config.ANALYSIS_OUTPUT_DIR", tmp_path)

        conn = duckdb.connect(":memory:")
        with pytest.raises(RuntimeError, match="GOOGLE_SHEETS_SPREADSHEET_ID"):
            step_03_export.run(conn)
        conn.close()
        assert list(tmp_path.glob("*.csv")) == []

    def test_auth_failure_raises_before_export(self, monkeypatch, tmp_path):
        import duckdb
        from src.pipeline import step_03_export

        monkeypatch.setattr("src.config.PUSH_TO_SHEETS", True)
        monkeypatch.setattr("src.config.GOOGLE_SHEETS_SPREADSHEET_ID", "some_id")
        monkeypatch.setattr("src.config.ANALYSIS_OUTPUT_DIR", tmp_path)

        with patch(
            "src.sheets._authorize",
            side_effect=RuntimeError("auth failed"),
        ):
            conn = duckdb.connect(":memory:")
            with pytest.raises(RuntimeError):
                step_03_export.run(conn)
            conn.close()

    def test_push_to_sheets_false_skips_auth(self, monkeypatch, tmp_path):
        """When PUSH_TO_SHEETS=false, no Keychain or gspread call is ever made."""
        import duckdb
        from src.pipeline import step_03_export
        from src.utils.db import execute_sql_file, execute_sql_directory

        PROJECT_ROOT = __import__("pathlib").Path(__file__).resolve().parent.parent
        SQL_DIR = PROJECT_ROOT / "sql"
        DATA_SAMPLE_DIR = PROJECT_ROOT / "data_sample"

        monkeypatch.setattr("src.config.PUSH_TO_SHEETS", False)
        monkeypatch.setattr("src.config.ANALYSIS_OUTPUT_DIR", tmp_path)

        conn = duckdb.connect(":memory:")
        execute_sql_file(conn, SQL_DIR / "schema.sql")
        from src.pipeline.step_01_ingest import _ingest_capacity, _ingest_movements

        _ingest_movements(conn, DATA_SAMPLE_DIR)
        _ingest_capacity(conn, DATA_SAMPLE_DIR)
        execute_sql_directory(conn, SQL_DIR / "views")
        execute_sql_directory(conn, SQL_DIR / "queries")

        with patch("src.sheets.keyring.get_password") as mock_kp:
            step_03_export.run(conn)
            mock_kp.assert_not_called()

        conn.close()


# ===========================================================================
# Random tests — _validate_worksheet_name
# ===========================================================================

_VALID_CHARS: str = string.ascii_lowercase + string.digits + "_"
_INVALID_CHARS: str = (
    string.ascii_uppercase
    + "".join(c for c in string.punctuation if c != "_")
    + " \t\n"
)

_RNG_VALID = random.Random(42)
_RNG_INVALID = random.Random(99)


class TestValidateWorksheetNameRandom:
    """1000-sample random tests for worksheet name validation."""

    def test_random_valid_names_always_pass(self):
        """1000 randomly generated names using only allowed chars must all pass."""
        from src.sheets import _validate_worksheet_name

        failures: list[str] = []
        for _ in range(1000):
            length = _RNG_VALID.randint(1, 100)
            name = "".join(_RNG_VALID.choice(_VALID_CHARS) for _ in range(length))
            if not _validate_worksheet_name(name):
                failures.append(name)

        assert (
            not failures
        ), f"Expected valid but failed ({len(failures)}): {failures[:5]}"

    def test_random_invalid_char_injected_always_fails(self):
        """1000 valid-base names with one injected invalid char must all fail."""
        from src.sheets import _validate_worksheet_name

        passes: list[str] = []
        for _ in range(1000):
            length = _RNG_INVALID.randint(1, 90)
            base = "".join(_RNG_INVALID.choice(_VALID_CHARS) for _ in range(length))
            pos = _RNG_INVALID.randint(0, len(base))
            bad_char = _RNG_INVALID.choice(_INVALID_CHARS)
            name = base[:pos] + bad_char + base[pos:]
            if _validate_worksheet_name(name):
                passes.append(repr(name))

        assert not passes, f"Expected invalid but passed ({len(passes)}): {passes[:5]}"

    def test_random_overlength_names_always_fail(self):
        """500 names longer than 100 chars must all fail."""
        from src.sheets import _validate_worksheet_name

        rng = random.Random(7)
        passes: list[str] = []
        for _ in range(500):
            length = rng.randint(101, 300)
            name = "".join(rng.choice(_VALID_CHARS) for _ in range(length))
            if _validate_worksheet_name(name):
                passes.append(name[:20] + "...")

        assert not passes, f"Overlength names passed ({len(passes)}): {passes[:3]}"

    def test_boundary_exactly_100_chars_passes(self):
        """Name of exactly 100 valid chars must pass."""
        from src.sheets import _validate_worksheet_name

        name = "a" * 100
        assert _validate_worksheet_name(name)

    def test_boundary_exactly_101_chars_fails(self):
        """Name of exactly 101 valid chars must fail."""
        from src.sheets import _validate_worksheet_name

        name = "a" * 101
        assert not _validate_worksheet_name(name)


# ===========================================================================
# Stress tests — push_dataframe
# ===========================================================================


class TestPushDataframeStress:
    """Stress tests with large and varied DataFrames."""

    def test_large_dataframe_10k_rows(self):
        """10,000-row DataFrame: correct row count returned, full values passed."""
        from src.sheets import push_dataframe

        rng = random.Random(11)
        n = 10_000
        df = pd.DataFrame(
            {
                "id": range(n),
                "value": [rng.uniform(-1000, 1000) for _ in range(n)],
                "label": [f"label_{rng.randint(0, 100)}" for _ in range(n)],
            }
        )
        client, ws = _make_client()
        rows = push_dataframe(client, df, "q_priority_score", "sid")

        assert rows == n
        values_passed = ws.update.call_args[0][0]
        assert len(values_passed) == n + 1  # header + data rows

    def test_wide_dataframe_50_columns(self):
        """DataFrame with 50 columns: all column names in header row."""
        from src.sheets import push_dataframe

        cols = {f"col_{i:02d}": list(range(10)) for i in range(50)}
        df = pd.DataFrame(cols)
        client, ws = _make_client()
        push_dataframe(client, df, "v_demand_by_province", "sid")

        values_passed = ws.update.call_args[0][0]
        assert len(values_passed[0]) == 50
        assert values_passed[0] == df.columns.tolist()

    def test_random_nan_distribution_no_nan_strings(self):
        """DataFrame with ~30% random NaN: output must contain None, never 'nan'."""
        from src.sheets import push_dataframe

        rng = random.Random(17)
        n = 500
        df = pd.DataFrame(
            {
                "float_col": [
                    rng.uniform(0, 1) if rng.random() > 0.3 else float("nan")
                    for _ in range(n)
                ],
                "str_col": [f"v_{i}" if rng.random() > 0.3 else None for i in range(n)],
            }
        )
        client, ws = _make_client()
        push_dataframe(client, df, "q_priority_score", "sid")

        values_passed = ws.update.call_args[0][0]
        flat = [cell for row in values_passed[1:] for cell in row]
        nan_floats = [v for v in flat if isinstance(v, float) and str(v) == "nan"]
        nan_strings = [v for v in flat if v == "nan"]
        assert not nan_floats, "float NaN must not appear in output"
        assert not nan_strings, "'nan' string must not appear in output"

    def test_random_mixed_dtypes_no_error(self):
        """DataFrame mixing int, float, str, bool: pushes all rows without error."""
        from src.sheets import push_dataframe

        rng = random.Random(31)
        n = 300
        df = pd.DataFrame(
            {
                "int_col": [rng.randint(0, 10_000) for _ in range(n)],
                "float_col": [rng.gauss(0, 100) for _ in range(n)],
                "str_col": [f"item_{rng.randint(0, 999)}" for _ in range(n)],
                "bool_col": [rng.choice([True, False]) for _ in range(n)],
            }
        )
        client, _ = _make_client()
        rows = push_dataframe(client, df, "v_trend_yoy", "sid")
        assert rows == n

    def test_stress_repeated_pushes_same_worksheet(self):
        """50 sequential pushes to the same worksheet: each clears before writing."""
        from src.sheets import push_dataframe

        rng = random.Random(53)
        client, ws = _make_client()
        for i in range(50):
            n = rng.randint(1, 200)
            df = pd.DataFrame({"run": [i] * n, "val": [rng.random() for _ in range(n)]})
            rows = push_dataframe(client, df, "q_priority_score", "sid")
            assert rows == n

        assert ws.clear.call_count == 50
        assert ws.update.call_count == 50
