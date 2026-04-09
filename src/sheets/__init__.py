"""Google Sheets push integration via macOS Keychain authentication.

Authentication pattern:
  macOS Keychain → json.loads() → Credentials.from_service_account_info() → gspread.authorize()

Never use gspread.service_account() (requires file on disk) or
service_account_from_dict() (applies default scopes including Drive).
"""

import json
import logging
import re

import gspread
import keyring
import pandas as pd
from google.oauth2.service_account import Credentials

from src import config

logger: logging.Logger = logging.getLogger(__name__)

# Minimal scopes: Sheets only, no Drive.
_SCOPES: list[str] = ["https://www.googleapis.com/auth/spreadsheets"]

# Worksheet names must be lowercase alphanumeric + underscores, max 100 chars.
_WORKSHEET_NAME_RE: re.Pattern[str] = re.compile(r"^[a-z0-9_]{1,100}$")


def _get_credentials_from_keychain() -> dict:
    """Retrieve and validate the service account JSON from macOS Keychain.

    Returns:
        Parsed service account dict ready for Credentials.from_service_account_info().

    Raises:
        RuntimeError: If credentials are missing, invalid JSON, wrong type,
            or missing required fields. Error messages are intentionally generic
            and never expose credential values.
    """
    raw: str | None = keyring.get_password(config.KEYRING_SERVICE, config.KEYRING_KEY)
    if not raw:
        raise RuntimeError("Credentials not found in macOS Keychain.")

    try:
        info: dict = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError("Credentials in Keychain are not valid JSON.") from exc

    if info.get("type") != "service_account":
        raise RuntimeError("Keychain credentials are not a valid service account.")

    required: set[str] = {"type", "client_email", "private_key", "token_uri"}
    if required - info.keys():
        raise RuntimeError("Service account JSON is missing required fields.")

    return info


def _authorize() -> gspread.Client:
    """Create a gspread client authenticated via macOS Keychain.

    Uses explicit minimal scopes (Sheets only).

    Returns:
        Authenticated gspread.Client.

    Raises:
        RuntimeError: On Keychain or credential validation failure.
    """
    info: dict = _get_credentials_from_keychain()
    credentials: Credentials = Credentials.from_service_account_info(
        info, scopes=_SCOPES
    )
    return gspread.authorize(credentials)


def _validate_worksheet_name(name: str) -> bool:
    """Return True if name matches the allowed worksheet name pattern.

    Allowed: lowercase letters, digits, underscores, max 100 characters.

    Args:
        name: Worksheet name to validate.

    Returns:
        True if valid, False otherwise.
    """
    return bool(_WORKSHEET_NAME_RE.match(name))


def push_dataframe(
    client: gspread.Client,
    df: pd.DataFrame,
    worksheet_name: str,
    spreadsheet_id: str,
) -> int:
    """Push a DataFrame to a Google Sheets worksheet (full overwrite).

    Clears the worksheet before writing. Uses RAW input to prevent
    formula injection. Opens the spreadsheet by key (not by name).

    Args:
        client: Authenticated gspread client.
        df: DataFrame to push.
        worksheet_name: Target worksheet name (must match allowed pattern).
        spreadsheet_id: Google Sheets spreadsheet ID from the URL.

    Returns:
        Number of data rows written (header excluded).

    Raises:
        ValueError: If worksheet_name fails validation.
        gspread.exceptions.APIError: On Google API failure (logged, re-raised).
    """
    if not _validate_worksheet_name(worksheet_name):
        raise ValueError(f"Invalid worksheet name: '{worksheet_name}'.")

    # Replace NaN with None so gspread writes empty cells instead of "nan".
    # astype(object) is required first: float columns cannot store Python None,
    # so df.where(..., None) alone leaves NaN intact in float dtype columns.
    values: list = [df.columns.tolist()] + df.astype(object).where(
        pd.notnull(df), None
    ).values.tolist()

    try:
        spreadsheet = client.open_by_key(spreadsheet_id)
        try:
            worksheet = spreadsheet.worksheet(worksheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(
                title=worksheet_name,
                rows=len(df) + 1,
                cols=len(df.columns),
            )
        worksheet.clear()
        worksheet.update(values, value_input_option="RAW")
        logger.info("  %-40s %6d rows → Sheets", worksheet_name, len(df))
        return len(df)
    except gspread.exceptions.APIError:
        logger.error("Google Sheets API error on worksheet '%s'.", worksheet_name)
        raise
