import logging

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from config.global_variables import (
    CREDENTIALS_FILE,
    SCOPES,
)


def initialize_services():
    """Initialize and return Google Sheets and Drive services."""
    logging.info("Initializing Google services")
    creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
    sheets_service = build("sheets", "v4", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)
    return sheets_service, drive_service


def get_sheet_id(drive_service, spreadsheet_name):
    """Fetch the sheet ID for a given spreadsheet name."""
    logging.info(f"Fetching sheet ID for '{spreadsheet_name}'")
    query = f"name='{spreadsheet_name}' and mimeType='application/vnd.google-apps.spreadsheet'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    items = results.get("files", [])
    if not items:
        raise ValueError(f"No spreadsheet named '{spreadsheet_name}' found.")
    logging.info(f"Sheet ID found: {items[0]['id']}")
    return items[0]["id"]
