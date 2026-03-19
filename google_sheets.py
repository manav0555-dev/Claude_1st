"""
Google Sheets integration for HVAC Complaint Ticketing System.

Syncs complaint data to a live Google Sheet for analysis and reporting.

Setup:
1. Create a Google Cloud project and enable the Google Sheets API
2. Create a Service Account and download the JSON credentials
3. Set the GOOGLE_SHEETS_CREDENTIALS env var to the JSON string
4. Create a Google Sheet and share it with the service account email
5. Set GOOGLE_SHEET_ID env var to the spreadsheet ID from the URL
"""

import json
import logging
import os
import subprocess
import sys

logger = logging.getLogger(__name__)

# Headers for the complaints sheet
HEADERS = [
    "Ticket ID", "Title", "Description", "Customer Name", "Customer Phone",
    "Customer Email", "Job Site", "Technician", "Priority", "Status",
    "Category", "Created At", "Updated At", "Resolved At"
]

# Check gspread availability once at module load using a subprocess
# (gspread can trigger Rust panics that crash the Python process)
_gspread_available = None

def _check_gspread():
    global _gspread_available
    if _gspread_available is not None:
        return _gspread_available
    # Subprocess check first (avoids Rust panics that crash the process in some envs)
    try:
        result = subprocess.run(
            [sys.executable, "-c", "import gspread; print('ok')"],
            capture_output=True, timeout=10
        )
        _gspread_available = result.returncode == 0 and b'ok' in result.stdout
    except Exception:
        _gspread_available = False
    return _gspread_available


def _get_gspread():
    """Lazy-import gspread to avoid crashes if dependencies are broken."""
    if not _check_gspread():
        return None
    import gspread
    return gspread


def _get_client():
    """Get an authenticated gspread client using service account credentials."""
    gspread = _get_gspread()
    if not gspread:
        logger.warning("gspread not available – skipping Google Sheets sync")
        return None

    creds_json = os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
    if not creds_json:
        logger.warning("GOOGLE_SHEETS_CREDENTIALS not set – skipping Google Sheets sync")
        return None

    try:
        from google.oauth2.service_account import Credentials
        creds_data = json.loads(creds_json)
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        credentials = Credentials.from_service_account_info(creds_data, scopes=scopes)
        return gspread.authorize(credentials)
    except Exception as e:
        logger.error("Failed to authenticate with Google Sheets: %s", e)
        return None


def _get_worksheet():
    """Get the 'Complaints' worksheet, creating headers if needed."""
    sheet_id = os.environ.get("GOOGLE_SHEET_ID")
    if not sheet_id:
        logger.warning("GOOGLE_SHEET_ID not set – skipping Google Sheets sync")
        return None

    client = _get_client()
    if not client:
        return None

    gspread = _get_gspread()

    try:
        spreadsheet = client.open_by_key(sheet_id)

        # Try to get existing 'Complaints' worksheet, or use first sheet
        try:
            worksheet = spreadsheet.worksheet("Complaints")
        except Exception:
            worksheet = spreadsheet.sheet1
            worksheet.update_title("Complaints")

        # Ensure headers exist
        first_row = worksheet.row_values(1)
        if not first_row or first_row[0] != HEADERS[0]:
            worksheet.update("A1", [HEADERS])
            worksheet.format("A1:N1", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.7},
                "horizontalAlignment": "CENTER",
            })

        return worksheet
    except Exception as e:
        logger.error("Failed to access Google Sheet: %s", e)
        return None


def _complaint_to_row(complaint):
    """Convert a complaint dict/Row to a list of values for the sheet."""
    return [
        complaint.get("ticket_id", ""),
        complaint.get("title", ""),
        complaint.get("description", ""),
        complaint.get("customer_name", ""),
        complaint.get("customer_phone", "") or "",
        complaint.get("customer_email", "") or "",
        complaint.get("site_name", "") or "",
        complaint.get("technician_name", "") or "",
        str(complaint.get("priority", 3)),
        complaint.get("status", "open"),
        complaint.get("category", "") or "",
        complaint.get("created_at", "") or "",
        complaint.get("updated_at", "") or "",
        complaint.get("resolved_at", "") or "",
    ]


def sync_complaint(complaint):
    """Add or update a single complaint row in Google Sheets (with retry)."""
    import time
    max_retries = 2
    for attempt in range(max_retries + 1):
        try:
            worksheet = _get_worksheet()
            if not worksheet:
                return False

            ticket_id = complaint.get("ticket_id", "")
            row_data = _complaint_to_row(complaint)

            # Search for existing row by ticket_id
            cell = worksheet.find(ticket_id, in_column=1)
            if cell:
                # Update existing row
                worksheet.update(f"A{cell.row}:N{cell.row}", [row_data])
                logger.info("Updated ticket %s in Google Sheets (row %d)", ticket_id, cell.row)
            else:
                # Append new row
                worksheet.append_row(row_data, value_input_option="USER_ENTERED")
                logger.info("Added ticket %s to Google Sheets", ticket_id)

            return True
        except Exception as e:
            logger.error("Google Sheets sync attempt %d/%d failed: %s", attempt + 1, max_retries + 1, e)
            if attempt < max_retries:
                time.sleep(1)
    return False


def sync_all_complaints(complaints):
    """Full sync – replaces all data in the sheet with current DB data."""
    try:
        worksheet = _get_worksheet()
        if not worksheet:
            return False

        # Clear everything below headers and write all rows
        worksheet.clear()
        rows = [HEADERS] + [_complaint_to_row(c) for c in complaints]
        worksheet.update("A1", rows, value_input_option="USER_ENTERED")

        # Re-apply header formatting
        worksheet.format("A1:N1", {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.2, "green": 0.4, "blue": 0.7},
            "horizontalAlignment": "CENTER",
        })

        logger.info("Full sync complete – %d complaints written to Google Sheets", len(complaints))
        return True
    except Exception as e:
        logger.error("Failed to full-sync to Google Sheets: %s", e)
        return False


def is_configured():
    """Check if Google Sheets integration is configured."""
    return bool(
        os.environ.get("GOOGLE_SHEETS_CREDENTIALS")
        and os.environ.get("GOOGLE_SHEET_ID")
        and _check_gspread()
    )
