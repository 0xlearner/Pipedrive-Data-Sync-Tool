import logging
from datetime import datetime
import pytz
from pymongo import MongoClient

from utils.cleaning_data import clean_email, clean_phone
from config.global_variables import (
    SPREADSHEET_NAME,
    DIGISHEET_WORKSHEET_NAME,
    MONGO_URI,
    DB_NAME,
    PERSONS_COLLECTION,
)
from init_google_sheets.gs_service import initialize_services, get_sheet_id

# Logging setup
logging.basicConfig(
    filename="digisheet.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_mongodb_data():
    """Load data from MongoDB persons collection into dictionaries."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[PERSONS_COLLECTION]

    email_dict = {}
    phone_dict = {}

    for doc in collection.find():
        email = clean_email(doc.get("email"))
        phone = clean_phone(doc.get("phone_number"))
        updated_at = datetime.strptime(doc.get("updated_at", ""), "%Y-%m-%d %H:%M:%S")

        if email:
            if email not in email_dict or updated_at > email_dict[email]["updated_at"]:
                email_dict[email] = {
                    "data": doc,
                    "updated_at": updated_at,
                }

        if phone:
            if phone not in phone_dict or updated_at > phone_dict[phone]["updated_at"]:
                phone_dict[phone] = {
                    "data": doc,
                    "updated_at": updated_at,
                }

    logging.info(
        f"Loaded {len(email_dict)} email records and {len(phone_dict)} phone records from MongoDB"
    )
    return email_dict, phone_dict


def search_data(sheet_data, email_data, phone_data):
    """Search for matches in the loaded MongoDB data."""
    results = []
    for row_data in sheet_data:
        email = row_data.get("email")
        phone = row_data.get("phone_number")

        match = None
        if email and email in email_data:
            match = email_data[email]["data"]
            logging.info(f"Match found in email data for email: {email}")
        elif phone and phone in phone_data:
            match = phone_data[phone]["data"]
            logging.info(f"Match found in phone data for phone: {phone}")
        else:
            logging.info(f"No match found for email: {email}, phone: {phone}")

        if match:
            results.append(
                {
                    "row": row_data["row"],
                    "stage_status": extract_description(match.get("stage_status", "")),
                    "won/lost": extract_description(match.get("won_lost", "")),
                    "assigned_to": extract_description(match.get("assigned_to", "")),
                }
            )
        else:
            results.append(
                {
                    "row": row_data["row"],
                    "stage_status": "N/A",
                    "won/lost": "N/A",
                    "assigned_to": "N/A",
                }
            )

    return results


def extract_description(value):
    """Extract description from complex structures or return the value as is."""
    if isinstance(value, dict):
        return value.get("description", str(value))
    return str(value) if value is not None else ""


def batch_get_values(service, spreadsheet_id, ranges):
    """Batch get values from multiple ranges in the sheet."""
    request = (
        service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=ranges)
    )
    return request.execute().get("valueRanges", [])


def update_sheet_with_results(sheets_service, sheet_id, worksheet_name, results):
    """Update the Google Sheet with the search results."""
    logging.info("Updating sheet with results")
    now = datetime.now(tz=pytz.utc).astimezone(pytz.timezone("US/Pacific"))
    dt_string = now.strftime("%m/%d/%Y %H:%M:%S")

    batch_data = []
    for result in results:
        row_index = result["row"]
        values = [
            dt_string,
            result["stage_status"],
            result["won/lost"],
            result["assigned_to"],
        ]
        values = [str(value) for value in values]  # Ensure all values are strings
        batch_data.append(
            {
                "range": f"'{worksheet_name}'!A{row_index}:D{row_index}",
                "values": [values],
            }
        )

    if batch_data:
        body = {"valueInputOption": "RAW", "data": batch_data}
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=sheet_id, body=body
        ).execute()
        logging.info(f"Updated {len(batch_data)} rows with new data")
    else:
        logging.info("No updates to perform")


def main():
    logging.info("Starting script execution")

    # Initialize services
    sheets_service, drive_service = initialize_services()

    # Get sheet ID
    sheet_id = get_sheet_id(drive_service, SPREADSHEET_NAME)

    # Load data from MongoDB
    email_data, phone_data = load_mongodb_data()

    # Batch get values from the sheet
    ranges = [
        f"'{DIGISHEET_WORKSHEET_NAME}'!A2:G",
        f"'{DIGISHEET_WORKSHEET_NAME}'!K2:K",
    ]
    batch_data = batch_get_values(sheets_service, sheet_id, ranges)

    # Process sheet data
    sheet_data = []
    if len(batch_data) >= 2:
        emails = batch_data[0].get("values", [])
        phones = batch_data[1].get("values", [])

        for i, (email_row, phone_row) in enumerate(zip(emails, phones), start=2):
            email = clean_email(email_row[6]) if len(email_row) > 6 else None
            phone = clean_phone(phone_row[0]) if phone_row else None

            if email or phone:
                sheet_data.append(
                    {
                        "row": i,
                        "email": email,
                        "phone_number": phone,
                    }
                )

    logging.info(f"Processed {len(sheet_data)} rows from the sheet")

    # Search for matches in MongoDB data
    results = search_data(sheet_data, email_data, phone_data)

    # Update Google Sheet with results
    update_sheet_with_results(
        sheets_service, sheet_id, DIGISHEET_WORKSHEET_NAME, results
    )

    logging.info("Script execution completed")


if __name__ == "__main__":
    main()
