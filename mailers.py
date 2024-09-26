import logging
from datetime import datetime
import pytz
from pymongo import MongoClient

from utils.cleaning_data import clean_phone
from config.global_variables import (
    SPREADSHEET_NAME,
    MAILERS_WORKSHEET_NAME,
    MAILERS_RANGE_NAME,
    MONGO_URI,
    DB_NAME,
    PERSONS_COLLECTION,
)
from init_google_sheets.gs_service import initialize_services, get_sheet_id

# Logging setup
logging.basicConfig(
    filename="mailers.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def load_mongodb_data():
    """Load data from MongoDB into a dictionary."""
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[PERSONS_COLLECTION]

    phone_dict = {}
    for doc in collection.find():
        phone_numbers = doc.get("phone_number", "").split(", ")
        for phone in phone_numbers:
            cleaned_phone = clean_phone(phone)
            if cleaned_phone:
                phone_dict[cleaned_phone] = doc
                logging.info(f"Loaded data for phone: {cleaned_phone}")
            else:
                logging.warning(f"Invalid phone number in document: {phone}")

    logging.info(f"Total loaded phone records: {len(phone_dict)}")
    return phone_dict


def search_data(sheet_data, phone_data):
    """Search for matches in the loaded data."""
    results = []
    for row_data in sheet_data:
        phone = row_data["phone_number"]
        logging.info(f"Searching for phone: {phone}")
        match = phone_data.get(phone)

        if match:
            logging.info(f"Match found for phone: {phone}")
            results.append(
                {
                    "row": row_data["row"],
                    "benefit_id": match.get("benefit_id", ""),
                    "stage_status": extract_description(match.get("stage_status", "")),
                    "won/lost": extract_description(match.get("won_lost", "")),
                    "assigned_to": match.get("assigned_to", ""),
                    "name": match.get("name", ""),
                    "address": match.get("address", ""),
                    "email": match.get("email", ""),
                }
            )
        else:
            logging.info(f"No match found for phone: {phone}")
            results.append(
                {
                    "row": row_data["row"],
                    "benefit_id": "N/A",
                    "stage_status": "N/A",
                    "won/lost": "N/A",
                    "assigned_to": "N/A",
                    "name": "N/A",
                    "address": "N/A",
                    "email": "N/A",
                }
            )

    matches_found = sum(1 for r in results if r["benefit_id"] != "N/A")
    logging.info(f"Total results: {len(results)}, Matches found: {matches_found}")
    if matches_found == 0:
        logging.warning(
            "No matches found. Check phone number formats and data consistency."
        )
    return results


def extract_description(value):
    """Extract description from complex structures or return the value as is."""
    if isinstance(value, dict) and "description" in value:
        return value["description"]
    return value


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
            result["benefit_id"],
            result["stage_status"],
            result["won/lost"],
            result["assigned_to"],
            result["name"],
            result["address"],
            result["email"],
        ]
        # Convert any non-string values to strings and handle None values
        values = [str(value) if value is not None else "" for value in values]
        batch_data.append(
            {
                "range": f"'{worksheet_name}'!C{row_index}:J{row_index}",
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

    # Load MongoDB data
    phone_data = load_mongodb_data()
    logging.info(f"Loaded {len(phone_data)} phone records")
    logging.info(f"Sample of loaded phone numbers: {list(phone_data.keys())[:5]}")

    # Batch get values from the sheet
    ranges = [f"'{MAILERS_WORKSHEET_NAME}'!{MAILERS_RANGE_NAME}"]
    batch_data = batch_get_values(sheets_service, sheet_id, ranges)

    # Process sheet data
    sheet_data = []
    if batch_data:
        rows = batch_data[0].get("values", [])
        for i, row in enumerate(rows, start=2):
            if len(row) > 1:
                phone = clean_phone(row[1])
                if phone:
                    sheet_data.append(
                        {
                            "row": i,
                            "phone_number": phone,
                        }
                    )
                    logging.info(f"Processed phone from sheet: {phone}")

    logging.info(f"Processed {len(sheet_data)} rows from the sheet")
    logging.info(
        f"Sample of processed phone numbers: {[d['phone_number'] for d in sheet_data[:5]]}"
    )

    # Search for matches
    results = search_data(sheet_data, phone_data)

    # Update Google Sheet with results
    update_sheet_with_results(sheets_service, sheet_id, MAILERS_WORKSHEET_NAME, results)

    logging.info("Script execution completed")


if __name__ == "__main__":
    main()
