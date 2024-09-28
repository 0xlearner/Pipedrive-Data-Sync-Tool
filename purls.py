import logging
from datetime import datetime
import pytz
from pymongo import MongoClient

from utils.cleaning_data import clean_benefit_id
from config.global_variables import (
    SPREADSHEET_NAME,
    PURLS_WORKSHEET_NAME,
    PURLS_RANGE_NAME,
    MONGO_URI,
    DB_NAME,
    PERSONS_COLLECTION,
)
from init_google_sheets.gs_service import initialize_services, get_sheet_id
from config.global_variables import LOG_FILE_PATH

# Logging setup
logging.basicConfig(
    filename=LOG_FILE_PATH + "purls.log",
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

    benefit_dict = {}
    for doc in collection.find():
        benefit_id = doc.get("benefit_id", "")
        if benefit_id:
            benefit_dict[benefit_id] = doc
            logging.info(f"Loaded data for benefit ID: {benefit_id}")
        else:
            logging.warning(f"Invalid benefit ID in file {benefit_id}")

    logging.info(f"Total loaded benefit ID records: {len(benefit_dict)}")
    return benefit_dict


def search_data(sheet_data, benefit_data):
    """Search for matches in the loaded data."""
    results = []
    for row_data in sheet_data:
        benefit_id = row_data["benefit_id"]
        logging.info(f"Searching for benefit ID: {benefit_id}")
        match = benefit_data.get(benefit_id)

        if match:
            logging.info(f"Match found for benefit ID: {benefit_id}")
            results.append(
                {
                    "row": row_data["row"],
                    "phone_number": match.get("phone_number", ""),
                    "email": match.get("email", ""),
                    "stage_status": extract_description(match.get("stage_status", "")),
                    "won/lost": extract_description(match.get("won/lost", "")),
                    "assigned_to": match.get("assigned_to", ""),
                    "name": match.get("name", ""),
                    "address": match.get("address", ""),
                }
            )
        else:
            logging.info(f"No match found for benefit ID: {benefit_id}")
            results.append(
                {
                    "row": row_data["row"],
                    "phone_number": "N/A",
                    "email": "N/A",
                    "stage_status": "N/A",
                    "won/lost": "N/A",
                    "assigned_to": "N/A",
                    "name": "N/A",
                    "address": "N/A",
                }
            )

    matches_found = sum(1 for r in results if r["phone_number"] != "N/A")
    logging.info(f"Total results: {len(results)}, Matches found: {matches_found}")
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
            result["phone_number"],
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

    # Load JSON data
    benefit_data = load_mongodb_data()
    logging.info(f"Loaded {len(benefit_data)} benefit ID records")

    # Batch get values from the sheet
    ranges = [f"'{PURLS_WORKSHEET_NAME}'!{PURLS_RANGE_NAME}"]
    batch_data = batch_get_values(sheets_service, sheet_id, ranges)

    # Process sheet data
    sheet_data = []
    if batch_data:
        rows = batch_data[0].get("values", [])
        for i, row in enumerate(rows, start=2):
            if len(row) > 1:
                benefit_id = clean_benefit_id(row[1])
                if benefit_id:
                    sheet_data.append(
                        {
                            "row": i,
                            "benefit_id": benefit_id,
                        }
                    )
                    logging.info(f"Processed benefit ID from sheet: {benefit_id}")

    logging.info(f"Processed {len(sheet_data)} rows from the sheet")

    # Search for matches
    results = search_data(sheet_data, benefit_data)

    # Update Google Sheet with results
    update_sheet_with_results(sheets_service, sheet_id, PURLS_WORKSHEET_NAME, results)

    logging.info("Script execution completed")


if __name__ == "__main__":
    main()
