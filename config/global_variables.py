import os

from dotenv import load_dotenv

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
PERSONS_COLLECTION = os.getenv("PERSONS_COLLECTION")
DEALS_COLLECTION = os.getenv("DEALS_COLLECTION")
CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]
SPREADSHEET_NAME = os.getenv("SPREADSHEET_NAME")
MAILERS_WORKSHEET_NAME = os.getenv("MAILERS_WORKSHEET_NAME")
PURLS_WORKSHEET_NAME = os.getenv("PURLS_WORKSHEET_NAME")
DIGISHEET_WORKSHEET_NAME = os.getenv("DIGISHEET_WORKSHEET_NAME")
MAILERS_RANGE_NAME = os.getenv("MAILERS_RANGE_NAME")
PURLS_RANGE_NAME = os.getenv("PURLS_RANGE_NAME")
DIGISHEET_RANGE_NAME = os.getenv("DIGISHEET_RANGE_NAME")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH")
