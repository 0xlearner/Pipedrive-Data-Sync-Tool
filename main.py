import logging

import asyncio

from config.global_variables import LOG_FILE_PATH


from extraction import run as extract_data
from mailers import main as mailers_sheet_update
from purls import main as purls_sheet_update
from digisheet import main as digisheet_sheet_update

logging.basicConfig(
    filename=LOG_FILE_PATH + "extraction.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logging.info("Cron job script started.")

asyncio.run(extract_data())
mailers_sheet_update()
purls_sheet_update()
digisheet_sheet_update()
