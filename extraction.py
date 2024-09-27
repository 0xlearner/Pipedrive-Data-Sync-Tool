import math
import logging
import asyncio
from typing import List, Optional

import httpx
from httpx import AsyncClient, Response
from asynciolimiter import Limiter
from tenacity import retry, stop_after_attempt, wait_fixed

from config.global_variables import (
    API_TOKEN,
    PERSONS_COLLECTION,
    DEALS_COLLECTION,
)
from models.data_models import StageStatus, PersonInfo, DealInfo
from db.get_db import get_database
from db.data_access_layer import insert_or_update_document

# Configure logging
logging.basicConfig(
    filename="extraction.log",
    filemode="w",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


TOKEN = {"api_token": API_TOKEN}

# Rate limiter for API requests
LIMITER = Limiter(50 / 5)


# Fetch total pages for deal pagination
def get_total_pages(base_url: str) -> List[str]:
    """Retrieve total pages for deal pagination."""
    pages = []
    logging.info(f"Processing {base_url}")

    response: Response = httpx.get(base_url, params=TOKEN)
    logging.info(f"URL: {base_url} ==> Response: {response.status_code}")

    if response.status_code != 200:
        logging.error(f"Error fetching data: {response.status_code} - {response.text}")
        response.raise_for_status()

    total_count = response.json()["additional_data"]["summary"]["total_count"]
    total_pages = math.ceil(total_count / 100)
    for p in range(total_pages):
        page = p * 100
        pages.append(f"https://shc2.pipedrive.com/api/v1/deals?start={page}&limit=100")

    return pages


# Fetch and process deals data
@retry(
    stop=stop_after_attempt(3), wait=wait_fixed(300)
)  # Retry 3 times with 5 minutes (300 seconds) delay
async def get_deals(deal_url: str, client: AsyncClient, db):
    try:
        await LIMITER.wait()

        response: Response = await client.get(deal_url)
        logging.info(f"URL: {deal_url} ==> Response: {response.status_code}")

        if response.status_code != 200:
            logging.error(
                f"Error fetching data: {response.status_code} - {response.text}"
            )
            response.raise_for_status()

        response_data = response.json().get("data", [])
        if not response_data:
            logging.error(f"No data found for {deal_url}")
            return

        for data in response_data:
            person_info = await fetch_person_data(data, client)
            if person_info:
                await save_data_to_mongodb(data, person_info, client, db)
            else:
                logging.warning(
                    f"Skipping deal {data.get('id')} due to missing person info"
                )
    except httpx.ReadTimeout:
        logging.warning(
            f"ReadTimeout occurred while fetching deals for {data.get('id')}. Retrying..."
        )
        raise  # Re-raise the exception to trigger retry


# Fetch person-related data
@retry(
    stop=stop_after_attempt(3), wait=wait_fixed(300)
)  # Retry 3 times with 5 minutes (300 seconds) delay
async def fetch_person_data(data: dict, client: AsyncClient) -> Optional[PersonInfo]:
    person_id = data.get("person_id")
    if not person_id:
        logging.error(f"Person ID missing in deal data {data['id']}.")
        return None

    # Check if person_id is a dictionary and has a 'value' key
    if isinstance(person_id, dict):
        person_id = person_id.get("value")

    if not person_id:
        logging.error(f"Invalid person ID format in deal data {data['id']}.")
        return None

    person_endpoint = f"https://api.pipedrive.com/v1/persons/{person_id}"
    logging.info(f"Fetching Person Data: {person_endpoint}")
    try:
        await LIMITER.wait()
        person_response = await client.get(person_endpoint)

        person_data = person_response.json().get("data", {})
        if not person_data:
            logging.error(f"Failed to fetch person info for {person_endpoint}")
            return None

        benefit_id = person_data.get(
            "ca8fd59fb797a92665b29c4ee38a45524a6ad51b"
        ) or person_data.get("a1a2bdea3ec02b42cc9baa376fd5ac79a750813b")

        phone_number = ", ".join(
            [
                ph.get("value", "").replace("-", "")
                for ph in person_data.get("phone", [])
                if ph
            ]
        )

        person_info = PersonInfo(
            id=str(person_id),
            benefit_id=benefit_id,
            phone_number=phone_number,
            email=", ".join(
                [email.get("value", "") for email in person_data.get("email", [])]
            ),
            address=get_person_address(person_data),
        )

        return person_info
    except httpx.ReadTimeout:
        logging.warning(
            f"ReadTimeout occurred while fetching person data for {person_id}. Retrying..."
        )
        raise  # Re-raise the exception to trigger retry


# Extract address information
def get_person_address(person_data: dict) -> str:
    address_parts = [
        person_data.get("2a556bd22d2c0374f609f6fafcca7949cf9b2ba2", ""),
        person_data.get("c003c48faccbde63860456ee2f1a5a50f25529a5", ""),
        person_data.get("14d2126d1386f43fdbd18ca803c3faab87315d46", ""),
        person_data.get("2d762978f235765bbd5fc547c55beb173c0a7101", ""),
    ]
    return " ".join(filter(None, address_parts)).strip()


async def save_data_to_mongodb(
    data: dict, person_info: PersonInfo, client: AsyncClient, db
):
    deal_id = data["id"]
    deal_info = await fetch_deal_data(deal_id, client)

    if deal_info:
        info = {
            "benefit_id": person_info.benefit_id,
            "phone_number": person_info.phone_number,
            "updated_at": data.get("update_time"),
            "email": person_info.email,
            "stage_status": (
                deal_info.stage_status.to_dict() if deal_info.stage_status else None
            ),
            "won_lost": deal_info.status,
            "assigned_to": deal_info.assigned_to,
            "name": deal_info.name,
            "address": person_info.address,
        }

        # Insert or update person data
        person_collection = db[PERSONS_COLLECTION]
        person_filter = {"_id": person_info.id}
        await insert_or_update_document(person_collection, person_filter, info)

        # Insert or update deal data
        deal_collection = db[DEALS_COLLECTION]
        deal_filter = {"_id": deal_id}
        deal_data = {
            "person_id": person_info.id,
            "stage_status": (
                deal_info.stage_status.to_dict() if deal_info.stage_status else None
            ),
            "status": deal_info.status,
            "assigned_to": deal_info.assigned_to,
            "updated_at": deal_info.updated_at,
            "name": deal_info.name,
        }
        await insert_or_update_document(deal_collection, deal_filter, deal_data)


# Fetch deal data
@retry(
    stop=stop_after_attempt(3), wait=wait_fixed(300)
)  # Retry 3 times with 5 minutes (300 seconds) delay
async def fetch_deal_data(deal_id: str, client: AsyncClient) -> Optional[DealInfo]:
    deal_endpoint = f"https://api.pipedrive.com/v1/deals/{deal_id}"
    logging.info(f"Fetching Deal Data: {deal_endpoint}")
    try:
        await LIMITER.wait()
        deal_response = await client.get(deal_endpoint)

        deal_data = deal_response.json().get("data", {})
        if not deal_data:
            logging.error(f"Failed to fetch deal info for {deal_endpoint}")
            return None

        stage_number = deal_data.get("stage_order_nr")
        stage_status = StageStatus.from_number(stage_number)
        return DealInfo(
            id=str(deal_id),
            person_id=str(deal_data.get("person_id", {}).get("value")),
            stage_status=stage_status,
            status=(
                "WON"
                if deal_data.get("status") == "won"
                else "LOST" if deal_data.get("status") == "lost" else ""
            ),
            assigned_to=deal_data.get("user_id", {}).get("name"),
            updated_at=deal_data.get("update_time"),
            name=deal_data.get("person_id", {}).get("name"),
        )
    except httpx.ReadTimeout:
        logging.warning(
            f"ReadTimeout occurred while fetching deal data for {deal_id}. Retrying..."
        )
        raise  # Re-raise the exception to trigger retry


async def run():
    try:
        db = await get_database()
        base_url = (
            "https://shc2.pipedrive.com/api/v1/deals?start=0&limit=100&get_summary=1"
        )
        deal_pages = get_total_pages(base_url)

        async with AsyncClient(params=TOKEN, timeout=30) as client:
            # Create a list of tasks for all deal URLs
            tasks = [get_deals(deal_url, client, db) for deal_url in deal_pages]

            # Run all tasks concurrently
            await asyncio.gather(*tasks)
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        raise  # Re-raise the exception to ensure the script exits with a non-zero status


# Entry point
if __name__ == "__main__":
    asyncio.run(run())
