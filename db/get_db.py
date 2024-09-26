from motor.motor_asyncio import AsyncIOMotorClient

from config.global_variables import MONGO_URI, DB_NAME


async def get_database():
    client = AsyncIOMotorClient(MONGO_URI)
    return client[DB_NAME]
