import logging

from pymongo.errors import DuplicateKeyError


async def insert_or_update_document(collection, filter_dict, update_dict):
    try:
        result = await collection.update_one(
            filter_dict, {"$set": update_dict}, upsert=True
        )
        if result.upserted_id:
            logging.info(
                f"Inserted new document in {collection.name}: {result.upserted_id}"
            )
        else:
            logging.info(
                f"Updated existing document in {collection.name}: {filter_dict}"
            )
    except DuplicateKeyError:
        logging.warning(
            f"Duplicate key error for document in {collection.name}: {filter_dict}"
        )
    except Exception as e:
        logging.error(f"Error inserting/updating document in {collection.name}: {e}")
