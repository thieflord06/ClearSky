import asyncio
import json
import requests
import database_handler
from config_helper import logger
from datetime import datetime


def fetch_data_with_after_parameter(url, after_value):
    response = requests.get(url, params={'after': after_value})
    if response.status_code == 200:
        db_data = []

        for line in response.iter_lines():
            try:
                record = json.loads(line)
                did = record.get("did")
                in_record = record.get("operation")
                service = in_record.get("service")
                handle = in_record.get("handle")
                if not service or handle is None:
                    in_endpoint = in_record.get("services")
                    in_services = in_endpoint.get("atproto_pds")
                    preprocessed_handle = in_record.get("alsoKnownAs")
                    handle = [item.replace("at://", "") for item in preprocessed_handle]
                    handle = handle[0]
                    service = in_services.get("endpoint")

                created_date = record.get("createdAt")
                created_date = datetime.fromisoformat(created_date)

                db_data.append([did, created_date, service, handle])
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON line: {line}")
                continue

        # Check if there's any data in the list before getting the last created_date
        if db_data:
            last_created_date = db_data[-1][1]  # Access the last element and the created_date (index 1)
        else:
            last_created_date = None

        return db_data, last_created_date
    else:
        # Handle any errors or exceptions here
        logger.error(f"Error fetching data. Status code: {response.status_code}")

        return None, None


async def get_all_did_records():
    url = 'https://plc.directory/export'
    after_value = None

    while True:
        data, last_created = fetch_data_with_after_parameter(url, after_value)

        logger.info("data batch fetched.")
        if data is None:
            break
        else:
            # print(data)
            await database_handler.update_did_service(data)

        if last_created:
            logger.info(f"Data fetched until createdAt: {last_created}")

            await database_handler.update_last_created_did_date(last_created)

            # Update the after_value for the next request
            after_value = last_created
        else:
            logger.warning("Exiting.")
            break


async def main():
    await database_handler.create_connection_pool("write")

    logger.info("connedted to db.")

    await get_all_did_records()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
