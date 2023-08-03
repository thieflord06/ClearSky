# utils.py

import configparser
import asyncpg
import httpx
import urllib.parse
from datetime import datetime
import asyncio
import database_handler
import requests
import logging
import on_wire


# ======================================================================================================================
# ============================================= Features functions =====================================================
def get_database_config():
    pg_user = config.get("database", "pg_user")
    pg_password = config.get("database", "pg_password")
    pg_host = config.get("database", "pg_host")
    pg_database = config.get("database", "pg_database")

    return {
        "user": pg_user,
        "password": pg_password,
        "host": pg_host,
        "database": pg_database
    }


def get_all_users():
    base_url = "https://bsky.social/xrpc/"
    limit = 1000
    cursor = None
    records = []

    logger.info("Getting all dids.")

    while True:
        url = urllib.parse.urljoin(base_url, "com.atproto.sync.listRepos")
        params = {
            "limit": limit,
        }
        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)
        response = requests.get(full_url)

        if response.status_code == 200:
            response_json = response.json()
            repos = response_json.get("repos", [])
            for repo in repos:
                records.append((repo["did"],))

            cursor = response_json.get("cursor")
            if not cursor:
                break
        else:
            logger.warning("Response status code: " + str(response.status_code))
            pass
    return records


async def get_user_handle(did):
    async with asyncpg.create_pool(
        user=pg_user,
        password=pg_password,
        host=pg_host,
        database=pg_database
    ) as new_connection_pool:
        async with new_connection_pool.acquire() as connection:
            handle = await connection.fetchval('SELECT handle FROM users WHERE did = $1', did)
        return handle


async def get_user_count():
    async with asyncpg.create_pool(
            user=pg_user,
            password=pg_password,
            host=pg_host,
            database=pg_database
    ) as new_connection_pool:
        async with new_connection_pool.acquire() as connection:
            count = await connection.fetchval('SELECT COUNT(*) FROM users')
            return count


async def get_single_user_blocks(ident):
    # Execute the SQL query to get all the user_dids that have the specified did in their blocklist
    async with database_handler.connection_pool.acquire() as connection:
        result = await connection.fetch('SELECT user_did, block_date FROM blocklists WHERE blocked_did = $1', ident)
        logger.info("here")
        if result:
            # Extract the user_dids from the query result
            user_dids = [item[0] for item in result]
            block_dates = [item[1] for item in result]
            count = len(user_dids)

            resolved_handles = []

            # Fetch handles concurrently using asyncio.gather
            resolved_handles = await asyncio.gather(*[get_user_handle(user_did) for user_did in user_dids])

            return resolved_handles, block_dates, count
        else:
            # ident = resolve_handle(ident)
            no_blocks = ident + ": has not been blocked by anyone."
            date = datetime.now().date()
            connection.release()
            return no_blocks, date, 0


async def get_user_block_list(ident):
    base_url = "https://bsky.social/xrpc/"
    collection = "app.bsky.graph.block"
    limit = 100
    blocked_users = []
    created_dates = []
    cursor = None
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
        params = {
            "repo": ident,
            "limit": limit,
            "collection": collection,
            # "cursor": cursor
        }

        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)
                response.raise_for_status()  # Raise an exception for any HTTP error status codes
        except httpx.ReadTimeout as e:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(5)
            continue
        except httpx.RequestError as e:
            logger.warning("Error during API call: %s", e)
            retry_count += 1
            await asyncio.sleep(5)
            continue

        if response.status_code == 200:
            response_json = response.json()
            records = response_json.get("records", [])

            for record in records:
                value = record.get("value", {})
                subject = value.get("subject")
                created_at_value = value.get("createdAt")
                if subject:
                    blocked_users.append(subject)
                if created_at_value:
                    try:
                        created_date = datetime.strptime(created_at_value, "%Y-%m-%dT%H:%M:%S.%fZ").date()
                    except ValueError:
                        created_date = None
                    created_dates.append(created_date)

            cursor = response_json.get("cursor")
            if not cursor:
                break
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)
            continue

    if retry_count == max_retries:
        logger.warning("Could not get block list for: " + ident)
        return ["error"], [str(datetime.now().date())]
    if not blocked_users and retry_count != max_retries:
        return [], []

    return blocked_users, created_dates


async def fetch_handles_batch(batch_dids):
    tasks = [on_wire.resolve_did(did[0].strip()) for did in batch_dids]
    resolved_handles = await asyncio.gather(*tasks)
    handles = [(did[0], handle) for did, handle in zip(batch_dids, resolved_handles) if handle is not None]
    return handles
    # tasks = [resolve_did(did.strip('\'\",')) for did in batch_dids]
    # return await asyncio.gather(*tasks)


# Read log directory from .ini and if directory structure doesn't, exist create it.
config = configparser.ConfigParser()
config.read("config.ini")

# Initialize the logger
logger = logging.getLogger(__name__)

# Get the database configuration
database_config = get_database_config()

# Now you can access the configuration values using dictionary keys
pg_user = database_config["user"]
pg_password = database_config["password"]
pg_host = database_config["host"]
pg_database = database_config["database"]
