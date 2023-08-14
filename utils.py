# utils.py

import httpx
import urllib.parse
from datetime import datetime
import asyncio
import database_handler
import requests
from config_helper import logger
import on_wire
import re
from cachetools import TTLCache
# ======================================================================================================================
# ============================================= Features functions =====================================================
resolved_blocked = []
resolved_blockers = []

resolved_blocked_cache = TTLCache(maxsize=100, ttl=60)
resolved_blockers_cache = TTLCache(maxsize=100, ttl=60)


async def resolve_top_block_lists():
    blocked, blockers = await database_handler.get_top_blocks_list()
    logger.info("Resolving top blocks lists.")

    resolved_blocked = []
    resolved_blockers = []

    # Resolve each DID and create a list of dictionaries
    for did, count in blocked:
        blocked_resolved_did = await on_wire.resolve_did(did)  # Replace with your actual resolving function
        if blocked_resolved_did is not None:
            resolved_blocked.append({'Handle': blocked_resolved_did, 'block_count': str(count), 'ProfileURL': f'https://bsky.app/profile/{did}'})

    for did, count in blockers:
        blocker_resolved_did = await on_wire.resolve_did(did)  # Replace with your actual resolving function
        if blocker_resolved_did is not None:
            resolved_blockers.append({'Handle': blocker_resolved_did, 'block_count': str(count), 'ProfileURL': f'https://bsky.app/profile/{did}'})

    # Cache the resolved lists
    resolved_blocked_cache["resolved_blocked"] = resolved_blocked
    resolved_blockers_cache["resolved_blockers"] = resolved_blockers


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
        try:
            response = requests.get(full_url)
        except httpx.RequestError as e:
            logger.warning("Error during API call: %s", e)
            if "429 Too Many Requests" in str(e):
                logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                asyncio.sleep(60)  # Retry after 60 seconds
        except Exception:
            asyncio.sleep(60)  # Retry after 60 seconds

        if response.status_code == 200:
            response_json = response.json()
            repos = response_json.get("repos", [])
            for repo in repos:
                records.append((repo["did"],))

            cursor = response_json.get("cursor")
            if not cursor:
                break
        elif "429 Too Many Requests" in str(e):
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            asyncio.sleep(60)  # Retry after 60 seconds
        else:
            logger.warning("Response status code: " + str(response.status_code))
            pass
    return records


async def get_user_handle(did):
    async with database_handler.connection_pool.acquire() as connection:
        handle = await connection.fetchval('SELECT handle FROM users WHERE did = $1', did)
    return handle


async def get_user_count():
    async with database_handler.connection_pool.acquire() as connection:
        count = await connection.fetchval('SELECT COUNT(*) FROM users')
        return count


async def get_single_user_blocks(ident):
    try:
        # Execute the SQL query to get all the user_dids that have the specified did/ident in their blocklist
        async with database_handler.connection_pool.acquire() as connection:
            result = await connection.fetch('SELECT user_did, block_date FROM blocklists WHERE blocked_did = $1 ORDER BY block_date DESC', ident)
            if result:
                # Extract the user_dids from the query result
                user_dids = [item[0] for item in result]
                block_dates = [item[1] for item in result]
                count = len(user_dids)

                # Fetch handles concurrently using asyncio.gather
                resolved_handles = await asyncio.gather(*[get_user_handle(user_did) for user_did in user_dids])

                return resolved_handles, block_dates, count
            else:
                # ident = resolve_handle(ident)
                no_blocks = ident + ": has not been blocked by anyone."
                date = datetime.now().date()
                return no_blocks, date, 0
    except Exception as e:
        logger.error(f"Error fetching blocklists for {ident}: {e}")
        blocks = "there was an error"
        date = datetime.now().date()
        count = 0
        return blocks, date, count


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
        }

        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)
        except httpx.ReadTimeout:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(10)
            continue
        except httpx.RequestError as e:
            logger.warning("Error during API call: %s", e)
            if "429 Too Many Requests" in str(e):
                logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                await asyncio.sleep(60)  # Retry after 60 seconds
            elif "400 Bad Request" in str(e):
                logger.warning("Did doesn't exist: " + str(ident))
                continue
            retry_count += 1
            logger.info("sleeping 5")
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
                else:
                    logger.info(f"didn't update no blocks: {ident}")
                    continue
                if created_at_value:
                    try:  # Have to check for different time formats in blocklists :/
                        if '.' in created_at_value and 'Z' in created_at_value:
                            # If the value contains fractional seconds and 'Z' (UTC time)
                            created_date = datetime.strptime(created_at_value, "%Y-%m-%dT%H:%M:%S.%fZ").date()
                        elif '.' in created_at_value:
                            # If the value contains fractional seconds (but no 'Z' indicating time zone)
                            created_date = datetime.strptime(created_at_value, "%Y-%m-%dT%H:%M:%S.%f").date()
                        elif 'Z' in created_at_value:
                            # If the value has 'Z' indicating UTC time (but no fractional seconds)
                            created_date = datetime.strptime(created_at_value, "%Y-%m-%dT%H:%M:%SZ").date()
                        else:
                            # If the value has no fractional seconds and no 'Z' indicating time zone
                            created_date = datetime.strptime(created_at_value, "%Y-%m-%dT%H:%M:%S").date()
                    except ValueError as ve:
                        logger.warning("No date in blocklist for: " + str(ident) + " | " + str(full_url))
                        logger.error("error: " + str(ve))
                        continue
                        # created_date = None
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
        pass
    if not blocked_users and retry_count != max_retries:
        return [], []

    return blocked_users, created_dates


async def fetch_handles_batch(batch_dids, ad_hoc=False):
    if not ad_hoc:
        tasks = [on_wire.resolve_did(did[0].strip()) for did in batch_dids]
    else:
        tasks = [on_wire.resolve_did(did.strip()) for did in batch_dids]
    resolved_handles = await asyncio.gather(*tasks)

    if not ad_hoc:
        handles = [(did[0], handle) for did, handle in zip(batch_dids, resolved_handles) if handle is not None]
    else:
        handles = [(did, handle) for did, handle in zip(batch_dids, resolved_handles) if handle is not None]
    return handles


def is_did(identifier):
    did_pattern = r'^did:[a-z]+:[a-zA-Z0-9._:%-]*[a-zA-Z0-9._-]$'
    return re.match(did_pattern, identifier) is not None


def is_handle(identifier):
    handle_pattern = r'^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$'
    return re.match(handle_pattern, identifier) is not None


async def use_handle(identifier):
    if is_did(identifier):
        handle_identifier = await on_wire.resolve_did(identifier)

        return handle_identifier
    else:
        return identifier


async def use_did(identifier):
    if is_handle(identifier):
        did_identifier = await on_wire.resolve_handle(identifier)

        return did_identifier
    else:
        return identifier
