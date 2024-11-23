# utils.py

import math
import httpx
import urllib.parse
from datetime import datetime
import asyncio
import database_handler
import requests
from config_helper import logger, limiter
import on_wire
import re
from cachetools import Cache
import json

# ======================================================================================================================
# ================================================ cache/global variables ==============================================
resolved_blocked_cache = Cache(maxsize=100)  # Every 12 hours
resolved_blockers_cache = Cache(maxsize=100)  # Every 12 hours

resolved_24_blocked_cache = Cache(maxsize=100)  # Every 12 hours
resolved_24blockers_cache = Cache(maxsize=100)  # Every 12 hours

blocked_avatar_ids_cache = Cache(maxsize=100)  # Every 12 hours
blocker_avatar_ids_cache = Cache(maxsize=100)  # Every 12 hours

blocked_24_avatar_ids_cache = Cache(maxsize=100)  # Every 12 hours
blocker_24_avatar_ids_cache = Cache(maxsize=100)  # Every 12 hours

number_of_total_blocks_cache = Cache(maxsize=50)  # Every 12 hours
number_of_unique_users_blocked_cache = Cache(maxsize=50)  # Every 12 hours
number_of_unique_users_blocking_cache = Cache(maxsize=50)  # Every 12 hours
number_block_1_cache = Cache(maxsize=50)  # Every 12 hours
number_blocking_2_and_100_cache = Cache(maxsize=50)  # Every 12 hours
number_blocking_101_and_1000_cache = Cache(maxsize=50)  # Every 12 hours
number_blocking_greater_than_1000_cache = Cache(maxsize=50)  # Every 12 hours
average_number_of_blocking_cache = Cache(maxsize=50)  # Every 12 hours
number_blocked_1_cache = Cache(maxsize=50)  # Every 12 hours
number_blocked_2_and_100_cache = Cache(maxsize=50)  # Every 12 hours
number_blocked_101_and_1000_cache = Cache(maxsize=50)  # Every 12 hours
number_blocked_greater_than_1000_cache = Cache(maxsize=50)  # Every 12 hours
average_number_of_blocked_cache = Cache(maxsize=50)  # Every 12 hours
block_stats_total_users_cache = Cache(maxsize=50)  # Every 4 hours
total_users_cache = Cache(maxsize=50)  # Every 4 hour
total_active_users_cache = Cache(maxsize=50)  # Every 4 hour
total_deleted_users_cache = Cache(maxsize=50)  # Every 4 hour

block_stats_status = asyncio.Event()
total_users_status = asyncio.Event()

block_stats_process_time = None
block_stats_last_update = None
block_stats_start_time = None
block_stats_as_of_time = None

total_users_process_time = None
total_users_last_update = None
total_users_start_time = None
total_users_as_of_time = None

BATCH_SIZE = 1000


# ======================================================================================================================
# ============================================= Features functions =====================================================
async def resolve_did(did: str, count: int, test: bool = False):
    if not test:
        resolved_did = await on_wire.resolve_did(did)
        if resolved_did[0] is not None:

            return {'did': did, 'Handle': resolved_did[0], 'block_count': count, 'ProfileURL': f'https://bsky.app/profile/{did}'}
    elif test:

        return {'did': did, 'Handle': did, 'block_count': count, 'ProfileURL': f'https://bsky.app/profile/{did}'}

    return None


async def resolve_top_block_lists():
    logger.info("Resolving top blocks lists.")
    blocked, blockers = await database_handler.get_top_blocks_list()

    # Extract DID values from blocked and blockers
    blocked_dids = [record['did'] for record in blocked]
    blocker_dids = [record['did'] for record in blockers]

    if await database_handler.local_db():
        blocked_avatar_dict = {did: did for did in blocked_dids}
        blocker_avatar_dict = {did: did for did in blocker_dids}
    else:
        # Create a dictionary to store the DID and avatar_id pairs
        blocked_avatar_dict = {did: await on_wire.get_avatar_id(did) for did in blocked_dids}
        blocker_avatar_dict = {did: await on_wire.get_avatar_id(did) for did in blocker_dids}

    if await database_handler.local_db():
        resolved_blocked = []
        for blocked_did, blocked_count in blocked:
            blocked_resolution = await resolve_did(blocked_did, blocked_count, True)
            resolved_blocked.append(blocked_resolution)

        resolved_blockers = []
        for blocker_did, blocker_count in blockers:
            blocker_resolution = await resolve_did(blocker_did, blocker_count, True)
            resolved_blockers.append(blocker_resolution)
    else:
        resolved_blocked = []
        for blocked_did, blocked_count in blocked:
            blocked_resolution = await resolve_did(blocked_did, blocked_count)
            resolved_blocked.append(blocked_resolution)

        resolved_blockers = []
        for blocker_did, blocker_count in blockers:
            blocker_resolution = await resolve_did(blocker_did, blocker_count)
            resolved_blockers.append(blocker_resolution)

    # Remove any None entries (failed resolutions)
    resolved_blocked = [entry for entry in resolved_blocked if entry is not None]
    resolved_blockers = [entry for entry in resolved_blockers if entry is not None]

    # Remove any none entries from avatar_id lists
    resolved_blocked_avatar_dict = {did: blocked_avatar_dict[did] for did in blocked_avatar_dict if did in [entry['did'] for entry in resolved_blocked]}
    resolved_blocker_avatar_dict = {did: blocker_avatar_dict[did] for did in blocker_avatar_dict if did in [entry['did'] for entry in resolved_blockers]}

    # Sort the lists based on block_count in descending order
    sorted_resolved_blocked = sorted(resolved_blocked, key=lambda x: (x["block_count"], x["did"]), reverse=True)
    sorted_resolved_blockers = sorted(resolved_blockers, key=lambda x: (x["block_count"], x["did"]), reverse=True)

    sorted_resolved_blocked_avatar_dict = {}
    sorted_resolved_blockers_avatar_dict = {}

    for entry in sorted_resolved_blocked:
        did = entry['did']
        if did in resolved_blocked_avatar_dict:
            sorted_resolved_blocked_avatar_dict[did] = resolved_blocked_avatar_dict[did]

    for entry in sorted_resolved_blockers:
        did = entry['did']
        if did in resolved_blocker_avatar_dict:
            sorted_resolved_blockers_avatar_dict[did] = resolved_blocker_avatar_dict[did]

    # Extract and return only the top 20 entries
    top_resolved_blocked = sorted_resolved_blocked[:20]
    top_resolved_blockers = sorted_resolved_blockers[:20]

    # Get the first 20 items from blocked_avatar_dict and blocker_avatar_dict
    blocked_avatar_ids = dict(list(sorted_resolved_blocked_avatar_dict.items())[:20])
    blocker_avatar_ids = dict(list(sorted_resolved_blockers_avatar_dict.items())[:20])

    # Cache the resolved lists
    resolved_blocked_cache["resolved_blocked"] = top_resolved_blocked
    resolved_blockers_cache["resolved_blockers"] = top_resolved_blockers

    blocked_avatar_ids_cache["blocked_aid"] = blocked_avatar_ids
    blocker_avatar_ids_cache["blocker_aid"] = blocker_avatar_ids

    return top_resolved_blocked, top_resolved_blockers, blocked_avatar_ids, blocker_avatar_ids


async def resolve_top24_block_lists():
    logger.info("Resolving top blocks lists.")
    blocked, blockers = await database_handler.get_24_hour_block_list()

    # Extract DID values from blocked and blockers
    blocked_dids = [record['did'] for record in blocked]
    blocker_dids = [record['did'] for record in blockers]

    if await database_handler.local_db():
        blocked_avatar_dict = {did: did for did in blocked_dids}
        blocker_avatar_dict = {did: did for did in blocker_dids}
    else:
        # Create a dictionary to store the DID and avatar_id pairs
        blocked_avatar_dict = {did: await on_wire.get_avatar_id(did) for did in blocked_dids}
        blocker_avatar_dict = {did: await on_wire.get_avatar_id(did) for did in blocker_dids}

    if await database_handler.local_db():
        resolved_blocked = []
        for blocked_did, blocked_count in blocked:
            blocked_resolution = await resolve_did(blocked_did, blocked_count, True)
            resolved_blocked.append(blocked_resolution)

        resolved_blockers = []
        for blocker_did, blocker_count in blockers:
            blocker_resolution = await resolve_did(blocker_did, blocker_count, True)
            resolved_blockers.append(blocker_resolution)
    else:
        resolved_blocked = []
        for blocked_did, blocked_count in blocked:
            blocked_resolution = await resolve_did(blocked_did, blocked_count)
            resolved_blocked.append(blocked_resolution)

        resolved_blockers = []
        for blocker_did, blocker_count in blockers:
            blocker_resolution = await resolve_did(blocker_did, blocker_count)
            resolved_blockers.append(blocker_resolution)

    # Remove any None entries (failed resolutions)
    resolved_blocked = [entry for entry in resolved_blocked if entry is not None]
    resolved_blockers = [entry for entry in resolved_blockers if entry is not None]

    # Remove any none entries from avatar_id lists
    resolved_blocked_avatar_dict = {did: blocked_avatar_dict[did] for did in blocked_avatar_dict if did in [entry['did'] for entry in resolved_blocked]}
    resolved_blocker_avatar_dict = {did: blocker_avatar_dict[did] for did in blocker_avatar_dict if did in [entry['did'] for entry in resolved_blockers]}

    # Sort the lists based on block_count in descending order
    sorted_resolved_blocked = sorted(resolved_blocked, key=lambda x: (x["block_count"], x["did"]), reverse=True)
    sorted_resolved_blockers = sorted(resolved_blockers, key=lambda x: (x["block_count"], x["did"]), reverse=True)

    sorted_resolved_blocked_avatar_dict = {}
    sorted_resolved_blockers_avatar_dict = {}

    for entry in sorted_resolved_blocked:
        did = entry['did']
        if did in resolved_blocked_avatar_dict:
            sorted_resolved_blocked_avatar_dict[did] = resolved_blocked_avatar_dict[did]

    for entry in sorted_resolved_blockers:
        did = entry['did']
        if did in resolved_blocker_avatar_dict:
            sorted_resolved_blockers_avatar_dict[did] = resolved_blocker_avatar_dict[did]

    # Extract and return only the top 20 entries
    top_resolved_blocked = sorted_resolved_blocked[:20]
    top_resolved_blockers = sorted_resolved_blockers[:20]

    # Get the first 20 items from blocked_avatar_dict and blocker_avatar_dict
    blocked_avatar_ids = dict(list(sorted_resolved_blocked_avatar_dict.items())[:20])
    blocker_avatar_ids = dict(list(sorted_resolved_blockers_avatar_dict.items())[:20])

    # Cache the resolved lists
    resolved_24_blocked_cache["resolved_blocked"] = top_resolved_blocked
    resolved_24blockers_cache["resolved_blockers"] = top_resolved_blockers

    blocked_24_avatar_ids_cache["blocked_aid"] = blocked_avatar_ids
    blocker_24_avatar_ids_cache["blocker_aid"] = blocker_avatar_ids

    return top_resolved_blocked, top_resolved_blockers, blocked_avatar_ids, blocker_avatar_ids


async def update_block_statistics():
    global block_stats_process_time
    global block_stats_last_update
    global block_stats_status
    global block_stats_start_time
    global block_stats_as_of_time

    logger.info("Updating block statsitics.")

    block_stats_start_time = datetime.now()

    block_stats_status.set()

    (number_of_total_blocks, number_of_unique_users_blocked, number_of_unique_users_blocking,
     number_block_1, number_blocking_2_and_100, number_blocking_101_and_1000, number_blocking_greater_than_1000, average_number_of_blocks,
     number_blocked_1, number_blocked_2_and_100, number_blocked_101_and_1000, number_blocked_greater_than_1000, average_number_of_blocked, total_users
     ) = await database_handler.get_block_stats()

    values = (number_of_total_blocks, number_of_unique_users_blocked, number_of_unique_users_blocking, number_block_1,
              number_blocking_2_and_100, number_blocking_101_and_1000, number_blocking_greater_than_1000, average_number_of_blocks,
              number_blocked_1, number_blocked_2_and_100, number_blocked_101_and_1000, number_blocked_greater_than_1000, average_number_of_blocked, total_users)

    for value in values:
        if value is None:
            logger.warning(f"{value=}")

    number_of_total_blocks_cache["total_blocks"] = number_of_total_blocks
    number_of_unique_users_blocked_cache["unique_blocked"] = number_of_unique_users_blocked
    number_of_unique_users_blocking_cache["unique_blocker"] = number_of_unique_users_blocking
    number_block_1_cache["block1"] = number_block_1
    number_blocking_2_and_100_cache["block2to100"] = number_blocking_2_and_100
    number_blocking_101_and_1000_cache["block101to1000"] = number_blocking_101_and_1000
    number_blocking_greater_than_1000_cache["blockmore1000"] = number_blocking_greater_than_1000
    average_number_of_blocking_cache["averageblocks"] = average_number_of_blocks
    number_blocked_1_cache["blocked1"] = number_blocked_1
    number_blocked_2_and_100_cache["blocked2to100"] = number_blocked_2_and_100
    number_blocked_101_and_1000_cache["blocked101to1000"] = number_blocked_101_and_1000
    number_blocked_greater_than_1000_cache["blockedmore1000"] = number_blocked_greater_than_1000
    average_number_of_blocked_cache["averageblocked"] = average_number_of_blocked
    block_stats_total_users_cache["total_users"] = total_users

    block_stats_status.clear()

    end_time = datetime.now()

    if block_stats_start_time is not None:
        block_stats_process_time = end_time - block_stats_start_time
    block_stats_last_update = end_time
    block_stats_start_time = None

    block_stats_as_of_time = datetime.now().isoformat()

    return (number_of_total_blocks, number_of_unique_users_blocked, number_of_unique_users_blocking,
            number_block_1, number_blocking_2_and_100, number_blocking_101_and_1000, number_blocking_greater_than_1000,
            average_number_of_blocks, number_blocked_1, number_blocked_2_and_100, number_blocked_101_and_1000,
            number_blocked_greater_than_1000, average_number_of_blocked, total_users)


async def update_total_users() -> (int, int, int):
    global total_users_status
    global total_users_process_time
    global total_users_last_update
    global total_users_start_time
    global total_users_as_of_time

    logger.info("Updating total users.")

    total_users_start_time = datetime.now()

    total_users_status.set()

    active_count = await database_handler.get_user_count(get_active=True)
    total_count = await database_handler.get_user_count(get_active=False)
    deleted_count = await database_handler.get_deleted_users_count()

    total_users_cache["total_users"] = total_count
    total_active_users_cache["total_active_users"] = active_count
    total_deleted_users_cache["total_deleted_users"] = deleted_count

    total_users_status.clear()

    end_time = datetime.now()

    if total_users_start_time is not None:
        total_users_process_time = end_time - total_users_start_time

    total_users_last_update = end_time

    total_users_start_time = None

    total_users_as_of_time = datetime.now().isoformat()

    logger.info("Total users update complete.")

    return active_count, total_count, deleted_count


async def process_user_block_list(ident, limit, offset):
    block_list = []

    blocked_users, count = await database_handler.get_blocklist(ident, limit=limit, offset=offset)

    if blocked_users:
        # Iterate over blocked_users and extract handle and status
        for user_did, block_date, handle, status in blocked_users:
            block_list.append({"handle": handle, "status": status, "blocked_date": block_date.isoformat()})

    if count > 0:
        pages = count / 100

        pages = math.ceil(pages)
    else:
        pages = 0

    if not blocked_users:
        total_blocked = 0
        logger.info(f"{ident} Hasn't blocked anyone.")

        return block_list, total_blocked, pages
    elif "no repo" in blocked_users:
        total_blocked = 0
        logger.info(f"{ident} doesn't exist.")

        return block_list, total_blocked, pages
    else:

        return block_list, count, pages


async def process_subscribe_blocks(ident, limit, offset):
    block_list = {}

    blocked_users, count = await database_handler.get_subscribe_blocks(ident, limit=limit, offset=offset)

    if count > 0:
        pages = count / 100

        pages = math.ceil(pages)
    else:
        pages = 0

    if not blocked_users:
        total_blocked = 0
        logger.info(f"{ident} Hasn't subscribed blocked any lists.")

        return block_list, total_blocked, pages
    elif "no repo" in blocked_users:
        total_blocked = 0
        logger.info(f"{ident} doesn't exist.")

        return block_list, total_blocked, pages
    else:

        return blocked_users, count, pages


async def process_subscribe_blocks_single(ident, list_of_lists, limit, offset):
    block_list = {}

    blocked_users, count = await database_handler.get_subscribe_blocks_single(ident, list_of_lists, limit=limit, offset=offset)

    if count > 0:
        pages = count / 100

        pages = math.ceil(pages)
    else:
        pages = 0

    if not blocked_users:
        total_blocked = 0
        logger.info(f"{ident} Hasn't subscribed blocked any lists.")

        return block_list, total_blocked, pages
    elif "no repo" in blocked_users:
        total_blocked = 0
        logger.info(f"{ident} doesn't exist.")

        return block_list, total_blocked, pages
    else:

        return blocked_users, count, pages


def is_did(identifier) -> bool:
    # Check if the identifier contains percent-encoded characters
    if '%' in identifier:
        logger.warning(f"Identifier contains percent-encoded characters: {identifier}")

        return False

    # Check if the identifier ends with ':' or '%'
    if identifier.endswith(':') or identifier.endswith('%'):
        logger.warning(f"Identifier ends with ':' or '%': {identifier}")

        return False

    if not len(identifier.encode('utf-8')) <= 2 * 1024:
        logger.warning(f"Identifier is too long: {identifier}")

        return False

    # Define the regex pattern for DID
    did_pattern = r'^did:(?:web|plc):[a-zA-Z0-9._:-]*[a-zA-Z0-9._-]$'

    # Match the identifier against the regex pattern
    return re.match(did_pattern, identifier) is not None


def is_handle(identifier) -> bool:
    handle_pattern = r'^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$'

    return re.match(handle_pattern, identifier) is not None


async def use_handle(identifier):
    if is_did(identifier):
        handle_identifier = await on_wire.resolve_did(identifier)

        if handle_identifier is None:  # If the DID is not found, return null // Being done because UI is sending handles in did format
            return_ident = "null"

            return return_ident
        else:
            return handle_identifier[0]
    else:

        return identifier


async def use_did(identifier):
    if is_handle(identifier):
        did_identifier = await on_wire.resolve_handle(identifier)

        return did_identifier
    else:

        return identifier


async def get_handle_history(identifier):
    base_url = "https://plc.directory/"
    collection = "log/audit"
    retry_count = 0
    max_retries = 5
    also_known_as_list = []

    if "did:web" in identifier:
        history = await database_handler.get_did_web_handle_history(identifier)

        return history
    while retry_count < max_retries:
        full_url = f"{base_url}{identifier}/{collection}"
        logger.debug(f"full url: {full_url}")

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)
        except httpx.ReadTimeout:
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

            for record in response_json:
                try:
                    also_known_as = record["operation"]["alsoKnownAs"]
                except KeyError:
                    also_known_as = record["operation"]["handle"]
                try:
                    endpoint = record["operation"]["services"]["atproto_pds"]["endpoint"]
                except KeyError:
                    endpoint = record["operation"]["service"]

                created_at_value = record["createdAt"]
                # created_at = datetime.fromisoformat(created_at_value)

                if isinstance(also_known_as, str):  # Handle single string case
                    if "at://" not in also_known_as[0]:
                        cleaned_also_known_as = [(also_known_as, created_at_value, endpoint)]
                    else:
                        cleaned_also_known_as = [(item.replace("at://", ""), created_at_value, endpoint) for item in also_known_as]
                else:
                    cleaned_also_known_as = [(item.replace("at://", ""), created_at_value, endpoint) for item in also_known_as if "at://" in item]

                also_known_as_list.extend(cleaned_also_known_as)

            # Sort the list by the date in created_at
            also_known_as_list.sort(key=lambda x: x[1])
            also_known_as_list.reverse()

            return also_known_as_list
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 30 seconds...")
            await asyncio.sleep(30)  # Retry after 60 seconds
        elif response.status_code == 400:
            try:
                error_message = response.json()["error"]
                message = response.json()["message"]
                if error_message == "InvalidRequest" and "Could not find repo" in message:
                    logger.warning("Could not find repo: " + str(identifier))

                    return ["no repo"]
            except KeyError:
                return None
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)

            continue
    if retry_count == max_retries:
        logger.warning("Could not get handle history for: " + identifier)

        return None


async def list_uri_to_url(uri):
    pattern = r'did:plc:[a-zA-Z0-9]+'
    pattern2 = r'/([^/]+)$'
    list_base_url = "https://bsky.app/profile"
    match = re.search(pattern, uri)
    match2 = re.search(pattern2, uri)
    did = match.group(0)
    commit_rev = match2.group(1)
    list_full_url = f"""{list_base_url}/{did}/lists/{commit_rev}"""

    return list_full_url
