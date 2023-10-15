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
# ================================================ cache/global variables ==============================================
resolved_blocked_cache = TTLCache(maxsize=100, ttl=3600)
resolved_blockers_cache = TTLCache(maxsize=100, ttl=3600)

resolved_24_blocked_cache = TTLCache(maxsize=100, ttl=3600)
resolved_24blockers_cache = TTLCache(maxsize=100, ttl=3600)

blocked_avatar_ids_cache = TTLCache(maxsize=100, ttl=3600)
blocker_avatar_ids_cache = TTLCache(maxsize=100, ttl=3600)

blocked_24_avatar_ids_cache = TTLCache(maxsize=100, ttl=3600)
blocker_24_avatar_ids_cache = TTLCache(maxsize=100, ttl=3600)

number_of_total_blocks_cache = TTLCache(maxsize=2, ttl=14400)
number_of_unique_users_blocked_cache = TTLCache(maxsize=2, ttl=14400)
number_of_unique_users_blocking_cache = TTLCache(maxsize=2, ttl=14400)
number_block_1_cache = TTLCache(maxsize=2, ttl=14400)
number_blocking_2_and_100_cache = TTLCache(maxsize=2, ttl=14400)
number_blocking_101_and_1000_cache = TTLCache(maxsize=2, ttl=14400)
number_blocking_greater_than_1000_cache = TTLCache(maxsize=2, ttl=14400)
average_number_of_blocking_cache = TTLCache(maxsize=2, ttl=14400)
number_blocked_1_cache = TTLCache(maxsize=2, ttl=14400)
number_blocked_2_and_100_cache = TTLCache(maxsize=2, ttl=14400)
number_blocked_101_and_1000_cache = TTLCache(maxsize=2, ttl=14400)
number_blocked_greater_than_1000_cache = TTLCache(maxsize=2, ttl=14400)
average_number_of_blocked_cache = TTLCache(maxsize=2, ttl=14400)
total_users_cache = TTLCache(maxsize=2, ttl=14400)

block_stats_status = asyncio.Event()

block_stats_process_time = None
block_stats_last_update = None
block_stats_start_time = None


# ======================================================================================================================
# ============================================= Features functions =====================================================
async def identifier_exists_in_db(identifier):
    async with database_handler.connection_pool.acquire() as connection:
        if is_did(identifier):
            result = await connection.fetchrow('SELECT did, status FROM users WHERE did = $1', identifier)

            if result:
                ident = result['did']
                status = result['status']
            else:
                ident = None
                status = None

            if ident and status is True:
                ident = True
                status = True
            elif ident and status is False:
                ident = True
                status = False
            else:
                ident = False
                status = False
        elif is_handle(identifier):
            result = await connection.fetchrow('SELECT handle, status FROM users WHERE handle = $1', identifier)

            if result:
                ident = result['handle']
                status = result['status']
            else:
                ident = None
                status = None

            if ident is not None and status is True:
                ident = True
                status = True
            elif ident is not None and status is False:
                ident = True
                status = False
            else:
                ident = False
                status = False
        else:
            ident = False
            status = False

        return ident, status


async def resolve_did(did, count, test=False):
    if not test:
        resolved_did = await on_wire.resolve_did(did)
        if resolved_did is not None:

            return {'did': did, 'Handle': resolved_did, 'block_count': count, 'ProfileURL': f'https://bsky.app/profile/{did}'}
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
        # Prepare tasks to resolve DIDs concurrently
        blocked_tasks = [resolve_did(did, count, True) for did, count in blocked]
        blocker_tasks = [resolve_did(did, count, True) for did, count in blockers]
    else:
        # Prepare tasks to resolve DIDs concurrently
        blocked_tasks = [resolve_did(did, count) for did, count in blocked]
        blocker_tasks = [resolve_did(did, count) for did, count in blockers]

    # Run the resolution tasks concurrently
    resolved_blocked = await asyncio.gather(*blocked_tasks)
    resolved_blockers = await asyncio.gather(*blocker_tasks)

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
        # Prepare tasks to resolve DIDs concurrently
        blocked_tasks = [resolve_did(did, count, True) for did, count in blocked]
        blocker_tasks = [resolve_did(did, count, True) for did, count in blockers]
    else:
        # Prepare tasks to resolve DIDs concurrently
        blocked_tasks = [resolve_did(did, count) for did, count in blocked]
        blocker_tasks = [resolve_did(did, count) for did, count in blockers]

    # Run the resolution tasks concurrently
    resolved_blocked = await asyncio.gather(*blocked_tasks)
    resolved_blockers = await asyncio.gather(*blocker_tasks)

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
    total_users_cache["total_users"] = total_users

    block_stats_status.clear()

    end_time = datetime.now()

    block_stats_process_time = end_time - block_stats_start_time
    block_stats_last_update = end_time
    block_stats_start_time = None

    return (number_of_total_blocks, number_of_unique_users_blocked, number_of_unique_users_blocking,
            number_block_1, number_blocking_2_and_100, number_blocking_101_and_1000, number_blocking_greater_than_1000,
            average_number_of_blocks, number_blocked_1, number_blocked_2_and_100, number_blocked_101_and_1000,
            number_blocked_greater_than_1000, average_number_of_blocked, total_users)


async def get_all_users():
    base_url = "https://bsky.social/xrpc/"
    limit = 1000
    cursor = None
    records = set()

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
            await asyncio.sleep(60)  # Retry after 60 seconds
        except Exception as e:
            logger.warning("Error during API call: %s", str(e))
            await asyncio.sleep(60)  # Retry after 60 seconds

        if response.status_code == 200:
            response_json = response.json()
            repos = response_json.get("repos", [])
            for repo in repos:
                records.add(repo["did"])

            cursor = response_json.get("cursor")
            if not cursor:
                break
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            await asyncio.sleep(60)  # Retry after 60 seconds
        else:
            logger.warning("Response status code: " + str(response.status_code))
            pass

    return records


async def get_user_handle(did):
    async with database_handler.connection_pool.acquire() as connection:
        handle = await connection.fetchval('SELECT handle FROM users WHERE did = $1', did)

    return handle


async def get_user_did(handle):
    async with database_handler.connection_pool.acquire() as connection:
        did = await connection.fetchval('SELECT did FROM users WHERE handle = $1', handle)

    return did


async def get_user_count(get_active=True):
    async with database_handler.connection_pool.acquire() as connection:
        if get_active:
            count = await connection.fetchval('SELECT COUNT(*) FROM users WHERE status is TRUE')
        else:
            count = await connection.fetchval('SELECT COUNT(*) FROM users')
        return count


async def get_single_user_blocks(ident, limit=100, offset=0):
    try:
        # Execute the SQL query to get all the user_dids that have the specified did/ident in their blocklist
        async with database_handler.connection_pool.acquire() as connection:
            result = await connection.fetch('SELECT b.user_did, b.block_date, u.handle, u.status FROM blocklists AS b JOIN users as u ON b.user_did = u.did WHERE b.blocked_did = $1 ORDER BY block_date DESC LIMIT $2 OFFSET $3', ident, limit, offset)
            count = await connection.fetchval('SELECT COUNT(user_did) FROM blocklists WHERE blocked_did = $1', ident)

            block_list = []

            if result:
                # Iterate over blocked_users and extract handle and status
                for user_did, block_date, handle, status in result:
                    block_list.append({"handle": handle, "status": status, "blocked_date": block_date})

                return block_list, count
            else:
                total_blocked = 0
                if is_did(ident):
                    ident = await use_handle(ident)
                handles = [f"{ident} hasn't blocked anyone."]
                timestamp = datetime.now().date()
                status = False
                block_list.append({"handle": handles, "status": status, "blocked_date": timestamp})

                return block_list, total_blocked
    except Exception as e:
        logger.error(f"Error fetching blocklists for {ident}: {e}")
        handles = "there was an error"
        timestamp = datetime.now().date()
        count = 0
        status = False
        block_list.append({"handle": handles, "status": status, "blocked_date": timestamp})

        return block_list, count


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
                    created_dates.append(created_date)

            cursor = response_json.get("cursor")
            if not cursor:
                break
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            await asyncio.sleep(60)  # Retry after 60 seconds
        elif response.status_code == 400:
            try:
                error_message = response.json()["error"]
                message = response.json()["message"]
                if error_message == "InvalidRequest" and "Could not find repo" in message:
                    logger.warning("Could not find repo: " + str(ident))
                    return ["no repo"], []
            except KeyError:
                pass
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


async def process_user_block_list(ident, limit, offset):
    blocked_users, count = await database_handler.get_blocklist(ident, limit=limit, offset=offset)

    block_list = []

    if not blocked_users:
        total_blocked = 0
        if is_did(ident):
            ident = await use_handle(ident)
        handles = [f"{ident} hasn't blocked anyone."]
        timestamp = datetime.now().date()
        status = False
        block_list.append({"handle": handles, "status": status, "blocked_date": timestamp})
        logger.info(f"{ident} Hasn't blocked anyone.")

        return block_list, total_blocked
    elif "no repo" in blocked_users:
        total_blocked = 0
        handles = [f"Couldn't find {ident}, there may be a typo."]
        timestamp = datetime.now().date()
        status = False
        block_list.append({"handle": handles, "status": status, "blocked_date": timestamp})
        logger.info(f"{ident} doesn't exist.")

        return block_list, total_blocked
    else:
        # blocked_users now contains blocked_did, handle, and status
        total_blocked = count

        # Iterate over blocked_users and extract handle and status
        for user_did, blocked_date, handle, status in blocked_users:
            block_list.append({"handle": handle, "status": status, "blocked_date": blocked_date})

        return block_list, total_blocked


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


async def get_handle_history(identifier):
    base_url = "https://plc.directory/"
    collection = "log"
    retry_count = 0
    max_retries = 5
    also_known_as_list = []

    while retry_count < max_retries:
        full_url = f"{base_url}{identifier}/{collection}"
        logger.debug(f"full url: {full_url}")

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
            retry_count += 1
            await asyncio.sleep(5)
            continue

        if response.status_code == 200:
            response_json = response.json()

            for record in response_json:
                if "alsoKnownAs" in record:
                    also_known_as = record["alsoKnownAs"]
                    cleaned_also_known_as = [item.replace("at://", "") for item in also_known_as]
                    also_known_as_list.extend(cleaned_also_known_as)

            # Remove adjacent duplicates while preserving non-adjacent duplicates
            cleaned_also_known_as_list = []
            prev_item = None
            for item in also_known_as_list:
                if item != prev_item:
                    cleaned_also_known_as_list.append(item)
                prev_item = item

            cleaned_also_known_as_list.reverse()

            return cleaned_also_known_as_list
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
                pass
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)

            continue
    if retry_count == max_retries:
        logger.warning("Could not get handle history for: " + identifier)

        return None
