# utils.py

import httpx
import urllib.parse
from datetime import datetime, time
import asyncio
import database_handler
import requests
from config_helper import logger, limiter
import on_wire
import re
from cachetools import TTLCache
import json
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

sleep_time = 15


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

    if block_stats_start_time is not None:
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
            response = None
            logger.warning("Error during API call: %s", e)
            await asyncio.sleep(60)  # Retry after 60 seconds
        except Exception as e:
            response = None
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
            await asyncio.sleep(60)  # Retry after 60 seconds

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


async def get_deleted_users_count():
    async with database_handler.connection_pool.acquire() as connection:
        count = await connection.fetchval('SELECT COUNT(*) FROM USERS WHERE status is FALSE')

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
        block_list = []
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
    blocked_data = []
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
            async with limiter:
                async with httpx.AsyncClient() as client:
                    response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))
                if ratelimit_remaining < 100:
                    logger.warning(f"Blocklist Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
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
                timestamp = datetime.fromisoformat(created_at_value)
                uri = record.get("uri")
                cid = record.get("cid")

                logger.debug(f"subject: {subject} created: {timestamp} uri: {uri} cid: {cid}")

                if subject and timestamp and uri and cid:
                    blocked_data.append((subject, timestamp, uri, cid))
                else:
                    if not timestamp:
                        timestamp = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.warning("missing timestamp")
                    elif not uri:
                        uri = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.warning("Missing uri")
                    elif not cid:
                        cid = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.warning("missing cid")
                    else:
                        return None

            cursor = response_json.get("cursor")

            if not cursor:
                break
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            await asyncio.sleep(60)  # Retry after 60 seconds
        elif response.status_code == 400:
            retry_count += 1
            try:
                error_message = response.json()["error"]
                message = response.json()["message"]
                if error_message == "InvalidRequest" and "Could not find repo" in message:
                    logger.warning("Could not find repo: " + str(ident))

                    return None
            except KeyError:
                return None
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)
            continue

        logger.debug(blocked_data)

        return blocked_data

    if retry_count == max_retries:
        logger.warning("Could not get block list for: " + ident)

        return None
    if not blocked_data and retry_count != max_retries:

        return None


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
    handles = []

    if not ad_hoc:
        for did in batch_dids:
            handle = await on_wire.resolve_did(did[0].strip())
            if handle is not None:
                handles.append((did[0].strip(), handle))
    else:
        for did in batch_dids:
            handle = await on_wire.resolve_did(did.strip())
            if handle is not None:
                handles.append((did.strip(), handle))

    # if not ad_hoc:
    #     tasks = [on_wire.resolve_did(did[0].strip()) for did in batch_dids]
    # else:
    #     tasks = [on_wire.resolve_did(did.strip()) for did in batch_dids]
    # resolved_handles = await asyncio.gather(*tasks)
    #
    # if not ad_hoc:
    #     handles = [(did[0], handle) for did, handle in zip(batch_dids, resolved_handles) if handle is not None]
    # else:
    #     handles = [(did, handle) for did, handle in zip(batch_dids, resolved_handles) if handle is not None]

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
    collection = "log/audit"
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
                also_known_as = record["operation"]["alsoKnownAs"]
                created_at_value = record["createdAt"]
                created_at = datetime.fromisoformat(created_at_value)
                cleaned_also_known_as = [(item.replace("at://", ""), created_at) for item in also_known_as]
                also_known_as_list.extend(cleaned_also_known_as)

            # Remove adjacent duplicates while preserving non-adjacent duplicates
            # cleaned_also_known_as_list = []
            # prev_item = None
            # for item in also_known_as_list:
            #     if item != prev_item:
            #         cleaned_also_known_as_list.append(item)
            #     prev_item = item

            also_known_as_list.reverse()
            # cleaned_also_known_as_list.reverse()

            return also_known_as_list
            # return cleaned_also_known_as_list
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


async def get_mutelists(ident):
    base_url = "https://bsky.social/xrpc/"
    mute_lists_collection = "app.bsky.graph.list"
    limit = 100
    mutelists_data = []
    cursor = None
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
        params = {
            "repo": ident,
            "limit": limit,
            "collection": mute_lists_collection,
        }

        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)

        try:
            async with limiter:
                async with httpx.AsyncClient() as client:
                    response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))
                if ratelimit_remaining < 100:
                    logger.warning(f"Mutelist Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)

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
            list_records = response_json.get("records", [])

            for record in list_records:
                cid = record.get("cid", {})  # List ID
                value = record.get("value", {})
                subject = value.get("name")
                created_at_value = value.get("createdAt")
                timestamp = datetime.fromisoformat(created_at_value)
                description = value.get("description")
                uri = record.get("uri")

                parts = uri.split('/')
                list_id = parts[-1]

                list_base_url = "https://bsky.app/profile"
                list_full_url = f"""{list_base_url}/{ident}/lists/{list_id}"""

                # Create a dictionary to store this record's data
                record_data = {
                    "url": list_full_url,
                    "uri": uri,
                    "did": ident,
                    "cid": cid,
                    "name": subject,
                    "created_at": timestamp,
                    "description": description
                }

                # Add this record's data to the list
                mutelists_data.append(record_data)

            cursor = response_json.get("cursor")
            if not cursor:
                break
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            await asyncio.sleep(60)  # Retry after 60 seconds
        elif response.status_code == 400:
            retry_count += 1
            try:
                error_message = response.json()["error"]
                message = response.json()["message"]
                if error_message == "InvalidRequest" and "Could not find repo" in message:
                    logger.warning("Could not find repo: " + str(ident))

                    return None
            except KeyError:
                return None
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)
            continue

    if retry_count == max_retries:
        logger.warning("Could not get mute lists for: " + ident)

        return None
    if not mutelists_data and retry_count >= max_retries:

        return None

    logger.debug(mutelists_data)
    return mutelists_data


async def get_mutelist_users(ident):
    base_url = "https://bsky.social/xrpc/"
    mute_users_collection = "app.bsky.graph.listitem"
    limit = 100
    mutelists_users_data = []
    cursor = None
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
        params = {
            "repo": ident,
            "limit": limit,
            "collection": mute_users_collection,
        }

        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)

        try:
            async with limiter:
                async with httpx.AsyncClient() as client:
                    response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))

                if ratelimit_remaining < 100:
                    logger.warning(f"Mutelist users Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
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
                cid = record.get("cid", {})  # List ID
                value = record.get("value", {})
                subject = value.get("subject")
                created_at_value = value.get("createdAt")
                timestamp = datetime.fromisoformat(created_at_value)
                listitem_uri = record.get("uri")
                list_uri = value.get("list")

                # Create a dictionary to store this record's data
                user_record_data = {
                    "list_uri": list_uri,
                    "cid": cid,
                    "subject": subject,
                    "author": ident,
                    "created_at": timestamp,
                    "listitem_uri": listitem_uri
                }

                # Add this record's data to the list
                mutelists_users_data.append(user_record_data)
            cursor = response_json.get("cursor")
            if not cursor:
                break
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            await asyncio.sleep(60)  # Retry after 60 seconds
        elif response.status_code == 400:
            retry_count += 1
            try:
                error_message = response.json()["error"]
                message = response.json()["message"]
                if error_message == "InvalidRequest" and "Could not find repo" in message:
                    logger.warning("Could not find repo: " + str(ident))
                    return None
            except KeyError:
                pass
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)
            continue

    if retry_count == max_retries:
        logger.warning("Could not get mute list for: " + ident)
        return None
    if not mutelists_users_data and retry_count != max_retries:

        return None

    logger.debug(mutelists_users_data)
    return mutelists_users_data


async def get_subscribelists(ident):
    base_url = "https://bsky.social/xrpc/"
    subscribe_lists_collection = "app.bsky.graph.listblock"
    limit = 100
    subscribe_data = []
    cursor = None
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
        params = {
            "repo": ident,
            "limit": limit,
            "collection": subscribe_lists_collection,
        }

        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)

        try:
            async with limiter:
                async with httpx.AsyncClient() as client:
                    response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))

                if ratelimit_remaining < 100:
                    logger.warning(f"Subscribe Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
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
            list_records = response_json.get("records", [])

            for record in list_records:
                uri = record.get("uri")
                cid = record.get("cid", {})  # List ID
                value = record.get("value", {})
                list_uri = value.get("subject")
                record_type = value.get("$type")
                created_at_value = value.get("createdAt")
                timestamp = datetime.fromisoformat(created_at_value)

                # Create a dictionary to store this record's data
                record_data = {
                    "did": ident,
                    "uri": uri,
                    "list_uri": list_uri,
                    "cid": cid,
                    "date_added": timestamp,
                    "record_type": record_type
                }

                # Add this record's data to the list
                subscribe_data.append(record_data)

            cursor = response_json.get("cursor")
            if not cursor:
                break
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            await asyncio.sleep(60)  # Retry after 60 seconds
        elif response.status_code == 400:
            retry_count += 1
            try:
                error_message = response.json()["error"]
                message = response.json()["message"]
                if error_message == "InvalidRequest" and "Could not find repo" in message:
                    logger.warning("Could not find repo: " + str(ident))

                    return None
            except KeyError:
                return None
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)
            continue

    if retry_count == max_retries:
        logger.warning("Could not get mute lists for: " + ident)

        return None
    if not subscribe_data and retry_count >= max_retries:

        return None

    logger.debug(subscribe_data)

    return subscribe_data


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
                    # logger.info(record)
                    in_endpoint = in_record.get("services")
                    in_services = in_endpoint.get("atproto_pds")
                    preprocessed_handle = in_record.get("alsoKnownAs")
                    try:
                        handle = [item.replace("at://", "") for item in preprocessed_handle]
                        handle = handle[0]
                    except Exception as e:
                        logger.warning(f"There was an issue retrieving the handle: {record}")
                        logger.error(f"Error: {e}")
                        handle = None
                    try:
                        service = in_services.get("endpoint")
                    except AttributeError:
                        logger.warning(f"There was an issue retrieving the pds: {record}")
                        service = None

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


async def get_all_did_records(last_cursor=None):
    url = 'https://plc.directory/export'
    after_value = last_cursor

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
