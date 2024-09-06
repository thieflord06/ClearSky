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
resolved_blocked_cache = Cache(maxsize=100)  # Every 24 hours
resolved_blockers_cache = Cache(maxsize=100)  # Every 24 hours

resolved_24_blocked_cache = Cache(maxsize=100)  # Every 24 hours
resolved_24blockers_cache = Cache(maxsize=100)  # Every 12 hours

blocked_avatar_ids_cache = Cache(maxsize=100)  # Every 24 hours
blocker_avatar_ids_cache = Cache(maxsize=100)  # Every 24 hours

blocked_24_avatar_ids_cache = Cache(maxsize=100)  # Every 24 hours
blocker_24_avatar_ids_cache = Cache(maxsize=100)  # Every 24 hours

number_of_total_blocks_cache = Cache(maxsize=50)  # Every 24 hours
number_of_unique_users_blocked_cache = Cache(maxsize=50)  # Every 24 hours
number_of_unique_users_blocking_cache = Cache(maxsize=50)  # Every 24 hours
number_block_1_cache = Cache(maxsize=50)  # Every 24 hours
number_blocking_2_and_100_cache = Cache(maxsize=50)  # Every 24 hours
number_blocking_101_and_1000_cache = Cache(maxsize=50)  # Every 24 hours
number_blocking_greater_than_1000_cache = Cache(maxsize=50)  # Every 24 hours
average_number_of_blocking_cache = Cache(maxsize=50)  # Every 24 hours
number_blocked_1_cache = Cache(maxsize=50)  # Every 24 hours
number_blocked_2_and_100_cache = Cache(maxsize=50)  # Every 24 hours
number_blocked_101_and_1000_cache = Cache(maxsize=50)  # Every 24 hours
number_blocked_greater_than_1000_cache = Cache(maxsize=50)  # Every 24 hours
average_number_of_blocked_cache = Cache(maxsize=50)  # Every 24 hours
block_stats_total_users_cache = Cache(maxsize=50)  # Every 24 hours
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


async def update_total_users():
    global total_users_status
    global total_users_process_time
    global total_users_last_update
    global total_users_start_time

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

    logger.info("Total users update complete.")

    return active_count, total_count, deleted_count


async def get_all_users(pds) -> set:
    base_url = f"{pds}/xrpc/"
    limit = 1000
    cursor = None
    records = set()
    count = 0
    full_url = None

    logger.info(f"Getting all dids from {pds}")

    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
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
            response = requests.get(full_url, allow_redirects=True)
            try:
                num_redirects = len(response.history)
                if num_redirects > 0:
                    logger.info(f"Number of redirects: {num_redirects}")
            except Exception:
                pass
        except httpx.RequestError as e:
            response = None
            logger.warning("Error during API call: %s", e)
        except Exception as e:
            response = None
            logger.warning("Error during API call: %s", str(e))
        try:
            if response.status_code == 200:
                if response.content:
                    try:
                        response_json = response.json()
                    except Exception as e:
                        logger.error(f"Error fetching users for: {pds} {full_url}")
                        logger.error(f"Error: {e}")
                        break
                    repos = response_json.get("repos", [])
                    for repo in repos:
                        records.add(repo["did"])

                    cursor = response_json.get("cursor")
                    if not cursor:
                        break
                else:
                    logger.warning(f"Received empty response from the server: {full_url}")
                    break
            elif response.status_code == 429:
                if count < 10:
                    count += 1
                    logger.warning(f"Received 429 Too Many Requests. Retrying after 60 seconds...{full_url}")
                    await asyncio.sleep(60)  # Retry after 60 seconds
                else:
                    logger.error(f"Received 429. Max retries reached. {full_url}")
                    records = None
                    break
            elif response.status_code == 522:
                logger.warning(f"Received 522 Origin Connection Time-out, skipping. {full_url}")
                break
            elif response.status_code == 530:
                logger.warning(f"Received 530 Origin DNS Error, skipping. {full_url}")
                break
            elif response.status_code == 504:
                logger.warning(f"Received 504 Gateway Timeout, skipping. {full_url}")
                break
            elif response.status_code == 104:
                logger.warning(f"Received 104 Connection Reset by Peer, skipping. {full_url}")
                break
            elif response.status_code == 403:
                logger.warning(f"Received 403 Forbidden, skipping. {full_url}")
                break
            elif response.status_code == 502:
                logger.warning(f"Received 502 Bad Gateway, skipping. {full_url}")
                break
            else:
                logger.warning("Response status code: " + str(response.status_code) + f" {full_url}")
                break
        except httpx.RequestError as e:
            logger.warning(f"Error during API call: {e} | {full_url}")
            await asyncio.sleep(5)
            retry_count += 1
            continue
        except TimeoutError:
            logger.warning(f"Timeout error: {full_url}")
            if cursor:
                await asyncio.sleep(5)
                retry_count += 1
                continue
            else:
                break
        except AttributeError:
            logger.warning(f"{pds} didn't return a response code: {full_url}")
            break
        except Exception as e:
            logger.warning(f"General Error during API call: {e} | {full_url}")
            if cursor:
                await asyncio.sleep(5)
                retry_count += 1
                continue
            else:
                break

    if retry_count >= max_retries:
        logger.warning(f"Max retries reached for {pds} {full_url}")

    return records


async def get_user_block_list(ident, pds):
    base_url = f"{pds}/xrpc/"
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
                    response = await client.get(full_url, timeout=10, follow_redirects=True)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))
                if ratelimit_remaining < 100:
                    logger.warning(f"Blocklist Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    current_time = datetime.now()
                    sleep_time = ratelimit_reset - current_time.timestamp() + 1
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
        except httpx.ReadTimeout:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(10)
            continue
        except httpx.RequestError as e:
            logger.warning(f"Error during API call: {e} : {full_url}")
            retry_count += 1
            logger.info("sleeping 5")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logger.warning(f"General Error during API call: {e} : {full_url}")
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
                        logger.error(f"{full_url}: missing timestamp")
                    elif not uri:
                        uri = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.error(f"{full_url}: Missing uri")
                    elif not cid:
                        cid = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.error(f"{full_url}: missing cid")
                    elif not subject:
                        subject = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.error(f"{full_url}: missing subject")
                    else:
                        logger.error(f"{full_url}: missing data")
                        return None

            cursor = response_json.get("cursor")

        else:
            if response.status_code == 429:
                logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                await asyncio.sleep(60)  # Retry after 60 seconds
            elif response.status_code == 400:
                retry_count += 1
                try:
                    error_message = response.json()["error"]
                    message = response.json()["message"]
                    if error_message == "InvalidRequest" and "Could not find repo" in message:
                        logger.debug("Could not find repo: " + str(ident))

                        return None
                except KeyError:
                    return None
            else:
                retry_count += 1
                logger.warning("Error during API call. Status code: %s", response.status_code)
                await asyncio.sleep(5)
                continue

        if not cursor:
            break

    logger.debug(blocked_data)

    if retry_count == max_retries:
        logger.warning("Could not get block list for: " + ident)

        return None

    return blocked_data


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


async def fetch_handles_batch(batch_dids, ad_hoc=False) -> list:
    handles = []

    if not ad_hoc:
        for did in batch_dids:
            handle = await on_wire.resolve_did(did[0].strip())

            if not handle:
                logger.warning(f"Could not resolve handle for: {did[0].strip()}")
                continue

            handles.append((did[0].strip(), handle[0]))
    else:
        for did in batch_dids:
            handle = await on_wire.resolve_did(did.strip())

            if not handle:
                logger.warning(f"Could not resolve handle for: {did.strip()}")
                continue

            handles.append((did.strip(), handle[0]))

    return handles


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


async def get_mutelists(ident, pds):
    base_url = f"{pds}/xrpc/"
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
                    response = await client.get(full_url, timeout=10, follow_redirects=True)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))
                if ratelimit_remaining < 100:
                    logger.warning(f"Mutelist Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    current_time = datetime.now()
                    sleep_time = ratelimit_reset - current_time.timestamp() + 1
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)

        except httpx.ReadTimeout:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(10)
            continue
        except httpx.RequestError as e:
            logger.warning(f"Error during API call: {e} : {full_url}")
            retry_count += 1
            logger.info("sleeping 5")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logger.warning(f"General Error during API call: {e} : {full_url}")
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

        else:
            if response.status_code == 429:
                logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                await asyncio.sleep(60)  # Retry after 60 seconds
            elif response.status_code == 400:
                retry_count += 1
                try:
                    error_message = response.json()["error"]
                    message = response.json()["message"]
                    if error_message == "InvalidRequest" and "Could not find repo" in message:
                        logger.debug("Could not find repo: " + str(ident))

                        return None
                except KeyError:
                    return None
            else:
                retry_count += 1
                logger.warning("Error during API call. Status code: %s", response.status_code)
                await asyncio.sleep(5)
                continue

        if not cursor:
            break

    if retry_count == max_retries:
        logger.warning("Could not get mute lists for: " + ident)

        return None

    logger.debug(mutelists_data)

    return mutelists_data


async def get_mutelist_users(ident, pds):
    base_url = f"{pds}/xrpc/"
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
                    response = await client.get(full_url, timeout=10, follow_redirects=True)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))

                if ratelimit_remaining < 100:
                    logger.warning(f"Mutelist users Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    current_time = datetime.now()
                    sleep_time = ratelimit_reset - current_time.timestamp() + 1
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
        except httpx.ReadTimeout:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(10)
            continue
        except httpx.RequestError as e:
            logger.warning(f"Error during API call: {e} : {full_url}")
            retry_count += 1
            logger.info("sleeping 5")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logger.warning(f"General Error during API call: {e} : {full_url}")
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
        else:
            if response.status_code == 429:
                logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                await asyncio.sleep(60)  # Retry after 60 seconds
            elif response.status_code == 400:
                retry_count += 1
                try:
                    error_message = response.json()["error"]
                    message = response.json()["message"]
                    if error_message == "InvalidRequest" and "Could not find repo" in message:
                        logger.debug("Could not find repo: " + str(ident))
                        return None
                except KeyError:
                    pass
            else:
                retry_count += 1
                logger.warning("Error during API call. Status code: %s", response.status_code)
                await asyncio.sleep(5)
                continue

        if not cursor:
            break

    if retry_count == max_retries:
        logger.warning("Could not get mute list for: " + ident)
        return None

    logger.debug(mutelists_users_data)

    return mutelists_users_data


async def get_subscribelists(ident, pds):
    base_url = f"{pds}/xrpc/"
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
                    response = await client.get(full_url, timeout=10, follow_redirects=True)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))

                if ratelimit_remaining < 100:
                    logger.warning(f"Subscribe Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    current_time = datetime.now()
                    sleep_time = ratelimit_reset - current_time.timestamp() + 1
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
        except httpx.ReadTimeout:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(10)
            continue
        except httpx.RequestError as e:
            logger.warning(f"Error during API call: {e} : {full_url}")
            retry_count += 1
            logger.info("sleeping 5")
            await asyncio.sleep(5)
            continue
        except Exception as e:
            logger.warning(f"General Error during API call: {e} : {full_url}")
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
        else:
            if response.status_code == 429:
                logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                await asyncio.sleep(60)  # Retry after 60 seconds
            elif response.status_code == 400:
                retry_count += 1
                try:
                    error_message = response.json()["error"]
                    message = response.json()["message"]
                    if error_message == "InvalidRequest" and "Could not find repo" in message:
                        logger.debug("Could not find repo: " + str(ident))

                        return None
                except KeyError:
                    return None
            else:
                retry_count += 1
                logger.warning("Error during API call. Status code: %s", response.status_code)
                await asyncio.sleep(5)
                continue

        if not cursor:
            break

    if retry_count == max_retries:
        logger.warning("Could not get mute lists for: " + ident)

        return None

    logger.debug(subscribe_data)

    return subscribe_data


def fetch_data_with_after_parameter(url, after_value):
    response = requests.get(url, params={'after': after_value})
    if response.status_code == 200:
        db_data = []
        atproto_labelers = []
        in_endpoint = None

        for line in response.iter_lines():
            try:
                record = json.loads(line)
                logger.debug(record)
                did = record.get("did")
                in_record = record.get("operation")
                service = in_record.get("service")
                handle = in_record.get("handle")
                if "plc_tombstone" in in_record.get("type"):
                    continue
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

                if in_endpoint:
                    try:
                        # Extract type and endpoint if atproto_labeler is present for the first time
                        if "atproto_labeler" in in_endpoint:
                            atproto_labeler = {
                                "did": did,
                                "type": in_endpoint["atproto_labeler"]["type"],
                                "endpoint": in_endpoint["atproto_labeler"]["endpoint"],
                                "createdAt": created_date
                            }
                            atproto_labelers.append(atproto_labeler)
                    except Exception as e:
                        logger.error(f"Error: {e}")

                db_data.append([did, created_date, service, handle])
            except json.JSONDecodeError:
                logger.error(f"Failed to parse JSON line: {line}")
                continue

        # Check if there's any data in the list before getting the last created_date
        if db_data:
            last_created_date = db_data[-1][1]  # Access the last element and the created_date (index 1)
        else:
            last_created_date = None

        return db_data, last_created_date, atproto_labelers
    else:
        # Handle any errors or exceptions here
        logger.error(f"Error fetching data. Status code: {response.status_code}")

        return None, None, None


async def get_federated_pdses():
    active = 0
    not_active = 0
    processed_pds = {}
    doa_processed_pds = {}
    attempt = 0
    last_pds = None

    records = await database_handler.get_unique_did_to_pds()

    if not records:
        logger.error("No PDS records found.")

        return None, None

    for did, pds in records:
        if "https://" not in pds:
            if pds in doa_processed_pds:
                continue
            doa_processed_pds[pds] = True
            await database_handler.update_pds_status(pds, False)
            continue

        current_pds = pds
        if current_pds != last_pds:
            attempt = 0
            pds_status = await on_wire.describe_pds(pds)
            if not pds_status:
                not_active += 1
                doa_processed_pds[pds] = True
                await database_handler.update_pds_status(pds, False)
                continue
        if pds in processed_pds:
            continue
        base_url = f"https://bsky.network/xrpc/"

        url = urllib.parse.urljoin(base_url, "com.atproto.sync.getLatestCommit")
        params = {
            "did": did,
        }

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"

        logger.info(full_url)

        try:
            response = requests.get(full_url)
            last_pds = current_pds
        except httpx.RequestError as e:
            response = None
            logger.warning("Error during API call: %s", e)
            await asyncio.sleep(5)  # Retry after 5 seconds
        except Exception as e:
            response = None
            logger.warning("Error during API call: %s", str(e))
            await asyncio.sleep(5)  # Retry after 10 seconds

        if response.status_code == 200:
            response_json = response.json()
            try:
                cid = response_json.get("cid", [])
                rev = response_json.get("rev", [])

                if cid and rev:
                    logger.debug(f"PDS: {pds} is valid.")
                    active += 1
                    await database_handler.update_pds_status(pds, True)
                    processed_pds[pds] = True  # Mark PDS as processed
                    attempt = 0
            except AttributeError:
                try:
                    error = response_json.get("error", [])

                    if "user not found" in error:
                        logger.warning(f"PDS: {pds} not valid.")
                        not_active += 1
                        if attempt == 0:
                            await database_handler.update_pds_status(pds, False)
                        attempt += 1
                    else:
                        logger.error(f"Unexpected error message: {error} {full_url}")
                        not_active += 1
                        if attempt == 0:
                            await database_handler.update_pds_status(pds, False)
                        attempt += 1
                except AttributeError:
                    logger.error(f"Error fetching data: {full_url}")
                    not_active += 1
                    if attempt == 0:
                        await database_handler.update_pds_status(pds, False)
                    attempt += 1
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            await asyncio.sleep(60)  # Retry after 60 seconds
        elif response.status_code == 404:
            logger.warning(f"PDS: {pds} not valid.")
            not_active += 1
            if attempt == 0:
                await database_handler.update_pds_status(pds, False)
            attempt += 1
        elif response.status_code == 500:
            logger.warning(f"PDS: {pds} not valid.")
            not_active += 1
            if attempt == 0:
                await database_handler.update_pds_status(pds, False)
            attempt += 1
        else:
            logger.warning("Response status code: " + str(response.status_code) + f" for PDS: {pds} url: {full_url} not valid.")
            if attempt == 0:
                await database_handler.update_pds_status(pds, False)
            attempt += 1

        if attempt > 0:
            logger.info(f"Attempt {attempt}/10 failed for {pds}.")
            if attempt == 10:
                attempt = 0

    return active, not_active


async def validate_did_atproto(did):
    base_url = f"https://bsky.network/xrpc/"

    url = urllib.parse.urljoin(base_url, "com.atproto.sync.getLatestCommit")
    params = {
        "did": did,
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"

    logger.info(full_url)

    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            response = requests.get(full_url)
        except httpx.RequestError as e:
            response = None
            logger.warning("Error during API call: %s", e)
            retry_count += 1
            await asyncio.sleep(5)  # Retry after 5 seconds
        except Exception as e:
            logger.warning("Error during API call: %s", str(e))

            return False

        if response.status_code == 200:
            response_json = response.json()
            try:
                cid = response_json.get("cid", [])
                rev = response_json.get("rev", [])

                if cid and rev:
                    return True
            except AttributeError:
                try:
                    error = response_json.get("error", [])

                    if "user not found" in error:
                        return False
                    else:
                        logger.error(f"Unexpected error message: {error} {full_url}")

                        return False
                except AttributeError:
                    logger.error(f"Error fetching data: {full_url}")
                    return False
        elif response.status_code == 429:
            logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
            retry_count += 1
            await asyncio.sleep(10)  # Retry after 60 seconds
        elif response.status_code == 404:
            return False
        elif response.status_code == 500:
            return False
        else:
            return False

    if retry_count == max_retries:
        logger.warning("Could not validate DID: " + did)

        return False


async def get_all_did_records(last_cursor=None):
    url = 'https://plc.directory/export'
    after_value = last_cursor
    old_last_created = None

    while True:
        data, last_created, label_data = fetch_data_with_after_parameter(url, after_value)

        logger.info("data batch fetched.")
        if data is None and label_data is None:
            break
        else:
            # print(data)
            await database_handler.update_did_service(data, label_data)

        if last_created != old_last_created:
            logger.info(f"Data fetched until createdAt: {last_created}")
            if last_created:
                await database_handler.update_last_created_did_date(last_created)
            else:
                logger.warning("No last_created date found, keeping last value.")
                break
            # Update the after_value for the next request
            after_value = last_created
            old_last_created = last_created
        else:
            logger.warning("DIDs up to date. Exiting.")
            break

    await database_handler.update_did_webs()


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


def batch_queue(queue, batch_size):
    for i in range(0, len(queue), batch_size):
        yield queue[i:i + batch_size]


async def get_resolution_queue_info(batching=False):
    count = 0
    queue_info = {}
    batch_size = 1000
    offset = 0
    total_handles_updated = 0

    if batching:
        while True:

            all_dids = await database_handler.get_dids_without_handles()

            if all_dids:
                logger.info("Updating dids with no handles.")

                total_dids = len(all_dids)

                async with database_handler.connection_pools["write"].acquire() as connection:
                    async with connection.transaction():
                        # Concurrently process batches and update the handles
                        for i in range(0, total_dids, batch_size):
                            logger.info("Getting batch to resolve.")
                            batch_dids = all_dids[i:i + batch_size]

                            # Process the batch asynchronously
                            batch_handles_updated = await database_handler.process_batch(batch_dids, True, None,
                                                                                         batch_size)
                            total_handles_updated += batch_handles_updated

                            # Log progress for the current batch
                            logger.info(f"Handles updated: {total_handles_updated}/{total_dids}")
                            logger.info(f"First few DIDs in the batch: {batch_dids[:5]}")

                            # Pause after each batch of handles resolved
                            logger.info("Pausing...")
                            await asyncio.sleep(60)  # Pause for 60 seconds

            resolution_queue = await database_handler.get_resolution_queue(offset, batch_size)

            if resolution_queue:
                for batch in batch_queue(resolution_queue, BATCH_SIZE):
                    count += 1
                    # batch_count = math.ceil(len(resolution_queue) / BATCH_SIZE)

                    logger.info(f"Processing batch {count}.")

                    for did in batch:
                        handle = await on_wire.resolve_did(did)

                        if "did:web:" in did:
                            pds = await on_wire.resolve_did(did, True)
                        else:
                            pds = await on_wire.get_pds(did)

                        if handle[0] and pds:
                            info = {
                                "handle": handle[0],
                                "pds": pds
                            }

                            queue_info[did] = info

                    if queue_info:
                        await database_handler.process_resolution_queue(queue_info)
    else:
        resolution_queue = await database_handler.get_resolution_queue()

    if not resolution_queue:
        logger.info("Nothing in queue.")

        return

    for batch in batch_queue(resolution_queue, BATCH_SIZE):
        count += 1
        batch_count = math.ceil(len(resolution_queue) / BATCH_SIZE)

        logger.info(f"Processing batch {count} of {batch_count}.")

        for did in batch:
            handle = await on_wire.resolve_did(did)

            if "did:web:" in did:
                pds = await on_wire.resolve_did(did, True)
            else:
                pds = await on_wire.get_pds(did)

            if handle[0] and pds:
                info = {
                    "handle": handle[0],
                    "pds": pds
                }

                queue_info[did] = info

        if queue_info:
            await database_handler.process_resolution_queue(queue_info)
