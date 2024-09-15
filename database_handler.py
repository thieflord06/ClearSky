# database_handler.py

import asyncio
import os
from typing import Optional, Tuple, List
import asyncpg
import config_helper
import utils
from config_helper import logger
from cachetools import TTLCache
from redis import asyncio as aioredis
from datetime import datetime
from collections import defaultdict
import pytz
import on_wire
import math
import functools
from errors import NotFound, DatabaseConnectionError
from config_helper import check_override

# ======================================================================================================================
# ===================================================  global variables ================================================

connection_pools = {}
db_lock = asyncio.Lock()
redis_connection = None
once = None
no_tables = asyncio.Event()

all_blocks_cache = TTLCache(maxsize=5000000, ttl=86400)  # every 24 hours

blocklist_updater_status = asyncio.Event()
blocklist_24_updater_status = asyncio.Event()
block_cache_status = asyncio.Event()

last_update_top_block = None
last_update_top_24_block = None
all_blocks_process_time = None
all_blocks_last_update = None
top_blocks_start_time = None
top_24_blocks_start_time = None
top_blocks_process_time = None
top_24_blocks_process_time = None
top_blocked_as_of_time = None
top_24_blocked_as_of_time = None
total_users_as_of_time = None

users_table = "users"
blocklist_table = "blocklists"
top_blocks_table = "top_block"
top_24_blocks_table = "top_twentyfour_hour_block"
mute_lists_table = "mutelists"
mute_lists_users_table = "mutelists_users"
last_created_table = "last_did_created_date"


# ======================================================================================================================
# ========================================= database handling functions ================================================
async def create_connection_pool(db):
    global connection_pools

    if "local" in db:
        async with db_lock:
            if db not in connection_pools:
                try:
                    local_connection_pool = await asyncpg.create_pool(
                        user=write_pg_user,
                        password=write_pg_password,
                        database=database_config["local_db"]
                    )

                    connection_pools[db] = local_connection_pool

                    return True
                except OSError:
                    logger.error("Network connection issue. db connection not established.")

                    return False
                except (asyncpg.exceptions.InvalidAuthorizationSpecificationError,
                        asyncpg.exceptions.CannotConnectNowError):
                    # Handle specific exceptions that indicate a connection issue
                    logger.error("db connection issue.")

                    return False
    elif "write" in db:
        # Acquire the lock before creating the connection pool
        async with db_lock:
            if db not in connection_pools:
                try:
                    write_connection_pool = await asyncpg.create_pool(
                        user=write_pg_user,
                        password=write_pg_password,
                        host=write_pg_host,
                        database=write_pg_database
                    )

                    connection_pools[db] = write_connection_pool

                    return True
                except OSError:
                    logger.error("Network connection issue. db connection not established.")

                    return False
                except (asyncpg.exceptions.InvalidAuthorizationSpecificationError,
                        asyncpg.exceptions.CannotConnectNowError):
                    logger.error("db connection issue.")

                    return False
                except asyncpg.InvalidAuthorizationSpecificationError:
                    logger.error("db connection issue.")

                    return False
    elif "read" in db:
        # Acquire the lock before creating the connection pool
        async with db_lock:
            if db not in connection_pools:
                try:
                    read_connection_pool = await asyncpg.create_pool(
                        user=read_pg_user,
                        password=read_pg_password,
                        host=read_pg_host,
                        database=read_pg_database
                    )

                    connection_pools[db] = read_connection_pool

                    return True
                except OSError:
                    logger.error("Network connection issue. db connection not established.")

                    return False
                except (asyncpg.exceptions.InvalidAuthorizationSpecificationError,
                        asyncpg.exceptions.CannotConnectNowError):
                    logger.error("db connection issue.")

                    return False
                except asyncpg.InvalidAuthorizationSpecificationError:
                    logger.error("db connection issue.")

                    return False
    else:
        logger.error("No db connection made.")
        return False


async def check_database_connection(db):
    if db in connection_pools:
        pool = connection_pools[db]
        if pool is None:
            logger.error(f"Database connection pool for {db} does not exist.")

            return False
        try:
            async with pool.acquire() as connection:
                try:
                    # Perform a simple query to check if the connection is alive
                    await connection.fetchval('SELECT 1')
                    return True
                except Exception as e:
                    logger.error(f"Error checking database connection for {db}: {e}")
                    return False
        except Exception as e:
            logger.error(f"Error checking database connection for {db}: {e}")
            return False
    else:
        logger.error(f"Database connection pool for {db} does not exist.")
        return False


def check_db_connection(*dbs):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for db in dbs:
                if not await check_database_connection(db):  # Implement your function to check the database connection here
                    raise DatabaseConnectionError("Database connection not available")

            return await func(*args, **kwargs)

        return wrapper
    return decorator


# Function to close the connection pool
async def close_connection_pool():
    global connection_pools

    if connection_pools:
        for db in connection_pools:
            await db.close()


async def populate_redis_with_handles():
    if await redis_connected():
        try:
            # Query handles in batches
            batch_size = 1000  # Adjust the batch size as needed
            offset = 0
            batch_count = 0
            while True:
                async with connection_pools["read"].acquire() as connection:
                    async with connection.transaction():
                        logger.info("Transferring handles to cache.")
                        # Query all handles from the database
                        query_text = "SELECT handle FROM users LIMIT $1 OFFSET $2"
                        result = await connection.fetch(query_text, batch_size, offset)

                        if not result:
                            break  # No more handles to fetch

                        # Create a temporary dictionary to hold handles by level
                        handles_by_level = defaultdict(list)

                        # Organize handles into a trie-like structure
                        for row in result:
                            handle = row['handle']
                            logger.debug(str(handle))
                            if handle:  # Check if handle is not empty
                                # Group handles by level, e.g., {"a": ["abc", "ade"], "ab": ["abc"]}
                                for i in range(1, len(handle) + 1):
                                    prefix = handle[:i]
                                    handles_by_level[prefix].append(handle)

                        logger.info("inserting into redis")
                        # Store handles in Redis ZSETs by level
                        async with redis_conn as pipe:
                            for prefix, handles in handles_by_level.items():
                                zset_key = f"handles:{prefix}"
                                # Create a dictionary with new handles as members and scores (use 0 for simplicity)
                                zset_data = {handle: 0 for handle in handles}
                                await pipe.zadd(zset_key, zset_data, nx=True)

                        offset += batch_size
                        batch_count += 1

                        logger.info(f"Batch {str(batch_count)} processed. Total handles added: {str(offset)}")

            logger.info("Handles added to cache successfully.")

            return None
        except Exception as e:
            logger.error(f"Error adding handles to Redis: {e}")

            return "Error"
        finally:
            # Close the Redis connection
            await redis_conn.close()


async def retrieve_autocomplete_handles(query):
    global redis_connection
    global once

    key = f"handles:{query}"
    try:
        matching_handles = await asyncio.wait_for(redis_conn.zrange(key, start=0, end=4), timeout=1.5)  # Fetch the first 5 handles
        if matching_handles:
            decoded = [handle.decode('utf-8') for handle in matching_handles]

            logger.debug("From redis")

            return decoded
        else:
            results = await asyncio.wait_for(find_handles(query), timeout=5.0)

            logger.info("from db, not in redis")
            if not results:
                results = None

            return results
    except asyncio.TimeoutError:
        # Query the database for autocomplete results
        try:
            results = await asyncio.wait_for(find_handles(query), timeout=5.0)

            logger.debug("from db, timeout in redis")
            logger.debug(str(results))

            return results
        except asyncio.TimeoutError:
            logger.info("not quick enough.")

            results = None

            return results
    except Exception as e:
        logger.error(f"Error getting data from redis, failing over to db: {e}")

        redis_connection = False
        once = False

        if not once:
            once = True
            asyncio.create_task(wait_for_redis())
        else:
            results = None

            return results

        results = await asyncio.wait_for(find_handles(query), timeout=5.0)

        return results


async def find_handles(value):
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                logger.debug(f"{value}")

                query_text1 = """SELECT handle 
                                FROM users
                                WHERE handle LIKE $1 || '%'
                                LIMIT 5"""

                result = await asyncio.wait_for(connection.fetch(query_text1, value), timeout=5.0)

                logger.debug("autocomplete fulfilled.")

                if not result:

                    return None

                # Extract matching handles from the database query result
                matching_handles = [row['handle'] for row in result]

                return matching_handles
    except asyncpg.ConnectionDoesNotExistError:
        logger.error("db connection issue.")

        return None
    except Exception as e:
        logger.error(f"Error retrieving autocomplete handles: {e}")

        return None


@check_db_connection("read")
async def count_users_table():
    async with connection_pools["read"].acquire() as connection:
        # Execute the SQL query to count the rows in the "users" table
        return await connection.fetchval('SELECT COUNT(*) FROM users WHERE status is TRUE')


@check_db_connection("read")
async def get_blocklist(ident, limit=100, offset=0):
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT DISTINCT b.blocked_did, b.block_date, u.handle, u.status 
                FROM blocklists AS b JOIN users AS u ON b.blocked_did = u.did 
                WHERE b.user_did = $1 ORDER BY block_date DESC LIMIT $2 OFFSET $3"""
                blocklist_rows = await connection.fetch(query, ident, limit, offset)

                query2 = """SELECT COUNT(DISTINCT blocked_did) 
                FROM blocklists 
                WHERE user_did = $1"""
                total_blocked_count = await connection.fetchval(query2, ident)

                return blocklist_rows, total_blocked_count
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
    except AttributeError:
        logger.error(f"db connection issue.")
    except Exception as e:
        logger.error(f"Error retrieving blocklist for {ident}: {e} {type(e)}")

        return None, None


@check_db_connection("read")
async def get_subscribe_blocks(ident, limit=100, offset=0):
    data_list = []

    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                query_1 = """SELECT mu.subject_did, u.handle, s.date_added, u.status, s.list_uri, m.url, sc.user_count
                                FROM subscribe_blocklists AS s
                                INNER JOIN mutelists_users AS mu ON s.list_uri = mu.list_uri
                                INNER JOIN users AS u ON mu.subject_did = u.did
                                INNER JOIN mutelists AS m ON s.list_uri = m.uri
                                INNER JOIN subscribe_blocklists_user_count AS sc ON s.list_uri = sc.list_uri
                                WHERE s.did = $1
                                ORDER BY s.date_added DESC
                                LIMIT $2
                                OFFSET $3"""

                query_2 = """SELECT COUNT(mu.subject_did)
                                FROM subscribe_blocklists AS s
                                INNER JOIN mutelists_users AS mu ON s.list_uri = mu.list_uri
                                WHERE s.did = $1"""

                sub_list = await connection.fetch(query_1, ident, limit, offset)

                total_blocked_count = await connection.fetchval(query_2, ident)

                if not sub_list:
                    return None, 0

                for record in sub_list:
                    list_dict = {
                        "handle": record['handle'],
                        "subject_did": record['subject_did'],
                        "date_added": record['date_added'].isoformat(),
                        "status": record['status'],
                        "list_uri": record['list_uri'],
                        "list_url": record['url'],
                        "list count": record['user_count']
                    }

                    data_list.append(list_dict)

                return data_list, total_blocked_count
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
    except AttributeError:
        logger.error(f"db connection issue.")
    except Exception as e:
        logger.error(f"Error retrieving subscribe blocklist for {ident}: {e} {type(e)}")

        return None, None


@check_db_connection("read")
async def get_subscribe_blocks_single(ident, list_of_lists, limit=100, offset=0):
    data_list = []
    total_data = []
    total_count = 0

    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                query_1 = """SELECT s.did, u.handle, s.date_added, u.status, s.list_uri, m.url, sc.user_count
                                FROM subscribe_blocklists AS s
                                INNER JOIN users AS u ON s.did = u.did
                                INNER JOIN mutelists AS m ON s.list_uri = m.uri
                                INNER JOIN subscribe_blocklists_user_count AS sc ON s.list_uri = sc.list_uri
                                WHERE m.url = $1
                                ORDER BY s.date_added DESC
                                LIMIT $2
                                OFFSET $3"""

                query_2 = """SELECT COUNT(s.did)
                                FROM subscribe_blocklists AS s
                                INNER JOIN mutelists AS m ON s.list_uri = m.uri
                                WHERE m.url = $1"""

                if not list_of_lists:
                    return None, 0

                for list_url in list_of_lists:
                    count = await connection.fetchval(query_2, list_url)
                    if count:
                        data = await connection.fetch(query_1, list_url, limit, offset)

                        total_data.append(data)
                    total_count += count

                if not total_data:
                    return None, 0

                for record in total_data:
                    for data in record:
                        list_dict = {
                            "handle": data['handle'],
                            "did": data['did'],
                            "date_added": data['date_added'].isoformat(),
                            "status": data['status'],
                            "list_uri": data['list_uri'],
                            "list_url": data['url']
                        }

                        data_list.append(list_dict)

                return data_list, total_count
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
    except AttributeError:
        logger.error(f"db connection issue.")
    except Exception as e:
        logger.error(f"Error retrieving subscribe blocklist for {ident}: {e} {type(e)}")

        return None, None


@check_db_connection("read")
async def get_listitem_url(uri):
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT list_uri FROM mutelist_users WHERE listitem_uri = $1"
                list_uri = await connection.fetchval(query, uri)

                url = await utils.list_uri_to_url(list_uri)

                return url
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
    except AttributeError:
        logger.error(f"db connection issue.")
    except Exception as e:
        logger.error(f"Error retrieving URL {uri}: {e} {type(e)}")

        return None


@check_db_connection("read")
async def get_moderation_list(name, limit=100, offset=0):
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                search_string = f'%{name}%'

                name_query = """SELECT ml.url, u.handle, u.status, ml.name, ml.description, ml.created_date, mc.user_count
                FROM mutelists AS ml 
                INNER JOIN users AS u ON ml.did = u.did -- Join the users table to get the handle 
                LEFT mutelists_user_count AS mc ON ml.uri = mc.list_uri
                WHERE ml.name ILIKE $1
                LIMIT $2
                OFFSET $3"""

                name_mod_lists = await connection.fetch(name_query, search_string, limit, offset)

                description_query = """SELECT ml.url, u.handle, u.status, ml.name, ml.description, ml.created_date, mc.user_count
                FROM mutelists AS ml
                INNER JOIN users AS u ON ml.did = u.did -- Join the users table to get the handle
                LEFT mutelists_user_count AS mc ON ml.uri = mc.list_uri
                WHERE ml.description ILIKE $1
                LIMIT $2
                OFFSET $3"""

                description_mod_lists = await connection.fetch(description_query, search_string, limit, offset)

                name_count_query = """SELECT COUNT(*) FROM mutelists WHERE name ILIKE $1"""

                description_count_query = """SELECT COUNT(*) FROM mutelists WHERE description ILIKE $1"""

                name_count = await connection.fetchval(name_count_query, search_string)

                description_count = await connection.fetchval(description_count_query, search_string)
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
    except AttributeError:
        logger.error(f"db connection issue.")
    except Exception as e:
        logger.error(f"Error retrieving results for {name}: {e} {type(e)}")

        return None, 0

    count = name_count + description_count

    if count > 0:
        pages = count / 100

        pages = math.ceil(pages)
    else:
        pages = 0

    lists = []

    if name_mod_lists or description_mod_lists:
        for record in name_mod_lists:
            data = {
                "url": record['url'],
                "handle": record['handle'],
                "status": record['status'],
                "name": record['name'],
                "description": record['description'],
                "created_date": record['created_date'].isoformat(),
                "list count": record['user_count']
            }
            lists.append(data)

        for record in description_mod_lists:
            data = {
                "url": record['url'],
                "handle": record['handle'],
                "status": record['status'],
                "name": record['name'],
                "description": record['description'],
                "created_date": record['created_date'].isoformat(),
                "list count": record['user_count']
            }
            lists.append(data)
    else:
        lists = None

    return lists, pages


@check_db_connection("read")
async def get_listblock_url(uri):
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT list_uri FROM subscribe_blocklists WHERE uri = $1"
                list_uri = await connection.fetchval(query, uri)

                url = await utils.list_uri_to_url(list_uri)

                return url
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
    except AttributeError:
        logger.error(f"db connection issue.")
    except Exception as e:
        logger.error(f"Error retrieving URL {uri}: {e} {type(e)}")

        return None


@check_db_connection("write")
async def get_dids_without_handles():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT did FROM users WHERE handle IS NULL and status is TRUE"
                rows = await connection.fetch(query)
                dids_without_handles = [record['did'] for record in rows]
                return dids_without_handles
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return []


@check_db_connection("write")
async def get_dids_without_handles_and_are_not_active():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT did FROM users WHERE handle IS NULL and status is FALSE"
                rows = await connection.fetch(query)
                dids_without_handles = [record['did'] for record in rows]
                return dids_without_handles
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return []


@check_db_connection("write")
async def get_pdses():
    # update PDS table with unique PDSes from users table
    # try:
    #     async with connection_pools["write"].acquire() as connection:
    #         async with connection.transaction():
    #             query = """INSERT INTO pds (pds)
    #                         SELECT DISTINCT pds
    #                         FROM users
    #                         WHERE pds IS NOT NULL
    #                         ON CONFLICT (pds) DO NOTHING"""
    #             await connection.execute(query)
    # except Exception as e:
    #     logger.error(f"Error inserting PDSes: {e}")

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT pds from pds where status is TRUE"
                results = await connection.fetch(query)

                result = [pds["pds"] for pds in results]

                return result
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return None


@check_db_connection("write")
async def get_pds(did):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT pds FROM users WHERE did = $1"
                pds = await connection.fetchval(query, did)

                return pds
    except Exception as e:
        logger.error(f"Error retrieving PDS for DID {did}: {e}")

        return None


@check_db_connection("write")
async def update_did_status(did, info) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "UPDATE users SET status = $1, reason = $2 WHERE did = $3"

                await connection.execute(query, info.get("status"), info.get("reason"), did)
    except Exception as e:
        logger.error(f"Error updating status for DID {info['did']}: {e}")


@check_db_connection("write")
async def get_dids_without_pdses() -> List[str]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT did FROM users WHERE pds IS NULL"
                rows = await connection.fetch(query)
                dids_without_pdses = [record['did'] for record in rows]

                return dids_without_pdses
    except Exception as e:
        logger.error(f"Error retrieving DIDs without PDSes: {e}")

        return []


@check_db_connection("write")
async def update_did_pds(did, pds):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "UPDATE users SET pds = $1 WHERE did = $2"
                await connection.execute(query, pds, did)
    except Exception as e:
        logger.error(f"Error updating PDS for DID {did}: {e}")


@check_db_connection("write")
async def blocklist_search(search_list, lookup, switch):
    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            try:
                blocking = """SELECT b.user_did, b.blocked_did, b.block_date, u1.handle, u1.status
                                FROM blocklists AS b
                                INNER JOIN users AS u1 ON b.user_did = u1.did
                                INNER JOIN users AS u2 ON b.blocked_did = u2.did
                                WHERE u1.handle = $1
                                  AND u2.handle = $2"""

                blocked = """SELECT b.user_did, b.blocked_did, b.block_date, u1.handle, u1.status
                                FROM blocklists AS b
                                INNER JOIN users AS u1 ON b.user_did = u1.did
                                INNER JOIN users AS u2 ON b.blocked_did = u2.did
                                WHERE u1.handle = $2
                                  AND u2.handle = $1"""

                if "blocking" in switch:
                    query = blocking
                elif "blocked" in switch:
                    query = blocked
                else:
                    result = None

                    return result

                result = await connection.fetch(query, search_list, lookup)

                if result:
                    resultslist = {}

                    for record in result:
                        block_date = record['block_date']
                        handle = record['handle']
                        status = record['status']

                    results = {
                        "blocked_date": block_date.isoformat(),
                        "handle": handle,
                        "status": status
                    }

                    resultslist.update(results)

                    return resultslist
                else:
                    resultslist = None

                    return resultslist

            except Exception as e:
                logger.error(f"Error retrieving blocklist search result: {e}")

                resultslist = None

                return resultslist


async def crawl_all_retry_batch(batch_dids, forced=False):
    max_retries = 10  # Maximum number of retries
    retry_count = 0

    while retry_count < max_retries:
        try:
            await crawler_batch(batch_dids, forced=forced)
            return  # Successfully processed batch, exit loop
        except DatabaseConnectionError as e:
            logger.error(f"Database connection error occurred: {e}")
            retry_count += 1
            logger.info(f"Retrying batch (Retry {retry_count}/{max_retries})")
            await asyncio.sleep(180)  # Wait before retrying
        except asyncpg.InterfaceError as e:
            logger.error(f"Interface error occurred: {e}")
            retry_count += 1
            logger.info(f"Retrying batch (Retry {retry_count}/{max_retries})")
            await asyncio.sleep(180)  # Wait before retrying
        except Exception as e:
            logger.error(f"An unexpected error occurred: {e}")
            retry_count += 1
            logger.info(f"Retrying batch (Retry {retry_count}/{max_retries})")
            await asyncio.sleep(180)  # Wait before retrying

    logger.error(f"Failed to process batch after {max_retries} retries.")


@check_db_connection("write")
async def crawl_all(forced=False, quarter=None, total_crawlers=None):
    all_dids = await get_all_users_db(False, True, quarter=quarter, total_crawlers=total_crawlers)
    total_dids = len(all_dids)
    batch_size = 500
    pause_interval = 500  # Pause every x DID requests
    processed_count = 0
    cumulative_processed_count = 0  # Initialize cumulative count
    total_blocks_updated = 0
    table = f"temporary_table_{quarter}"

    # Check if there is a last processed DID in the temporary table
    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            try:
                query = f"SELECT last_processed_did FROM {table}"
                last_processed_did = await connection.fetchval(query)
                logger.debug("last did from db: " + str(last_processed_did))
            except Exception as e:
                last_processed_did = None
                logger.error(f"Exception getting from db: {str(e)}")

    if not last_processed_did:
        await create_temporary_table(table)

    if last_processed_did:
        # Find the index of the last processed DID in the list
        start_index = next((i for i, (did, _) in enumerate(all_dids) if did == last_processed_did), None)
        if start_index is None:
            logger.warning(
                f"Last processed DID '{last_processed_did}' not found in the list. Starting from the beginning.")
        else:
            logger.info(f"Resuming processing from DID: {last_processed_did}")
            all_dids = all_dids[start_index:]

    logger.info("Starting crawl.")

    for i in range(0, total_dids, batch_size):
        remaining_dids = total_dids - i
        current_batch_size = min(batch_size, remaining_dids)

        batch_dids = all_dids[i:i + current_batch_size]

        await crawl_all_retry_batch(batch_dids, forced=forced)

        # Update the temporary table with the last processed DID
        if batch_dids:
            try:
                last_processed_did = batch_dids[-1][0]  # Assuming DID is the first element in each tuple
            except IndexError:
                logger.error(f"Batch of DIDs is empty: {batch_dids}")

            logger.debug("Last processed DID: " + str(last_processed_did))

            await update_temporary_table(last_processed_did, table)
        else:
            logger.warning("Batch of DIDs is empty. Skipping update of the temporary table.")
            logger.error(f"Batch_dids that caused issue: {batch_dids}")
            continue

        cumulative_processed_count += len(batch_dids)

        # Pause every 100 DID requests
        if processed_count % pause_interval == 0:
            logger.info(f"Pausing after {i + 1} DID requests...")

            # Log information for each batch
            logger.info(f"Processed batch {i // batch_size + 1}/{total_dids // batch_size + 1}")
            logger.info(f"Processed {cumulative_processed_count}/{total_dids} DIDs")
            await asyncio.sleep(5)  # Pause for 30 seconds

        processed_count += batch_size

    logger.info(f"Block lists updated: {total_blocks_updated}/{total_dids}")


async def crawler_batch(batch_dids, forced=False):
    total_blocks_updated = 0
    mute_lists = 0
    mute_users_list = 0
    total_subscribed_updated = 0
    total_handles_updated = 0
    handle_count = 0
    total_mutes_updated = [mute_lists, mute_users_list]
    handles_to_update = []
    max_retries = 3

    for did, pds in batch_dids:
        if pds is None:
            continue
        if "bsky.network" not in pds:
            continue
        handle = await on_wire.resolve_did(did)

        if handle[0] is not None:
            # Check if the DID and handle combination already exists in the database
            logger.debug("Did: " + str(did) + " | handle: " + str(handle[0]))

            if await does_did_and_handle_exist_in_database(did, handle[0]):
                logger.debug(f"DID {did} with handle {handle[0]} already exists in the database. Skipping...")
            else:
                handles_to_update.append((did, handle[0]))
                handle_count += 1
        else:
            logger.warning(f"DID: {did} not resolved.")
            continue

        retry_count = 0
        success = False
        while retry_count < max_retries and not success:
            try:
                blocked_data = await utils.get_user_block_list(did, pds)
                mutelists_data = await utils.get_mutelists(did, pds)
                mutelists_users_data = await utils.get_mutelist_users(did, pds)
                subscribe_data = await utils.get_subscribelists(did, pds)

                if blocked_data:
                    # Update the blocklists table in the database with the retrieved data
                    total_blocks_updated += await update_blocklist_table(did, blocked_data, forced=forced)
                else:
                    logger.debug(f"didn't update no blocks: {did}")

                if mutelists_data or mutelists_users_data:
                    # Update the mutelist tables in the database with the retrieved data
                    total_mutes_updated += await update_mutelist_tables(did, mutelists_data, mutelists_users_data, forced=forced)

                    logger.debug(f"Updated mute lists for DID: {did}")
                else:
                    logger.debug(f"didn't update no mutelists: {did}")

                if subscribe_data:
                    total_subscribed_updated += await update_subscribe_table(did, subscribe_data, forced=forced)

                    logger.debug(f"Updated subscribe lists for DID: {did}")
                else:
                    logger.debug(f"didn't update not subscribed to any lists: {did}")

                success = True  # Mark the operation as successful if no exception is raised
            except Exception as e:
                logger.error(f"Error updating for DID {did}: {e}")
                retry_count += 1  # Increment the retry count
                await asyncio.sleep(5)  # Wait for a short interval before retrying

        if not success:
            logger.error(f"Failed to update for DID {did} after {max_retries} retries.")

    # Update the database with the batch of handles
    if handles_to_update:
        # only_handles = []
        while True:
            try:
                # Update the database with the batch of handles
                logger.info("committing batch.")
                async with connection_pools["write"].acquire() as connection:
                    async with connection.transaction():
                        await update_user_handles(handles_to_update)
                        total_handles_updated += len(handles_to_update)

                break
            except asyncpg.ConnectionDoesNotExistError as e:
                logger.warning(f"Connection error: {e}, retrying in 30 seconds...")
                await asyncio.sleep(30)  # Retry after 60 seconds
            except Exception as e:
                # Handle other exceptions as needed
                logger.error(f"Error during batch update: {e}")
                break  # Break the loop on other exceptions

        logger.info("Batch resolved.")
    else:
        logger.info("No handles to update in this batch.")

    logger.info(f"Updated in batch: handles: {handle_count} blocks: {total_blocks_updated} | mute lists: {total_mutes_updated[0]} | mute lists users: {total_mutes_updated[1]} | subscribe lists: {total_subscribed_updated}")

    total_items_updated = total_blocks_updated + total_mutes_updated[0] + total_mutes_updated[1] + total_subscribed_updated + handle_count

    return total_items_updated


@check_db_connection("write")
async def get_quarter_of_users_db(quarter_number, total_crawlers=4):
    async with connection_pools["write"].acquire() as connection:
        logger.info(f"Getting quarter {quarter_number} of dids.")
        quarter_number = int(quarter_number)
        total_rows = await connection.fetchval('SELECT COUNT(*) FROM users')
        quarter_size = total_rows / int(total_crawlers)
        offset = math.floor((quarter_number - 1) * quarter_size)

        logger.info(f"Total rows: {total_rows} | quarter size: {quarter_size} | offset: {offset}")

        # Fetch a quarter of the DIDs from the database based on the quarter number
        records = await connection.fetch('SELECT did, pds FROM users ORDER BY did OFFSET $1 LIMIT $2', offset,
                                         quarter_size)

        logger.info(f"Quarter {quarter_number} of DIDs fetched.")

        # Return the fetched records
        return records


@check_db_connection("write")
async def get_all_users_db(run_update=False, get_dids=False, init_db_run=False, quarter=None, total_crawlers=None):
    batch_size = 10000
    pds_records = set()
    dids = []
    total_dids = 0

    async with connection_pools["write"].acquire() as connection:
        if not run_update:
            if get_dids:
                # Return the user_dids from the "users" table
                # records = await connection.fetch('SELECT did, pds FROM users')
                records = await get_quarter_of_users_db(quarter, total_crawlers)
                for record in records:
                    dids.append((record['did'], record['pds']))

                return dids
        else:
            pdses = await get_pdses()

            # Get all DIDs
            for pds in pdses:
                active_dids_in_pds = set()
                inactive_dids_in_pds = set()

                pds_dids = await utils.get_all_users(pds)
                if pds_dids:
                    for did in pds_dids:
                        if did.get('status'):
                            active_dids_in_pds.add(did.get('did'))
                        else:
                            inactive_dids_in_pds.add((did.get('did'), did.get('status'), did.get('reason')))

                    logger.info(f"{len(active_dids_in_pds)} user(s) in {pds}")

                    current_true_pds_records = await connection.fetch('SELECT did FROM users WHERE PDS = $1 AND status = TRUE', pds)
                    current_true_pds_set = set(record["did"] for record in current_true_pds_records)

                    logger.info("Getting DIDs to deactivate.")

                    # Find the DIDs in the database that are no longer active in the new list
                    dids_to_deactivate = current_true_pds_set - active_dids_in_pds

                    # Find new DIDs to add
                    new_dids = active_dids_in_pds - current_true_pds_set

                    logger.info(f"Total new DIDs: {len(new_dids)}")
                    logger.info(f"Total DIDs to deactivate: {len(dids_to_deactivate)}")

                    total_dids += len(pds_records)

                    if dids_to_deactivate:
                        count = 0

                        logger.info(f"deactivating {len(dids_to_deactivate)} dids in {pds}.")

                        for did in dids_to_deactivate:
                            reason = next((item[2] for item in inactive_dids_in_pds if item[0] == did),
                                          None)
                            await connection.execute("""UPDATE users SET status = FALSE, reason = $2 WHERE did = $1""", did, reason)
                            # await connection.execute("delete from resolution_queue where did = $1", did)  # Remove the DID from the resolution queue
                            count += 1
                            logger.debug(f"DIDs deactivated: {count}")

                        logger.info(f"{str(len(dids_to_deactivate))} dids deactivated in {pds}.")

                    if init_db_run:
                        if new_dids:
                            logger.info(f"Adding new DIDs {len(new_dids)} to the database.")

                            records = list(new_dids)

                            async with connection.transaction():
                                # Insert data in batches
                                for i in range(0, len(records), batch_size):
                                    batch_data = [(did, True, pds) if utils.is_did(did) else (did, False, pds) for did in records[i: i + batch_size]]
                                    try:
                                        await connection.executemany(
                                            """INSERT INTO users (did, status, pds)
                                            VALUES ($1, $2, $3) 
                                            ON CONFLICT (did) 
                                            DO UPDATE SET pds = EXCLUDED.pds, 
                                            status = TRUE WHERE users.status <> TRUE
                                            """,
                                            batch_data)

                                        logger.info(
                                            f"Inserted batch {i // batch_size + 1} of {len(records) // batch_size + 1} batches.")
                                    except Exception as e:
                                        logger.error(f"Error inserting batch {i // batch_size + 1}: {str(e)}")
                                # for did in batch_data:
                                    # await connection.execute("delete from resolution_queue where did = $1", did[0])  # Remove the DID from the resolution queue
                else:
                    logger.warning(f"No users in {pds} or could not get users.")

            logger.info(f"Total DIDs: {total_dids}")
            logger.info(f"Total PDSes processed: {len(pdses)}")


@check_db_connection("write")
async def update_blocklist_table(ident, blocked_data, forced=False):
    counter = 0

    if not forced:
        touched_actor = "crawler"
    else:
        touched_actor = "forced_crawler"

    if blocked_data is None:
        return counter

    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT uri FROM blocklists WHERE user_did = $1', ident
            )
            existing_blocklist_entries = {record['uri'] for record in existing_records}
            logger.debug("Existing entires " + ident + ": " + str(existing_blocklist_entries))

            # Iterate through blocked_data and check for None values
            for subject, created_date, uri, cid in blocked_data:
                if any(value is None for value in (subject, created_date, uri, cid)):
                    logger.error(f"Blocked data contains a None value skipping: {blocked_data}")

                    return counter
            # Prepare the data to be inserted into the database
            data = [(ident, subject, created_date, uri, cid, datetime.now(pytz.utc), touched_actor) for subject, created_date, uri, cid in blocked_data]
            logger.debug("Data to be inserted: " + str(data))

            # Convert the new blocklist entries to a set for comparison
            new_blocklist_entries = {record[3] for record in data}
            logger.debug("new blocklist entry " + ident + " : " + str(new_blocklist_entries))

            if existing_blocklist_entries != new_blocklist_entries or forced:
                try:
                    await connection.execute('DELETE FROM blocklists WHERE user_did = $1', ident)
                except Exception as e:
                    logger.error(f"Error deleting blocklist for {ident} : {e}")
                    logger.error(existing_blocklist_entries)
                try:
                    for uri in existing_blocklist_entries:
                        if uri is None:
                            logger.error(f"a URI is None in: {ident}")
                            continue  # Skip processing when uri is None
                        await connection.execute('INSERT INTO blocklists_transaction (user_did, uri, block_date, touched, touched_actor, delete) VALUES ($1, $2, $3, $4, $5, $6)', ident, uri, datetime.now(pytz.utc), datetime.now(pytz.utc), touched_actor, True)
                except Exception as e:
                    logger.error(f"Error updating blocklists_transaction on delete : {e}")
                    logger.error(existing_blocklist_entries)

                logger.info("Blocklist transaction[deleted] updated.")

                if data:
                    try:
                        # Insert the new blocklist entries
                        await connection.executemany(
                            'INSERT INTO blocklists (user_did, blocked_did, block_date, cid, uri, touched, touched_actor) VALUES ($1, $2, $3, $5, $4, $6, $7)', data
                        )

                        await connection.executemany(
                            'INSERT INTO blocklists_transaction (user_did, blocked_did, block_date, cid, uri, touched, touched_actor) VALUES ($1, $2, $3, $5, $4, $6, $7)', data
                        )
                    except Exception as e:
                        logger.error(f"Error updating blocklists or blocklists_transaction on create : {e}")
                        logger.error(data)

                    logger.info("Blocklist transaction[created] updated.")
                    logger.info(f"Blocks added for: {ident}")

                counter += 1

                return counter
            else:
                logger.debug("Blocklist not updated already exists.")

                return counter


@check_db_connection("write")
async def update_subscribe_table(ident, subscribelists_data, forced=False):
    subscribe_list_counter = 0

    if not forced:
        touched_actor = "crawler"
    else:
        touched_actor = "forced_crawler"

    if subscribelists_data is None:
        counter = subscribe_list_counter

        return counter

    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT uri FROM subscribe_blocklists WHERE did = $1', ident
            )
            existing_blocklist_entries = {record['uri'] for record in existing_records}
            logger.debug("Existing subscribe entires " + ident + ": " + str(existing_blocklist_entries))

            # Prepare the data to be inserted into the database
            data = [(record_type['did'], record_type['uri'], record_type['list_uri'], record_type['cid'], record_type['date_added'], record_type['record_type'], datetime.now(pytz.utc), touched_actor) for record_type in subscribelists_data]
            logger.debug("Data to be inserted: " + str(data))

            # Convert the new blocklist entries to a set for comparison
            new_blocklist_entries = {record[1] for record in data}
            logger.debug("new subscribe blocklist entry " + ident + " : " + str(new_blocklist_entries))

            if existing_blocklist_entries != new_blocklist_entries or forced:
                await connection.execute('DELETE FROM subscribe_blocklists WHERE did = $1', ident)

                for uri in existing_blocklist_entries:
                    try:
                        await connection.execute('INSERT INTO subscribe_blocklists_transaction (uri, date_added, touched, touched_actor) VALUES ($1, $2, $3, $4)', uri, datetime.now(pytz.utc), datetime.now(pytz.utc), touched_actor)
                    except Exception as e:
                        logger.error(f"Error updating subscribe_blocklists_transaction on delete : {e}")
                        logger.error(existing_blocklist_entries)

                logger.info("subscribe Blocklist transaction[deleted] updated.")

                if data:
                    try:
                        # Insert the new blocklist entries
                        await connection.executemany('INSERT INTO subscribe_blocklists (did, uri, list_uri, cid, date_added, record_type, touched, touched_actor) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', data)

                        await connection.executemany('INSERT INTO subscribe_blocklists_transaction (did, uri, list_uri, cid, date_added, record_type, touched, touched_actor) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)', data)
                    except Exception as e:
                        logger.error(f"Error updating subscribe_blocklists or subscribe_blocklists_transaction on create: {e}")
                        logger.error(new_blocklist_entries)

                    logger.info("Subscribe Blocklist transaction[created] updated.")
                    logger.info(f"Subscribe blocklist added for: {ident}")

                subscribe_list_counter += 1

                return subscribe_list_counter
            else:
                logger.info("Blocklist not updated already exists.")

                return subscribe_list_counter


@check_db_connection("write")
async def update_mutelist_tables(ident, mutelists_data, mutelists_users_data, forced=False):
    list_counter = 0
    user_counter = 0

    if not forced:
        touched_actor = "crawler"
    else:
        touched_actor = "forced_crawler"

    if mutelists_data is None:
        counter = [list_counter, user_counter]

        return counter

    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():

            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT uri FROM {} WHERE did = $1'.format(mute_lists_table), ident
            )

            existing_mutelist_entries = {(record['uri']) for record in existing_records}
            logger.debug("Existing entires " + ident + ": " + str(existing_mutelist_entries))

            # Create a list of tuples containing the data to be inserted
            mutelist_records_to_insert = [
                (record["url"], record["uri"], record["did"], record["cid"], record["name"], record["created_at"], record["description"], datetime.now(pytz.utc), touched_actor)
                for record in mutelists_data]

            # Convert the new mutelist entries to a set for comparison
            new_mutelist_entries = {(record[1]) for record in mutelist_records_to_insert}
            logger.debug("New mutelist entries for " + ident + ": " + str(new_mutelist_entries))

            # Check if there are differences between the existing and new mutelist entries
            if existing_mutelist_entries != new_mutelist_entries or forced:
                # Delete existing mutelist entries for the specified ident
                await connection.execute('DELETE FROM mutelists WHERE did = $1', ident)

                for uri in existing_mutelist_entries:
                    try:
                        await connection.execute('INSERT INTO mutelists_transaction (uri, created_date, touched, touched_actor) VALUES ($1, $2, $3, $4)', uri, datetime.now(pytz.utc), datetime.now(pytz.utc), touched_actor)
                    except Exception as e:
                        logger.error(f"Error updating mutelists_transaction on delete: {e}")
                        logger.error(existing_mutelist_entries)

                logger.info("Mutelist transaction[deleted] updated.")

                if mutelist_records_to_insert:
                    try:
                        # Insert the new mutelist entries
                        await connection.executemany("""INSERT INTO {} (url, uri, did, cid, name, created_date, description, touched, touched_actor) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""".format(mute_lists_table), mutelist_records_to_insert)

                        await connection.executemany("""INSERT INTO mutelists_transaction (url, uri, did, cid, name, created_date, description, touched, touched_actor) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""", mutelist_records_to_insert)
                    except Exception as e:
                        logger.error(f"Error updating mutelists or mutelists_transaction on create: {e}")
                        logger.error(new_mutelist_entries)

                    logger.info("Mutelist transaction[created] updated.")
                    logger.info(f"Mute list(s) added for: {ident}")

                list_counter += 1
            else:
                logger.debug("Mutelist not updated; already exists.")

            if mutelists_users_data:
                # Retrieve the existing mutelist entries for the specified ident
                existing_users_records = await connection.fetch(
                    """SELECT listitem_uri
                        FROM mutelists_users
                        WHERE owner_did = $1""", ident
                )

                existing_mutelist_users_entries = {(record['listitem_uri']) for record in existing_users_records}

                logger.debug("Existing entires " + ident + ": " + str(existing_mutelist_users_entries))

                # Create a list of tuples containing the data to be inserted
                mutelistusers_records_to_insert = [
                    (record['list_uri'], record["cid"], record['subject'], record['author'], record["created_at"], datetime.now(pytz.utc), touched_actor, record['listitem_uri'])
                    for record in mutelists_users_data]

                # Convert the new mutelist entries to a set for comparison
                new_mutelist_users_entries = {(record[7]) for record in mutelistusers_records_to_insert}

                logger.debug("New mutelist users entries for " + ident + ": " + str(new_mutelist_users_entries))

                # Check if there are differences between the existing and new mutelist entries
                if existing_mutelist_users_entries != new_mutelist_users_entries or forced:
                    for uri in existing_mutelist_users_entries:
                        try:
                            # Delete existing mutelist entries for the specified ident
                            await connection.execute("""DELETE FROM mutelists_users WHERE listitem_uri = $1""", uri)

                            await connection.execute("""INSERT INTO mutelists_users_transaction (listitem_uri, date_added, touched, touched_actor) VALUES ($1, $2, $3, $4)""", uri, datetime.now(pytz.utc), datetime.now(pytz.utc), touched_actor)
                        except Exception as e:
                            logger.error(f"Error updating mutelists_users or mutelists_users_transaction on delete: {e}")
                            logger.error(existing_mutelist_users_entries)

                    logger.info("Mutelist users transaction[deleted] updated.")

                    if mutelistusers_records_to_insert:
                        try:
                            # Insert the new mutelist entries
                            await connection.executemany("""INSERT INTO {} (list_uri, cid, subject_did, owner_did, date_added, touched, touched_actor, listitem_uri) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""".format(mute_lists_users_table), mutelistusers_records_to_insert)

                            await connection.executemany("""INSERT INTO mutelists_users_transaction (list_uri, cid, subject_did, owner_did, date_added, touched, touched_actor, listitem_uri) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""", mutelistusers_records_to_insert)
                        except Exception as e:
                            logger.error(f"Error updating mutelists_users or mutelists_users_transaction on create: {e}")
                            logger.error(new_mutelist_users_entries)

                        logger.info("Mutelist users transaction[created] updated.")
                        logger.info(f"Mute list user(s) added for: {ident}")

                    user_counter += 1

                    counter = [list_counter, user_counter]

                    return counter
                else:
                    logger.debug("Mutelist not updated; already exists.")

                    counter = [list_counter, user_counter]

                    return counter
            else:
                counter = [list_counter, user_counter]

                return counter


@check_db_connection("write")
async def does_did_and_handle_exist_in_database(did, handle):
    max_retries = 5
    for retry in range(max_retries):
        try:
            async with connection_pools["write"].acquire() as connection:
                # Execute the SQL query to check if the given DID exists in the "users" table
                exists = await connection.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE did = $1 AND handle = $2)', did, handle)

                return exists
        except TimeoutError:
            logger.error(f"Timeout error on attempt {retry + 1}. Retrying in 10 sec...")
            await asyncio.sleep(10)
            if retry == max_retries - 1:
                logger.error("Max retries reached, failing to False.")

                return False
        except Exception as e:
            logger.error(f"Error during does_did_and_handle_exist_in_database: {e}")

            return False


@check_db_connection("write")
async def update_user_handles(handles_to_update):
    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            # Drop the temporary table if it exists
            await connection.execute('DROP TABLE IF EXISTS temp_handles')

            # Create a temporary table to hold the handles to update
            await connection.execute('''
                CREATE TEMP TABLE temp_handles (
                    did TEXT PRIMARY KEY,
                    handle TEXT
                )
            ''')

            # Populate the temporary table with the handles to update
            for did, handle in handles_to_update:
                await connection.execute('''
                    INSERT INTO temp_handles (did, handle)
                    VALUES ($1, $2)
                ''', did, handle)

            # Update the users table using the temporary table
            await connection.execute('''
                INSERT INTO users (did, handle)
                SELECT t.did, t.handle
                FROM temp_handles AS t
                ON CONFLICT (did) DO UPDATE
                SET handle = EXCLUDED.handle
            ''')

            # await connection.execute("delete from resolution_queue where did = $1", did)  # Remove the DID from the resolution queue

        logger.info(f"Updated {len(handles_to_update)} handles in the database.")


@check_db_connection("write")
async def add_new_prefixes(handles):
    for handle in handles:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                try:
                    query = """INSERT INTO user_prefixes(handle, prefix1, prefix2, prefix3)
                                    VALUES($1,
                                    SUBSTRING($1, 1, 1),
                                    SUBSTRING($1, 1, 2),
                                    SUBSTRING($1, 1, 3)
                                    ) ON CONFLICT (handle) DO NOTHING;"""

                    await connection.execute(query, handle)
                except Exception as e:
                    logger.error(f"Error updating prefixes: {e}")


@check_db_connection("write")
async def process_batch(batch_dids, ad_hoc, batch_size):
    batch_handles_and_dids = await utils.fetch_handles_batch(batch_dids, ad_hoc)
    logger.info("Batch resolved.")

    # Split the batch of handles into smaller batches
    batch_size = batch_size  # You can adjust this batch size based on your needs
    handle_batches = [batch_handles_and_dids[i:i + batch_size] for i in range(0, len(batch_handles_and_dids), batch_size)]

    # Update the database with the batch of handles
    total_handles_updated = 0
    for handle_batch in handle_batches:
        # Collect handles that need to be updated in this batch
        handles_to_update = []
        logger.debug(str(handle_batch))
        for did, handle in handle_batch:
            if "did:web" in did:
                pds = await on_wire.resolve_did(did, did_web_pds=True)
                if pds:
                    await update_pds(did, pds)

            # Check if the DID and handle combination already exists in the database
            logger.debug("Did: " + str(did) + " | handle: " + str(handle))
            if await does_did_and_handle_exist_in_database(did, handle):
                logger.debug(f"DID {did} with handle {handle} already exists in the database. Skipping...")
            else:
                handles_to_update.append((did, handle))

        if handles_to_update:
            # only_handles = []
            try:
                # Update the database with the batch of handles
                logger.info("committing batch.")
                async with connection_pools["write"].acquire() as connection:
                    async with connection.transaction():
                        await update_user_handles(handles_to_update)
                        total_handles_updated += len(handles_to_update)

                # for did, handle in handles_to_update:
                #     only_handles.append(handle)

                # logger.info("Adding new prefixes.")
                # await add_new_prefixes(only_handles)

                # Update the temporary table with the last processed DID
                # last_processed_did = handle_batch[-1][0]  # Assuming DID is the first element in each tuple
                # logger.debug("Last processed DID: " + str(last_processed_did))
                # if table:
                #     await update_temporary_table(last_processed_did, table)
            except asyncpg.ConnectionDoesNotExistError as e:
                logger.warning("Connection error, retrying in 30 seconds...")
                await asyncio.sleep(30)  # Retry after 60 seconds
            except Exception as e:
                # Handle other exceptions as needed
                logger.error(f"Error during batch update: {e}")
                break  # Break the loop on other exceptions

    return total_handles_updated


@check_db_connection("write")
async def create_temporary_table(table):
    try:
        logger.info(f"Creating temp table: {table}")
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    last_processed_did text PRIMARY KEY,
                    touched timestamptz
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error(f"Error creating temporary table: {table} %s", e)


@check_db_connection("write")
async def create_new_users_temporary_table():
    try:
        logger.info("Creating temp table.")
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """
                CREATE TABLE IF NOT EXISTS new_users_temporary_table (
                    last_processed_did text PRIMARY KEY,
                    touched timestamptz
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error("Error creating temporary table: %s", e)


@check_db_connection("write")
async def update_24_hour_block_list_table(entries, list_type):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                data = [(did, count, list_type) for did, count in entries]
                # Insert the new row with the given last_processed_did
                query = "INSERT INTO top_twentyfour_hour_block (did, count, list_type) VALUES ($1, $2, $3)"
                await connection.executemany(query, data)
                logger.info("Updated top 24 block table.")
    except asyncpg.exceptions.UniqueViolationError:
        logger.warning("Attempted to insert duplicate entry into top block table")
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


@check_db_connection("write")
async def truncate_top_blocks_table():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing rows if it exists
                await connection.execute("TRUNCATE top_block")
                logger.info("Truncated block table.")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


@check_db_connection("write")
async def truncate_top24_blocks_table():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                await connection.execute("TRUNCATE top_twentyfour_hour_block")
                logger.info("Truncated top 24 block table.")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


@check_db_connection("write")
async def update_top_block_list_table(entries, list_type):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                data = [(did, count, list_type) for did, count in entries]
                # Insert the new row with the given last_processed_did
                query = "INSERT INTO top_block (did, count, list_type) VALUES ($1, $2, $3)"
                await connection.executemany(query, data)
                logger.info("Updated top block table")
    except asyncpg.exceptions.UniqueViolationError:
        logger.warning("Attempted to insert duplicate entry into top block table")
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


@check_db_connection("write")
async def get_top_blocks_list() -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query1 = "SELECT distinct did, count FROM top_block WHERE list_type = 'blocked'"
                query2 = "SELECT distinct did, count FROM top_block WHERE list_type = 'blocker'"
                blocked_rows = await connection.fetch(query1)
                blocker_rows = await connection.fetch(query2)

                return blocked_rows, blocker_rows
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return [], []


@check_db_connection("write")
async def get_24_hour_block_list():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query1 = "SELECT distinct did, count FROM top_twentyfour_hour_block WHERE list_type = 'blocked'"
                query2 = "SELECT distinct did, count FROM top_twentyfour_hour_block WHERE list_type = 'blocker'"
                blocked_rows = await connection.fetch(query1)
                blocker_rows = await connection.fetch(query2)

                return blocked_rows, blocker_rows
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return [], []


@check_db_connection("write")
async def delete_temporary_table(table):
    if table:
        try:
            async with connection_pools["write"].acquire() as connection:
                async with connection.transaction():
                    query = f"DROP TABLE IF EXISTS {table}"
                    await connection.execute(query)
        except Exception as e:
            logger.error("Error deleting temporary table: %s", e)


@check_db_connection("write")
async def delete_new_users_temporary_table():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "DROP TABLE IF EXISTS new_users_temporary_table"
                await connection.execute(query)
    except Exception as e:
        logger.error("Error deleting temporary table: %s", e)


@check_db_connection("write")
async def update_temporary_table(last_processed_did, table):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                delete_query = f"TRUNCATE {table}"
                await connection.execute(delete_query)
                touched = datetime.now(pytz.utc)
                # Insert the new row with the given last_processed_did
                insert_query = f"INSERT INTO {table} (last_processed_did, touched) VALUES ($1, $2)"
                await connection.execute(insert_query, last_processed_did, touched)
    except Exception as e:
        logger.error("Error updating temporary table: %s", e)


@check_db_connection("write")
async def update_new_users_temporary_table(last_processed_did):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                await connection.execute("TRUNCATE new_users_temporary_table")

                # Insert the new row with the given last_processed_did
                query = "INSERT INTO new_users_temporary_table (last_processed_did) VALUES ($1)"
                await connection.execute(query, last_processed_did)
    except Exception as e:
        logger.error("Error updating temporary table: %s", e)


@check_db_connection("read")
async def get_top_blocks():
    blocked_results = []
    blockers_results = []

    logger.info("Getting top blocks from db.")
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():

                # Insert the new row with the given last_processed_did
                blocked_query = '''SELECT b.blocked_did, COUNT(*) AS block_count
                                    FROM blocklists AS b
                                    WHERE b.blocked_did IN (
                                    SELECT u.did
                                    FROM users AS u
                                    WHERE u.status = TRUE
                                )
                                    GROUP BY b.blocked_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blocked_data = await connection.fetch(blocked_query)
                blocked_results.append(blocked_data)

                blockers_query = '''SELECT b.user_did, COUNT(*) AS block_count
                                    FROM blocklists AS b
                                    WHERE b.user_did IN (
                                    SELECT u.did
                                    FROM users AS u
                                    WHERE u.status = TRUE
                                )
                                    GROUP BY b.user_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blockers_data = await connection.fetch(blockers_query)
                blockers_results.append(blockers_data)

                return blocked_data, blockers_data
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")

        return None, None
    except Exception as e:
        logger.error("Error retrieving data from db", e)


@check_db_connection("write")
async def update_did_service(data, label_data):
    pop_count = 0
    logger.info("Updating services information for batch.")

    # pop = "delete from resolution_queue where did = $1"

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                if data:
                    for record in data:
                        query = """SELECT did, pds, created_date FROM users where did = $1"""

                        did_exists = await connection.fetch(query, record[0])

                        if did_exists:
                            if not did_exists[0]["pds"] or not did_exists[0]["created_date"]:
                                insert_pds_query = """UPDATE users SET created_date = $2, pds = $3 WHERE did = $1"""

                                await connection.execute(insert_pds_query, record[0], record[1], record[2])
                                # await connection.execute(pop, record[0])
                                # logger.debug(f"pop: {record[0]}")
                                # pop_count += 1
                            elif did_exists[0]["pds"] != record[2]:
                                old_pds = did_exists[0]["pds"]
                                update_query = """UPDATE users SET pds = $2 WHERE did = $1"""

                                await connection.execute(update_query, record[0], record[2])
                                # await connection.execute(pop, record[0])
                                # logger.debug(f"pop: {record[0]}")
                                # pop_count += 1

                                logger.info(f"Updated pds for: {record[0]} | from {old_pds} to {record[2]}")
                            else:
                                # await connection.execute(pop, record[0])
                                # logger.debug(f"pop: {record[0]}")
                                # pop_count += 1
                                # logger.debug("Up to date.")
                                continue
                        else:
                            insert_query = """INSERT INTO users (did, created_date, pds, handle, status) VALUES ($1, $2, $3, $4, $5)"""

                            if utils.is_did(record[0]):
                                await connection.execute(insert_query, record[0], record[1], record[2], record[3], True)
                            else:
                                await connection.execute(insert_query, record[0], record[1], record[2], record[3], False)

                            # await connection.execute(pop, record[0])
                            # logger.debug(f"pop: {record[0]}")
                            # pop_count += 1

                    if label_data:
                        for label in label_data:
                            display_name, description = await on_wire.get_avatar_id(label['did'], True)

                            query = """SELECT did, endpoint, created_date FROM labelers where did = $1"""

                            did_exists = await connection.fetch(query, label["did"])

                            if did_exists:
                                if not did_exists[0]["endpoint"] or not did_exists[0]["created_date"]:
                                    insert_label_query = """UPDATE labelers SET created_date = $2, endpoint = $3 WHERE did = $1"""

                                    await connection.execute(insert_label_query, label["did"], label["endpoint"], label["createdAt"])
                                    # await connection.execute(pop, label["did"])
                                    # logger.debug(f"pop: {label['did']}")
                                    # pop_count += 1
                                elif not did_exists[0].get("name"):
                                    insert_label_query = """UPDATE labelers SET name = $2 WHERE did = $1"""

                                    await connection.execute(insert_label_query, label["did"], display_name)
                                elif not did_exists[0].get("description"):
                                    insert_label_query = """UPDATE labelers SET description = $2 WHERE did = $1"""

                                    await connection.execute(insert_label_query, label["did"], description)
                                elif did_exists[0].get("description") != description:
                                    old_description = did_exists[0]["description"]
                                    update_query = """UPDATE labelers SET description = $2 WHERE did = $1"""

                                    await connection.execute(update_query, label["did"], description)

                                    logger.debug(f"Updated description for: {label['did']} | from {old_description} to {description}")
                                elif did_exists[0]["name"] != display_name:
                                    old_name = did_exists[0]["name"]
                                    update_query = """UPDATE labelers SET name = $2 WHERE did = $1"""

                                    await connection.execute(update_query, label["did"], display_name)

                                    logger.debug(f"Updated name for: {label['did']} | from {old_name} to {display_name}")
                                elif did_exists[0]["endpoint"] != label["endpoint"]:
                                    old_endpoint = did_exists[0]["endpoint"]
                                    update_query = """UPDATE labelers SET endpoint = $2 WHERE did = $1"""

                                    await connection.execute(update_query, label["did"], label["endpoint"])

                                    logger.debug(f"Updated endpoint for: {label['did']} | from {old_endpoint} to {label['endpoint']}")
                                    # await connection.execute(pop, label['did'])
                                    # logger.debug(f"pop: {label['did']}")
                                    # pop_count += 1
                                else:
                                    # await connection.execute(pop, label['did'])
                                    # logger.debug(f"pop: {label['did']}")
                                    # pop_count += 1
                                    logger.debug("Up to date.")
                            else:
                                insert_label_query = """INSERT INTO labelers (did, endpoint, created_date, name, description) VALUES ($1, $2, $3, $4, $5)"""
                                await connection.execute(insert_label_query, label["did"], label["endpoint"], label["createdAt"], display_name, description)
                                # await connection.execute(pop, label["did"])
                                logger.debug(f"pop: {label['did']}")
                                pop_count += 1

                logger.info(f"Popped {pop_count} times from resolution queue.")
    except Exception as e:
        logger.error("Error retrieving/inserting labeler data to db", e)


@check_db_connection("write")
async def update_last_created_did_date(last_created):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                delete_query = f"TRUNCATE {last_created_table}"
                await connection.execute(delete_query)

                # Insert the new row with the given last_processed_did
                insert_query = f"INSERT INTO {last_created_table} (last_created) VALUES ($1)"
                await connection.execute(insert_query, last_created)
    except Exception as e:
        logger.error("Error updating temporary table: %s", e)


@check_db_connection("write")
async def check_last_created_did_date():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT * FROM last_did_created_date"""

                value = await connection.fetchval(query)

                return value
    except Exception as e:
        logger.error("Error retrieving data", e)

        return None


@check_db_connection("read")
async def get_block_stats():
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                logger.info("Getting block statistics.")

                query_1 = '''SELECT COUNT(blocked_did) from blocklists'''
                query_2 = '''select count(distinct blocked_did) from blocklists'''
                query_3 = '''select count(distinct user_did) from blocklists'''
                query_4 = '''SELECT COUNT(*) AS user_count 
                                FROM (
                                SELECT user_did
                                FROM blocklists
                                GROUP BY user_did
                                HAVING COUNT(DISTINCT blocked_did) = 1
                            ) AS subquery'''
                query_5 = '''SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) BETWEEN 2 AND 100
                                ) AS subquery'''
                query_6 = '''SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) BETWEEN 101 AND 1000
                                ) AS subquery'''
                query_7 = '''SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) > 1000
                                ) AS subquery'''
                query_8 = '''SELECT AVG(block_count) AS mean_blocks
                                FROM (
                                    SELECT user_did, COUNT(DISTINCT blocked_did) AS block_count
                                    FROM blocklists
                                    GROUP BY user_did
                                ) AS subquery'''
                query_9 = '''SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) = 1
                                ) AS subquery'''
                query_10 = '''SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) BETWEEN 2 AND 100
                                ) AS subquery'''
                query_11 = '''SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) BETWEEN 101 AND 1000
                                ) AS subquery'''
                query_12 = '''SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) > 1000
                                ) AS subquery'''
                query_13 = '''SELECT AVG(block_count) AS mean_blocks
                                FROM (
                                    SELECT blocked_did, COUNT(DISTINCT user_did) AS block_count
                                    FROM blocklists
                                    GROUP BY blocked_did
                                ) AS subquery'''

                number_of_total_blocks = await connection.fetchval(query_1)
                logger.info("Completed query 1")
                number_of_unique_users_blocked = await connection.fetchval(query_2)
                logger.info("Completed query 2")
                number_of_unique_users_blocking = await connection.fetchval(query_3)
                logger.info("Completed query 3")
                number_block_1 = await connection.fetchval(query_4)
                logger.info("Completed query 4")
                number_blocking_2_and_100 = await connection.fetchval(query_5)
                logger.info("Completed query 5")
                number_blocking_101_and_1000 = await connection.fetchval(query_6)
                logger.info("Completed query 6")
                number_blocking_greater_than_1000 = await connection.fetchval(query_7)
                logger.info("Completed query 7")
                average_number_of_blocks = await connection.fetchval(query_8)
                logger.info("Completed query 8")
                number_blocked_1 = await connection.fetchval(query_9)
                logger.info("Completed query 9")
                number_blocked_2_and_100 = await connection.fetchval(query_10)
                logger.info("Completed query 10")
                number_blocked_101_and_1000 = await connection.fetchval(query_11)
                logger.info("Completed query 11")
                number_blocked_greater_than_1000 = await connection.fetchval(query_12)
                logger.info("Completed query 12")
                average_number_blocked = await connection.fetchval(query_13)
                logger.info("Completed query 13")
                total_users = await get_user_count(get_active=False)
                logger.info("Completed query 14")

                logger.info("All blocklist queries complete.")

                return (number_of_total_blocks, number_of_unique_users_blocked, number_of_unique_users_blocking,
                        number_block_1, number_blocking_2_and_100, number_blocking_101_and_1000, number_blocking_greater_than_1000,
                        average_number_of_blocks, number_blocked_1, number_blocked_2_and_100, number_blocked_101_and_1000,
                        number_blocked_greater_than_1000, average_number_blocked, total_users)
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")

        return None, None, None, None, None, None, None, None, None, None, None, None, None, None
    except Exception as e:
        logger.error(f"Error retrieving data from db: {e}")


@check_db_connection("read")
async def get_top24_blocks():
    blocked_results = []
    blockers_results = []

    logger.info("Getting top 24 blocks from db.")
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():

                # Insert the new row with the given last_processed_did
                blocked_query = '''SELECT b.blocked_did, COUNT(*) AS block_count
                                    FROM blocklists AS b
                                    JOIN users AS u ON b.blocked_did = u.did AND u.status = TRUE
                                    WHERE b.block_date::date >= CURRENT_DATE - INTERVAL '1 day'
                                    GROUP BY b.blocked_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blocked_data = await connection.fetch(blocked_query)
                blocked_results.append(blocked_data)

                blockers_query = '''SELECT b.user_did, COUNT(*) AS block_count
                                    FROM blocklists as b
                                    JOIN users AS u ON b.user_did = u.did AND u.status = TRUE
                                    WHERE b.block_date::date >= CURRENT_DATE - INTERVAL '1 day'
                                    GROUP BY user_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blockers_data = await connection.fetch(blockers_query)
                blockers_results.append(blockers_data)

                return blocked_data, blockers_data
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")

        return None, None
    except Exception as e:
        logger.error("Error retrieving data from db", e)


@check_db_connection("read")
async def get_similar_blocked_by(user_did):
    global all_blocks_cache

    async with connection_pools["read"].acquire() as connection:
        blocked_by_users = await connection.fetch(
            'SELECT user_did FROM blocklists WHERE blocked_did = $1', user_did)

    # Extract the values from the records
    blocked_by_users_ids = [record['user_did'] for record in blocked_by_users]

    if not all_blocks_cache:
        logger.info("Caching all blocklists.")

        block_cache_status.set()

        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                # Fetch all blocklists except for the specified user's blocklist
                all_blocklists_rows = await connection.fetch(
                    'SELECT user_did, blocked_did FROM blocklists'
                )
                all_blocks_cache = all_blocklists_rows

                block_cache_status.clear()
    else:
        all_blocklists_rows = all_blocks_cache

    # Create a dictionary to store blocklists as sets
    blocklists = {}
    for row in all_blocklists_rows:
        user_id = row['user_did']
        blocked_id = row['blocked_did']
        if user_id not in blocklists:
            blocklists[user_id] = set()
        blocklists[user_id].add(blocked_id)

    # Calculate match percentages for each person's block list
    user_match_percentages = {}
    for blocked_by_user_id in blocked_by_users_ids:
        if blocked_by_user_id != user_did:
            common_blocked = set(blocklists.get(blocked_by_user_id, set())) & set(blocklists.get(user_did, set()))
            common_count = len(common_blocked)
            person_blocked_count = len(blocklists.get(blocked_by_user_id, set()))

            # Check for division by zero
            if person_blocked_count == 0:
                continue  # Skip this comparison

            match_percentage = (common_count / person_blocked_count) * 100

            # Only include matches with a match_percentage greater than 1
            if match_percentage > 1:
                user_match_percentages[blocked_by_user_id] = match_percentage

    # Sort users by match percentage
    sorted_users = sorted(user_match_percentages.items(), key=lambda x: x[1], reverse=True)

    # Select top 20 users
    top_similar_users = sorted_users[:20]

    logger.info(top_similar_users)

    users = [user for user, percentage in top_similar_users]
    percentages = [percentage for user, percentage in top_similar_users]
    status_list = []
    for user, percentage in top_similar_users:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                status = await connection.fetch(
                    'SELECT status FROM users WHERE did = $1', user)
                status_list.append(status)

    # Return the sorted list of users and their match percentages
    return users, percentages, status_list


@check_db_connection("read")
async def get_similar_users(user_did):
    global all_blocks_cache
    global all_blocks_process_time
    global all_blocks_last_update

    all_blocks = all_blocks_cache.get("blocks")

    if not all_blocks:
        logger.info("Caching all blocklists.")
        start_time = datetime.now()

        block_cache_status.set()

        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                # Fetch all blocklists except for the specified user's blocklist
                all_blocklists_rows = await connection.fetch(
                    'SELECT user_did, blocked_did FROM blocklists'
                )
                all_blocks_cache["blocks"] = all_blocklists_rows

                block_cache_status.clear()
                end_time = datetime.now()
                if start_time is not None:
                    all_blocks_process_time = end_time - start_time
                all_blocks_last_update = end_time
    else:
        all_blocklists_rows = all_blocks

    # Create a dictionary to store blocklists as sets
    blocklists = {}
    specific_user_blocklist = set()

    for row in all_blocklists_rows:
        user_id = row['user_did']
        blocked_id = row['blocked_did']
        if user_id != user_did:
            if user_id not in blocklists:
                blocklists[user_id] = set()
            blocklists[user_id].add(blocked_id)
        else:
            # Get the specific user's blocklist
            specific_user_blocklist.add(blocked_id)

    if not len(specific_user_blocklist):
        users = "no blocks"
        percentages = 0
        status = None

        return users, percentages, status

    # Calculate match percentage for each user
    user_match_percentages = {}

    for other_user_did, other_user_blocklist in blocklists.items():
        common_blocked_users = specific_user_blocklist & other_user_blocklist
        common_count = len(common_blocked_users)
        specific_count = len(specific_user_blocklist)
        other_count = len(other_user_blocklist)

        # Check for division by zero
        if specific_count == 0 or other_count == 0:
            continue  # Skip this comparison

        match_percentage = (common_count / specific_count) * 100

        if match_percentage > 1:  # set threshold for results
            user_match_percentages[other_user_did] = match_percentage

    # Sort users by match percentage
    sorted_users = sorted(user_match_percentages.items(), key=lambda x: x[1], reverse=True)

    # Select top 20 users
    top_similar_users = sorted_users[:20]

    logger.info(f"Similar blocks: {await get_user_handle(user_did)} | {top_similar_users}")

    users = [user for user, percentage in top_similar_users]
    percentages = [percentage for user, percentage in top_similar_users]
    status_list = []
    for user, percentage in top_similar_users:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                status = await connection.fetchval(
                    'SELECT status FROM users WHERE did = $1', user)
                status_list.append(status)
    logger.info(status_list)
    return users, percentages, status_list


async def blocklists_updater():
    global last_update_top_block
    global blocklist_updater_status
    global top_blocks_start_time
    global top_blocks_process_time
    global top_blocked_as_of_time

    blocked_list = "blocked"
    blocker_list = "blocker"

    top_blocks_start_time = datetime.now()
    blocklist_updater_status.set()

    logger.info("Updating top blocks lists requested.")

    await truncate_top_blocks_table()

    blocked_results, blockers_results = await get_top_blocks()  # Get blocks for db
    logger.debug(f"blocked count: {len(blocked_results)} blockers count: {len(blockers_results)}")

    # Update blocked entries
    await update_top_block_list_table(blocked_results, blocked_list)  # add blocked to top blocks table
    logger.info("Updated top blocked db.")

    await update_top_block_list_table(blockers_results, blocker_list)  # add blocker to top blocks table
    logger.info("Updated top blockers db.")

    top_blocked, top_blockers, blocked_aid, blocker_aid = await utils.resolve_top_block_lists()
    logger.info("Top blocks lists page updated.")

    blocklist_updater_status.clear()

    last_update_top_block = datetime.now()
    end_time = datetime.now()

    if top_blocks_start_time is not None:
        top_blocks_process_time = end_time - top_blocks_start_time

    top_blocks_start_time = None

    top_blocked_as_of_time = datetime.now().isoformat()

    return top_blocked, top_blockers, blocked_aid, blocker_aid


async def top_24blocklists_updater():
    global last_update_top_24_block
    global blocklist_24_updater_status
    global top_24_blocks_start_time
    global top_24_blocks_process_time
    global top_24_blocked_as_of_time

    blocked_list_24 = "blocked"
    blocker_list_24 = "blocker"

    top_24_blocks_start_time = datetime.now()
    blocklist_24_updater_status.set()

    logger.info("Updating top 24 blocks lists requested.")

    await truncate_top24_blocks_table()

    blocked_results_24, blockers_results_24 = await get_top24_blocks()  # Get blocks for db

    logger.debug(f"blocked count: {len(blocked_results_24)} blockers count: {len(blockers_results_24)}")

    # Update blocked entries
    await update_24_hour_block_list_table(blocked_results_24, blocked_list_24)  # add blocked to top blocks table

    logger.info("Updated top blocked db.")

    await update_24_hour_block_list_table(blockers_results_24, blocker_list_24)  # add blocker to top blocks table

    logger.info("Updated top blockers db.")

    top_blocked_24, top_blockers_24, blocked_aid_24, blocker_aid_24 = await utils.resolve_top24_block_lists()

    logger.info("Top 24 hour blocks lists page updated.")

    blocklist_24_updater_status.clear()

    last_update_top_24_block = datetime.now()
    end_time = datetime.now()

    if top_24_blocks_start_time is not None:
        top_24_blocks_process_time = end_time - top_24_blocks_start_time

    top_24_blocks_start_time = None

    top_24_blocked_as_of_time = datetime.now().isoformat()

    return top_blocked_24, top_blockers_24, blocked_aid_24, blocker_aid_24


@check_db_connection("read")
async def get_mutelists(ident) -> Optional[list]:
    async with connection_pools["read"].acquire() as connection:
        async with connection.transaction():
            query = """
            SELECT ml.url, u.handle, u.status, ml.name, ml.description, ml.created_date, mu.date_added, mc.user_count
            FROM mutelists AS ml
            INNER JOIN mutelists_users AS mu ON ml.uri = mu.list_uri
            INNER JOIN users AS u ON ml.did = u.did -- Join the users table to get the handle
            LEFT JOIN mutelists_user_count AS mc ON ml.uri = mc.list_uri
            WHERE mu.subject_did = $1
            """
            try:
                mute_lists = await connection.fetch(query, ident)
            except Exception as e:
                logger.error(f"Error retrieving DIDs without handles: {e}")

                return None

            lists = []
            for record in mute_lists:
                data = {
                    "url": record['url'],
                    "handle": record['handle'],
                    "status": record['status'],
                    "name": record['name'],
                    "description": record['description'],
                    "created_date": record['created_date'].isoformat(),
                    "date_added": record['date_added'].isoformat(),
                    "list user count": record['user_count']
                }
                lists.append(data)

            return lists


@check_db_connection("read")
async def check_api_key(api_environment, key_type, key_value) -> bool:
    async with connection_pools["read"].acquire() as connection:
        async with connection.transaction():
            query = """SELECT valid FROM API WHERE key = $3 environment = $1 AND access_type LIKE '%' || $2 || '%'"""

            status = await connection.fetchval(query, api_environment, key_type, key_value)

            return status


async def wait_for_redis() -> None:
    while True:
        status = await redis_connected()

        if status:
            global redis_connection
            global once

            redis_connection = True
            once = False
            logger.info("Redis reconnected.")

            break
        else:
            redis_connection = False

            logger.warning("Redis disconnected.")

            await asyncio.sleep(30)


@check_db_connection("write")
async def tables_exists() -> bool:
    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            try:
                query1 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
                query2 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
                query3 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
                query4 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""

                users_exist = await connection.fetchval(query1, users_table)
                blocklists_exist = await connection.fetchval(query2, blocklist_table)
                top_blocks_exist = await connection.fetchval(query3, top_blocks_table)
                top_24_exist = await connection.fetchval(query4, top_24_blocks_table)

                values = (users_exist, blocklists_exist, top_blocks_exist, top_24_exist)

                if any(value is False for value in values):

                    return False
                else:

                    return True
            except asyncpg.ConnectionDoesNotExistError:
                logger.error("Error checking if schema exists, db not connected.")

                return False


@check_db_connection("write")
async def get_unique_did_to_pds() -> Optional[list]:
    logger.info("Getting unique did to pds.")

    records = []

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                records_to_check = await connection.fetch("""SELECT did, pds
                                                                FROM (
                                                                    SELECT did, pds, row_number() OVER (PARTITION BY pds ORDER BY random()) as rn
                                                                    FROM users
                                                                    WHERE pds IS NOT NULL
                                                                ) sub
                                                                WHERE rn <= 10
                                                                ORDER BY pds, rn
                """)

                for record in records_to_check:
                    records.append((record['did'], record['pds']))

                logger.info("Retrieved data.")
                logger.info(f"Processing {len(records)} records.")

                return records
    except Exception as e:
        logger.error(f"Error fetching federated pdses: {e}")

        return None


@check_db_connection("write")
async def update_pds_status(pds, status) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """UPDATE pds SET status = $2 WHERE pds = $1"""
                await connection.execute(query, pds, status)
    except Exception as e:
        logger.error(f"Error updating pds status: {e}")


@check_db_connection("write")
async def update_pds(did, pds) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                exists = await connection.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE did = $1 AND pds = $2)', did, pds)

                if not exists:
                    query = """UPDATE users SET pds = $2 WHERE did = $1"""
                    await connection.execute(query, did, pds)
    except Exception as e:
        logger.error(f"Error updating pds: {e}")


@check_db_connection("write")
async def get_didwebs_without_pds() -> Optional[list]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT did FROM users WHERE pds IS NULL AND did LIKE 'did:web%'"""
                dids = await connection.fetch(query)

                records = [record['did'] for record in dids]

                return records
    except Exception as e:
        logger.error(f"Error getting didwebs without pds: {e}")

        return None


@check_db_connection("write")
async def update_did_webs() -> None:
    pop = 0
    did_webs_in_queue = None

    query1 = """UPDATE users SET handle = $2, pds = $3 WHERE did = $1"""
    query2 = """UPDATE users SET pds = $2 WHERE did = $1"""
    query3 = """UPDATE users SET handle = $2 WHERE did = $1"""
    query4 = """SELECT handle FROM did_web_history WHERE did = $1"""

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT DISTINCT(did), timestamp FROM resolution_queue WHERE did LIKE 'did:web%'"""
                try:
                    records = await connection.fetch(query)

                    if not records:
                        records = None
                except Exception as e:
                    logger.error(f"Error getting did:web from resolution queue: {e}")
                    records = None

                if records:
                    did_webs_in_queue = [(record['did'], record['timestamp']) for record in records]

                if did_webs_in_queue:
                    for did, timestamp in did_webs_in_queue:
                        change_handle = False
                        change_pds = False

                        get_info = """SELECT handle, pds, created_date FROM users WHERE did = $1"""
                        user_info = await connection.fetch(get_info, did)
                        if user_info:
                            old_handle = user_info[0]['handle']
                            old_pds = user_info[0]['pds']
                        else:
                            logger.info(f"new did:web: {did}")
                            try:
                                await connection.execute("""INSERT INTO users (did, created_date) VALUES ($1, $2)""", did, timestamp)
                            except Exception as e:
                                logger.error(f"Error inserting new did:web: {e}")

                        handles = await on_wire.resolve_did(did)
                        handle = handles[0] if handles else None

                        try:
                            if handles:
                                handles_in_db = await connection.execute(query4, did)
                            else:
                                handles_in_db = []
                        except Exception as e:
                            logger.error(f"Error getting handles from db: {e}")
                            handles_in_db = []

                        # Compare handles obtained from on_wire.resolve_did with handles in the database
                        missing_handles = [h for h in handles if h not in handles_in_db]

                        # Add missing handles to the database
                        for missing_handle in missing_handles:
                            try:
                                await connection.execute("INSERT INTO did_web_history (did, handle, pds) VALUES ($1, $2, $3)", did, missing_handle, pds)
                            except Exception as e:
                                logger.error(f"Error adding handle {missing_handle} to the database: {e}")

                        pds = await on_wire.resolve_did(did, True)

                        if not await utils.validate_did_atproto(did) and pds is None and handle is None:
                            logger.warning(f"did:web dead: {did}")
                            await connection.execute("""UPDATE users SET status = FALSE WHERE did = $1""", did)
                            await connection.execute(""""UPDATE did_web_history SET status = FALSE WHERE did = $1""", did)
                            await connection.execute("""INSERT INTO did_web_history (did, handle, pds, timestamp, status) VALUES ($1, $2, $3, $4, FALSE)""", did, handle, pds, timestamp)
                            await connection.execute("""delete from resolution_queue where did = $1""", did)
                            continue
                        else:
                            if old_handle != handle:
                                if await on_wire.verify_handle(handle):
                                    change_handle = True
                                else:
                                    change_handle = False
                                    logger.warning(f"invalid handle: {handle} for did:web: {did}")

                            if old_pds != pds:
                                change_pds = True
                            try:
                                if change_handle and change_pds:
                                    await connection.execute(query1, did, handle, pds)
                                elif change_handle and not change_pds:
                                    await connection.execute(query3, did, handle)
                                elif change_pds and not change_handle:
                                    await connection.execute(query2, did, pds)
                                else:
                                    continue
                            except Exception as e:
                                logger.error(f"Error updating did:web: {e}")

                        await connection.execute("""INSERT INTO did_web_history (did, handle, pds, timestamp) VALUES ($1, $2, $3, $4)""", did, handle, pds, timestamp)
                        await connection.execute("""delete from resolution_queue where did = $1""", did)
                        pop += 1

                    logger.info(f"Popped {pop} did:web from resolution queue.")

                logger.info(f"No did:web in queue.")
    except Exception as e:
        logger.error(f"Error updating didwebs: {e}")


@check_db_connection("write")
async def get_didwebs_pdses() -> Optional[list]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT did, pds FROM users WHERE did LIKE 'did:web%'"""
                pds = await connection.fetch(query)

                records = [(record['did'], record['pds']) for record in pds]

                return records
    except Exception as e:
        logger.error(f"Error checking didweb pds: {e}")

        return None


@check_db_connection("write")
async def get_api_keys(environment, key_type, key) -> Optional[dict]:
    if not key and not key_type and not environment:
        logger.error("Missing required parameters for API verification.")

        return None

    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            try:
                query = f"""SELECT a.key as key, a.valid, aa.*
                            FROM api AS a
                            INNER JOIN api_access AS aa ON a.key = aa.key
                            WHERE a.key = $2 AND a.environment = $1 AND a.valid is TRUE"""

                results = await connection.fetch(query, environment, key)

                for item in results:
                    data = {
                        "key": item['key'],
                        "valid": item['valid'],
                        key_type: item[key_type.lower()]
                    }
                return data
            except Exception as e:
                # Handle other exceptions as needed
                logger.error(f"Error getting API keys: {e}")

                return None


@check_db_connection("read")
async def get_dids_per_pds() -> Optional[dict]:
    data_dict = {}

    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT users.pds, COUNT(did) AS did_count
                            FROM users
                            join pds on users.pds = pds.pds
                            WHERE users.pds IS NOT NULL AND pds.status is TRUE
                            GROUP BY users.pds
                            ORDER BY did_count desc"""

                results = await connection.fetch(query)
                for record in results:
                    data_dict[record['pds']] = record['did_count']

                return data_dict
    except Exception as e:
        logger.error(f"Error getting dids per pds: {e}")

        return None


@check_db_connection("write")
async def set_status_code(pds, status_code) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """UPDATE pds SET last_status_code = $2 WHERE pds = $1"""
                await connection.execute(query, pds, status_code)
    except Exception as e:
        logger.error(f"Error updating status code: {e}")


@check_db_connection("write")
async def get_block_row(uri) -> Optional[dict]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT user_did, blocked_did, block_date, cid, uri
                            FROM blocklists
                            WHERE uri = $1"""

                record = await connection.fetch(query, uri)

                if record:
                    result = record[0]
                    response = {
                        "user did": result['user_did'],
                        "blocked did": result['blocked_did'],
                        "block date": result['block_date'],
                        "cid": result['cid'],
                        "uri": result['uri']
                    }

                    return response
                else:
                    raise NotFound
    except Exception as e:
        logger.error(f"Error updating status code: {e}")


@check_db_connection("write")
async def update_mutelist_count() -> None:
    limit = 100
    offset = 0
    list_count = 0
    touched_actor = "crawler"

    logger.info("Updating mutelists counts.")

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                while True:
                    query_1 = """SELECT uri FROM mutelists LIMIT $1 OFFSET $2"""

                    lists = await connection.fetch(query_1, limit, offset)

                    if not lists:
                        break

                    for list_entry in lists:
                        list_uri = list_entry['uri']

                        query_2 = """SELECT COUNT(*) FROM mutelists_users WHERE list_uri = $1"""
                        count_result = await connection.fetchval(query_2, list_uri)

                        try:
                            await connection.execute("""INSERT INTO mutelists_user_count (list_uri, user_count, touched_actor, touched)
                                        VALUES ($1, $2, $3, $4)""", list_uri, count_result, touched_actor, datetime.now(pytz.utc))
                        except Exception as e:
                            logger.error(f"Error updating count: {e}")

                    list_count += len(lists)
                    offset += 100

        logger.info("Mutelists counts updated.")
        logger.info(f"Counted: {list_count} lists")
    except Exception as e:
        logger.error(f"Error updating mutelist count (general): {e}")


@check_db_connection("write")
async def update_subscribe_list_count() -> None:
    limit = 100
    offset = 0
    list_count = 0
    touched_actor = "crawler"

    logger.info("Updating subscribe block list counts.")

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                while True:
                    query_1 = """SELECT uri FROM mutelists LIMIT $1 OFFSET $2"""

                    lists = await connection.fetch(query_1, limit, offset)

                    if not lists:
                        break

                    for list_entry in lists:
                        list_uri = list_entry['uri']

                        query_2 = """SELECT COUNT(*) FROM subscribe_blocklists WHERE list_uri = $1"""
                        count_result = await connection.fetchval(query_2, list_uri)

                        try:
                            await connection.execute("""INSERT INTO subscribe_blocklists_user_count (list_uri, user_count, touched_actor, touched)
                                        VALUES ($1, $2, $3, $4)""", list_uri, count_result, touched_actor, datetime.now(pytz.utc))
                        except Exception as e:
                            logger.error(f"Error updating count: {e}")

                    list_count += len(lists)
                    offset += 100

        logger.info("Subscribe lists counts updated.")
        logger.info(f"Counted: {list_count} subscribe lists")
    except Exception as e:
        logger.error(f"Error updating mutelist count (general): {e}")


@check_db_connection("write")
async def process_delete_queue() -> None:
    logger.info("Processing delete queue.")

    limit = 100
    offset = 0
    pop = 0

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                while True:
                    query = """SELECT uri FROM count_delete_queue LIMIT $1 OFFSET $2"""
                    uris = await connection.fetch(query, limit, offset)

                    if not uris:
                        break

                    for uri in uris:
                        item = uri['uri']

                        if "listitem" in item:
                            try:
                                await connection.execute("""UPDATE mutelists_user_count
                                        SET user_count = user_count - 1
                                        WHERE list_uri IN (
                                        SELECT list_uri
                                        FROM mutelists_users
                                        WHERE listitem_uri = $1)""", item)

                                await connection.execute("""DELETE FROM count_delete_queue WHERE uri = $1""", item)
                                pop = +1
                            except Exception as e:
                                logger.error(f"Error deleting listitem: {e}")

                        elif "listblock" in item:
                            try:
                                await connection.execute("""UPDATE subscribe_blocklists_user_count
                                        SET user_count = user_count - 1
                                        WHERE list_uri IN (
                                        SELECT list_uri
                                        FROM subscribe_blocklists
                                        WHERE uri = $1)""", item)

                                await connection.execute("""DELETE FROM count_delete_queue WHERE uri = $1""", item)
                                pop += 1
                            except Exception as e:
                                logger.error(f"Error deleting listblock: {e}")
                        else:
                            logger.warning(f"Unknown item type: {item}")

                    offset += 100

        logger.info(f"Deleted {pop} entries from delete queue.")
    except Exception as e:
        logger.error(f"Error processing delete queue: {e}")


@check_db_connection("read")
async def identifier_exists_in_db(identifier):
    async with connection_pools["read"].acquire() as connection:
        if utils.is_did(identifier):
            results = await connection.fetch('SELECT did, status FROM users WHERE did = $1', identifier)

            true_record = None

            for result in results:
                # ident = result['did']
                status = result['status']

                if status:
                    ident = True
                    true_record = (ident, status)
                    break

            if true_record:
                ident, status = true_record
            else:
                ident = False
                status = False
        elif utils.is_handle(identifier):
            results = await connection.fetch('SELECT handle, status FROM users WHERE handle = $1', identifier)

            true_record = None

            for result in results:
                # ident = result['handle']
                status = result['status']

                if status:
                    ident = True
                    true_record = (ident, status)
                    break

            if true_record:
                ident, status = true_record
            else:
                ident = False
                status = False
        else:
            ident = False
            status = False

    return ident, status


@check_db_connection("read")
async def get_user_did(handle) -> Optional[str]:
    async with connection_pools["read"].acquire() as connection:
        did = await connection.fetchval('SELECT did FROM users WHERE handle = $1 AND status is True', handle)

    return did


@check_db_connection("read")
async def get_user_handle(did) -> Optional[str]:
    async with connection_pools["read"].acquire() as connection:
        handle = await connection.fetchval('SELECT handle FROM users WHERE did = $1', did)

    return handle


@check_db_connection("read")
async def get_user_count(get_active=True) -> int:
    async with connection_pools["read"].acquire() as connection:
        if get_active:
            count = await connection.fetchval("""SELECT COUNT(*)
                                                FROM users
                                                JOIN pds ON users.pds = pds.pds
                                                WHERE users.status IS TRUE AND pds.status IS TRUE""")

            # count = await connection.fetchval("""SELECT COUNT(*)
            #                                     FROM users
            #                                     WHERE users.status IS TRUE""")
        else:
            count = await connection.fetchval("""SELECT COUNT(*) FROM users JOIN pds ON users.pds = pds.pds WHERE pds.status is TRUE""")
        return count


@check_db_connection("write")
async def get_dids_with_no_status() -> Optional[list]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT did FROM users WHERE status IS NULL"""
                dids = await connection.fetch(query)

                records = [record['did'] for record in dids]

                return records
    except Exception as e:
        logger.error(f"Error getting dids with no status: {e}")

        return None


@check_db_connection("write")
async def get_new_pdses() -> Optional[list]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT DISTINCT users.pds
                            FROM users
                            LEFT JOIN pds ON users.pds = pds.pds
                            WHERE pds.pds IS NULL;
                            """
                pdses = await connection.fetch(query)

                records = [record['pds'] for record in pdses]

                return records
    except Exception as e:
        logger.error(f"Error getting new pdses: {e}")

        return None


@check_db_connection("write")
async def add_new_pdses(pdses) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                for pds in pdses:
                    query = """INSERT INTO pds (pds) VALUES ($1)"""

                    await connection.execute(query, pds)
    except Exception as e:
        logger.error(f"Error getting new pdses: {e}")


@check_db_connection("read")
async def get_deleted_users_count() -> int:
    async with connection_pools["read"].acquire() as connection:
        count = await connection.fetchval('SELECT COUNT(*) FROM USERS JOIN pds ON users.pds = pds.pds WHERE pds.status is TRUE AND users.status is FALSE')

        return count


@check_db_connection("read")
async def get_single_user_blocks(ident, limit=100, offset=0):
    try:
        # Execute the SQL query to get all the user_dids that have the specified did/ident in their blocklist
        async with connection_pools["read"].acquire() as connection:
            result = await connection.fetch('''SELECT DISTINCT b.user_did, b.block_date, u.handle, u.status 
                                                FROM blocklists AS b 
                                                JOIN users as u ON b.user_did = u.did 
                                                WHERE b.blocked_did = $1 
                                                ORDER BY block_date DESC LIMIT $2 OFFSET $3''', ident, limit, offset)

            count = await connection.fetchval('SELECT COUNT(DISTINCT user_did) FROM blocklists WHERE blocked_did = $1', ident)

            block_list = []

            if count > 0:
                pages = count / 100

                pages = math.ceil(pages)
            else:
                pages = 0

            if result:
                # Iterate over blocked_users and extract handle and status
                for user_did, block_date, handle, status in result:
                    block_list.append({"handle": handle, "status": status, "blocked_date": block_date.isoformat()})

                return block_list, count, pages
            else:
                block_list = []
                total_blocked = 0

                return block_list, total_blocked, pages
    except Exception as e:
        block_list = []
        logger.error(f"Error fetching blocklists for {ident}: {e}")
        count = 0

        return block_list, count, pages


async def get_did_web_handle_history(identifier) -> Optional[list]:
    handle_history = []
    try:
        async with connection_pools["read"].acquire() as connection:
            history = await connection.fetch('SELECT handle, pds, timestamp FROM did_web_history WHERE did = $1', identifier)

            if history is None:
                return None

            for record in history:
                if record['timestamp'] is None:
                    timestamp = None
                else:
                    timestamp = record['timestamp'].isoformat()

                handle_history.append((record['handle'], timestamp, record['pds']))

            handle_history.sort(key=lambda x: x[2])

            return handle_history
    except Exception as e:
        logger.error(f"Error fetching did:web history: {e}")

        return None


@check_db_connection("write")
async def get_labelers() -> dict[str, dict[str, str]]:
    data = {}

    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                labelers = await connection.fetch('SELECT did, name, description FROM labelers')

                for item in labelers:
                    name = item['name']
                    description = item['description']

                    if name == '':
                        name = None

                    if description == '':
                        description = None

                    data[item['did']] = {
                        "displayName": name,
                        "description": description
                    }

                return data
    except Exception as e:
        logger.error(f"Error fetching labelers: {e}")

        return {}


@check_db_connection("write")
async def update_labeler_data(data) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                for did, info in data.items():
                    response = await on_wire.get_labeler_info(did)

                    if "error" in response:
                        continue

                    name = info.get('displayName')
                    description = info.get('description')

                    if response.get('displayName') != name or response.get('description') != description:
                        await connection.execute('UPDATE labelers SET name = $2, description = $3 WHERE did = $1', did, response['displayName'], response['description'])
    except Exception as e:
        logger.error(f"Error updating labeler data: {e}")


async def get_resolution_queue(batch_size: int = 0, batching=False) -> Optional[list]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                if batching:
                    query = """SELECT DISTINCT(did), timestamp FROM resolution_queue ORDER BY timestamp LIMIT $1"""

                    records = await connection.fetch(query, batch_size)

                    processed_records = [record['did'] for record in records]

                    return processed_records
                else:
                    query = """SELECT DISTINCT(did), timestamp FROM resolution_queue"""

                    records = await connection.fetch(query)

                    processed_records = [record['did'] for record in records]

                    return processed_records
    except Exception as e:
        logger.error(f"Error updating didwebs: {e}")


async def process_resolution_queue(info):
    pop = 0
    update_pds = False
    update_handle = False

    for did, data in info.items():
        handle = data.get("handle")
        pds = data.get("pds")

        try:
            async with connection_pools["write"].acquire() as connection:
                async with connection.transaction():
                    query = """SELECT handle, pds FROM USERS WHERE did = $1"""

                    user_info = await connection.fetch(query, did)

                    if user_info:
                        old_handle = user_info[0]['handle']

                        old_pds = user_info[0]['pds']

                        if pds != old_pds:
                            update_pds = True

                        if handle != old_handle:
                            update_handle = True

                        if update_pds and update_handle:
                            await connection.execute("""UPDATE users SET handle = $2, pds = $3 WHERE did = $1""", did, handle, pds)
                        elif update_pds and not update_handle:
                            await connection.execute("""UPDATE users SET pds = $2 WHERE did = $1""", did, pds)
                        elif update_handle and not update_pds:
                            await connection.execute("""UPDATE users SET handle = $2 WHERE did = $1""", did, handle)
                    else:
                        try:
                            await connection.execute("""INSERT INTO users (did, handle, pds) VALUES ($1, $2, $3)""", did, handle, pds)
                        except Exception as e:
                            logger.error(f"Error inserting new did:web: {e}")

                    pop += 1
                    await connection.execute("delete from resolution_queue where did = $1", did)

        except Exception as e:
            logger.error(f"Error updating didwebs: {e}")

    logger.info(f"Popped {pop} from resolution queue.")


async def get_dids_without_created_date() -> Optional[list]:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """SELECT did FROM users WHERE created_date IS NULL"""
                dids = await connection.fetch(query)

                records = [record['did'] for record in dids]

                return records
    except Exception as e:
        logger.error(f"Error getting dids without created date: {e}")

        return None


async def update_did_created_date(did, created_date) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                await connection.execute("UPDATE users SET created_date = $1 WHERE did = $2", created_date, did)
    except Exception as e:
        logger.error(f"Error updating did created date: {e}")


async def set_labeler_status(did, status) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                await connection.execute('UPDATE labelers SET status = $2 WHERE did = $1', did, status)
    except Exception as e:
        logger.error(f"Error updating labeler status: {e}")


async def deactivate_user(user) -> None:
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                await connection.execute('UPDATE users SET status = FALSE WHERE did = $1', user)
    except Exception as e:
        logger.error(f"Error deactivating user: {e}")


# ======================================================================================================================
# ============================================ get database credentials ================================================
def get_database_config(ovride=False) -> dict:
    try:
        if not os.getenv('CLEAR_SKY') or ovride:
            logger.info("Database connection: Using config.ini.")
            read_pg_user = config.get("database_read", "pg_user")
            read_pg_password = config.get("database_read", "pg_password")
            read_pg_host = config.get("database_read", "pg_host")
            read_pg_database = config.get("database_read", "pg_database")
            write_pg_user = config.get("database_write", "pg_user")
            write_pg_password = config.get("database_write", "pg_password")
            write_pg_host = config.get("database_write", "pg_host")
            write_pg_database = config.get("database_write", "pg_database")
            redis_host = config.get("redis", "host")
            redis_port = config.get("redis", "port")
            redis_username = config.get("redis", "username")
            redis_password = config.get("redis", "password")
            redis_key_name = config.get("redis", "autocomplete")
            use_local_db = config.get("database", "use_local", fallback=False)
            local_db = config.get("database_local", "local_db_connection", fallback=False)
        else:
            logger.info("Database connection: Using environment variables.")
            read_pg_user = os.environ.get("READ_PG_USER")
            read_pg_password = os.environ.get("READ_PG_PASSWORD")
            read_pg_host = os.environ.get("READ_PG_HOST")
            read_pg_database = os.environ.get("READ_PG_DATABASE")
            write_pg_user = os.environ.get("WRITE_PG_USER")
            write_pg_password = os.environ.get("WRITE_PG_PASSWORD")
            write_pg_host = os.environ.get("WRITE_PG_HOST")
            write_pg_database = os.environ.get("WRITE_PG_DATABASE")
            redis_host = os.environ.get("REDIS_HOST")
            redis_port = os.environ.get("REDIS_PORT")
            redis_username = os.environ.get("REDIS_USERNAME")
            redis_password = os.environ.get("REDIS_PASSWORD")
            redis_key_name = os.environ.get("REDIS_AUTOCOMPLETE")
            use_local_db = os.environ.get("USE_LOCAL_DB")
            local_db = os.environ.get("LOCAL_DB_CONNECTION")

        return {
            "read_user": read_pg_user,
            "read_password": read_pg_password,
            "read_host": read_pg_host,
            "read_database": read_pg_database,
            "write_user": write_pg_user,
            "write_password": write_pg_password,
            "write_host": write_pg_host,
            "write_database": write_pg_database,
            "redis_host": redis_host,
            "redis_port": redis_port,
            "redis_username": redis_username,
            "redis_password": redis_password,
            "redis_autocomplete": redis_key_name,
            "use_local_db": use_local_db,
            "local_db": local_db
        }
    except Exception:
        logger.error("Database connection information not present: Set environment variables or config.ini")


config = config_helper.read_config()


override = check_override()

# Get the database configuration
if override:
    database_config = get_database_config(True)
else:
    database_config = get_database_config()

# Now you can access the configuration values using dictionary keys
read_pg_user = database_config["read_user"]
read_pg_password = database_config["read_password"]
read_pg_host = database_config["read_host"]
read_pg_database = database_config["read_database"]
write_pg_user = database_config["write_user"]
write_pg_password = database_config["write_password"]
write_pg_host = database_config["write_host"]
write_pg_database = database_config["write_database"]
redis_host = database_config["redis_host"]
redis_port = database_config["redis_port"]
redis_username = database_config["redis_username"]
redis_password = database_config["redis_password"]
redis_key_name = database_config["redis_autocomplete"]

if redis_username == "none":
    logger.warning("Using failover redis.")
    redis_conn = aioredis.from_url(f"redis://{redis_host}:{redis_port}", password=redis_password)
else:
    redis_conn = aioredis.from_url(f"rediss://{redis_username}:{redis_password}@{redis_host}:{redis_port}")


async def local_db() -> bool:
    if database_config["use_local_db"]:
        logger.warning("Using local db.")

        return True
    else:

        return False


async def redis_connected() -> bool:
    try:
        async with redis_conn:
            response = await redis_conn.ping()
        if response:

            return True
        else:

            return False
    except aioredis.ConnectionError:
        logger.error(f"Could not connect to Redis.")
    except Exception as e:
        logger.error(f"An error occurred connecting to Redis: {e}")
    finally:
        await redis_conn.close()
