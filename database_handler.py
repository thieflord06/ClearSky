# database_handler.py

import asyncio
import os
import asyncpg
import config_helper
import setup
import utils
from config_helper import logger
from cachetools import TTLCache
from redis import asyncio as aioredis
from datetime import datetime
from collections import defaultdict
import pytz
import on_wire
import math

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


async def count_users_table():
    async with connection_pools["read"].acquire() as connection:
        # Execute the SQL query to count the rows in the "users" table
        return await connection.fetchval('SELECT COUNT(*) FROM users WHERE status is TRUE')


async def get_blocklist(ident, limit=100, offset=0):
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT b.blocked_did, b.block_date, u.handle, u.status FROM blocklists AS b JOIN users AS u ON b.blocked_did = u.did WHERE b.user_did = $1 ORDER BY block_date DESC LIMIT $2 OFFSET $3"
                blocklist_rows = await connection.fetch(query, ident, limit, offset)

                query2 = "SELECT COUNT(blocked_did) FROM blocklists WHERE user_did = $1"
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


async def get_moderation_list(name, limit=100, offset=0):
    try:
        async with connection_pools["read"].acquire() as connection:
            async with connection.transaction():
                search_string = f'%{name}%'

                name_query = """SELECT ml.url, u.handle, u.status, ml.name, ml.description, ml.created_date
                FROM mutelists AS ml 
                INNER JOIN users AS u ON ml.did = u.did -- Join the users table to get the handle 
                WHERE ml.name ILIKE $1
                LIMIT $2
                OFFSET $3"""

                name_mod_lists = await connection.fetch(name_query, search_string, limit, offset)

                description_query = """SELECT ml.url, u.handle, u.status, ml.name, ml.description, ml.created_date
                FROM mutelists AS ml
                INNER JOIN users AS u ON ml.did = u.did -- Join the users table to get the handle
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
                "created_date": record['created_date'].isoformat()
            }
            lists.append(data)

        for record in description_mod_lists:
            data = {
                "url": record['url'],
                "handle": record['handle'],
                "status": record['status'],
                "name": record['name'],
                "description": record['description'],
                "created_date": record['created_date'].isoformat()
            }
            lists.append(data)
    else:
        lists = None

    return lists, pages


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


async def get_dids_without_handles():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "SELECT did FROM users WHERE handle IS NULL"
                rows = await connection.fetch(query)
                dids_without_handles = [record['did'] for record in rows]
                return dids_without_handles
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return []


async def get_pdses():
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


async def crawl_all(forced=False):
    all_dids = await get_all_users_db(False, True)
    total_dids = len(all_dids)
    batch_size = 500
    pause_interval = 500  # Pause every x DID requests
    processed_count = 0
    cumulative_processed_count = 0  # Initialize cumulative count
    total_blocks_updated = 0
    table = "temporary_table"

    # Check if there is a last processed DID in the temporary table
    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            try:
                query = "SELECT last_processed_did FROM temporary_table"
                last_processed_did = await connection.fetchval(query)
                logger.debug("last did from db: " + str(last_processed_did))
            except Exception as e:
                last_processed_did = None
                logger.error(f"Exception getting from db: {str(e)}")

    if not last_processed_did:
        await create_temporary_table()

    if last_processed_did:
        # Find the index of the last processed DID in the list
        start_index = next((i for i, (did) in enumerate(all_dids) if did == last_processed_did), None)
        if start_index is None:
            logger.warning(
                f"Last processed DID '{last_processed_did}' not found in the list. Starting from the beginning.")
        else:
            logger.info(f"Resuming processing from DID: {last_processed_did}")
            all_dids = all_dids[start_index:]

    for i in range(0, total_dids, batch_size):
        remaining_dids = total_dids - i
        current_batch_size = min(batch_size, remaining_dids)

        batch_dids = all_dids[i:i + current_batch_size]

        await crawler_batch(batch_dids, forced=forced)

        # Update the temporary table with the last processed DID
        last_processed_did = batch_dids[-1]  # Assuming DID is the first element in each tuple
        logger.debug("Last processed DID: " + str(last_processed_did))
        await update_temporary_table(last_processed_did, table)

        cumulative_processed_count += len(batch_dids)

        # Pause every 100 DID requests
        if processed_count % pause_interval == 0:
            logger.info(f"Pausing after {i + 1} DID requests...")

            # Log information for each batch
            logger.info(f"Processed batch {i // batch_size + 1}/{total_dids // batch_size + 1}...")
            logger.info(f"Processed {cumulative_processed_count}/{total_dids} DIDs...")
            await asyncio.sleep(5)  # Pause for 30 secondsx

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

    for did in batch_dids:
        handle = await on_wire.resolve_did(did)

        if handle is not None:
            # Check if the DID and handle combination already exists in the database
            logger.debug("Did: " + str(did) + " | handle: " + str(handle))

            if await does_did_and_handle_exist_in_database(did, handle):
                logger.debug(f"DID {did} with handle {handle} already exists in the database. Skipping...")
            else:
                handles_to_update.append((did, handle))
                handle_count += 1
        else:
            logger.warning(f"DID: {did} not resolved.")
            continue

        try:
            blocked_data = await utils.get_user_block_list(did)
            mutelists_data = await utils.get_mutelists(did)
            mutelists_users_data = await utils.get_mutelist_users(did)
            subscribe_data = await utils.get_subscribelists(did)

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
        except Exception as e:
            logger.error(f"Error updating for DID {did}: {e}")

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

                # for did, handle in handles_to_update:
                #     only_handles.append(handle)

                # logger.info("Adding new prefixes.")
                # await add_new_prefixes(only_handles)

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


async def get_all_users_db(run_update=False, get_dids=False, get_count=False, init_db_run=False):
    batch_size = 10000
    all_records = set()

    async with connection_pools["write"].acquire() as connection:
        if get_count:
            # Fetch the total count of users in the "users" table
            count = await connection.fetchval('SELECT COUNT(*) FROM users')

            return count
        if not run_update:
            if get_dids:
                # Return the user_dids from the "users" table
                records = await connection.fetch('SELECT did FROM users')
                dids = [record['did'] for record in records]

                return dids
        else:
            pdses = await get_pdses()

            # Get all DIDs
            for pds in pdses:
                new_records = await utils.get_all_users(pds)
                logger.info(f"{len(new_records)} users in {pds}")
                all_records.update(new_records)

            logger.info(f"Total DIDs: {len(all_records)}")

            current_true_records = await connection.fetch('SELECT did FROM users WHERE status = TRUE')

            current_true_set = set()
            for record in current_true_records:
                current_true_set.add(record["did"])

            logger.info("Getting DIDs to deactivate.")
            dids_deactivated = current_true_set - all_records

            logger.info("Getting new DIDs.")
            new_dids = all_records - current_true_set

            logger.info(f"Total new DIDs: {len(new_dids)}")

            if init_db_run:
                logger.info("Connected to db.")

                records = list(new_dids)

                async with connection.transaction():
                    # Insert data in batches
                    for i in range(0, len(records), batch_size):
                        batch_data = [(did, True) for did in records[i: i + batch_size]]
                        try:
                            await connection.executemany('INSERT INTO users (did, status) VALUES ($1, $2) ON CONFLICT (did) DO UPDATE SET status = TRUE WHERE users.status <> TRUE', batch_data)

                            logger.info(f"Inserted batch {i // batch_size + 1} of {len(records) // batch_size + 1} batches.")
                        except Exception as e:
                            logger.error(f"Error inserting batch {i // batch_size + 1}: {str(e)}")

        logger.info("Comparing status")

        if dids_deactivated:
            count = 0

            logger.info(f"deactivating {len(dids_deactivated)} dids")

            for did in dids_deactivated:
                await connection.execute("""UPDATE users SET status = FALSE WHERE did = $1""", did)
                count += 1
                logger.info(f"DIDs deactivated: {count}")

            logger.info(f"{str(len(dids_deactivated))} dids deactivated.")

        # Return the records when run_update is false and get_count is called
        return records


async def update_blocklist_table(ident, blocked_data, forced=False):
    counter = 0

    if not forced:
        touched_actor = "crawler"
    else:
        touched_actor = "forced_crawler"

    if not blocked_data:
        return counter

    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT uri FROM blocklists WHERE user_did = $1', ident
            )
            existing_blocklist_entries = {record['uri'] for record in existing_records}
            logger.debug("Existing entires " + ident + ": " + str(existing_blocklist_entries))

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
                            continue  # Skip processing when uri is None
                        await connection.execute('INSERT INTO blocklists_transaction (uri, block_date, touched, touched_actor) VALUES ($1, $2, $3, $4)', uri, datetime.now(pytz.utc), datetime.now(pytz.utc), touched_actor)
                except Exception as e:
                    logger.error(f"Error updating blocklists_transaction on delete : {e}")
                    logger.error(existing_blocklist_entries)

                logger.info("Blocklist transaction[deleted] updated.")

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
                logger.info("Blocklist not updated already exists.")

                return counter


async def update_subscribe_table(ident, subscribelists_data, forced=False):
    subscribe_list_counter = 0

    if not forced:
        touched_actor = "crawler"
    else:
        touched_actor = "forced_crawler"

    if not subscribelists_data:
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


async def update_mutelist_tables(ident, mutelists_data, mutelists_users_data, forced=False):
    list_counter = 0
    user_counter = 0

    if not forced:
        touched_actor = "crawler"
    else:
        touched_actor = "forced_crawler"

    if not mutelists_data:
        counter = [list_counter, user_counter]

        return counter

    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():

            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT uri FROM {} WHERE did = $1'.format(setup.mute_lists_table), ident
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

                try:
                    # Insert the new mutelist entries
                    await connection.executemany("""INSERT INTO {} (url, uri, did, cid, name, created_date, description, touched, touched_actor) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""".format(setup.mute_lists_table), mutelist_records_to_insert)

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

                    try:
                        # Insert the new mutelist entries
                        await connection.executemany("""INSERT INTO {} (list_uri, cid, subject_did, owner_did, date_added, touched, touched_actor, listitem_uri) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)""".format(setup.mute_lists_users_table), mutelistusers_records_to_insert)

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


async def does_did_and_handle_exist_in_database(did, handle):
    async with connection_pools["write"].acquire() as connection:
        # Execute the SQL query to check if the given DID exists in the "users" table
        exists = await connection.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE did = $1 AND handle = $2)', did, handle)

        return exists


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

        logger.info(f"Updated {len(handles_to_update)} handles in the database.")


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


async def process_batch(batch_dids, ad_hoc, table, batch_size):
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
            # Check if the DID and handle combination already exists in the database
            logger.debug("Did: " + str(did) + " | handle: " + str(handle))
            if await does_did_and_handle_exist_in_database(did, handle):
                logger.debug(f"DID {did} with handle {handle} already exists in the database. Skipping...")
            else:
                handles_to_update.append((did, handle))

        if handles_to_update:
            only_handles = []
            while True:
                try:
                    # Update the database with the batch of handles
                    logger.info("committing batch.")
                    async with connection_pools["write"].acquire() as connection:
                        async with connection.transaction():
                            await update_user_handles(handles_to_update)
                            total_handles_updated += len(handles_to_update)

                    for did, handle in handles_to_update:
                        only_handles.append(handle)

                    logger.info("Adding new prefixes.")
                    await add_new_prefixes(only_handles)

                    # Update the temporary table with the last processed DID
                    last_processed_did = handle_batch[-1][0]  # Assuming DID is the first element in each tuple
                    logger.debug("Last processed DID: " + str(last_processed_did))
                    await update_temporary_table(last_processed_did, table)

                    break
                except asyncpg.ConnectionDoesNotExistError as e:
                    logger.warning("Connection error, retrying in 30 seconds...")
                    await asyncio.sleep(30)  # Retry after 60 seconds
                except Exception as e:
                    # Handle other exceptions as needed
                    logger.error(f"Error during batch update: {e}")
                    break  # Break the loop on other exceptions

    return total_handles_updated


async def create_temporary_table():
    try:
        logger.info("Creating temp table.")
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = """
                CREATE TABLE IF NOT EXISTS temporary_table (
                    last_processed_did text PRIMARY KEY,
                    touched timestamptz
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error("Error creating temporary table: %s", e)


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


async def update_24_hour_block_list_table(entries, list_type):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                data = [(did, count, list_type) for did, count in entries]
                # Insert the new row with the given last_processed_did
                query = "INSERT INTO top_twentyfour_hour_block (did, count, list_type) VALUES ($1, $2, $3)"
                await connection.executemany(query, data)
                logger.info("Updated top 24 block table.")
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def truncate_top_blocks_table():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing rows if it exists
                await connection.execute("TRUNCATE top_block")
                logger.info("Truncated block table.")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def truncate_top24_blocks_table():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                await connection.execute("TRUNCATE top_twentyfour_hour_block")
                logger.info("Truncated top 24 block table.")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def update_top_block_list_table(entries, list_type):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                data = [(did, count, list_type) for did, count in entries]
                # Insert the new row with the given last_processed_did
                query = "INSERT INTO top_block (did, count, list_type) VALUES ($1, $2, $3)"
                await connection.executemany(query, data)
                logger.info("Updated top block table")
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def get_top_blocks_list():
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


async def delete_temporary_table():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "DROP TABLE IF EXISTS temporary_table"
                await connection.execute(query)
    except Exception as e:
        logger.error("Error deleting temporary table: %s", e)


async def delete_new_users_temporary_table():
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                query = "DROP TABLE IF EXISTS new_users_temporary_table"
                await connection.execute(query)
    except Exception as e:
        logger.error("Error deleting temporary table: %s", e)


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


async def update_did_service(data):
    logger.info("Updating services information for batch.")
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                for record in data:
                    query = """SELECT did, pds, created_date FROM users where did = $1"""

                    did_exists = await connection.fetch(query, record[0])

                    if did_exists:
                        if not did_exists[0]["pds"] or not did_exists[0]["created_date"]:
                            insert_pds_query = """UPDATE users SET created_date = $2, pds = $3 WHERE did = $1"""

                            await connection.execute(insert_pds_query, record[0], record[1], record[2])
                        elif did_exists[0]["pds"] != record[2]:
                            old_pds = did_exists[0]["pds"]
                            update_query = """UPDATE users SET pds = $2 WHERE did = $1"""

                            await connection.execute(update_query, record[0], record[2])

                            logger.info(f"Updated pds for: {record[0]} | from {old_pds} to {record[2]}")
                        else:
                            logger.debug("Up to date.")
                            continue
                    else:
                        insert_query = """INSERT INTO users (did, created_date, pds, handle, status) VALUES ($1, $2, $3, $4, $5)"""

                        await connection.execute(insert_query, record[0], record[1], record[2], record[3], True)
    except Exception as e:
        logger.error("Error retrieving/inserting data to db", e)


async def update_last_created_did_date(last_created):
    try:
        async with connection_pools["write"].acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                delete_query = f"TRUNCATE {setup.last_created_table}"
                await connection.execute(delete_query)

                # Insert the new row with the given last_processed_did
                insert_query = f"INSERT INTO {setup.last_created_table} (last_created) VALUES ($1)"
                await connection.execute(insert_query, last_created)
    except Exception as e:
        logger.error("Error updating temporary table: %s", e)


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
                total_users = await utils.get_user_count(get_active=False)
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
                user_match_percentages[user_id] = match_percentage

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

    logger.info(f"Similar blocks: {await utils.get_user_handle(user_id)} | {top_similar_users}")

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

    return top_blocked, top_blockers, blocked_aid, blocker_aid


async def top_24blocklists_updater():
    global last_update_top_24_block
    global blocklist_24_updater_status
    global top_24_blocks_start_time
    global top_24_blocks_process_time

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

    return top_blocked_24, top_blockers_24, blocked_aid_24, blocker_aid_24


async def get_mutelists(ident):
    async with connection_pools["read"].acquire() as connection:
        async with connection.transaction():
            query = """
            SELECT ml.url, u.handle, u.status, ml.name, ml.description, ml.created_date, mu.date_added
            FROM mutelists AS ml
            INNER JOIN mutelists_users AS mu ON ml.uri = mu.list_uri
            INNER JOIN users AS u ON ml.did = u.did -- Join the users table to get the handle
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
                    "date_added": record['date_added'].isoformat()
                }
                lists.append(data)

            return lists


async def check_api_key(api_environment, key_type, key_value):
    async with connection_pools["read"].acquire() as connection:
        async with connection.transaction():
            query = """SELECT valid FROM API WHERE key = $3 environment = $1 AND access_type LIKE '%' || $2 || '%'"""

            status = await connection.fetchval(query, api_environment, key_type, key_value)

            return status


async def wait_for_redis():
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


async def tables_exists():
    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            try:
                query1 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
                query2 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
                query3 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
                query4 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""

                users_exist = await connection.fetchval(query1, setup.users_table)
                blocklists_exist = await connection.fetchval(query2, setup.blocklist_table)
                top_blocks_exist = await connection.fetchval(query3, setup.top_blocks_table)
                top_24_exist = await connection.fetchval(query4, setup.top_24_blocks_table)

                values = (users_exist, blocklists_exist, top_blocks_exist, top_24_exist)

                if any(value is False for value in values):

                    return False
                else:

                    return True
            except asyncpg.ConnectionDoesNotExistError:
                logger.error("Error checking if schema exists, db not connected.")

                return False


async def get_api_keys(environment):
    async with connection_pools["write"].acquire() as connection:
        async with connection.transaction():
            try:
                query = """SELECT key FROM API WHERE environment = $1 AND valid is True"""

                results = await connection.fetch(query, environment)

                key_list = [key['key'] for key in results]

                return key_list
            except Exception as e:
                # Handle other exceptions as needed
                logger.error(f"Error getting API keys: {e}")

                return []


# ======================================================================================================================
# ============================================ get database credentials ================================================
def get_database_config():
    try:
        if not os.getenv('CLEAR_SKY'):
            logger.info("Database connection: Using config.ini.")
            read_pg_user = config.get("database_read", "pg_user")
            read_pg_password = config.get("database_read", "pg_password")
            read_pg_host = config.get("database_read", "pg_host")
            read_pg_database = config.get("database_read", "pg_database")
            write_pg_user = config.get("database_write", "pg_user")
            write_pg_password = config.get("database_write", "pg_password")
            write_pg_host = config.get("database_write", "pg_host")
            write_pg_database = config.get("databas_write", "pg_database")
            redis_host = config.get("redis", "host")
            redis_port = config.get("redis", "port")
            redis_username = config.get("redis", "username")
            redis_password = config.get("redis", "password")
            redis_key_name = config.get("redis", "autocomplete")
            use_local_db = config.get("database", "use_local")
            local_db = config.get("database_local", "local_db_connection")
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

# Get the database configuration
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


async def local_db():
    if database_config["use_local_db"]:
        logger.warning("Using local db.")

        return True
    else:

        return False


async def redis_connected():
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
        logger.error(f"An error occured connecting to Redis: {e}")
    finally:
        await redis_conn.close()
