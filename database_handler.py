# database_handler.py

import asyncio
import os
import asyncpg
import config_helper
import utils
from config_helper import logger
from aiolimiter import AsyncLimiter
from cachetools import TTLCache

# Connection pool and lock
connection_pool = None
db_lock = asyncio.Lock()

# Create a limiter with a rate limit of 10 requests per second
limiter = AsyncLimiter(3)

all_blocks_cache = TTLCache(maxsize=2000000, ttl=172800)


# ======================================================================================================================
# ========================================= database handling functions ================================================
async def create_connection_pool():
    global connection_pool

    # Acquire the lock before creating the connection pool
    async with db_lock:
        if connection_pool is None:
            try:
                connection_pool = await asyncpg.create_pool(
                    user=pg_user,
                    password=pg_password,
                    host=pg_host,
                    database=pg_database
                )
            except OSError:
                logger.error("Network connection issue.")


async def find_handles(value):
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query_text = "SELECT handle FROM users WHERE lower(handle) LIKE $1 || '%' LIMIT 5"

                result = await connection.fetch(query_text, value)

                # Extract matching handles from the database query result
                matching_handles = [row['handle'] for row in result]

                return matching_handles
    except Exception as e:
        logger.error(f"Error retrieving autocomplete handles: {e}")


async def count_users_table():
    async with connection_pool.acquire() as connection:
        # Execute the SQL query to count the rows in the "users" table
        return await connection.fetchval('SELECT COUNT(*) FROM users')


async def get_blocklist(ident, limit=100, offset=0):
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = "SELECT blocked_did, block_date FROM blocklists WHERE user_did = $1 LIMIT $2 OFFSET $3"
                blocklist_rows = await connection.fetch(query, ident, limit, offset)

                query2 = "SELECT COUNT(blocked_did) FROM blocklists WHERE user_did = $1"
                total_blocked_count = await connection.fetchval(query2, ident)

                # Extract the blocked_did and block_date values into separate lists
                blocked_did_list = [row['blocked_did'] for row in blocklist_rows]
                block_date_list = [row['block_date'] for row in blocklist_rows]

                return blocked_did_list, block_date_list, total_blocked_count
    except Exception as e:
        logger.error(f"Error retrieving blocklist for {ident}: {e}")

        return None, None


async def get_dids_with_blocks():
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = "SELECT DISTINCT user_did FROM blocklists"
                rows = await connection.fetch(query)
                logger.info("Fetched DIDs with blocks.")
                logger.info("Processing results...")
                dids_with_blocks = [row['user_did'] for row in rows]
                return dids_with_blocks
    except Exception as e:
        logger.error(f"Error retrieving DIDs with blocks: {e}")

        return []


async def get_dids_without_handles():
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = "SELECT did FROM users WHERE handle IS NULL"
                rows = await connection.fetch(query)
                dids_without_handles = [record['did'] for record in rows]
                return dids_without_handles
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return []


async def update_all_blocklists(run_diff=False):
    all_dids = await get_all_users_db(False, True)
    total_dids = len(all_dids)
    batch_size = 200
    pause_interval = 200  # Pause every x DID requests
    processed_count = 0

    total_blocks_updated = 0
    tasks = []

    if run_diff:
        logger.info("getting diff.")
        dids_with_blocks = await get_dids_with_blocks()  # Retrieve DIDs with blocks from the database
        dids_without_blocks = [did for did in all_dids if did not in dids_with_blocks]
        logger.info("DIDs that will be processed: " + str(len(dids_without_blocks)))
        logger.info("DIDs that have blocks: " + str(len(dids_with_blocks)))
        all_dids = dids_without_blocks

    # Check if there is a last processed DID in the temporary table
    async with connection_pool.acquire() as connection:
        async with connection.transaction():
            try:
                query = "SELECT last_processed_did FROM block_temporary_table"
                last_processed_did = await connection.fetchval(query)
                logger.debug("last did from db: " + str(last_processed_did))
            except Exception as e:
                last_processed_did = None
                logger.error(f"Exception getting from db: {str(e)}")

    if not last_processed_did:
        await create_blocklist_temporary_table()

    if last_processed_did:
        # Find the index of the last processed DID in the list
        start_index = next((i for i, (did) in enumerate(all_dids) if did == last_processed_did), None)
        if start_index is None:
            logger.warning(
                f"Last processed DID '{last_processed_did}' not found in the list. Starting from the beginning.")
        else:
            logger.info(f"Resuming processing from DID: {last_processed_did}")
            all_dids = all_dids[start_index:]

    cumulative_processed_count = 0  # Initialize cumulative count

    for i in range(0, total_dids, batch_size):
        remaining_dids = total_dids - i
        current_batch_size = min(batch_size, remaining_dids)

        batch_dids = all_dids[i:i + current_batch_size]
        # Use the limiter to rate-limit the function calls
        while True:
            try:
                task = asyncio.create_task(update_blocklists_batch(batch_dids))
                tasks.append(task)

                # Update the temporary table with the last processed DID
                last_processed_did = batch_dids[-1]  # Assuming DID is the first element in each tuple
                logger.debug("Last processed DID: " + str(last_processed_did))
                await update_blocklist_temporary_table(last_processed_did)

                # Pause every 100 DID requests
                if processed_count % pause_interval == 0:
                    logger.info(f"Pausing after {i + 1} DID requests...")
                    await asyncio.sleep(30)  # Pause for 30 seconds

                cumulative_processed_count += len(batch_dids)
                # Log information for each batch
                logger.info(f"Processing batch {i // batch_size + 1}/{total_dids // batch_size + 1}...")
                logger.info(f"Processing {cumulative_processed_count}/{total_dids} DIDs...")

                break  # Break the loop if the request is successful
            except asyncpg.ConnectionDoesNotExistError:
                logger.warning("Connection error. Retrying after 30 seconds...")
                await asyncio.sleep(30)  # Retry after 30 seconds
            except IndexError:
                logger.warning("Reached end of DID list to update...")
                break
            except Exception as e:
                if "429 Too Many Requests" in str(e):
                    logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                    await asyncio.sleep(60)  # Retry after 60 seconds
                else:
                    logger.error("None excepted error, sleeping..." + str(e))
                    await asyncio.sleep(60)
                    # raise e

        processed_count += batch_size

    await asyncio.gather(*tasks)
    logger.info(f"Block lists updated: {total_blocks_updated}/{total_dids}")


async def update_blocklists_batch(batch_dids):
    total_blocks_updated = 0

    for did in batch_dids:
        try:
            # Logic to retrieve block list for the current DID
            blocked_users, block_dates = await utils.get_user_block_list(did)
            if blocked_users and block_dates:
                # Update the blocklists table in the database with the retrieved data
                await update_blocklist_table(did, blocked_users, block_dates)
                total_blocks_updated += 1  # Increment the counter for updated block lists
                logger.debug(f"Updated block list for DID: {did}")
            else:
                logger.info(f"didn't update no blocks: {did}")
                continue
        except Exception as e:
            logger.error(f"Error updating block list for DID {did}: {e}")

    return total_blocks_updated


async def get_all_users_db(run_update=False, get_dids=False, get_count=False, init_db_run=False):
    batch_size = 10000
    async with connection_pool.acquire() as connection:
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
            # Get all DIDs
            records = await utils.get_all_users()

            # Transform the records into a list of tuples with the correct format for insertion
            formatted_records = [(record[0],) for record in records]

            logger.info(f"Total DIDs: {len(formatted_records)}")

            if init_db_run:
                logger.info("Connected to db.")
                async with connection.transaction():
                    # Insert data in batches
                    for i in range(0, len(formatted_records), batch_size):
                        batch_data = records[i: i + batch_size]
                        try:
                            await connection.executemany('INSERT INTO users (did) VALUES ($1) ON CONFLICT DO NOTHING', batch_data)
                            logger.info(f"Inserted batch {i // batch_size + 1} of {len(formatted_records) // batch_size + 1} batches.")
                        except Exception as e:
                            logger.error(f"Error inserting batch {i // batch_size + 1}: {str(e)}")

        # Return the records when run_update is false and get_count is called
        return records


async def update_blocklist_table(ident, blocked_by_list, block_date):
    # blocked_by_list, block_date = utils.get_user_block_list(ident)

    if not blocked_by_list:
        return

    async with connection_pool.acquire() as connection:
        async with connection.transaction():
            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT blocked_did, block_date FROM blocklists WHERE user_did = $1', ident
            )
            existing_blocklist_entries = {(record['blocked_did'], record['block_date']) for record in existing_records}
            logger.debug("Existing entires " + ident + ": " + str(existing_blocklist_entries))

            # Prepare the data to be inserted into the database
            data = [(ident, blocked_did, date.strftime('%Y-%m-%d')) for blocked_did, date in zip(blocked_by_list, block_date)]
            logger.debug("Data to be inserted: " + str(data))

            # Convert the new blocklist entries to a set for comparison
            new_blocklist_entries = {(record[1], record[2]) for record in data}
            logger.debug("new blocklist entry " + ident + " : " + str(new_blocklist_entries))

            # Check if there are differences between the existing and new blocklist entries
            if existing_blocklist_entries != new_blocklist_entries:
                # Delete existing blocklist entries for the specified ident
                await connection.execute('DELETE FROM blocklists WHERE user_did = $1', ident)

                # Insert the new blocklist entries
                await connection.executemany(
                    'INSERT INTO blocklists (user_did, blocked_did, block_date) VALUES ($1, $2, $3)', data
                )
            else:
                logger.info("Blocklist not updated already exists.")


async def does_did_and_handle_exist_in_database(did, handle):
    async with connection_pool.acquire() as connection:
        # Execute the SQL query to check if the given DID exists in the "users" table
        exists = await connection.fetchval('SELECT EXISTS(SELECT 1 FROM users WHERE did = $1 AND handle = $2)', did, handle)

        return exists


async def update_user_handles(handles_to_update):
    async with connection_pool.acquire() as connection:
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
            while True:
                try:
                    # Update the database with the batch of handles
                    logger.info("committing batch.")
                    async with connection_pool.acquire() as connection:
                        async with connection.transaction():
                            await update_user_handles(handles_to_update)
                            total_handles_updated += len(handles_to_update)

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
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = """
                CREATE TABLE IF NOT EXISTS temporary_table (
                    last_processed_did text PRIMARY KEY
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error("Error creating temporary table: %s", e)


async def create_new_users_temporary_table():
    try:
        logger.info("Creating temp table.")
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = """
                CREATE TABLE IF NOT EXISTS new_users_temporary_table (
                    last_processed_did text PRIMARY KEY
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error("Error creating temporary table: %s", e)


async def create_blocklist_temporary_table():
    try:
        logger.info("Creating blocklist temp table.")
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = """
                CREATE TABLE IF NOT EXISTS block_temporary_table (
                    last_processed_did text PRIMARY KEY
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error("Error creating temporary table: %s", e)


async def create_top_block_list_table():
    try:
        logger.info("Creating Top block list table.")
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = """
                CREATE TABLE IF NOT EXISTS top_block (
                    did text,
                    count int,
                    list_type text
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error("Error creating top block table: %s", e)


async def create_24_hour_block_table():
    try:
        logger.info("Creating Top 24 hour block list table.")
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = """
                CREATE TABLE IF NOT EXISTS top_twentyfour_hour_block (
                    did text,
                    count int,
                    list_type text
                )
                """
                await connection.execute(query)
    except Exception as e:
        logger.error("Error creating Top 24 hour block list table: %s", e)


async def update_24_hour_block_list_table(entries, list_type):
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                data = [(did, count, list_type) for did, count in entries]
                # Insert the new row with the given last_processed_did
                query = "INSERT INTO top_twentyfour_hour_block (did, count, list_type) VALUES ($1, $2, $3)"
                await connection.executemany(query, data)
                logger.info("Updated top 24 block table.")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def truncate_top_blocks_table():
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                # Delete the existing rows if it exists
                await connection.execute("TRUNCATE top_block")
                logger.info("Truncated block table.")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def truncate_top24_blocks_table():
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                await connection.execute("TRUNCATE top_twentyfour_hour_block")
                logger.info("Truncated top 24 block table.")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def update_top_block_list_table(entries, list_type):
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                data = [(did, count, list_type) for did, count in entries]
                # Insert the new row with the given last_processed_did
                query = "INSERT INTO top_block (did, count, list_type) VALUES ($1, $2, $3)"
                await connection.executemany(query, data)
                logger.info("Updated top block table")
    except Exception as e:
        logger.error("Error updating top block table: %s", e)


async def get_top_blocks_list():
    try:
        async with connection_pool.acquire() as connection:
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
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query1 = "SELECT distinct did, count FROM top_twentyfour_hour_block WHERE list_type = 'blocked'"
                query2 = "SELECT distinct did, count FROM top_twentyfour_hour_block WHERE list_type = 'blocker'"
                blocked_rows = await connection.fetch(query1)
                blocker_rows = await connection.fetch(query2)

                return blocked_rows, blocker_rows
    except Exception as e:
        logger.error(f"Error retrieving DIDs without handles: {e}")

        return [], []


async def delete_blocklist_temporary_table():
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = "DROP TABLE IF EXISTS block_temporary_table"
                await connection.execute(query)
    except Exception as e:
        logger.error("Error deleting temporary table: %s", e)


async def delete_temporary_table():
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = "DROP TABLE IF EXISTS temporary_table"
                await connection.execute(query)
    except Exception as e:
        logger.error("Error deleting temporary table: %s", e)


async def delete_new_users_temporary_table():
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                query = "DROP TABLE IF EXISTS new_users_temporary_table"
                await connection.execute(query)
    except Exception as e:
        logger.error("Error deleting temporary table: %s", e)


async def update_blocklist_temporary_table(last_processed_did):
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                await connection.execute("TRUNCATE block_temporary_table")

                # Insert the new row with the given last_processed_did
                query = "INSERT INTO block_temporary_table (last_processed_did) VALUES ($1)"
                await connection.execute(query, last_processed_did)
    except Exception as e:
        logger.error("Error updating temporary table: %s", e)


async def update_temporary_table(last_processed_did, table):
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                # Delete the existing row if it exists
                delete_query = f"TRUNCATE {table}"
                await connection.execute(delete_query)

                # Insert the new row with the given last_processed_did
                insert_query = f"INSERT INTO {table} (last_processed_did) VALUES ($1)"
                await connection.execute(insert_query, last_processed_did)
    except Exception as e:
        logger.error("Error updating temporary table: %s", e)


async def update_new_users_temporary_table(last_processed_did):
    try:
        async with connection_pool.acquire() as connection:
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
        async with connection_pool.acquire() as connection:
            async with connection.transaction():

                # Insert the new row with the given last_processed_did
                blocked_query = '''SELECT blocked_did, COUNT(*) AS block_count
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blocked_data = await connection.fetch(blocked_query)
                blocked_results.append(blocked_data)

                blockers_query = '''SELECT user_did, COUNT(*) AS block_count
                                    FROM blocklists
                                    GROUP BY user_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blockers_data = await connection.fetch(blockers_query)
                blockers_results.append(blockers_data)

                return blocked_data, blockers_data
    except Exception as e:
        logger.error("Error retrieving data from db", e)


async def get_block_stats():
    try:
        async with connection_pool.acquire() as connection:
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

                logger.info("All blocklist queries complete.")
                return (number_of_total_blocks, number_of_unique_users_blocked, number_of_unique_users_blocking,
                        number_block_1, number_blocking_2_and_100, number_blocking_101_and_1000, number_blocking_greater_than_1000,
                        average_number_of_blocks, number_blocked_1, number_blocked_2_and_100, number_blocked_101_and_1000,
                        number_blocked_greater_than_1000, average_number_blocked)
    except Exception as e:
        logger.error(f"Error retrieving data from db: {e}")


async def get_top24_blocks():
    blocked_results = []
    blockers_results = []

    logger.info("Getting top 24 blocks from db.")
    try:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():

                # Insert the new row with the given last_processed_did
                blocked_query = '''SELECT blocked_did, COUNT(*) AS block_count
                                    FROM blocklists
                                    WHERE TO_DATE(block_date, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '1 day'
                                    GROUP BY blocked_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blocked_data = await connection.fetch(blocked_query)
                blocked_results.append(blocked_data)

                blockers_query = '''SELECT user_did, COUNT(*) AS block_count
                                    FROM blocklists
                                    WHERE TO_DATE(block_date, 'YYYY-MM-DD') >= CURRENT_DATE - INTERVAL '1 day'
                                    GROUP BY user_did
                                    ORDER BY block_count DESC
                                    LIMIT 25'''

                blockers_data = await connection.fetch(blockers_query)
                blockers_results.append(blockers_data)

                return blocked_data, blockers_data
    except Exception as e:
        logger.error("Error retrieving data from db", e)


async def get_similar_blocked_by(user_did):
    global all_blocks_cache
    async with connection_pool.acquire() as connection:
        blocked_by_users = await connection.fetch(
            'SELECT user_did FROM blocklists WHERE blocked_did = $1', user_did)

    # Extract the values from the records
    blocked_by_users_ids = [record['user_did'] for record in blocked_by_users]

    if len(all_blocks_cache) == 0:
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                # Fetch all blocklists except for the specified user's blocklist
                all_blocklists_rows = await connection.fetch(
                    'SELECT user_did, blocked_did FROM blocklists'
                )
                all_blocks_cache = all_blocklists_rows
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

    # Return the sorted list of users and their match percentages
    return users, percentages


async def get_similar_users(user_did):
    global all_blocks_cache
    if len(all_blocks_cache) == 0:
        logger.info("Caching all blocklists.")
        async with connection_pool.acquire() as connection:
            async with connection.transaction():
                # Fetch all blocklists except for the specified user's blocklist
                all_blocklists_rows = await connection.fetch(
                    'SELECT user_did, blocked_did FROM blocklists'
                )
                all_blocks_cache = all_blocklists_rows
    else:
        all_blocklists_rows = all_blocks_cache

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

        return users, percentages

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

    return users, percentages


async def blocklists_updater():
    blocked_list = "blocked"
    blocker_list = "blocker"

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

    return top_blocked, top_blockers, blocked_aid, blocker_aid


async def top_24blocklists_updater():
    blocked_list = "blocked"
    blocker_list = "blocker"

    logger.info("Updating top blocks lists requested.")
    await truncate_top24_blocks_table()
    blocked_results, blockers_results = await get_top24_blocks()  # Get blocks for db
    logger.debug(f"blocked count: {len(blocked_results)} blockers count: {len(blockers_results)}")
    # Update blocked entries
    await update_24_hour_block_list_table(blocked_results, blocked_list)  # add blocked to top blocks table
    logger.info("Updated top blocked db.")
    await update_24_hour_block_list_table(blockers_results, blocker_list)  # add blocker to top blocks table
    logger.info("Updated top blockers db.")
    top_blocked, top_blockers, blocked_aid, blocker_aid = await utils.resolve_top24_block_lists()

    logger.info("Top blocks lists page updated.")

    return top_blocked, top_blockers, blocked_aid, blocker_aid


# ======================================================================================================================
# ============================================ get database credentials ================================================
def get_database_config():
    try:
        if not os.getenv('CLEAR_SKY'):
            logger.info("Database connection: Using config.ini.")
            pg_user = config.get("database", "pg_user")
            pg_password = config.get("database", "pg_password")
            pg_host = config.get("database", "pg_host")
            pg_database = config.get("database", "pg_database")
        else:
            logger.info("Database connection: Using environment variables.")
            pg_user = os.environ.get("PG_USER")
            pg_password = os.environ.get("PG_PASSWORD")
            pg_host = os.environ.get("PG_HOST")
            pg_database = os.environ.get("PG_DATABASE")

        return {
            "user": pg_user,
            "password": pg_password,
            "host": pg_host,
            "database": pg_database
        }
    except Exception:
        logger.error("Database connection information not present: Set environment variables or config.ini")


config = config_helper.read_config()

# Get the database configuration
database_config = get_database_config()

# Now you can access the configuration values using dictionary keys
pg_user = database_config["user"]
pg_password = database_config["password"]
pg_host = database_config["host"]
pg_database = database_config["database"]
