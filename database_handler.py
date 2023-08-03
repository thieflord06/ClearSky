# database_handler.py

import asyncio
import asyncpg
import time
from tqdm import tqdm
import logging
import utils


# ======================================================================================================================
# ========================================= database handling functions ================================================
async def create_connection_pool():
    global connection_pool

    # Acquire the lock before creating the connection pool
    async with db_lock:
        if connection_pool is None:
            connection_pool = await asyncpg.create_pool(
                user=pg_user,
                password=pg_password,
                host=pg_host,
                database=pg_database
            )


async def close_connection_pool(pool):
    await pool.close()


async def count_users_table():
    async with connection_pool.acquire() as connection:
        # Execute the SQL query to count the rows in the "users" table
        return await connection.fetchval('SELECT COUNT(*) FROM users')


def get_single_users_blocks_db(run_update=False, get_dids=False):
    all_dids = get_all_users_db(run_update=run_update, get_dids=get_dids)

    for i, ident in enumerate(tqdm(all_dids, desc="Updating blocklists", unit="DID", ncols=100)):
        user_did = ident[0]
        update_blocklist_table(user_did)

        # Sleep for 60 seconds every 5 minutes
        if (i + 1) % (300000 // 100) == 0:  # Assuming you have 100 dids in all_dids
            logger.info("Pausing...")
            time.sleep(60)


async def update_all_blocklists():
    all_dids = await get_all_users_db(False, True)
    total_dids = len(all_dids)
    batch_size = 1000

    total_blocks_updated = 0
    for i in range(0, total_dids, batch_size):
        batch_dids = all_dids[i:i + batch_size]
        # Process the batch asynchronously
        batch_blocks_updated = await update_blocklists_batch(batch_dids)
        total_blocks_updated += batch_blocks_updated

        logger.info(f"Block lists updated: {total_blocks_updated}/{total_dids}")
        logger.info(f"First few DIDs in the batch: {batch_dids[:5]}")


async def update_blocklists_batch(batch_dids):
    total_blocks_updated = 0

    for did in batch_dids:
        try:
            # Logic to retrieve block list for the current DID
            blocked_users, block_dates = await utils.get_user_block_list(did)

            # Update the blocklists table in the database with the retrieved data
            await update_blocklist_table(did, blocked_users, block_dates)

            # Increment the counter for updated block lists
            total_blocks_updated += 1

            logger.debug(f"Updated block list for DID: {did}")
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
                return await connection.fetch('SELECT did FROM users')
        else:
            # Get all DIDs
            records = utils.get_all_users()

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
                            await connection.executemany('INSERT INTO users (did) VALUES ($1) ON CONFLICT DO NOTHING',
                                                         batch_data)
                            logger.info(
                                f"Inserted batch {i // batch_size + 1} of {len(formatted_records) // batch_size + 1} batches.")
                        except Exception as e:
                            logger.error(f"Error inserting batch {i // batch_size + 1}: {str(e)}")

        # Return the records when run_update is false and get_count is called
        return records


async def update_blocklist_table(ident):
    blocked_by_list, block_date = utils.get_user_block_list(ident)

    if not blocked_by_list:
        return

    async with connection_pool.acquire() as connection:
        logger.info("Connected to db.")
        async with connection.transaction():
            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT blocked_did, block_date FROM blocklists WHERE user_did = $1', ident
            )
            existing_blocklist_entries = {(record['blocked_did'], record['block_date']) for record in existing_records}

            # Prepare the data to be inserted into the database
            data = [(ident, blocked_did, date) for blocked_did, date in zip(blocked_by_list, block_date)]

            # Convert the new blocklist entries to a set for comparison
            new_blocklist_entries = {(record['blocked_did'], record['block_date']) for record in data}

            # Check if there are differences between the existing and new blocklist entries
            if existing_blocklist_entries != new_blocklist_entries:
                # Delete existing blocklist entries for the specified ident
                await connection.execute('DELETE FROM blocklists WHERE user_did = $1', ident)

                # Insert the new blocklist entries
                await connection.executemany(
                    'INSERT INTO blocklists (user_did, blocked_did, block_date) VALUES ($1, $2, $3)', data
                )


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

            # # Insert the handles into the temporary table
            # await connection.copy_records_to_table('temp_handles', records=handles_to_update)

            # Update the users table using the temporary table
            await connection.execute('''
                INSERT INTO users (did, handle)
                SELECT t.did, t.handle
                FROM temp_handles AS t
                ON CONFLICT (did) DO UPDATE
                SET handle = EXCLUDED.handle
            ''')

        logger.info(f"Updated {len(handles_to_update)} handles in the database.")


async def process_batch(batch_dids):
    batch_handles_and_dids = await utils.fetch_handles_batch(batch_dids)
    logger.info("Batch resolved.")

    # Split the batch of handles into smaller batches
    batch_size = 1000  # You can adjust this batch size based on your needs
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
            # Update the database with the batch of handles
            logger.info("committing batch.")
            async with connection_pool.acquire() as connection:
                async with connection.transaction():
                    await update_user_handles(handles_to_update)
                    total_handles_updated += len(handles_to_update)

            # # Commit changes after each batch update
            # await connection_pool.acquire().commit()

        logger.info(f"Batch handles updated: {total_handles_updated}")

    return total_handles_updated


# Close the connection pool when this module is loaded
async def close_connection_pool():
    await connection_pool.close()


logger = logging.getLogger(__name__)

# Get the database configuration
database_config = utils.get_database_config()

# Now you can access the configuration values using dictionary keys
pg_user = database_config["user"]
pg_password = database_config["password"]
pg_host = database_config["host"]
pg_database = database_config["database"]

# Connection pool and lock
connection_pool = None
db_lock = asyncio.Lock()
