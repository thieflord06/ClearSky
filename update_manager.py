# update_manager.py

import asyncpg
import database_handler
from config_helper import logger
import sys
import argparse
import asyncio
import app

# python update_manager.py --update-users-handles // update handles that have changed (initial or re-initialize)
# python update_manager.py --update-users-did-only-db // command to update users db
# python update_manager.py --fetch-users-count // command to get current count in db
# python update_manager.py --update-blocklists-db // command to update all users blocklists
# python update_manager.py --retrieve-blocklists-db // initial/re-initialize get for blocklists database
# python update_manager.py --update-users-dids // update db with new dids and handles
# python update_manager.py --update-redis-cache // update handles in redis


async def main():
    parser = argparse.ArgumentParser(description='ClearSky Update Manager: ' + app.version)
    parser.add_argument('--update-users-handles', action='store_true', help='update handles that have changed')
    parser.add_argument('--update-users-did-only-db', action='store_true', help='Update the database with all users')
    parser.add_argument('--fetch-users-count', action='store_true', help='Fetch the count of users')
    parser.add_argument('--update-blocklists-db', action='store_true', help='Update the blocklists table')
    parser.add_argument('--retrieve-blocklists-db', action='store_true', help='Initial/re-initialize get for blocklists database')
    parser.add_argument('--update-users-dids', action='store_true', help='update db with new dids and handles')
    parser.add_argument('--update-redis-cache', action='store_true', help='Update the redis cache')
    args = parser.parse_args()

    await database_handler.create_connection_pool()  # Creates connection pool for db

    if args.update_users_handles:
        # Call the function to update the database with all users
        logger.info("Users db update requested.")
        all_dids = await database_handler.get_all_users_db(False, True)
        logger.info("Users db updated dids.")
        logger.info("Update users handles requested.")
        batch_size = 1000
        total_dids = len(all_dids)
        total_handles_updated = 0
        table = "temporary_table"

        # Check if there is a last processed DID in the temporary table
        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                try:
                    query = "SELECT last_processed_did FROM temporary_table"
                    last_processed_did = await connection.fetchval(query)
                except asyncpg.UndefinedTableError:
                    logger.warning("Temporary table doesn't exist.")
                    last_processed_did = None
                except Exception as e:
                    last_processed_did = None
                    logger.error(f"Exception getting from db: {str(e)}")

        if not last_processed_did:
            await database_handler.create_temporary_table()

        if last_processed_did:
            # Find the index of the last processed DID in the list
            start_index = next((i for i, (did) in enumerate(all_dids) if did == last_processed_did), None)
            if start_index is None:
                logger.warning(
                    f"Last processed DID '{last_processed_did}' not found in the list. Starting from the beginning.")
            else:
                logger.info(f"Resuming processing from DID: {last_processed_did}")
                all_dids = all_dids[start_index:]

        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                # Concurrently process batches and update the handles
                for i in range(0, total_dids, batch_size):
                    logger.info("Getting batch to resolve.")
                    batch_dids = all_dids[i:i + batch_size]
                    # Process the batch asynchronously
                    batch_handles_updated = await database_handler.process_batch(batch_dids, True, table, batch_size)
                    total_handles_updated += batch_handles_updated

                    # Log progress for the current batch
                    logger.info(f"Handles updated: {total_handles_updated}/{total_dids}")
                    logger.info(f"First few DIDs in the batch: {batch_dids[:5]}")

                    # Pause after each batch of handles resolved
                    logger.info("Pausing...")
                    await asyncio.sleep(60)  # Pause for 60 seconds

                logger.info("Users db update finished.")
                await database_handler.delete_temporary_table()
                sys.exit()
    elif args.update_users_did_only_db:
        # Call the function to update the database with all users dids
        logger.info("Users db update did only requested.")
        await database_handler.get_all_users_db(True, False, init_db_run=True)
        logger.info("Users db updated dids finished.")
        sys.exit()
    elif args.fetch_users_count:
        # Call the function to fetch the count of users
        count = await database_handler.count_users_table()
        logger.info(f"Total users in the database: {count}")
        sys.exit()
    elif args.update_users_dids:
        # await database_handler.create_user_status_temporary_table()
        # Call the function to update the database with all users dids
        logger.info("Users db update dids only requested.")
        await database_handler.get_all_users_db(True, False, init_db_run=True)
        logger.info("Users db updated dids finished.")

        logger.info("Users db update requested.")
        all_dids = await database_handler.get_dids_without_handles()
        logger.info("Update users handles requested.")
        total_dids = len(all_dids)
        batch_size = 500
        total_handles_updated = 0
        table = "new_users_temporary_table"

        # Check if there is a last processed DID in the temporary table
        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                try:
                    query = "SELECT last_processed_did FROM new_users_temporary_table"
                    last_processed_did = await connection.fetchval(query)
                except asyncpg.UndefinedTableError:
                    logger.warning("Temporary table doesn't exist.")
                    last_processed_did = None
                except Exception as e:
                    last_processed_did = None
                    logger.error(f"Exception getting from db: {str(e)}")

        if not last_processed_did:
            await database_handler.create_new_users_temporary_table()

        if last_processed_did:
            # Find the index of the last processed DID in the list
            start_index = next((i for i, (did) in enumerate(all_dids) if did == last_processed_did), None)
            if start_index is None:
                logger.warning(
                    f"Last processed DID '{last_processed_did}' not found in the list. Starting from the beginning.")
            else:
                logger.info(f"Resuming processing from DID: {last_processed_did}")
                all_dids = all_dids[start_index:]

        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                # Concurrently process batches and update the handles
                for i in range(0, total_dids, batch_size):
                    logger.info("Getting batch to resolve.")
                    batch_dids = all_dids[i:i + batch_size]

                    # Process the batch asynchronously
                    batch_handles_updated = await database_handler.process_batch(batch_dids, True, table, batch_size)
                    total_handles_updated += batch_handles_updated

                    # Log progress for the current batch
                    logger.info(f"Handles updated: {total_handles_updated}/{total_dids}")
                    logger.info(f"First few DIDs in the batch: {batch_dids[:5]}")

                    # Pause after each batch of handles resolved
                    logger.info("Pausing...")
                    await asyncio.sleep(60)  # Pause for 60 seconds

        logger.info("Users db update finished.")
        await database_handler.delete_new_users_temporary_table()
        sys.exit()
    elif args.retrieve_blocklists_db:
        logger.info("Get Blocklists db requested.")
        await database_handler.update_all_blocklists()
        await database_handler.delete_blocklist_temporary_table()
        logger.info("Blocklist db fetch finished.")
        sys.exit()
    elif args.update_blocklists_db:
        logger.info("Update Blocklists db requested.")
        await database_handler.update_all_blocklists(True)
        await database_handler.delete_blocklist_temporary_table()
        logger.info("Update Blocklists db finished.")
        sys.exit()
    elif args.update_redis_cache:
        logger.info("Cache update requested.")
        status = await database_handler.populate_redis_with_handles()
        if not status:
            logger.info("Cache update complete.")


if __name__ == '__main__':
    asyncio.run(main())
