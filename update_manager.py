# update_manager.py

import asyncpg
import database_handler
from config_helper import logger
import sys
import argparse
import asyncio
import app
import utils

# python update_manager.py --update-users-handles // update handles that have changed (initial or re-initialize)
# python update_manager.py --update-users-did-only-db // command to update users db
# python update_manager.py --fetch-users-count // command to get current count in db
# python update_manager.py --update-blocklists-db // command to update all users blocklists
# python update_manager.py --retrieve-blocklists-db // initial/update get for blocklists database
# python update_manager.py --retrieve-blocklists-forced-db // force re-initialize get blocklists
# python update_manager.py --update-users-dids // update db with new dids and handles
# python update_manager.py --update-redis-cache // update handles in redis
# python update_manager.py --retrieve-mutelists-db // initial/re-initialize get for mutelists database
# python update_manager.py --update-all-did-pds-service-info // get past dids and service info


async def main():
    parser = argparse.ArgumentParser(description='ClearSky Update Manager: ' + app.version)
    parser.add_argument('--crawler', action='store_true', help='Update all info')
    parser.add_argument('--crawler-forced', action='store_true', help='force update all info')
    parser.add_argument('--update-users-dids', action='store_true', help='update db with new dids and handles')
    parser.add_argument('--update-all-did-pds-service-info', action='store_true', help='get past dids and service info')
    parser.add_argument('--fetch-users-count', action='store_true', help='Fetch the count of users')
    parser.add_argument('--update-redis-cache', action='store_true', help='Update the redis cache')
    args = parser.parse_args()

    await database_handler.create_connection_pool()  # Creates connection pool for db

    if args.fetch_users_count:
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
    elif args.crawler:
        logger.info("Crawler requested.")
        await database_handler.crawl_all()
        await database_handler.delete_temporary_table()
        logger.info("Crawl fetch finished.")
        sys.exit()
    elif args.crawler_forced:
        logger.info("Crawler forced requested.")
        await database_handler.crawl_all(forced=True)
        await database_handler.delete_temporary_table()
        logger.info("Crawl forced fetch finished.")
        sys.exit()
    elif args.update_redis_cache:
        logger.info("Cache update requested.")
        status = await database_handler.populate_redis_with_handles()
        if not status:
            logger.info("Cache update complete.")
    elif args.update_all_did_pds_service_info:
        logger.info("Update did pds service information.")
        last_value = await database_handler.check_last_created_did_date()
        if last_value:
            logger.info(f"last value retrieved, starting from: {last_value}")
        await utils.get_all_did_records(last_value)
        logger.info("Finished processing data.")

if __name__ == '__main__':
    asyncio.run(main())
