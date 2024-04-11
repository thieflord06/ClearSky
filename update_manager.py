# update_manager.py

import asyncpg
import database_handler
import on_wire
from config_helper import logger, config
import sys
import argparse
import asyncio
import app
import utils
import os

# python update_manager.py --crawler // Update all info
# python update_manager.py --crawler-forced // Force update all info
# python update_manager.py --update-users-dids // update db with new dids and handles
# python update_manager.py --update-all-did-pds-service-info // get past dids and service info
# python update_manager.py --get-federated-pdses // validate PDSes
# python update_manager.py --count-list-users // count list users


async def main():
    parser = argparse.ArgumentParser(description='ClearSky Update Manager: ' + app.version)
    parser.add_argument('--crawler', action='store_true', help='Update all info')
    parser.add_argument('--crawler-forced', action='store_true', help='Force update all info')
    parser.add_argument('--update-users-dids', action='store_true', help='update db with new dids and handles')
    parser.add_argument('--update-all-did-pds-service-info', action='store_true', help='get past dids and service info')
    parser.add_argument('--get-federated-pdses', action='store_true', help='Validate PDSes')
    parser.add_argument('--count-list-users', action='store_true', help='Count list users')
    args = parser.parse_args()
    try:
        await database_handler.create_connection_pool("read")
        await database_handler.create_connection_pool("write")
    except Exception as e:
        logger.error(f"Error creating connection pool: {str(e)}")
        sys.exit()

    if args.update_users_dids:
        try:
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
            async with database_handler.connection_pools["write"].acquire() as connection:
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

            async with database_handler.connection_pools["write"].acquire() as connection:
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
            await database_handler.process_delete_queue()  # Process the delete count for lists
        except database_handler.DatabaseConnectionError:
            logger.error("Database connection error")
        except Exception as e:
            logger.error(f"Error updating users: {str(e)}")
        sys.exit()
    elif args.crawler:
        try:
            if not os.getenv('CLEAR_SKY'):
                quarter_value = config.get("environment", "quarter")
                total_crawlers = config.get("environment", "total_crawlers")
            else:
                quarter_value = os.environ.get("CLEARSKY_CRAWLER_NUMBER")
                total_crawlers = os.environ.get("CLEARSKY_CRAWLER_TOTAL")

            if not quarter_value:
                logger.warning("Using default quarter.")
                quarter_value = "1"

            if not total_crawlers:
                logger.warning("Using default total crawlers.")
                total_crawlers = "4"

            logger.info("Crawler requested.")
            logger.warning(f"This is crawler: {quarter_value}/{total_crawlers}")

            await asyncio.sleep(10)  # Pause for 10 seconds

            await database_handler.crawl_all(quarter=quarter_value, total_crawlers=total_crawlers)
            await database_handler.delete_temporary_table(quarter_value)
            logger.info("Crawl fetch finished.")
        except database_handler.DatabaseConnectionError:
            logger.error("Database connection error")
            quarter_value = config.get("environment", "quarter")
            await database_handler.delete_temporary_table(quarter_value)
            sys.exit()

        sys.exit()
    elif args.crawler_forced:
        try:
            if not os.getenv('CLEAR_SKY'):
                quarter_value = config.get("environment", "quarter")
                total_crawlers = config.get("environment", "total_crawlers")
            else:
                quarter_value = os.environ.get("CLEARSKY_CRAWLER_NUMBER")
                total_crawlers = os.environ.get("CLEARSKY_CRAWLER_TOTAL")

            if not quarter_value:
                logger.warning("Using default quarter.")
                quarter_value = "1"

            if not total_crawlers:
                logger.warning("Using default total crawlers.")
                total_crawlers = "4"

            logger.info("Crawler forced requested.")
            logger.warning(f"This is crawler: {quarter_value}/{total_crawlers}")

            await asyncio.sleep(10)  # Pause for 10 seconds

            await database_handler.crawl_all(forced=True, quarter=quarter_value)
            await database_handler.delete_temporary_table(quarter_value)
            logger.info("Crawl forced fetch finished.")
        except database_handler.DatabaseConnectionError:
            logger.error("Database connection error")
            quarter_value = config.get("environment", "quarter")
            await database_handler.delete_temporary_table(quarter_value)
            sys.exit()

        sys.exit()
    elif args.update_all_did_pds_service_info:
        try:
            logger.info("Update did pds service information.")
            last_value = await database_handler.check_last_created_did_date()
            if last_value:
                logger.info(f"last value retrieved, starting from: {last_value}")
            else:
                last_value = None
                logger.info(f"No last value retrieved, starting from beginning.")
            await utils.get_all_did_records(last_value)

            logger.info("Getting did:webs without PDSes.")

            dids = await database_handler.get_didwebs_without_pds()

            if dids:
                logger.info(f"Processing {len(dids)} did:webs")
                for did in dids:
                    pds = await on_wire.resolve_did(did, did_web_pds=True)
                    if pds:
                        await database_handler.update_pds(did, pds)
                        logger.info(f"Updated PDS for {did} PDS:{pds}")

            logger.info("Finished processing data.")
        except database_handler.DatabaseConnectionError:
            logger.error("Database connection error")
        except Exception as e:
            logger.error(f"Error updating did pds service information: {str(e)}")
        sys.exit()
    elif args.get_federated_pdses:
        try:
            logger.info("Get federated pdses requested.")
            active, not_active = await utils.get_federated_pdses()
            logger.info("Validated PDSes.")
            if active and not_active:
                logger.info(f"Active PDSes: {active}")
                logger.info(f"Not active PDSes: {not_active}")
                logger.info("Finished processing data. Exiting.")
                sys.exit()
            else:
                logger.info("No PDS info.")

            logger.info("Getting label information.")
            labelers = await database_handler.get_labelers()

            logger.info("Updating labeler data.")
            await database_handler.update_labeler_data(labelers)
        except database_handler.DatabaseConnectionError:
            logger.error("Database connection error")
        except Exception as e:
            logger.error(f"Error getting federated pdses: {str(e)}")
        sys.exit()
    elif args.count_list_users:
        try:
            logger.info("Count list users requested.")
            await database_handler.update_mutelist_count()
            logger.info("Count subscribe list users requested.")
            await database_handler.update_subscribe_list_count()
        except database_handler.DatabaseConnectionError:
            logger.error("Database connection error")
        except Exception as e:
            logger.error(f"Error counting list users: {str(e)}")
        sys.exit()
    else:
        logger.error("Not a recognized command.")
        sys.exit()
if __name__ == '__main__':
    asyncio.run(main())
