# setup.py

import sys
from config_helper import logger
import asyncio
import database_handler
import test
import argparse
from app import version


async def create_db():
    try:
        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                create_users_table = """
                CREATE TABLE IF NOT EXISTS users (
                    did text primary key,
                    handle text,
                    status bool
                )
                """

                create_blocklists_table = """
                CREATE TABLE IF NOT EXISTS blocklists (
                    user_did text,
                    blocked_did text,
                    block_date text
                )
                """

                create_top_blocks_table = """
                CREATE TABLE IF NOT EXISTS top_block (
                    did text,
                    count int,
                    list_type text
                )
                """

                create_top_24_blocks_table = """
                CREATE TABLE IF NOT EXISTS top_twentyfour_hour_block (
                    did text,
                    count int,
                    list_type text
                )
                """

                await connection.execute(create_users_table)
                await connection.execute(create_blocklists_table)
                await connection.execute(create_top_blocks_table)
                await connection.execute(create_top_24_blocks_table)
    except Exception as e:
        logger.error(f"Error creating db: {e}")


# ======================================================================================================================
# =============================================== Main Logic ===========================================================
async def main():
    # python update_manager.py --generate-test-data // generate test data
    # python update_manager.py --create-db // create db tables

    parser = argparse.ArgumentParser(description='ClearSky Update Manager: ' + version)
    parser.add_argument('--generate-test-data', action='store_true', help='generate test data')
    parser.add_argument('--create-db', action='store_true', help='create db tables')
    args = parser.parse_args()

    await database_handler.create_connection_pool()

    if args.generate_test_data:
        user_data_list = await test.generate_random_user_data()
        await test.generate_random_block_data(user_data_list)
        sys.exit()
    elif args.create_db:
        logger.info("creating db tables.")
        await create_db()
        sys.exit()
    else:
        database_handler.local_db()

if __name__ == '__main__':
    asyncio.run(main())
