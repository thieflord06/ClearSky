from config_helper import logger
import asyncio
import database_handler


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
                if database_handler.local_db(check_local=True):
                    logger.info("Creating local db and tables.")

                    conn = database_handler.connection_pool
                    cursor = conn.cursor()

                    # Execute the SQL statements to create tables
                    cursor.execute(create_users_table)
                    cursor.execute(create_blocklists_table)
                    cursor.execute(create_top_blocks_table)
                    cursor.execute(create_top_24_blocks_table)

                    # Commit changes and close the connection
                    conn.commit()
                    conn.close()
                else:
                    logger.info("Creating tables.")

                    await connection.execute(create_users_table)
                    await connection.execute(create_blocklists_table)
                    await connection.execute(create_top_blocks_table)
                    await connection.execute(create_top_24_blocks_table)
    except Exception as e:
        logger.error(f"Error creating db: {e}")


# ======================================================================================================================
# =============================================== Main Logic ===========================================================
async def main():
    await database_handler.create_connection_pool()
    await create_db()


if __name__ == '__main__':
    asyncio.run(main())
