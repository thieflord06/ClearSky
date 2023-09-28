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
