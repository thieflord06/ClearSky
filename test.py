# test.py
import asyncio
import sys

import core
import database_handler
from config_helper import logger


async def main():
    try:
        await database_handler.create_connection_pools(database_handler.database_config)
    except Exception as e:
        logger.error(f"Error creating connection pool: {e!s}")
        sys.exit()

    # try:
    #     await core.get_handle_history_info("did:plc:picillica.bsky.social")
    # except errors.NotFound:
    #     logger.error("Handle not found")

    # await database_handler.get_cursor_time()

    await core.time_behind()

    # database_handler.get_connection_pool('write')


if __name__ == "__main__":
    asyncio.run(main())
