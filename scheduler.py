# scheduler.py

import aioschedule
import asyncio
import database_handler
from config_helper import logger


async def run_scheduler():
    # Schedule the async function to run daily at a specific time
    logger.info("Starting Schedule...")
    aioschedule.every().day.at("14:42").do(database_handler.blocklists_updater)
    aioschedule.every().hour.do(database_handler.blocklists_updater)

    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)
