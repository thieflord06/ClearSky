# scheduler.py

import aioschedule
import asyncio
import database_handler
from config_helper import logger


async def run_scheduler():
    logger.debug("Starting Schedule...")
    # Schedule the async function to run daily at a specific time
    # aioschedule.every().day.at("15:22").do(await database_handler.get_top_blocks())
    aioschedule.every().hour.do(database_handler.get_top_blocks)

    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def start_scheduler():
    logger.info("Starting scheduler...")
    await run_scheduler()
