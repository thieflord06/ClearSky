# scheduler.py
import aioschedule
import asyncio
import database_handler
from config_helper import logger


async def run_get_all_users_db():
    await asyncio.run(database_handler.get_all_users_db(True, False, True))


def start_scheduler():
    logger.info("Starting scheduler...")
    # Schedule the async function to run daily at a specific time
    aioschedule.every().day.at("10:20").do(database_handler.get_top_blocks)
    aioschedule.every().day.at("10:22").do(run_get_all_users_db)
