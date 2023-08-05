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


# def run_scheduler():
#     aioschedule.run_all()
    # loop = asyncio.get_event_loop()
    # try:
    #     while True:
    #         loop.run_until_complete(aioschedule.run_pending())
    #         loop.run_until_complete(asyncio.sleep(1))
    # except KeyboardInterrupt:
    #     pass
    # finally:
    #     loop.close()


# # Run the scheduler loop
# loop = asyncio.get_event_loop()
# try:
#     while True:
#         aioschedule.run_pending()  # Run scheduled tasks
#         loop.run_until_complete(asyncio.sleep(1))
# except KeyboardInterrupt:
#     pass
# finally:
#     loop.close()

# if __name__ == "__main__":
#     start_scheduler()
#
#     # Run the scheduler loop
#     loop = asyncio.get_event_loop()
#     try:
#         while True:
#             aioschedule.run_pending()  # Run scheduled tasks
#             loop.run_until_complete(asyncio.sleep(1))
#     except KeyboardInterrupt:
#         pass
#     finally:
#         loop.close()
