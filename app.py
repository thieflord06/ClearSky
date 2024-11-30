# app.py

import asyncio
import functools
import os
import sys
from datetime import datetime, timezone

import aiocron
from quart import Quart, jsonify, request, session
from quart_cors import cors
from quart_rate_limiter import RateLimiter

import config_helper
import database_handler
import utils
from apis import api_blueprint
from config_helper import logger
from core import db_pool_acquired, initialize, load_api_statuses
from environment import get_api_var
from errors import DatabaseConnectionError, NotFound
from helpers import (
    blocklist_24_failed,
    blocklist_failed,
    get_ip,
    get_ip_address,
    version,
)

# ======================================================================================================================
# ======================================== global variables // Set up logging ==========================================
config = config_helper.read_config()

title_name = "ClearSky"
if sys.platform == "Windows":
    os.system("title " + title_name)  # nosec
current_dir = os.getcwd()
log_version = "ClearSky Version: " + version
current_time = datetime.now(timezone.utc).strftime("%m%d%Y::%H:%M:%S")

try:
    username = os.getlogin()
except OSError:
    username = "Unknown"

app = Quart(__name__)
app.register_blueprint(api_blueprint)
rate_limiter = RateLimiter(app)
cors(app, allow_origin="*")

# Configure session secret key
app.secret_key = "your-secret-key"


# ======================================================================================================================
# ============================================= Main functions =========================================================
async def preprocess_status(identifier) -> bool:
    if not identifier:
        return False

    try:
        persona, status = await database_handler.identifier_exists_in_db(identifier)
        logger.debug(f"persona: {persona} status: {status}")
    except AttributeError:
        logger.error("db connection issue.")

        raise DatabaseConnectionError

    if persona is True and status is True:
        return True
    elif persona is True and status is False:
        logger.info(f"Account: {identifier} deleted")

        return False
    elif status is False and persona is False:
        logger.info(f"{identifier}: does not exist.")

        raise NotFound
    else:
        logger.info(f"Error page loaded for resolution failure using: {identifier}")

        return False


async def run_web_server() -> None:
    ip_address, port_address = await get_ip_address()

    if not ip_address or not port_address:
        logger.error("No IP or port configured.")
        sys.exit()

    logger.info(f"Web server starting at: {ip_address}:{port_address}")

    await app.run_task(host=ip_address, port=int(port_address))


async def first_run() -> None:
    while not db_pool_acquired.is_set():
        logger.info("db connection not acquired, waiting for established connection.")
        await asyncio.sleep(5)

    while True:
        if db_pool_acquired.is_set():
            blocklist_24_failed.clear()
            blocklist_failed.clear()

            await database_handler.blocklists_updater()
            await database_handler.top_24blocklists_updater()
            await utils.update_block_statistics()
            await utils.update_total_users()

            break

        logger.warning("DB connection not established, waiting for connection before running block stats processes.")
        await asyncio.sleep(30)


@aiocron.crontab("0 */12 * * *")  # Every 12 hours
async def schedule_stats_update() -> None:
    logger.info("Starting scheduled stats update.")

    if database_handler.blocklist_updater_status.is_set():
        logger.warning("Blocklist updater is already running.")
    else:
        await database_handler.blocklists_updater()

    if database_handler.blocklist_24_updater_status.is_set():
        logger.warning("Top 24 blocklist updater is already running.")
    else:
        await database_handler.top_24blocklists_updater()

    if utils.block_stats_status.is_set():
        logger.warning("Block stats updater is already running.")
    else:
        await utils.update_block_statistics()

    if utils.total_users_status.is_set():
        logger.warning("Total users updater is already running.")
    else:
        await utils.update_total_users()

    logger.info("Scheduled stats update complete.")


@aiocron.crontab("*/10 * * * *")  # Every 10 mins
async def schedule_total_users_update() -> None:
    if utils.total_users_status.is_set():
        logger.warning("Total users updater is already running.")

        return

    logger.info("Starting scheduled total users update.")

    await utils.update_total_users()

    logger.info("Scheduled total users update complete.")


@aiocron.crontab("*/10 * * * *")  # Every 10 mins
async def refresh_cache():
    await load_api_statuses()


def api_key_required(key_type) -> callable:
    def decorator(func) -> callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> callable:
            api_environment = get_api_var()

            provided_api_key = request.headers.get("X-API-Key")

            api_keys = await database_handler.get_api_keys(api_environment, key_type, provided_api_key)
            try:
                if (
                    provided_api_key not in api_keys.get("key")
                    or api_keys.get("valid") is False
                    or api_keys.get(key_type) is False
                ):
                    ip = await get_ip()
                    logger.warning(f"<< {ip}: given key:{provided_api_key} Unauthorized API access.")
                    session["authenticated"] = False

                    return (
                        "Unauthorized",
                        401,
                    )  # Return an error response if the API key is not valid
            except AttributeError:
                logger.error(f"API key not found for type: {key_type}")
                session["authenticated"] = False

                return "Unauthorized", 401
            else:
                logger.info(f"Valid key {provided_api_key} for type: {key_type}")

                session["authenticated"] = True  # Set to True if authenticated

            return await func(*args, **kwargs)

        return wrapper

    return decorator


@app.errorhandler(429)
def ratelimit_error(e):
    return jsonify(error="ratelimit exceeded", message=str(e.description)), 429


# Load API statuses on startup
@app.before_serving
async def startup():
    await load_api_statuses()


# ======================================================================================================================
# =============================================== Main Logic ===========================================================
async def main():
    logger.info(log_version)
    logger.debug("Ran from: " + current_dir)
    logger.debug("Ran by: " + username)
    logger.debug("Ran at: " + str(current_time))
    logger.info("File Log level: " + str(config.get("handler_fileHandler", "level")))
    logger.info("Stdout Log level: " + str(config.get("handler_consoleHandler", "level")))

    initialize_task = asyncio.create_task(initialize())
    run_web_server_task = asyncio.create_task(run_web_server())

    await initialize_task

    await asyncio.gather(run_web_server_task, first_run())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
