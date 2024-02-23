# app.py

import sys
import quart
from quart import Quart, render_template, request, session, jsonify
from datetime import datetime, timedelta
import os
import uuid
import asyncio
from quart_rate_limiter import RateLimiter, rate_limit
from quart_cors import cors
import database_handler
import on_wire
import utils
import config_helper
from config_helper import logger
from environment import get_api_var
import aiocron
import aiohttp
import functools

# ======================================================================================================================
# ======================================== global variables // Set up logging ==========================================
config = config_helper.read_config()

title_name = "ClearSky"
os.system("title " + title_name)
version = "3.21.0d"
current_dir = os.getcwd()
log_version = "ClearSky Version: " + version
runtime = datetime.now()
current_time = runtime.strftime("%m%d%Y::%H:%M:%S")

try:
    username = os.getlogin()
except OSError:
    username = "Unknown"

app = Quart(__name__)
rate_limiter = RateLimiter(app)
cors(app, allow_origin="*")

# Configure session secret key
app.secret_key = 'your-secret-key'

session_ip = None
fun_start_time = None
funer_start_time = None
total_start_time = None
block_stats_app_start_time = None
db_connected = None
read_db_connected = None
write_db_connected = None
db_connected = None
blocklist_24_failed = asyncio.Event()
blocklist_failed = asyncio.Event()
db_pool_acquired = asyncio.Event()
push_server = None
default_push_server = "https://ui.staging.clearsky.app"
api_key = None
self_server = None


# ======================================================================================================================
# ============================================= Main functions =========================================================
async def sanitization(identifier):
    identifier = identifier.lower()
    identifier = identifier.strip()
    identifier = identifier.replace('@', '')

    return identifier


async def uri_sanitization(uri):
    if uri:
        if "at://" in uri:
            if "app.bsky.graph.listitem" in uri:
                url = await database_handler.get_listitem_url(uri)

                return url
            elif "app.bsky.graph.listblock" in uri:
                url = await database_handler.get_listblock_url(uri)

                return url
            elif "app.bsky.graph.list" in uri:
                url = await utils.list_uri_to_url(uri)

                return url
            else:
                url = None

                return url
        else:
            url = None

            return url
    else:
        url = None

        return url


async def pre_process_identifier(identifier):
    did_identifier = None
    handle_identifier = None

    if not identifier:  # If form is submitted without anything in the identifier return intentional error

        return None, None

    # Check if did or handle exists before processing
    if utils.is_did(identifier) or utils.is_handle(identifier):
        if utils.is_did(identifier):
            if not await database_handler.local_db():
                try:
                    did_identifier = identifier
                    handle_identifier = await asyncio.wait_for(utils.use_handle(identifier), timeout=30)
                except asyncio.TimeoutError:
                    handle_identifier = None
                    logger.warning("resolution failed, possible connection issue.")
            else:
                did_identifier = identifier
                handle_identifier = await utils.get_user_handle(identifier)
        elif utils.is_handle(identifier):
            if not await database_handler.local_db():
                try:
                    handle_identifier = identifier
                    did_identifier = await asyncio.wait_for(utils.use_did(identifier), timeout=30)
                except asyncio.TimeoutError:
                    did_identifier = None
                    logger.warning("resolution failed, possible connection issue.")
            else:
                handle_identifier = identifier
                did_identifier = await utils.get_user_did(identifier)
        else:
            did_identifier = None
            handle_identifier = None

        return did_identifier, handle_identifier


async def preprocess_status(identifier):
    try:
        persona, status = await utils.identifier_exists_in_db(identifier)
        logger.debug(f"persona: {persona} status: {status}")
    except AttributeError:
        logger.error("db connection issue.")

        return None

    if persona is True and status is True:

        return True
    elif persona is True and status is False:
        logger.info(f"Account: {identifier} deleted")

        return False
    elif status is False and persona is False:
        logger.info(f"{identifier}: does not exist.")

        return None
    else:
        logger.info(f"Error page loaded for resolution failure using: {identifier}")

        return False


def generate_session_number():
    return str(uuid.uuid4().hex)


async def get_ip():  # Get IP address of session request
    if 'X-Forwarded-For' in request.headers:
        # Get the client's IP address from the X-Forwarded-For header
        ip = request.headers.get('X-Forwarded-For')
        # The client's IP address may contain multiple comma-separated values
        # Extract the first IP address from the list
        ip = ip.split(',')[0].strip()
    else:
        # Use the remote address if the X-Forwarded-For header is not available
        ip = request.remote_addr

    return ip


async def get_time_since(time):
    if time is None:
        return "Not initialized"
    time_difference = datetime.now() - time

    minutes = int((time_difference.total_seconds() / 60))
    hours = minutes // 60
    remaining_minutes = minutes % 60

    if hours > 0 and remaining_minutes > 0:
        if hours == 1:
            elapsed_time = f"{int(hours)} hour {int(remaining_minutes)} minutes ago"
        else:
            elapsed_time = f"{int(hours)} hours {int(remaining_minutes)} minutes ago"
    elif hours > 0:
        if hours == 1:
            elapsed_time = f"{int(hours)} hour ago"
        else:
            elapsed_time = f"{int(hours)} hours ago"
    elif minutes > 0:
        if minutes == 1:
            elapsed_time = f"{int(minutes)} minute ago"
        else:
            elapsed_time = f"{int(minutes)} minutes ago"
    else:
        elapsed_time = "less than a minute ago"

    return elapsed_time


async def initialize():
    global read_db_connected, write_db_connected
    global db_pool_acquired
    global push_server
    global api_key
    global self_server

    read_db_connected = await database_handler.create_connection_pool("read")  # Creates connection pool for db if connection made
    write_db_connected = await database_handler.create_connection_pool("write")

    config_api_key = config.get("environment", "api_key")
    config_self_server = config.get("environment", "self_server")

    if not os.getenv('CLEAR_SKY'):
        push_server = config.get("environment", "push_server")
        api_key = config.get("environment", "api_key")
        self_server = config.get("environment", "self_server")
    else:
        push_server = os.environ.get("CLEARSKY_PUSH_SERVER")
        api_key = os.environ.get("CLEARSKY_API_KEY")
        self_server = os.environ.get("CLEARSKY_SELF_SERVER")

    if not api_key:
        logger.error(f"No API key configured, attempting to use config file: {config_api_key}")
        api_key = config_api_key

    if not push_server:
        logger.error(f"No push server configured, using default push server: {default_push_server}")
        push_server = default_push_server

    if not self_server:
        logger.error(f"No self server configured, attempting to use config file: {config_self_server}")
        self_server = config_self_server

    log_warning_once = True

    db_pool_acquired.set()

    if not await database_handler.redis_connected():
        logger.warning("Redis not connected.")
    else:
        database_handler.redis_connection = True

    logger.info("Initialized.")

    if not read_db_connected and write_db_connected:
        while True:
            read_db_connected = await database_handler.create_connection_pool("read")
            write_db_connected = await database_handler.create_connection_pool("write")

            if read_db_connected and write_db_connected:
                db_pool_acquired.set()

                if not log_warning_once:
                    logger.warning("db connection established.")

                logger.info("Initialized.")
                break
            else:
                if log_warning_once:
                    logger.warning("db not operational.")

                    log_warning_once = False

                    blocklist_24_failed.set()
                    blocklist_failed.set()

                logger.info("Waiting for db connection.")
                await asyncio.sleep(30)


async def get_ip_address():
    if not os.environ.get('CLEAR_SKY'):
        logger.info("IP connection: Using config.ini")
        ip_address = config.get("server", "ip")
        port_address = config.get("server", "port")

        return ip_address, port_address
    else:
        logger.info("IP connection: Using environment variables.")
        ip_address = os.environ.get('CLEAR_SKY_IP')
        port_address = os.environ.get('CLEAR_SKY_PORT')

        return ip_address, port_address


async def run_web_server():
    ip_address, port_address = await get_ip_address()

    if not ip_address or not port_address:
        logger.error("No IP or port configured.")
        sys.exit()

    logger.info(f"Web server starting at: {ip_address}:{port_address}")

    await app.run_task(host=ip_address, port=port_address)


async def first_run():
    while not db_pool_acquired.is_set():
        logger.info("db connection not acquired, waiting for established connection.")
        await asyncio.sleep(5)

    while True:
        if read_db_connected and write_db_connected:
            blocklist_24_failed.clear()
            blocklist_failed.clear()

            tables = await database_handler.tables_exists()

            if tables:
                await database_handler.blocklists_updater()
                await database_handler.top_24blocklists_updater()
                await utils.update_block_statistics()
                await utils.update_total_users()

                break
            else:
                logger.warning("Tables do not exist in db.")
                sys.exit()

        await asyncio.sleep(30)


def api_key_required(key_type):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            api_environment = get_api_var()

            provided_api_key = request.headers.get("X-API-Key")

            api_keys = await database_handler.get_api_keys(api_environment, key_type, provided_api_key)

            if provided_api_key not in api_keys.get('key') or api_keys.get('valid') is False:
                ip = await get_ip()
                logger.warning(f"<< {ip}: given key:{provided_api_key} Unauthorized API access.")
                session['authenticated'] = False

                return "Unauthorized", 401  # Return an error response if the API key is not valid
            else:
                logger.info(f"Valid key {provided_api_key} for type: {key_type}")

                session['authenticated'] = True  # Set to True if authenticated

            return await func(*args, **kwargs)

        return wrapper

    return decorator


@app.errorhandler(429)
def ratelimit_error(e):
    return jsonify(error="ratelimit exceeded", message=str(e.description)), 429


async def fetch_and_push_data():
    if api_key:
        try:
            fetch_api = {
                "top_blocked": f'{self_server}/api/v1/auth/lists/fun-facts',
                "top_24_blocked": f'{self_server}/api/v1/auth/lists/funer-facts',
                "block_stats": f'{self_server}/api/v1/auth/lists/block-stats',
                "total_users": f'{self_server}/api/v1/auth/lists/total-users'
            }
            send_api = {
                "top_blocked": f'{push_server}/api/v1/base/reporting/stats-cache/top-blocked',
                "top_24_blocked": f'{push_server}/api/v1/base/reporting/stats-cache/top-24-blocked',
                "block_stats": f'{push_server}/api/v1/base/reporting/stats-cache/block-stats',
                "total_users": f'{push_server}/api/v1/base/reporting/stats-cache/total-users'
            }
            headers = {'X-API-Key': f'{api_key}'}

            async with aiohttp.ClientSession(headers=headers) as session:
                for (fetch_name, fetch_api), (send_name, send_api) in zip(fetch_api.items(), send_api.items()):
                    logger.info(f"Fetching data from {fetch_name} API")

                    async with session.get(fetch_api) as internal_response:
                        if internal_response.status == 200:
                            internal_data = await internal_response.json()
                            if "timeLeft" in internal_data['data']:
                                logger.info(f"{fetch_name} Data not ready, skipping.")
                            else:
                                async with session.post(send_api, json=internal_data) as response:
                                    if response.status == 200:
                                        logger.info(f"Data successfully pushed to {send_api}")
                                    else:
                                        logger.error("Failed to push data to the destination server")
                                        continue
                        else:
                            logger.error(f"Failed to fetch data from {fetch_api}")
                            continue
        except Exception as e:
            logger.error(f"An error occurred: {e}")
    else:
        logger.error("PUSH not executed, no API key configured.")


# Schedule the task to run every hour
@aiocron.crontab('0 * * * *')
async def schedule_data_push():
    logger.info("Starting scheduled data push.")
    await fetch_and_push_data()


# ======================================================================================================================
# ================================================== HTML Pages ========================================================
@app.route('/', methods=['GET'])
async def index():
    # Generate a new session number and store it in the session
    if 'session_number' not in session:
        session['session_number'] = generate_session_number()

    return await render_template('index.html')


@app.route('/images/favicon.png', methods=['GET'])
async def favicon1():
    return await quart.send_from_directory('images', 'favicon.png')


@app.route('/images/apple-touch-icon.png', methods=['GET'])
async def favicon2():
    return await quart.send_from_directory('images', 'apple-touch-icon.png')


@app.route('/images/apple-touch-icon-120x120.png', methods=['GET'])
async def favicon3():
    return await quart.send_from_directory('images', 'apple-touch-icon-120x120.png')


@app.route('/images/apple-touch-icon-152x152.png', methods=['GET'])
async def favicon4():
    return await quart.send_from_directory('images', 'apple-touch-icon-152x152.png')


@app.route('/images/CleardayLarge.png', methods=['GET'])
async def logo():
    return await quart.send_from_directory('images', 'CleardayLarge.png')


@app.route('/frequently_asked', methods=['GET'])
async def faq():
    session_ip = await get_ip()

    logger.info(f"{session_ip} - FAQ requested.")

    return await render_template('coming_soon.html')


@app.route('/coming_soon', methods=['GET'])
async def coming_soon():
    session_ip = await get_ip()

    logger.info(f"{session_ip} - Coming soon requested.")

    return await render_template('coming_soon.html')


@app.route('/status', methods=['GET'])
async def always_200():
    return "OK", 200


@app.route('/contact', methods=['GET'])
async def contact():
    session_ip = await get_ip()

    logger.info(f"{session_ip} - Contact requested.")

    return await render_template('contact.html')


# ======================================================================================================================
# ================================================== API Services ======================================================
async def get_blocklist(client_identifier, page):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - blocklist request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if did_identifier and handle_identifier and status:

            items_per_page = 100
            offset = (page - 1) * items_per_page

            blocklist, count, pages = await utils.process_user_block_list(did_identifier, limit=items_per_page,
                                                                          offset=offset)
            formatted_count = '{:,}'.format(count)

            blocklist_data = {"blocklist": blocklist,
                              "count": formatted_count,
                              "pages": pages}
        else:
            blocklist = None
            count = 0

            blocklist_data = {"blocklist": blocklist,
                              "count": count}

        data = {"identity": identifier,
                "status": status,
                "data": blocklist_data}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - blocklist result returned: {identifier}")

    return jsonify(data)


async def get_single_blocklist(client_identifier, page):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - single blocklist request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if did_identifier and handle_identifier and status:
            items_per_page = 100
            offset = (page - 1) * items_per_page

            blocklist, count, pages = await utils.get_single_user_blocks(did_identifier, limit=items_per_page,
                                                                         offset=offset)
            formatted_count = '{:,}'.format(count)

            blocklist_data = {"blocklist": blocklist,
                              "count": formatted_count,
                              "pages": pages}
        else:
            blocklist_data = None
            count = 0

            blocklist_data = {"blocklist": blocklist_data,
                              "count": count}

        data = {"identity": identifier,
                "status": status,
                "data": blocklist_data}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - single blocklist result returned: {identifier}")

    return jsonify(data)


async def get_in_common_blocklist(client_identifier):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - in-common blocklist request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if did_identifier and handle_identifier and status:

            blocklist_data = await database_handler.get_similar_users(did_identifier)
        else:
            blocklist_data = None

        common_list = {"inCommonList": blocklist_data}

        data = {"identity": identifier,
                "data": common_list}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - in-common blocklist result returned: {identifier}")

    return jsonify(data)


async def get_in_common_blocked(client_identifier):
    not_implemented = True
    common_list = None

    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - in-common blocked request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if not not_implemented:
            if did_identifier and handle_identifier and status:

                blocklist_data = await database_handler.get_similar_blocked_by(did_identifier)
                # formatted_count = '{:,}'.format(count)

            else:
                blocklist_data = None

            common_list = {"inCommonList": blocklist_data}

        if not_implemented:
            data = {"error": "API not Implemented."}
        else:
            data = {"identity": identifier,
                    "data": common_list}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - in-common blocked result returned: {identifier}")

    return jsonify(data)


async def convert_uri_to_url(uri):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    url = await uri_sanitization(uri)

    logger.info(f"<< {session_ip} - {api_key} - get at-uri conversion request: {uri}")

    if url:
        url_data = {"url": url}
        data = {"data": url_data}
    else:
        result = "Malformed parameter"
        url_data = {"error": result}
        data = {"data": url_data}

    logger.info(f">> {session_ip} - {api_key} - at-uri conversion result returned: {uri}")

    return jsonify(data)


async def get_total_users():
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"<< {session_ip} - {api_key} - total users request")

    if utils.total_users_status.is_set():
        logger.info("Total users count is being updated.")

        process_time = utils.total_users_process_time

        if utils.total_users_start_time is None:
            start_time = total_start_time
        else:
            start_time = utils.total_users_start_time

        if process_time is None:
            remaining_time = "not yet determined"
        else:
            time_elapsed = datetime.now() - start_time

            if time_elapsed < process_time:
                # Calculate hours and minutes left
                time_difference = process_time - time_elapsed
                seconds_left = time_difference.total_seconds()
                minutes_left = seconds_left / 60
                # hours = minutes // 60
                remaining_seconds = seconds_left % 60

                if minutes_left > 1:
                    remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                elif seconds_left > 0:
                    remaining_time = f"{round(seconds_left)} seconds"
            else:
                remaining_time = "just finished"

        timing = {"timeLeft": remaining_time}
        data = {"data": timing}

        return jsonify(data)

    total_count = utils.total_users_cache.get('total_users')
    active_count = utils.total_active_users_cache.get('total_active_users')
    deleted_count = utils.total_deleted_users_cache.get('total_deleted_users')

    if total_count is None or active_count is None or deleted_count is None:
        logger.info("Getting total users new cache.")

        process_time = utils.total_users_process_time

        if utils.total_users_start_time is None:
            start_time = total_start_time
        else:
            start_time = utils.total_users_start_time

        if process_time is None:
            remaining_time = "not yet determined"
        else:
            time_elapsed = datetime.now() - start_time

            if time_elapsed < process_time:
                # Calculate hours and minutes left
                time_difference = process_time - time_elapsed
                seconds_left = time_difference.total_seconds()
                minutes_left = seconds_left / 60
                # hours = minutes // 60
                remaining_seconds = seconds_left % 60

                if minutes_left > 1:
                    remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                elif seconds_left > 0:
                    remaining_time = f"{round(seconds_left)} seconds"
            else:
                remaining_time = "just finished"

        timing = {"timeLeft": remaining_time}
        data = {"data": timing}

        asyncio.create_task(utils.update_total_users())

        return jsonify(data)

    formatted_active_count = '{:,}'.format(active_count)
    formatted_total_count = '{:,}'.format(total_count)
    formatted_deleted_count = '{:,}'.format(deleted_count)

    logger.info(f"{session_ip} > {str(*session.values())} | total users count: {formatted_total_count}")
    logger.info(f"{session_ip} > {str(*session.values())} | total active users count: {formatted_active_count}")
    logger.info(f"{session_ip} > {str(*session.values())} | total deleted users count: {formatted_deleted_count}")

    count_data = {
        "active_count": {
            "value": formatted_active_count,
            "displayname": "Active Users",
        },
        "total_count": {
            "value": formatted_total_count,
            "displayname": "Total Users",
        },
        "deleted_count": {
            "value": formatted_deleted_count,
            "displayname": "Deleted Users",
        }
    }

    data = {"data": count_data}

    logger.info(f">> {session_ip} - {api_key} - total users result returned")

    return jsonify(data)


async def get_did_info(client_identifier):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - get did request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if did_identifier and handle_identifier and status:

            avatar_id = await on_wire.get_avatar_id(did_identifier)

            did_data = {"identifier": identifier,
                        "did_identifier": did_identifier,
                        "user_url": f"https://bsky.app/profile/{did_identifier}",
                        "avatar_url": f"https://av-cdn.bsky.app/img/avatar/plain/{avatar_id}"
                        }
        else:
            did_data = None

        data = {"data": did_data}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - did result returned: {identifier}")

    return jsonify(data)


async def get_handle_info(client_identifier):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - get handle request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if did_identifier and handle_identifier and status:

            avatar_id = await on_wire.get_avatar_id(did_identifier)

            handle_data = {"identifier": identifier,
                           "handle_identifier": handle_identifier,
                           "user_url": f"https://bsky.app/profile/{did_identifier}",
                           "avatar_url": f"https://av-cdn.bsky.app/img/avatar/plain/{avatar_id}"
                           }
        else:
            handle_data = None

        data = {"data": handle_data}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - handle result returned: {identifier}")

    return jsonify(data)


async def get_handle_history_info(client_identifier):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - get handle history request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if did_identifier and handle_identifier and status:

            handle_history = await utils.get_handle_history(did_identifier)

            handle_history_data = {"identifier": identifier,
                                   "handle_history": handle_history
                                   }
        else:
            handle_history_data = None

        data = {"data": handle_history_data}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - handle history result returned: {identifier}")

    return jsonify(data)


async def get_list_info(client_identifier):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    identifier = await sanitization(client_identifier)

    logger.info(f"<< {session_ip} - {api_key} - get mute/block list request: {identifier}")

    if identifier:
        did_identifier, handle_identifier = await pre_process_identifier(identifier)
        status = await preprocess_status(identifier)

        if did_identifier and handle_identifier and status:

            mute_lists = await database_handler.get_mutelists(did_identifier)

            list_data = {"identifier": identifier,
                         "lists": mute_lists}
        else:
            list_data = None

        data = {"identifier": identifier,
                "data": list_data}
    else:
        identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - mute/block list result returned: {identifier}")

    return jsonify(data)


async def get_moderation_lists(input_name, page):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"<< {session_ip} - {api_key} - get moderation list request: {input_name}")

    if input_name:
        items_per_page = 100
        offset = (page - 1) * items_per_page

        name = input_name.lower()

        list_data, pages = await database_handler.get_moderation_list(name, limit=items_per_page, offset=offset)

        sub_data = {"lists": list_data,
                    "pages": pages}

        data = {"input": name,
                "data": sub_data}
    else:
        input_name = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - mute/block list result returned: {input_name}")

    return jsonify(data)


async def get_blocked_search(client_identifier, search_identifier):
    api_key = request.headers.get('X-API-Key')
    session_ip = await get_ip()

    client_identifier = await sanitization(client_identifier)
    search_identifier = await sanitization(search_identifier)

    logger.info(f"<< {session_ip} - {api_key} - blocklist[blocked] search request: {client_identifier}:{search_identifier}")

    if client_identifier and search_identifier:
        client_is_handle = utils.is_handle(client_identifier)
        search_is_handle = utils.is_handle(search_identifier)

        if client_is_handle and search_is_handle:
            result = await database_handler.blocklist_search(client_identifier, search_identifier, switch="blocked")
        else:
            result = None

        if result:
            block_data = result
        else:
            block_data = None

        data = {"data": block_data}
    else:
        if not client_identifier:
            client_identifier = "Missing parameter"
        if not search_identifier:
            search_identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - blocklist[blocked] search result returned: {client_identifier}:{search_identifier}")

    return jsonify(data)


async def get_blocking_search(client_identifier, search_identifier):
    api_key = request.headers.get('X-API-Key')
    session_ip = await get_ip()

    client_identifier = await sanitization(client_identifier)
    search_identifier = await sanitization(search_identifier)

    logger.info(f"<< {session_ip} - {api_key} - blocklist[blocking] search request: {client_identifier}:{search_identifier}")

    if client_identifier and search_identifier:
        client_is_handle = utils.is_handle(client_identifier)
        search_is_handle = utils.is_handle(search_identifier)

        if client_is_handle and search_is_handle:
            result = await database_handler.blocklist_search(client_identifier, search_identifier, switch="blocking")
        else:
            result = None

        if result:
            block_data = result
        else:
            block_data = None

        data = {"data": block_data}
    else:
        if not client_identifier:
            client_identifier = "Missing parameter"
        if not search_identifier:
            search_identifier = "Missing parameter"
        result = "Missing parameter"
        block_data = {"error": result}
        data = {"data": block_data}

    logger.info(f">> {session_ip} - {api_key} - blocklist[blocking] search result returned: {client_identifier}:{search_identifier}")

    return jsonify(data)


async def fun_facts():
    global fun_start_time

    api_key = request.headers.get('X-API-Key')
    session_ip = await get_ip()

    logger.info(f"<< Fun facts requested: {session_ip} - {api_key}")

    # if True:
    #
    #     return await render_template('known_issue.html')

    if not read_db_connected and write_db_connected:
        logger.error("Database connection is not live.")

        message = "db not connected"
        error = {"error": message}

        data = {"data": error}

        logger.info(f">> Fun facts result returned: {session_ip} - {api_key}")

        return jsonify(data)

    if database_handler.blocklist_updater_status.is_set():
        resolved_blocked = utils.memory_resolved_blocked_cache.get('resolved_blocked')
        resolved_blockers = utils.memory_resolved_blockers_cache.get('resolved_blockers')

        blocked_aid = utils.memory_blocked_avatar_ids_cache.get('blocked_aid')
        blocker_aid = utils.memory_blocker_avatar_ids_cache.get('blocker_aid')

        data_lists = {"blocked": resolved_blocked,
                      "blockers": resolved_blockers,
                      "blocked_aid": blocked_aid,
                      "blockers_aid": blocker_aid
                      }

        data = {"data": data_lists}

        if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:

            remaining_time = "not yet determined"

            logger.info("Updating top blocks.")

            process_time = database_handler.top_blocks_process_time

            if database_handler.top_blocks_start_time is None:
                start_time = fun_start_time
            else:
                start_time = database_handler.top_blocks_start_time

            if process_time is None:
                remaining_time = "not yet determined"
            else:
                time_elapsed = datetime.now() - start_time

                if time_elapsed < process_time:
                    # Calculate hours and minutes left
                    time_difference = process_time - time_elapsed
                    seconds_left = time_difference.total_seconds()
                    minutes_left = seconds_left / 60
                    # hours = minutes // 60
                    remaining_seconds = seconds_left % 60

                    if minutes_left > 1:
                        remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                    elif seconds_left > 0:
                        remaining_time = f"{round(seconds_left)} seconds"
                else:
                    remaining_time = "just finished"

            timing = {"timeLeft": remaining_time}
            data = {"data": timing}

        logger.info(f">> Fun facts result returned: {session_ip} - {api_key}")

        return jsonify(data)

    resolved_blocked = utils.resolved_blocked_cache.get('resolved_blocked')
    resolved_blockers = utils.resolved_blockers_cache.get('resolved_blockers')

    blocked_aid = utils.blocked_avatar_ids_cache.get('blocked_aid')
    blocker_aid = utils.blocker_avatar_ids_cache.get('blocker_aid')

    # Check if both lists are empty
    if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:
        asyncio.create_task(database_handler.blocklists_updater())

        resolved_blocked = utils.memory_resolved_blocked_cache.get('resolved_blocked')
        resolved_blockers = utils.memory_resolved_blockers_cache.get('resolved_blockers')

        blocked_aid = utils.memory_blocked_avatar_ids_cache.get('blocked_aid')
        blocker_aid = utils.memory_blocker_avatar_ids_cache.get('blocker_aid')

        data_lists = {"blocked": resolved_blocked,
                      "blockers": resolved_blockers,
                      "blocked_aid": blocked_aid,
                      "blockers_aid": blocker_aid
                      }

        data = {"data": data_lists}

        if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:
            remaining_time = "not yet determined"

            logger.info("Getting new cache.")

            process_time = database_handler.top_blocks_process_time

            if database_handler.top_blocks_start_time is None:
                start_time = datetime.now()
            else:
                start_time = datetime.now()

            if process_time is None:
                remaining_time = "not yet determined"
            else:
                time_elapsed = datetime.now() - start_time

                if time_elapsed < process_time:
                    # Calculate hours and minutes left
                    time_difference = process_time - time_elapsed
                    seconds_left = time_difference.total_seconds()
                    minutes_left = seconds_left / 60
                    # hours = minutes // 60
                    remaining_seconds = seconds_left % 60

                    if minutes_left > 1:
                        remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                    elif seconds_left > 0:
                        remaining_time = f"{round(seconds_left)} seconds"
                else:
                    remaining_time = "just finished"

            # asyncio.create_task(database_handler.blocklists_updater())

            timing = {"timeLeft": remaining_time}
            data = {"data": timing}

        logger.info(f">> Fun facts result returned: {session_ip} - {api_key}")

        return jsonify(data)

    data_lists = {"blocked": resolved_blocked,
                  "blockers": resolved_blockers,
                  "blocked_aid": blocked_aid,
                  "blockers_aid": blocker_aid
                  }

    # profile_url = "https://av-cdn.bsky.app/img/avatar/plain/{{item.did}}/{{blocked_aid[item.did]}}"

    data = {"data": data_lists,
            "as of": database_handler.top_blocked_as_of_time}

    logger.info(f">> Fun facts result returned: {session_ip} - {api_key}")

    return jsonify(data)


async def funer_facts():
    global funer_start_time

    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"<< Funer facts requested: {session_ip} - {api_key}")

    if not read_db_connected and write_db_connected:
        logger.error("Database connection is not live.")

        message = "db not connected"
        error = {"error": message}

        data = {"data": error}

        logger.info(f">> Funer facts result returned: {session_ip} - {api_key}")

        return jsonify(data)

    if database_handler.blocklist_24_updater_status.is_set():
        resolved_blocked_24 = utils.memory_resolved_24_blocked_cache.get('resolved_blocked')
        resolved_blockers_24 = utils.memory_resolved_24blockers_cache.get('resolved_blockers')

        blocked_aid_24 = utils.memory_blocked_24_avatar_ids_cache.get('blocked_aid')
        blocker_aid_24 = utils.memory_blocker_24_avatar_ids_cache.get('blocker_aid')

        data_lists = {"blocked24": resolved_blocked_24,
                      "blockers24": resolved_blockers_24,
                      "blocked_aid": blocked_aid_24,
                      "blockers_aid": blocker_aid_24
                      }

        data = {"data": data_lists}

        if resolved_blocked_24 is None or resolved_blockers_24 is None or blocker_aid_24 is None or blocker_aid_24 is None:
            remaining_time = "not yet determined"

            logger.info("Updating top 24 blocks.")

            process_time = database_handler.top_24_blocks_process_time

            if database_handler.top_24_blocks_start_time is None:
                start_time = funer_start_time
            else:
                start_time = database_handler.top_24_blocks_start_time

            if process_time is None:
                remaining_time = "not yet determined"
            else:
                time_elapsed = datetime.now() - start_time

                if time_elapsed < process_time:
                    # Calculate hours and minutes left
                    time_difference = process_time - time_elapsed
                    seconds_left = time_difference.total_seconds()
                    minutes_left = seconds_left / 60
                    # hours = minutes // 60
                    remaining_seconds = seconds_left % 60

                    if minutes_left > 1:
                        remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                    elif seconds_left > 0:
                        remaining_time = f"{round(seconds_left)} seconds"
                else:
                    remaining_time = "just finished"

            timing = {"timeLeft": remaining_time}
            data = {"data": timing}

        logger.info(f">> Funer facts result returned: {session_ip} - {api_key}")

        return jsonify(data)

    resolved_blocked_24 = utils.resolved_24_blocked_cache.get('resolved_blocked')
    resolved_blockers_24 = utils.resolved_24blockers_cache.get('resolved_blockers')

    blocked_aid_24 = utils.blocked_24_avatar_ids_cache.get('blocked_aid')
    blocker_aid_24 = utils.blocker_24_avatar_ids_cache.get('blocker_aid')

    # Check if both lists are empty
    if resolved_blocked_24 is None or resolved_blockers_24 is None or blocker_aid_24 is None or blocker_aid_24 is None:
        asyncio.create_task(database_handler.top_24blocklists_updater())

        resolved_blocked_24 = utils.memory_resolved_24_blocked_cache.get('resolved_blocked')
        resolved_blockers_24 = utils.memory_resolved_24blockers_cache.get('resolved_blockers')

        blocked_aid_24 = utils.memory_blocked_24_avatar_ids_cache.get('blocked_aid')
        blocker_aid_24 = utils.memory_blocker_24_avatar_ids_cache.get('blocker_aid')

        data_lists = {"blocked24": resolved_blocked_24,
                      "blockers24": resolved_blockers_24,
                      "blocked_aid": blocked_aid_24,
                      "blockers_aid": blocker_aid_24
                      }

        data = {"data": data_lists}

        if resolved_blocked_24 is None or resolved_blockers_24 is None or blocker_aid_24 is None or blocker_aid_24 is None:
            remaining_time = "not yet determined"

            logger.info("Getting new cache.")

            process_time = database_handler.top_24_blocks_process_time

            if process_time is None:
                funer_start_time = datetime.now()
            else:
                funer_start_time = datetime.now()

            if process_time is None:
                remaining_time = "not yet determined"
            else:
                time_elapsed = datetime.now() - funer_start_time

                if time_elapsed < process_time:
                    # Calculate hours and minutes left
                    time_difference = process_time - time_elapsed
                    seconds_left = time_difference.total_seconds()
                    minutes_left = seconds_left / 60
                    # hours = minutes // 60
                    remaining_seconds = seconds_left % 60

                    if minutes_left > 1:
                        remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                    elif seconds_left > 0:
                        remaining_time = f"{round(seconds_left)} seconds"
                else:
                    remaining_time = "just finished"

            # asyncio.create_task(database_handler.top_24blocklists_updater())

            timing = {"timeLeft": remaining_time}
            data = {"data": timing}

        logger.info(f">> Funer facts result returned: {session_ip} - {api_key}")

        return jsonify(data)

    data_lists = {"blocked24": resolved_blocked_24,
                  "blockers24": resolved_blockers_24,
                  "blocked_aid": blocked_aid_24,
                  "blockers_aid": blocker_aid_24
                  }

    # profile_url = "https://av-cdn.bsky.app/img/avatar/plain/{{item.did}}/{{blocked_aid[item.did]}}"

    data = {"data": data_lists,
            "as of": database_handler.top_24_blocked_as_of_time}

    logger.info(f">> Funer facts result returned: {session_ip} - {api_key}")

    return jsonify(data)


async def block_stats():
    global block_stats_app_start_time

    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"<< Requesting block statistics: {session_ip} - {api_key}")

    if not read_db_connected and write_db_connected:
        logger.error("Database connection is not live.")

        message = "db not connected"
        error = {"error": message}

        data = {"data": error}

        logger.info(f">> block stats result returned: {session_ip} - {api_key}")

        return jsonify(data)

    if utils.block_stats_status.is_set():
        remaining_time = "not yet determined"

        logger.info("Updating block stats.")

        process_time = utils.block_stats_process_time

        if utils.block_stats_start_time is None:
            start_time = block_stats_app_start_time
        else:
            start_time = utils.block_stats_start_time

        if process_time is None:
            remaining_time = "not yet determined"
        else:
            time_elapsed = datetime.now() - start_time

            if time_elapsed < process_time:
                # Calculate hours and minutes left
                time_difference = process_time - time_elapsed
                seconds_left = time_difference.total_seconds()
                minutes_left = seconds_left / 60
                # hours = minutes // 60
                remaining_seconds = seconds_left % 60

                if minutes_left > 1:
                    remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                elif seconds_left > 0:
                    remaining_time = f"{round(seconds_left)} seconds"
            else:
                remaining_time = "just finished"

        timing = {"timeLeft": remaining_time}
        data = {"data": timing}

        logger.info(f">> block stats result returned: {session_ip} - {api_key}")

        return jsonify(data)

    number_of_total_blocks = utils.number_of_total_blocks_cache.get("total_blocks")
    number_of_unique_users_blocked = utils.number_of_unique_users_blocked_cache.get("unique_blocked")
    number_of_unique_users_blocking = utils.number_of_unique_users_blocking_cache.get("unique_blocker")
    number_blocking_1 = utils.number_block_1_cache.get("block1")
    number_blocking_2_and_100 = utils.number_blocking_2_and_100_cache.get("block2to100")
    number_blocking_101_and_1000 = utils.number_blocking_101_and_1000_cache.get("block101to1000")
    number_blocking_greater_than_1000 = utils.number_blocking_greater_than_1000_cache.get("blockmore1000")
    average_number_of_blocks = utils.average_number_of_blocking_cache.get("averageblocks")
    number_blocked_1 = utils.number_blocked_1_cache.get("blocked1")
    number_blocked_2_and_100 = utils.number_blocked_2_and_100_cache.get("blocked2to100")
    number_blocked_101_and_1000 = utils.number_blocked_101_and_1000_cache.get("blocked101to1000")
    number_blocked_greater_than_1000 = utils.number_blocked_greater_than_1000_cache.get("blockedmore1000")
    average_number_of_blocked = utils.average_number_of_blocked_cache.get("averageblocked")
    total_users = utils.block_stats_total_users_cache.get("total_users")

    values_to_check = (
        number_of_total_blocks,
        number_of_unique_users_blocked,
        number_of_unique_users_blocking,
        number_blocking_1,
        number_blocking_2_and_100,
        number_blocking_101_and_1000,
        number_blocking_greater_than_1000,
        average_number_of_blocks,
        number_blocked_1,
        number_blocked_2_and_100,
        number_blocked_101_and_1000,
        number_blocked_greater_than_1000,
        average_number_of_blocked,
        total_users
    )

    if any(value is None for value in values_to_check) and not await database_handler.local_db():
        remaining_time = "not yet determined"

        logger.info("Getting new cache.")

        process_time = utils.block_stats_process_time

        if process_time is None:
            block_stats_app_start_time = datetime.now()
        else:
            block_stats_app_start_time = datetime.now()

        if process_time is None:
            remaining_time = "not yet determined"
        else:
            time_elapsed = datetime.now() - block_stats_app_start_time

            if time_elapsed < process_time:
                # Calculate hours and minutes left
                time_difference = process_time - time_elapsed
                seconds_left = time_difference.total_seconds()
                minutes_left = seconds_left / 60
                # hours = minutes // 60
                remaining_seconds = seconds_left % 60

                if minutes_left > 1:
                    remaining_time = f"{round(minutes_left)} mins {round(remaining_seconds)} seconds"
                elif seconds_left > 0:
                    remaining_time = f"{round(seconds_left)} seconds"
            else:
                remaining_time = "just finished"

        asyncio.create_task(utils.update_block_statistics())

        timing = {"timeLeft": remaining_time}
        data = {"data": timing}

        logger.info(f">> block stats result returned: {session_ip} - {api_key}")

        return jsonify(data)

    percent_users_blocked = (int(number_of_unique_users_blocked) / int(total_users)) * 100
    percent_users_blocking = (int(number_of_unique_users_blocking) / int(total_users)) * 100

    percent_users_blocked = round(percent_users_blocked, 2)
    percent_users_blocking = round(percent_users_blocking, 2)

    percent_number_blocking_1 = round((int(number_blocking_1) / int(number_of_unique_users_blocking) * 100), 2)
    percent_number_blocking_2_and_100 = round(
        (int(number_blocking_2_and_100) / int(number_of_unique_users_blocking) * 100), 2)
    percent_number_blocking_101_and_1000 = round(
        (int(number_blocking_101_and_1000) / int(number_of_unique_users_blocking) * 100), 2)
    percent_number_blocking_greater_than_1000 = round(
        (int(number_blocking_greater_than_1000) / int(number_of_unique_users_blocking) * 100), 2)

    percent_number_blocked_1 = round((int(number_blocked_1) / int(number_of_unique_users_blocked) * 100), 2)
    percent_number_blocked_2_and_100 = round(
        (int(number_blocked_2_and_100) / int(number_of_unique_users_blocked) * 100), 2)
    percent_number_blocked_101_and_1000 = round(
        (int(number_blocked_101_and_1000) / int(number_of_unique_users_blocked) * 100), 2)
    percent_number_blocked_greater_than_1000 = round(
        (int(number_blocked_greater_than_1000) / int(number_of_unique_users_blocked) * 100), 2)

    average_number_of_blocks_round = round(float(average_number_of_blocks), 2)
    average_number_of_blocked_round = round(float(average_number_of_blocked), 2)

    number_of_total_blocks_formatted = '{:,}'.format(number_of_total_blocks)
    number_of_unique_users_blocked_formatted = '{:,}'.format(number_of_unique_users_blocked)
    number_of_unique_users_blocking_formatted = '{:,}'.format(number_of_unique_users_blocking)
    total_users_formatted = '{:,}'.format(total_users)
    number_block_1_formatted = '{:,}'.format(number_blocking_1)
    number_blocking_2_and_100_formatted = '{:,}'.format(number_blocking_2_and_100)
    number_blocking_101_and_1000_formatted = '{:,}'.format(number_blocking_101_and_1000)
    number_blocking_greater_than_1000_formatted = '{:,}'.format(number_blocking_greater_than_1000)
    average_number_of_blocks_formatted = '{:,}'.format(average_number_of_blocks_round)
    number_blocked_1_formatted = '{:,}'.format(number_blocked_1)
    number_blocked_2_and_100_formatted = '{:,}'.format(number_blocked_2_and_100)
    number_blocked_101_and_1000_formatted = '{:,}'.format(number_blocked_101_and_1000)
    number_blocked_greater_than_1000_formatted = '{:,}'.format(number_blocked_greater_than_1000)

    stats_data = {
        "numberOfTotalBlocks": {
            "value": number_of_total_blocks_formatted,
            "displayname": "Number of Total Blocks",
        },
        "numberOfUniqueUsersBlocked": {
            "value": number_of_unique_users_blocked_formatted,
            "displayname": "Number of Unique Users Blocked",
        },
        "numberOfUniqueUsersBlocking": {
            "value": number_of_unique_users_blocking_formatted,
            "displayname": "Number of Unique Users Blocking",
        },
        "totalUsers": {
            "value": total_users_formatted,
            "displayname": "Total Users",
        },
        "percentUsersBlocked": {
            "value": percent_users_blocked,
            "displayname": "Percent Users Blocked",
        },
        "percentUsersBlocking": {
            "value": percent_users_blocking,
            "displayname": "Percent Users Blocking",
        },
        "numberBlock1": {
            "value": number_block_1_formatted,
            "displayname": "Number of Users Blocking 1 User",
        },
        "numberBlocking2and100": {
            "value": number_blocking_2_and_100_formatted,
            "displayname": "Number of Users Blocking 2-100 Users",
        },
        "numberBlocking101and1000": {
            "value": number_blocking_101_and_1000_formatted,
            "displayname": "Number of Users Blocking 101-1000 Users",
        },
        "numberBlockingGreaterThan1000": {
            "value": number_blocking_greater_than_1000_formatted,
            "displayname": "Number of Users Blocking More than 1000 Users",
        },
        "percentNumberBlocking1": {
            "value": percent_number_blocking_1,
            "displayname": "Percent of Users Blocking 1 User",
        },
        "percentNumberBlocking2and100": {
            "value": percent_number_blocking_2_and_100,
            "displayname": "Percent of Users Blocking 2-100 Users",
        },
        "percentNumberBlocking101and1000": {
            "value": percent_number_blocking_101_and_1000,
            "displayname": "Percent of Users Blocking 101-1000 Users",
        },
        "percentNumberBlockingGreaterThan1000": {
            "value": percent_number_blocking_greater_than_1000,
            "displayname": "Percent of Users Blocking More than 1000 Users",
        },
        "averageNumberOfBlocks": {
            "value": average_number_of_blocks_formatted,
            "displayname": "Average Number of Blocks",
        },
        "numberBlocked1": {
            "value": number_blocked_1_formatted,
            "displayname": "Number of Users Blocked by 1 User",
        },
        "numberBlocked2and100": {
            "value": number_blocked_2_and_100_formatted,
            "displayname": "Number of Users Blocked by 2-100 Users",
        },
        "numberBlocked101and1000": {
            "value": number_blocked_101_and_1000_formatted,
            "displayname": "Number of Users Blocked by 101-1000 Users",
        },
        "numberBlockedGreaterThan1000": {
            "value": number_blocked_greater_than_1000_formatted,
            "displayname": "Number of Users Blocked by More than 1000 Users",
        },
        "percentNumberBlocked1": {
            "value": percent_number_blocked_1,
            "displayname": "Percent of Users Blocked by 1 User",
        },
        "percentNumberBlocked2and100": {
            "value": percent_number_blocked_2_and_100,
            "displayname": "Percent of Users Blocked by 2-100 Users",
        },
        "percentNumberBlocked101and1000": {
            "value": percent_number_blocked_101_and_1000,
            "displayname": "Percent of Users Blocked by 101-1000 Users",
        },
        "percentNumberBlockedGreaterThan1000": {
            "value": percent_number_blocked_greater_than_1000,
            "displayname": "Percent of Users Blocked by More than 1000 Users",
        },
        "averageNumberOfBlocked": {
            "value": average_number_of_blocked_round,
            "displayname": "Average Number of Users Blocked",
        }
    }

    data = {"data": stats_data,
            "as of": utils.block_stats_as_of_time
            }

    logger.info(f">> block stats result returned: {session_ip} - {api_key}")

    return jsonify(data)


async def autocomplete(client_identifier):
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.debug(f"Autocomplete request: {session_ip} - {api_key}")

    query = client_identifier.lower()

    # Remove the '@' symbol if it exists
    query_without_at = query.lstrip('@')

    logger.debug(f"query: {query}")

    if not query_without_at:
        matching_handles = None

        return jsonify({"suggestions": matching_handles})
    elif "did:" in query_without_at:
        matching_handles = None

        return jsonify({"suggestions": matching_handles})
    else:
        if database_handler.redis_connection:
            matching_handles = await database_handler.retrieve_autocomplete_handles(
                query_without_at)  # Use redis, failover db
        elif read_db_connected:
            matching_handles = await database_handler.find_handles(query_without_at)  # Only use db
        else:
            matching_handles = None

        if not matching_handles:
            matching_handles = None

            return jsonify({"suggestions": matching_handles})
        # Add '@' symbol back to the suggestions
        if '@' in query:
            matching_handles_with_at = ['@' + handle for handle in matching_handles]

            return jsonify({'suggestions': matching_handles_with_at})
        else:

            return jsonify({'suggestions': matching_handles})


async def get_internal_status():
    db_status = None
    api_key = request.headers.get('X-API-Key')
    session_ip = await get_ip()

    logger.info(f"<< System status requested: {session_ip} - {api_key}")

    if utils.block_stats_status.is_set():
        stats_status = "processing"
    else:
        if not read_db_connected and write_db_connected:
            stats_status = "waiting"
        else:
            stats_status = "complete"

    if database_handler.blocklist_updater_status.is_set():
        top_blocked_status = "processing"
    else:
        if blocklist_failed.is_set():
            top_blocked_status = "waiting"
        else:
            top_blocked_status = "complete"

    if database_handler.blocklist_24_updater_status.is_set():
        top_24_blocked_status = "processing"
    else:
        if blocklist_24_failed.is_set():
            top_24_blocked_status = "waiting"
        else:
            top_24_blocked_status = "complete"

    redis_connection = await database_handler.redis_connected()

    if redis_connection:
        redis_status = "connected"
    else:
        redis_status = "disconnected"

    if database_handler.block_cache_status.is_set():
        block_cache_status = "processing"
    else:
        if len(database_handler.all_blocks_cache) == 0:
            block_cache_status = "not initialized"
        else:
            block_cache_status = "In memory"
    if not read_db_connected and write_db_connected:
        db_status = "disconnected"
    if not read_db_connected:
        read_db_status = "disconnected"
    else:
        read_db_status = "connected"
    if not write_db_connected:
        write_db_status = "disconnected"
    else:
        write_db_status = "connected"

    now = datetime.now()
    uptime = now - runtime

    block_stats_last_update = await get_time_since(utils.block_stats_last_update)
    top_block_last_update = await get_time_since(database_handler.last_update_top_block)
    top_24_block_last_update = await get_time_since(database_handler.last_update_top_24_block)
    all_blocks_last_update = await get_time_since(database_handler.all_blocks_last_update)

    status = {
        "clearsky version": version,
        "uptime": str(uptime),
        "block stats status": stats_status,
        "block stats last process time": str(utils.block_stats_process_time),
        "block stats last update": str(block_stats_last_update),
        "top blocked status": top_blocked_status,
        "last update top block": str(top_block_last_update),
        "top 24 blocked status": top_24_blocked_status,
        "last update top 24 block": str(top_24_block_last_update),
        "redis status": redis_status,
        "block cache status": block_cache_status,
        "block cache last process time": str(database_handler.all_blocks_process_time),
        "block cache last update": str(all_blocks_last_update),
        "current time": str(datetime.now()),
        "write db status": write_db_status,
        "read db status": read_db_status,
        "db status": db_status
    }

    logger.info(f">> System status result returned: {session_ip} - {api_key}")

    return jsonify(status)


async def check_api_keys():
    api_key = request.headers.get('X-API-Key')
    session_ip = await get_ip()

    api_environment = request.args.get('api_environment')
    key_type = request.args.get('key_type')
    key_value = request.args.get('key_value')

    logger.info(f"<< API key check requested: {session_ip} - {api_key}: {api_environment} - {key_type} - {key_value}")

    if not api_key or not api_environment or not key_type or not key_value:
        value = None

        status = {
            "api_status": "invalid",
            "api key": value
        }

        return jsonify(status)

    api_check = await database_handler.check_api_key(api_environment, key_type, key_value)

    if api_check:
        api_key_status = "valid"
    else:
        api_key_status = "invalid"

    status = {
        "api_status": api_key_status,
        "api key": key_value
    }

    logger.info(f">> API key check result returned: {session_ip} - auth key: {api_key} response: key: {key_value}- {api_key_status}")

    return jsonify(status)


# ======================================================================================================================
# ============================================= Authenticated API Endpoints ============================================
@app.route('/api/v1/auth/blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@app.route('/api/v1/auth/blocklist/<client_identifier>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_blocklist(client_identifier, page):
    return await get_blocklist(client_identifier, page)


@app.route('/api/v1/auth/single-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@app.route('/api/v1/auth/single-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_single_blocklist(client_identifier, page):
    return await get_single_blocklist(client_identifier, page)


@app.route('/api/v1/auth/in-common-blocklist/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_in_common_blocklist(client_identifier):
    return await get_in_common_blocklist(client_identifier)


@app.route('/api/v1/auth/in-common-blocked-by/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_in_common_blocked_by(client_identifier):
    return await get_in_common_blocked(client_identifier)


@app.route('/api/v1/auth/at-uri/<path:uri>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_convert_uri_to_url(uri):
    return await convert_uri_to_url(uri)


@app.route('/api/v1/auth/total-users', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_total_users():
    return await get_total_users()


@app.route('/api/v1/auth/get-did/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_did_info(client_identifier):
    return await get_did_info(client_identifier)


@app.route('/api/v1/auth/get-handle/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_handle_info(client_identifier):
    return await get_handle_info(client_identifier)


@app.route('/api/v1/auth/get-handle-history/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_handle_history_info(client_identifier):
    return await get_handle_history_info(client_identifier)


@app.route('/api/v1/auth/get-list/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_list_info(client_identifier):
    return await get_list_info(client_identifier)


@app.route('/api/v1/auth/get-moderation-list/<string:input_name>', defaults={'page': 1}, methods=['GET'])
@app.route('/api/v1/auth/get-moderation-list/<string:input_name>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_moderation_lists(input_name, page):
    return await get_moderation_lists(input_name, page)


@app.route('/api/v1/auth/blocklist-search-blocked/<client_identifier>/<search_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_blocked_search(client_identifier, search_identifier):
    return await get_blocked_search(client_identifier, search_identifier)


@app.route('/api/v1/auth/blocklist-search-blocking/<client_identifier>/<search_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_blocking_search(client_identifier, search_identifier):
    return await get_blocking_search(client_identifier, search_identifier)


@app.route('/api/v1/auth/lists/fun-facts', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_fun_facts():
    return await fun_facts()


@app.route('/api/v1/auth/lists/funer-facts', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_funer_facts():
    return await funer_facts()


@app.route('/api/v1/auth/lists/block-stats', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_block_stats():
    return await block_stats()


@app.route('/api/v1/auth/base/autocomplete/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_autocomplete(client_identifier):
    return await autocomplete(client_identifier)


@app.route('/api/v1/auth/base/internal/status/process-status', methods=['GET'])
@api_key_required("INTERNALSERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_get_internal_status():
    return await get_internal_status()


@app.route('/api/v1/auth/base/internal/api-check', methods=['GET'])
@api_key_required("INTERNALSERVER")
@rate_limit(50, timedelta(seconds=1))
async def auth_check_api_keys():
    return await check_api_keys()


# ======================================================================================================================
# ========================================== Unauthenticated API Endpoints =============================================
@app.route('/api/v1/anon/blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@app.route('/api/v1/anon/blocklist/<client_identifier>/<int:page>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_blocklist(client_identifier, page):
    return await get_blocklist(client_identifier, page)


@app.route('/api/v1/anon/single-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@app.route('/api/v1/anon/single-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_single_blocklist(client_identifier, page):
    return await get_single_blocklist(client_identifier, page)


@app.route('/api/v1/anon/in-common-blocklist/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_in_common_blocklist(client_identifier):
    return await get_in_common_blocklist(client_identifier)


@app.route('/api/v1/anon/in-common-blocked-by/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_in_common_blocked_by(client_identifier):
    return await get_in_common_blocked(client_identifier)


@app.route('/api/v1/anon/at-uri/<path:uri>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_convert_uri_to_url(uri):
    return await convert_uri_to_url(uri)


@app.route('/api/v1/anon/total-users', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_total_users():
    return await get_total_users()


@app.route('/api/v1/anon/get-did/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_did_info(client_identifier):
    return await get_did_info(client_identifier)


@app.route('/api/v1/anon/get-handle/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_handle_info(client_identifier):
    return await get_handle_info(client_identifier)


@app.route('/api/v1/anon/get-handle-history/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_handle_history_info(client_identifier):
    return await get_handle_history_info(client_identifier)


@app.route('/api/v1/anon/get-list/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_list_info(client_identifier):
    return await get_list_info(client_identifier)


@app.route('/api/v1/anon/get-moderation-list/<string:input_name>', defaults={'page': 1}, methods=['GET'])
@app.route('/api/v1/anon/get-moderation-list/<string:input_name>/<int:page>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_moderation_lists(input_name, page):
    return await get_moderation_lists(input_name, page)


@app.route('/api/v1/anon/blocklist-search-blocked/<client_identifier>/<search_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_blocked_search(client_identifier, search_identifier):
    return await get_blocked_search(client_identifier, search_identifier)


@app.route('/api/v1/anon/blocklist-search-blocking/<client_identifier>/<search_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_blocking_search(client_identifier, search_identifier):
    return await get_blocking_search(client_identifier, search_identifier)


@app.route('/api/v1/anon/lists/fun-facts', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_fun_facts():
    return await fun_facts()


@app.route('/api/v1/anon/lists/funer-facts', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_funer_facts():
    return await funer_facts()


@app.route('/api/anon/v1/lists/block-stats', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_block_stats():
    return await block_stats()


@app.route('/api/v1/auth/base/autocomplete/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_autocomplete(client_identifier):
    return await autocomplete(client_identifier)


@app.route('/api/v1/anon/base/internal/status/process-status', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def anon_get_internal_status():
    return await get_internal_status()


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

    # Fetch and push data immediately
    await fetch_and_push_data()

    aiocron.crontab('* * * * *', schedule_data_push)

    await asyncio.gather(run_web_server_task, first_run())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
