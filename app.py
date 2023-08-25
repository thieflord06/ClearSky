# app.py

import quart
from quart import Quart, render_template, request, session, jsonify
from datetime import datetime
import os
import uuid
import asyncio
import database_handler
import utils
import config_helper
from config_helper import logger
# ======================================================================================================================
# ============================= Pre-checks // Set up logging and debugging information =================================
config = config_helper.read_config()

title_name = "ClearSky"
os.system("title " + title_name)
version = "2.4.3"
current_dir = os.getcwd()
log_version = "ClearSky Version: " + version
runtime = datetime.now()
current_time = runtime.strftime("%m%d%Y::%H:%M:%S")
try:
    username = os.getlogin()
except OSError:
    username = "Cannot get username"
    pass
logger.info(log_version)
logger.debug("Ran from: " + current_dir)
logger.debug("Ran by: " + username)
logger.debug("Ran at: " + str(current_time))
logger.info("File Log level: " + str(config.get("handler_fileHandler", "level")))
logger.info("Stdout Log level: " + str(config.get("handler_consoleHandler", "level")))
app = Quart(__name__)

# Configure session secret key
app.secret_key = 'your-secret-key'
users_db_folder_path = config.get("database", "users_db_path")
users_db_filename = 'users_cache.db'
users_db_path = users_db_folder_path + users_db_filename

# Get the database configuration
database_config = database_handler.get_database_config()

# Now you can access the configuration values using dictionary keys
pg_user = database_config["user"]
pg_password = database_config["password"]
pg_host = database_config["host"]
pg_database = database_config["database"]

session_ip = None


# ======================================================================================================================
# ================================================== HTML Pages ========================================================
@app.route('/')
async def index():
    # Generate a new session number and store it in the session
    if 'session_number' not in session:
        session['session_number'] = generate_session_number()

    return await render_template('index.html')


@app.route('/loading')
async def loading():

    return await render_template('loading.html')


@app.route('/images/favicon.png')
async def favicon():

    return await quart.send_from_directory('images', 'favicon.png')


@app.route('/coming_soon')
async def coming_soon():
    return await render_template('coming_soon.html')


@app.route('/status')
async def always_200():

    return "OK", 200


@app.route('/contact')
async def contact():

    return await render_template('contact.html')


# Handles selection for form
@app.route('/selection_handle', methods=['POST'])
async def selection_handle():
    did_identifier = None
    handle_identifier = None
    global session_ip
    data = await request.form
    session_ip = await get_ip()
    logger.debug(data)

    selection = data.get('selection')
    identifier = data.get('identifier')
    identifier = identifier.lower()
    identifier = identifier.strip()
    identifier = identifier.replace('@', '')

    # Check if the flag to skip "Option 5" is present in the form data
    skip_option5 = data.get('skipOption5', '').lower()
    if skip_option5 == "true":
        skip_option5 = True
    else:
        skip_option5 = False
    if selection in ['1', '2', '3', '4', '5', '6']:
        if selection == "4":
            logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count requested")
            count = await utils.get_user_count()
            logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count: " + str(count))

            return jsonify({"count": count})
        # Check if did or handle exists before processing
        if utils.is_did(identifier) or utils.is_handle(identifier):
            if utils.is_did(identifier):
                handle_identifier = await utils.use_handle(identifier)
                did_identifier = identifier
            if utils.is_handle(identifier):
                did_identifier = await utils.use_did(identifier)
                handle_identifier = identifier
            if not handle_identifier or not did_identifier:
                return await render_template('no_longer_exists.html', content_type='text/html')
            if selection != "4":
                if not identifier:
                    return await render_template('error.html')
                if selection == "1":
                    logger.info(str(session_ip) + " > " + str(*session.values()) + ": " + "DID resolve request made for: " + identifier)
                    if utils.is_did(did_identifier):
                        result = did_identifier
                    else:
                        result = identifier
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + result)

                    return jsonify({"result": result})
                elif selection == "2":
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Handle resolve request made for: " + identifier)
                    if utils.is_handle(handle_identifier):
                        result = handle_identifier
                    else:
                        result = identifier
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + str(result))

                    return jsonify({"result": result})
                elif selection == "3":
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Block list requested for: " + identifier)
                    blocklist, count = await get_user_block_list(identifier)

                    if utils.is_did(identifier):
                        identifier = handle_identifier

                    return jsonify({"block_list": blocklist, "user": identifier, "count": count})
                elif selection == "5" and not skip_option5:
                    if utils.is_did(identifier):
                        did_identifier = await utils.use_did(identifier)
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Block list requested for: " + identifier)
                    blocks, dates, count = await utils.get_single_user_blocks(did_identifier)
                    if utils.is_did(identifier):
                        identifier = handle_identifier

                    if type(blocks) != list:
                        blocks = ["None"]
                        dates = [datetime.now().date()]
                        count = 0
                    response_data = {
                        "who_block_list": blocks,
                        "user": identifier,
                        "date": dates,
                        "counts": count
                    }
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Blocked by: " + str(blocks) + " :: " + "Total count: " + str(count))

                    return jsonify(response_data)
                elif selection == "6":
                    logger.info("Requesting in-common blocks for: " + identifier)
                    in_common_list, percentages = await database_handler.get_similar_users(did_identifier)

                    if "no blocks" in in_common_list:
                        in_common_list = ["No blocks to compare"]
                        percentage = [0]
                        response_data = {
                            "in_common_users": in_common_list,
                            "percentages": percentage,
                            "user": handle_identifier
                        }
                        return jsonify(response_data)

                    if not in_common_list:
                        in_common_list = ["No blocks in common with other users"]
                        percentage = [0]
                        response_data = {
                            "in_common_users": in_common_list,
                            "percentages": percentage,
                            "user": handle_identifier
                        }
                        return jsonify(response_data)

                    in_common_handles = []
                    rounded_percentages = [round(percent, 2) for percent in percentages]
                    for did in in_common_list:
                        handle = await utils.get_user_handle(did)
                        in_common_handles.append(handle)
                    logger.info(in_common_handles)
                    response_data = {
                        "in_common_users": in_common_handles,
                        "percentages": rounded_percentages,
                        "user": handle_identifier
                    }
                    return jsonify(response_data)
                elif skip_option5:

                    return jsonify({"message": "Option 5 skipped"})
        else:
            return await render_template('error.html')
    else:
        return await render_template('intentional_error.html')


@app.route('/fun_facts')
async def fun_facts():
    resolved_blocked = utils.resolved_blocked_cache.get('resolved_blocked')
    resolved_blockers = utils.resolved_blockers_cache.get('resolved_blockers')

    # Check if both lists are empty
    if resolved_blocked is None or resolved_blockers is None:
        logger.info("Getting new cache.")
        top_blocked, top_blockers = await database_handler.blocklists_updater()

        resolved_blocked = top_blocked
        resolved_blockers = top_blockers

    # If at least one list is not empty, render the regular page
    return await render_template('fun_facts.html', blocked_results=resolved_blocked, blockers_results=resolved_blockers)


# ======================================================================================================================
# ============================================= Main functions =========================================================
def get_timestamp(item):
    return item["timestamp"]


async def get_user_block_list(ident):
    blocked_users, timestamps = await utils.get_user_block_list(ident)
    block_list = []

    if not blocked_users:
        total_blocked = 0
        if utils.is_did(ident):
            ident = await utils.use_handle(ident)
        handles = [f"{ident} hasn't blocked anyone."]
        timestamp = datetime.now().date()
        block_list.append({"handle": handles, "timestamp": timestamp})
        return block_list, total_blocked
    else:
        async with database_handler.connection_pool.acquire() as connection:
            records = await connection.fetch(
                'SELECT did, handle FROM users WHERE did = ANY($1)',
                blocked_users
            )

            # Create a dictionary that maps did to handle
            did_to_handle = {record['did']: record['handle'] for record in records}

            # Sort records based on the order of blocked_users
            sorted_records = sorted(records, key=lambda record: blocked_users.index(record['did']))

        handles = [did_to_handle[record['did']] for record in sorted_records]
        total_blocked = len(handles)

        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Blocklist Request Result: " + ident + " | " + "Total blocked: " + str(total_blocked) + " :: " + str(list(zip(handles, timestamps))))

        for handle, timestamp in zip(handles, timestamps):
            block_list.append({"handle": handle, "timestamp": timestamp})

        # Sort the block_list by timestamp (newest to oldest)
        block_list = sorted(block_list, key=get_timestamp, reverse=True)

        return block_list, total_blocked


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


# ======================================================================================================================
# =============================================== Main Logic ===========================================================
if not os.environ.get('CLEAR_SKY'):
    logger.info("IP connection: Using config.ini")
    ip_address = config.get("server", "ip")
    port_address = config.get("server", "port")
else:
    logger.info("IP connection: Using environment variables.")
    ip_address = os.environ.get('CLEAR_SKY_IP')
    port_address = os.environ.get('CLEAR_SKY_PORT')


async def main():
    await database_handler.create_connection_pool()  # Creates connection pool for db
    await database_handler.create_top_block_list_table()
    await database_handler.blocklists_updater()
    logger.info("Web server starting at: " + ip_address + ":" + port_address)
    await app.run_task(host=ip_address, port=port_address)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
