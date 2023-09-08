# app.py

import quart
from quart import Quart, render_template, request, session, redirect
from datetime import datetime
import os
import uuid
import asyncio
import database_handler
import on_wire
import utils
import config_helper
from config_helper import logger
# ======================================================================================================================
# ============================= Pre-checks // Set up logging and debugging information =================================
config = config_helper.read_config()

title_name = "ClearSky"
os.system("title " + title_name)
version = "3.5.0"
current_dir = os.getcwd()
log_version = "ClearSky Version: " + version
runtime = datetime.now()
current_time = runtime.strftime("%m%d%Y::%H:%M:%S")

try:
    username = os.getlogin()
except OSError:
    username = "Unknown"

logger.info(log_version)
logger.debug("Ran from: " + current_dir)
logger.debug("Ran by: " + username)
logger.debug("Ran at: " + str(current_time))
logger.info("File Log level: " + str(config.get("handler_fileHandler", "level")))
logger.info("Stdout Log level: " + str(config.get("handler_consoleHandler", "level")))
app = Quart(__name__)

# Configure session secret key
app.secret_key = 'your-secret-key'

session_ip = None


# ======================================================================================================================
# ================================================== HTML Pages ========================================================
@app.route('/')
async def index():
    # Generate a new session number and store it in the session
    if 'session_number' not in session:
        session['session_number'] = generate_session_number()

    return await render_template('index.html')


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
@app.route('/selection_handle', methods=['POST', 'GET'])
async def selection_handle():
    # Check if the request method is GET direct to selection_handle
    if request.method == 'GET':
        # Redirect to the root URL '/'
        return redirect('/', code=302)

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

    if selection in ['1', '2', '3', '4', '5', '6', '8']:
        if selection == "4":
            logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count requested")
            count = await utils.get_user_count()
            formatted_count = '{:,}'.format(count)
            logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count: " + str(count))

            return await render_template('total_users.html', count=formatted_count)
        # Check if did or handle exists before processing
        if utils.is_did(identifier) or utils.is_handle(identifier):
            if utils.is_did(identifier):
                handle_identifier = await utils.use_handle(identifier)
                did_identifier = identifier
            if utils.is_handle(identifier):
                did_identifier = await utils.use_did(identifier)
                handle_identifier = identifier
            if not handle_identifier or "Could not find" in did_identifier:

                return await render_template('error.html', content_type='text/html')
            if selection != "4":
                if not identifier:

                    return await render_template('error.html')
                if selection == "1":
                    logger.info(str(session_ip) + " > " + str(*session.values()) + ": " + "DID resolve request made for: " + identifier)
                    if utils.is_did(did_identifier):
                        result = did_identifier
                    elif "Could not find, there may be a typo." in did_identifier:
                        result = did_identifier
                    else:
                        result = identifier
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + result)

                    avatar_id = await on_wire.get_avatar_id(did_identifier)

                    return await render_template('did.html', result=result, did=did_identifier, avatar_id=avatar_id)
                elif selection == "2":
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Handle resolve request made for: " + identifier)
                    if utils.is_handle(handle_identifier):
                        result = handle_identifier
                    else:
                        result = identifier
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + str(result))

                    avatar_id = await on_wire.get_avatar_id(did_identifier)

                    return await render_template('handle.html', result=result, did=did_identifier, avatar_id=avatar_id)
                elif selection == "3":
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Block list requested for: " + identifier)
                    blocklist, count = await utils.process_user_block_list(identifier)
                    formatted_count = '{:,}'.format(count)
                    if utils.is_did(identifier):
                        identifier = handle_identifier

                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Blocklist Request Result: " + identifier + " | " + "Total blocked: " + str(formatted_count) + " :: " + str(blocklist))

                    if count == 0:
                        message = "Not blocking anyone"
                        return await render_template('not_blocking.html', user=identifier, message=message)

                    return await render_template('blocklist.html', blocklist=blocklist, user=identifier, count=formatted_count)
                elif selection == "5":
                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Block list requested for: " + identifier)
                    if "Could not find, there may be a typo" in did_identifier:
                        message = "Could not find, there may be a typo"

                        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Blocked by: " + str(message))

                        return await render_template('no_result.html', user=identifier, message=message)
                    blocks, dates, count = await utils.get_single_user_blocks(did_identifier)
                    formatted_count = '{:,}'.format(count)
                    if utils.is_did(identifier):
                        identifier = handle_identifier

                    if count == 0:
                        message = "Not blocked by anyone"
                        return await render_template('not_blocked.html', user=identifier, message=message)

                    blocklist = list(zip(blocks, dates))

                    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Blocked by: " + str(blocks) + " :: " + "Total count: " + str(formatted_count))

                    return await render_template('single_blocklist.html', user=identifier, blocklist=blocklist, dates=dates, count=formatted_count)
                elif selection == "6":
                    logger.info("Requesting in-common blocks for: " + identifier)
                    in_common_list, percentages = await database_handler.get_similar_users(did_identifier)

                    if "no blocks" in in_common_list:
                        message = "No blocks to compare"

                        return await render_template('no_result.html', user=identifier, message=message)
                    if not in_common_list:
                        message = "No blocks in common with other users"

                        return await render_template('no_result.html', user=identifier, message=message)

                    in_common_handles = []
                    rounded_percentages = [round(percent, 2) for percent in percentages]
                    for did in in_common_list:
                        handle = await utils.get_user_handle(did)
                        in_common_handles.append(handle)
                    logger.info(in_common_handles)

                    in_common_data = zip(in_common_handles, rounded_percentages)

                    return await render_template('in_common.html', data=in_common_data, user=handle_identifier)
                elif selection == "7":
                    logger.info(f"Requesting in-common blocked for: {await utils.get_user_handle(identifier)}")
                    in_common_list, percentages = await database_handler.get_similar_blocked_by(did_identifier)

                    if "no blocks" in in_common_list:
                        in_common_list = ["No blocks to compare"]
                        percentage = [0]

                        return await render_template('in_common.html', in_common_list=in_common_list, percentages=percentage, user=handle_identifier)

                    if not in_common_list:
                        in_common_list = ["No blocks in common with other users"]
                        percentage = [0]

                        return await render_template('in_common.html', in_common_list=in_common_list, percentages=percentage, user=handle_identifier)

                    in_common_handles = []
                    rounded_percentages = [round(percent, 2) for percent in percentages]
                    for did in in_common_list:
                        handle = await utils.get_user_handle(did)
                        in_common_handles.append(handle)
                    logger.info(in_common_handles)

                    return await render_template('in_common.html', in_common_list=in_common_list, percentages=rounded_percentages, user=handle_identifier)
                elif selection == "8":
                    logger.info(f"Requesting handle history for {identifier}")
                    handle_history = await utils.get_handle_history(did_identifier)
                    logger.info(f"history for {identifier}: {str(handle_history)}")

                    return await render_template('handle_history.html', handle_history=handle_history, identity=identifier)
        else:

            return await render_template('error.html')
    else:
        logger.warning(f"Intentional error: selection = {selection}")

        return await render_template('intentional_error.html')


@app.route('/fun_facts')
async def fun_facts():
    logger.info("Fun facts requested.")

    resolved_blocked = utils.resolved_blocked_cache.get('resolved_blocked')
    resolved_blockers = utils.resolved_blockers_cache.get('resolved_blockers')

    blocked_aid = utils.blocked_avatar_ids_cache.get('blocked_aid')
    blocker_aid = utils.blocker_avatar_ids_cache.get('blocker_aid')

    # Check if both lists are empty
    if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:
        logger.info("Getting new cache.")
        resolved_blocked, resolved_blockers, blocked_aid, blocker_aid = await database_handler.blocklists_updater()

    # If at least one list is not empty, render the regular page
    return await render_template('fun_facts.html', blocked_results=resolved_blocked, blockers_results=resolved_blockers, blocked_aid=blocked_aid, blocker_aid=blocker_aid)


@app.route('/funer_facts')
async def funer_facts():
    logger.info("Funer facts requested.")

    resolved_blocked = utils.resolved_24_blocked_cache.get('resolved_blocked')
    resolved_blockers = utils.resolved_24blockers_cache.get('resolved_blockers')

    blocked_aid = utils.blocked_24_avatar_ids_cache.get('blocked_aid')
    blocker_aid = utils.blocker_24_avatar_ids_cache.get('blocker_aid')

    # Check if both lists are empty
    if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:
        logger.info("Getting new cache.")
        resolved_blocked, resolved_blockers, blocked_aid, blocker_aid = await database_handler.top_24blocklists_updater()

    # If at least one list is not empty, render the regular page
    return await render_template('funer_facts.html', blocked_results=resolved_blocked, blockers_results=resolved_blockers, blocked_aid=blocked_aid, blocker_aid=blocker_aid)


# ======================================================================================================================
# ============================================= Main functions =========================================================
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
    await database_handler.create_24_hour_block_table()
    await database_handler.blocklists_updater()
    await database_handler.top_24blocklists_updater()
    logger.info("Web server starting at: " + ip_address + ":" + port_address)
    await app.run_task(host=ip_address, port=port_address)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
