# app.py

import quart
from quart import Quart, render_template, request, session, redirect, jsonify
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
version = "3.9.6"
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

# Create a global lock
update_lock = asyncio.Lock()


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


@app.route('/frequently_asked')
async def faq():
    logger.info("FAQ requested.")

    return await render_template('coming_soon.html')


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
        if not identifier:  # If form is submitted without anything in the identifier return intentional error

            return await render_template('intentional_error.html')
        # Check if did or handle exists before processing
        if utils.is_did(identifier) or utils.is_handle(identifier):
            if utils.is_did(identifier):
                handle_identifier = await utils.use_handle(identifier)
                did_identifier = identifier
            if utils.is_handle(identifier):
                did_identifier = await utils.use_did(identifier)
                handle_identifier = identifier
            if not handle_identifier or "Could not find" in did_identifier:
                logger.info(f"Error page loaded for resolution failure using: {identifier}")

                return await render_template('error.html', content_type='text/html')
            if selection != "4":
                if not identifier:
                    return await render_template('error.html')
                if selection == "1":
                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + ": " + "DID resolve request made for: " + identifier)
                    if utils.is_did(did_identifier):
                        result = did_identifier
                    elif "Could not find, there may be a typo." in did_identifier:
                        result = did_identifier
                    else:
                        result = identifier
                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Request Result: " + identifier + " | " + result)

                    avatar_id = await on_wire.get_avatar_id(did_identifier)

                    return await render_template('did.html', result=result, did=did_identifier, avatar_id=avatar_id)
                elif selection == "2":
                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Handle resolve request made for: " + identifier)
                    if utils.is_handle(handle_identifier):
                        result = handle_identifier
                    else:
                        result = identifier
                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Request Result: " + identifier + " | " + str(result))

                    avatar_id = await on_wire.get_avatar_id(did_identifier)

                    return await render_template('handle.html', result=result, did=did_identifier, avatar_id=avatar_id)
                elif selection == "3":
                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Block list requested for: " + identifier)

                    page = request.args.get('page', default=1, type=int)
                    items_per_page = 100
                    offset = (page - 1) * items_per_page

                    blocklist, count = await utils.process_user_block_list(did_identifier, limit=items_per_page, offset=offset)
                    formatted_count = '{:,}'.format(count)
                    if utils.is_did(identifier):
                        identifier = handle_identifier

                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Blocklist Request Result: " + identifier + " | " + "Total blocked: " + str(
                        formatted_count) + " :: " + str(blocklist))

                    if count == 0:
                        message = "Not blocking anyone"
                        return await render_template('not_blocking.html', user=identifier, message=message)

                    more_data_available = len(blocklist) == items_per_page

                    return await render_template('blocklist.html', blocklist=blocklist, user=identifier,
                                                 count=formatted_count, identifier=did_identifier, page=page, more_data_available=more_data_available)
                elif selection == "5":
                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Single Block list requested for: " + identifier)

                    page = request.args.get('page', default=1, type=int)
                    items_per_page = 100
                    offset = (page - 1) * items_per_page

                    if "Could not find, there may be a typo" in did_identifier:
                        message = "Could not find, there may be a typo"

                        logger.info(str(session_ip) + " > " + str(
                            *session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Blocked by: " + str(
                            message))

                        return await render_template('no_result.html', user=identifier, message=message)
                    blocks, dates, count = await utils.get_single_user_blocks(did_identifier, limit=items_per_page, offset=offset)
                    formatted_count = '{:,}'.format(count)
                    if utils.is_did(identifier):
                        identifier = handle_identifier

                    if count == 0:
                        message = "Not blocked by anyone"
                        return await render_template('not_blocked.html', user=identifier, message=message)

                    blocklist = list(zip(blocks, dates))

                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Blocked by: " + str(
                        blocks) + " :: " + "Total count: " + str(formatted_count))

                    more_data_available = len(blocklist) == items_per_page

                    return await render_template('single_blocklist.html', user=identifier, blocklist=blocklist,
                                                 dates=dates, count=formatted_count, identifier=did_identifier, page=page, more_data_available=more_data_available)
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

                        return await render_template('in_common.html', in_common_list=in_common_list,
                                                     percentages=percentage, user=handle_identifier)

                    if not in_common_list:
                        in_common_list = ["No blocks in common with other users"]
                        percentage = [0]

                        return await render_template('in_common.html', in_common_list=in_common_list,
                                                     percentages=percentage, user=handle_identifier)

                    in_common_handles = []
                    avatar_id_list = []
                    did_list = []
                    rounded_percentages = [round(percent, 2) for percent in percentages]
                    for did in in_common_list:
                        handle = await utils.get_user_handle(did)
                        in_common_handles.append(handle)
                        avatar_id = on_wire.get_avatar_id(did)
                        avatar_id_list.append(avatar_id)
                        did_list.append(did)

                    logger.info(in_common_handles)

                    return await render_template('in_common.html', in_common_list=in_common_list,
                                                 percentages=rounded_percentages, user=handle_identifier,
                                                 did_list=did_list, avatar_id=avatar_id_list)
                elif selection == "8":
                    logger.info(f"Requesting handle history for {identifier}")
                    handle_history = await utils.get_handle_history(did_identifier)
                    logger.info(f"history for {identifier}: {str(handle_history)}")

                    return await render_template('handle_history.html', handle_history=handle_history,
                                                 identity=identifier)
        else:
            logger.info(f"Error page loaded because {identifier} isn't a did or handle")

            return await render_template('error.html')
    else:
        logger.warning(f"Intentional error: selection = {selection}")

        return await render_template('intentional_error.html')


@app.route('/fun_facts')
async def fun_facts():
    logger.info("Fun facts requested.")

    if database_handler.blocklist_updater_status.is_set():
        logger.info("Updating top blocks.")

        return await render_template('please_wait.html')

    resolved_blocked = utils.resolved_blocked_cache.get('resolved_blocked')
    resolved_blockers = utils.resolved_blockers_cache.get('resolved_blockers')

    blocked_aid = utils.blocked_avatar_ids_cache.get('blocked_aid')
    blocker_aid = utils.blocker_avatar_ids_cache.get('blocker_aid')

    # Acquire the lock to ensure only one request can proceed with the update
    async with update_lock:
        # Check if both lists are empty
        if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:
            logger.info("Getting new cache.")
            resolved_blocked, resolved_blockers, blocked_aid, blocker_aid = await database_handler.blocklists_updater()

    # If at least one list is not empty, render the regular page
    return await render_template('fun_facts.html', blocked_results=resolved_blocked, blockers_results=resolved_blockers,
                                 blocked_aid=blocked_aid, blocker_aid=blocker_aid)


@app.route('/funer_facts')
async def funer_facts():
    logger.info("Funer facts requested.")

    if database_handler.blocklist_24_updater_status.is_set():
        logger.info("Updating top 24 blocks.")

        return await render_template('please_wait.html')

    resolved_blocked = utils.resolved_24_blocked_cache.get('resolved_blocked')
    resolved_blockers = utils.resolved_24blockers_cache.get('resolved_blockers')

    blocked_aid = utils.blocked_24_avatar_ids_cache.get('blocked_aid')
    blocker_aid = utils.blocker_24_avatar_ids_cache.get('blocker_aid')

    # Acquire the lock to ensure only one request can proceed with the update
    async with update_lock:
        # Check if both lists are empty
        if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:
            logger.info("Getting new cache.")
            resolved_blocked, resolved_blockers, blocked_aid, blocker_aid = await database_handler.top_24blocklists_updater()

    # If at least one list is not empty, render the regular page
    return await render_template('funer_facts.html', blocked_results=resolved_blocked,
                                 blockers_results=resolved_blockers, blocked_aid=blocked_aid, blocker_aid=blocker_aid)


@app.route('/block_stats')
async def block_stats():
    logger.info(f"Requesting block statistics.")

    if utils.block_stats_status.is_set():
        logger.info("Updating block stats.")

        return await render_template('please_wait.html')

    # Check if the update_lock is locked
    if update_lock.locked():
        logger.info("Block stats waiting page requested.")

        return await render_template('please_wait.html')

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
        average_number_of_blocked
    )

    async with update_lock:
        # Check if both lists are empty
        if any(value is None for value in values_to_check):
            logger.info("Getting new cache.")

            (number_of_total_blocks, number_of_unique_users_blocked, number_of_unique_users_blocking, number_blocking_1,
             number_blocking_2_and_100, number_blocking_101_and_1000, number_blocking_greater_than_1000,
             mean_number_of_blocks, number_blocked_1, number_blocked_2_and_100, number_blocked_101_and_1000,
             number_blocked_greater_than_1000, mean_number_of_blocked) = await utils.update_block_statistics()

    total_users = await utils.get_user_count()

    percent_users_blocked = (int(number_of_unique_users_blocked) / int(total_users)) * 100
    percent_users_blocking = (int(number_of_unique_users_blocking) / int(total_users)) * 100

    percent_users_blocked = round(percent_users_blocked, 2)
    percent_users_blocking = round(percent_users_blocking, 2)

    percent_number_blocking_1 = round((int(number_blocking_1) / int(number_of_unique_users_blocking) * 100), 2)
    percent_number_blocking_2_and_100 = round((int(number_blocking_2_and_100) / int(number_of_unique_users_blocking) * 100), 2)
    percent_number_blocking_101_and_1000 = round((int(number_blocking_101_and_1000) / int(number_of_unique_users_blocking) * 100), 2)
    percent_number_blocking_greater_than_1000 = round((int(number_blocking_greater_than_1000) / int(number_of_unique_users_blocking) * 100), 2)

    percent_number_blocked_1 = round((int(number_blocked_1) / int(number_of_unique_users_blocked) * 100), 2)
    percent_number_blocked_2_and_100 = round((int(number_blocked_2_and_100) / int(number_of_unique_users_blocked) * 100), 2)
    percent_number_blocked_101_and_1000 = round((int(number_blocked_101_and_1000) / int(number_of_unique_users_blocked) * 100), 2)
    percent_number_blocked_greater_than_1000 = round((int(number_blocked_greater_than_1000) / int(number_of_unique_users_blocked) * 100), 2)

    average_number_of_blocks_round = round(float(average_number_of_blocks), 2)
    average_number_of_blocked_round = round(float(average_number_of_blocked), 2)

    return await render_template('blocklist_stats.html', number_of_total_blocks='{:,}'.format(number_of_total_blocks),
                                 number_of_unique_users_blocked='{:,}'.format(number_of_unique_users_blocked),
                                 number_of_unique_users_blocking='{:,}'.format(number_of_unique_users_blocking),
                                 total_users='{:,}'.format(total_users),
                                 percent_users_blocked=percent_users_blocked,
                                 percent_users_blocking=percent_users_blocking,
                                 number_block_1='{:,}'.format(number_blocking_1),
                                 number_blocking_2_and_100='{:,}'.format(number_blocking_2_and_100),
                                 number_blocking_101_and_1000='{:,}'.format(number_blocking_101_and_1000),
                                 number_blocking_greater_than_1000='{:,}'.format(number_blocking_greater_than_1000),
                                 percent_number_blocking_1=percent_number_blocking_1,
                                 percent_number_blocking_2_and_100=percent_number_blocking_2_and_100,
                                 percent_number_blocking_101_and_1000=percent_number_blocking_101_and_1000,
                                 percent_number_blocking_greater_than_1000=percent_number_blocking_greater_than_1000,
                                 average_number_of_blocks='{:,}'.format(average_number_of_blocks_round),
                                 number_blocked_1='{:,}'.format(number_blocked_1),
                                 number_blocked_2_and_100='{:,}'.format(number_blocked_2_and_100),
                                 number_blocked_101_and_1000='{:,}'.format(number_blocked_101_and_1000),
                                 number_blocked_greater_than_1000='{:,}'.format(number_blocked_greater_than_1000),
                                 percent_number_blocked_1=percent_number_blocked_1,
                                 percent_number_blocked_2_and_100=percent_number_blocked_2_and_100,
                                 percent_number_blocked_101_and_1000=percent_number_blocked_101_and_1000,
                                 percent_number_blocked_greater_than_1000=percent_number_blocked_greater_than_1000,
                                 average_number_of_blocked=average_number_of_blocked_round
                                 )


# ======================================================================================================================
# ============================================= API Endpoints +=========================================================
@app.route('/autocomplete')
async def autocomplete():
    query = request.args.get('query')
    query = query.lower()

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
        # matching_handles = await database_handler.retrieve_autocomplete_handles(query_without_at)
        matching_handles = await database_handler.find_handles(query_without_at)

        # Add '@' symbol back to the suggestions
        if '@' in query:
            matching_handles_with_at = ['@' + handle for handle in matching_handles]

            return jsonify({'suggestions': matching_handles_with_at})
        else:

            return jsonify({'suggestions': matching_handles})


@app.route('/blocklist')
async def blocklist_redirect():
    if request.method == 'GET':
        # Redirect to the root URL '/'
        return redirect('/', code=302)


@app.route('/blocklist/<identifier>')
async def blocklist(identifier):
    if not identifier:

        return redirect('/', code=302)

    # Check if the 'from' parameter is present in the query string
    request_from = request.args.get('from')

    if request_from == 'next' or request_from == 'previous':
        # Get pagination parameters from the request (e.g., page number)
        page = request.args.get('page', default=1, type=int)
        items_per_page = 100
        offset = (page - 1) * items_per_page

        blocklist, count = await utils.process_user_block_list(identifier, limit=items_per_page, offset=offset)

        formatted_count = '{:,}'.format(count)
        if utils.is_did(identifier):
            handle_identifier = await utils.use_handle(identifier)

        more_data_available = (offset + len(blocklist)) < count

        if offset + items_per_page > count:
            more_data_available = False

        # Pass the paginated data to your template
        return await render_template('blocklist.html', blocklist=blocklist, count=formatted_count, more_data_available=more_data_available, page=page, identifier=identifier, user=handle_identifier)
    else:
        return redirect('/')


@app.route('/single_blocklist')
async def single_blocklist_redirect():
    if request.method == 'GET':
        # Redirect to the root URL '/'
        return redirect('/', code=302)


@app.route('/single_blocklist/<identifier>')
async def single_blocklist(identifier):
    if not identifier:

        return redirect('/', code=302)

    # Check if the 'from' parameter is present in the query string
    request_from = request.args.get('from')

    if request_from == 'next' or request_from == 'previous':
        # Get pagination parameters from the request (e.g., page number)
        page = request.args.get('page', default=1, type=int)
        items_per_page = 100
        offset = (page - 1) * items_per_page

        blocks, dates, count = await utils.get_single_user_blocks(identifier, limit=items_per_page, offset=offset)

        blocklist = list(zip(blocks, dates))

        formatted_count = '{:,}'.format(count)
        if utils.is_did(identifier):
            handle_identifier = await utils.use_handle(identifier)

        more_data_available = (offset + len(blocks)) < count

        if offset + items_per_page > count:
            more_data_available = False

        # Pass the paginated data to your template
        return await render_template('single_blocklist.html', blocklist=blocklist, dates=dates, count=formatted_count, more_data_available=more_data_available, page=page, identifier=identifier, user=handle_identifier)
    else:
        return redirect('/')


@app.route('/process_status', methods=['GET'])
async def update_block_stats():
    if utils.block_stats_status.is_set():
        stats_status = "processing"
    else:
        stats_status = "complete"

    if database_handler.blocklist_updater_status.is_set():
        top_blocked_status = "processing"
    else:
        top_blocked_status = "complete"

    if database_handler.blocklist_24_updater_status.is_set():
        top_24_blocked_status = "processing"
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
            block_cache_status = "complete"
    now = datetime.now()
    uptime = now - runtime

    status = {
        "clearsky version": version,
        "uptime": str(uptime),
        "block stats status": stats_status,
        "top blocked status": top_blocked_status,
        "top 24 blocked status": top_24_blocked_status,
        "redis status": redis_status,
        "block cache status": block_cache_status
    }
    return jsonify(status)


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

    asyncio.create_task(database_handler.blocklists_updater())
    asyncio.create_task(database_handler.top_24blocklists_updater())
    asyncio.create_task(utils.update_block_statistics())

    logger.info("Web server starting at: " + ip_address + ":" + port_address)

    await app.run_task(host=ip_address, port=port_address)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
