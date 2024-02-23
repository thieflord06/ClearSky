# app.py

import sys
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
# ======================================== global variables // Set up logging ==========================================
config = config_helper.read_config()

title_name = "ClearSky"
os.system("title " + title_name)
version = "3.9.53"
current_dir = os.getcwd()
log_version = "ClearSky Version: " + version
runtime = datetime.now()
current_time = runtime.strftime("%m%d%Y::%H:%M:%S")

try:
    username = os.getlogin()
except OSError:
    username = "Unknown"

app = Quart(__name__)

# Configure session secret key
app.secret_key = 'your-secret-key'

session_ip = None
fun_start_time = None
funer_start_time = None
block_stats_app_start_time = None
db_connected = None
read_db_connected = None
write_db_connected = None
blocklist_24_failed = asyncio.Event()
blocklist_failed = asyncio.Event()
db_pool_acquired = asyncio.Event()


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

    global session_ip

    did_identifier = None
    handle_identifier = None
    data = await request.form
    session_ip = await get_ip()

    logger.debug(data)

    selection = data.get('selection')
    identifier = data.get('identifier')
    identifier = identifier.lower()
    identifier = identifier.strip()
    identifier = identifier.replace('@', '')

    if selection in ['1', '2', '3', '4', '5', '6', '8', '9']:
        # if selection in ['4', '3', '5', '6', '9']:
        #
        #     return await render_template('known_issue.html')

        if selection == "4":
            logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count requested")

            try:
                active_count = await utils.get_user_count(get_active=True)
                total_count = await utils.get_user_count(get_active=False)
                deleted_count = await utils.get_deleted_users_count()
            except AttributeError:
                logger.error("db connection issue.")

                return await render_template('issue.html')

            formatted_active_count = '{:,}'.format(active_count)
            formatted_total_count = '{:,}'.format(total_count)
            formatted_deleted_count = '{:,}'.format(deleted_count)

            logger.info(f"{session_ip} > {str(*session.values())} | total users count: {formatted_total_count}")
            logger.info(f"{session_ip} > {str(*session.values())} | total active users count: {formatted_active_count}")
            logger.info(f"{session_ip} > {str(*session.values())} | total deleted users count: {formatted_deleted_count}")

            return await render_template('total_users.html', active_count=formatted_active_count,
                                         total_count=formatted_total_count, deleted_count=formatted_deleted_count)

        if not identifier:  # If form is submitted without anything in the identifier return intentional error
            logger.warning(f"Intentional error. | {str(session_ip)} > " + str(*session.values()))

            return await render_template('intentional_error.html')

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

            if utils.is_handle(identifier):
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

            if did_identifier and handle_identifier:
                pass
            else:
                if did_identifier is None:
                    did_identifier = await utils.get_user_handle(identifier)
                elif handle_identifier is None:
                    handle_identifier = await utils.get_user_handle(identifier)

                try:
                    persona, status = await utils.identifier_exists_in_db(identifier)
                    logger.debug(f"persona: {persona} status: {status}")
                except AttributeError:
                    logger.error("db connection issue.")

                    return await render_template('issue.html')

                if persona is True and status is True:
                    pass
                elif persona is True and status is False:
                    logger.info(f"Account: {identifier} deleted")

                    return await render_template('account_deleted.html', account=identifier)
                elif status is False and persona is False:
                    logger.info(f"{identifier}: does not exist.")

                    return await render_template('error.html')
                else:
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

                    if not did_identifier:
                        message = "Could not find, there may be a typo"

                        return await render_template('no_result.html', user=identifier, message=message)

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

                    more_data_available = count > items_per_page

                    return await render_template('blocklist.html', blocklist=blocklist, user=identifier,
                                                 count=formatted_count, identifier=did_identifier, page=page, more_data_available=more_data_available)
                elif selection == "5":
                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Single Block list requested for: " + identifier)

                    page = request.args.get('page', default=1, type=int)
                    items_per_page = 100
                    offset = (page - 1) * items_per_page

                    if not did_identifier:
                        message = "Could not find, there may be a typo"

                        logger.info(str(session_ip) + " > " + str(
                            *session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Blocked by: " + str(
                            message))

                        return await render_template('no_result.html', user=identifier, message=message)

                    blocklist, count = await utils.get_single_user_blocks(did_identifier, limit=items_per_page, offset=offset)
                    formatted_count = '{:,}'.format(count)

                    if utils.is_did(identifier):
                        identifier = handle_identifier

                    if count == 0:
                        message = "Not blocked by anyone"
                        return await render_template('not_blocked.html', user=identifier, message=message)

                    logger.info(str(session_ip) + " > " + str(
                        *session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Blocked by: " + str(
                        blocklist) + " :: " + "Total count: " + str(formatted_count))

                    more_data_available = count > items_per_page

                    return await render_template('single_blocklist.html', user=identifier, blocklist=blocklist,
                                                 count=formatted_count, identifier=did_identifier, page=page, more_data_available=more_data_available)
                elif selection == "6":
                    logger.info(f"Requesting in-common blocks for: {identifier}")

                    logger.info("Returning known issue page.")

                    return await render_template('known_issue.html')

                    # in_common_list, percentages, status_list = await database_handler.get_similar_users(did_identifier)
                    #
                    # if "no blocks" in in_common_list:
                    #     message = "No blocks to compare"
                    #
                    #     return await render_template('no_result.html', user=identifier, message=message)
                    #
                    # if not in_common_list:
                    #     message = "No blocks in common with other users"
                    #
                    #     return await render_template('no_result.html', user=identifier, message=message)
                    #
                    # in_common_handles = []
                    # rounded_percentages = [round(percent, 2) for percent in percentages]
                    #
                    # for did in in_common_list:
                    #     handle = await utils.get_user_handle(did)
                    #     in_common_handles.append(handle)
                    # logger.info(in_common_handles)
                    #
                    # in_common_data = zip(in_common_handles, rounded_percentages, status_list)
                    #
                    # return await render_template('in_common.html', data=in_common_data, user=handle_identifier)
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
                elif selection == "9":
                    logger.info(f"Requesting mute list lookup for: {identifier}")

                    mute_lists = await database_handler.get_mutelists(did_identifier)

                    if not mute_lists:
                        message = "This users is not on any lists."

                        return await render_template('not_on_lists.html', user=identifier, message=message)

                    logger.debug(mute_lists)

                    return await render_template('mutelist.html', user=handle_identifier, mute_lists=mute_lists)

        else:
            logger.info(f"Error page loaded because {identifier} isn't a did or handle")

            return await render_template('error.html')
    else:
        logger.warning(f"Intentional error: selection = {selection} | {str(session_ip)} > " + str(*session.values()))

        return await render_template('intentional_error.html')


@app.route('/fun_facts')
async def fun_facts():
    global fun_start_time

    logger.info("Fun facts requested.")

    # if True:
    #
    #     return await render_template('feature_not_available.html')

    if not read_db_connected and write_db_connected:
        logger.error("Database connection is not live.")

        return await render_template('feature_not_available.html')

    if database_handler.blocklist_updater_status.is_set():
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

        return await render_template('please_wait.html', remaining_time=remaining_time)

    resolved_blocked = utils.resolved_blocked_cache.get('resolved_blocked')
    resolved_blockers = utils.resolved_blockers_cache.get('resolved_blockers')

    blocked_aid = utils.blocked_avatar_ids_cache.get('blocked_aid')
    blocker_aid = utils.blocker_avatar_ids_cache.get('blocker_aid')

    # Check if both lists are empty
    if resolved_blocked is None or resolved_blockers is None or blocker_aid is None or blocker_aid is None:
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

        asyncio.create_task(database_handler.blocklists_updater())

        return await render_template('please_wait.html', remaining_time=remaining_time)

    # If the lists aren't empty, render the regular page
    return await render_template('fun_facts.html', blocked_results=resolved_blocked, blockers_results=resolved_blockers,
                                 blocked_aid=blocked_aid, blocker_aid=blocker_aid)


@app.route('/funer_facts')
async def funer_facts():
    global funer_start_time

    logger.info("Funer facts requested.")

    # if True:
    #
    #     return await render_template('feature_not_available.html')

    if not read_db_connected and write_db_connected:
        logger.error("Database connection is not live.")

        return await render_template('feature_not_available.html')

    if database_handler.blocklist_24_updater_status.is_set():
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

        return await render_template('please_wait.html', remaining_time=remaining_time)

    resolved_blocked_24 = utils.resolved_24_blocked_cache.get('resolved_blocked')
    resolved_blockers_24 = utils.resolved_24blockers_cache.get('resolved_blockers')

    blocked_aid_24 = utils.blocked_24_avatar_ids_cache.get('blocked_aid')
    blocker_aid_24 = utils.blocker_24_avatar_ids_cache.get('blocker_aid')

    # Check if both lists are empty
    if resolved_blocked_24 is None or resolved_blockers_24 is None or blocker_aid_24 is None or blocker_aid_24 is None:
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

        asyncio.create_task(database_handler.top_24blocklists_updater())

        return await render_template('please_wait.html', remaining_time=remaining_time)

    # If at least one list is not empty, render the regular page
    return await render_template('funer_facts.html', blocked_results=resolved_blocked_24,
                                 blockers_results=resolved_blockers_24, blocked_aid=blocked_aid_24, blocker_aid=blocker_aid_24)


@app.route('/block_stats')
async def block_stats():
    global block_stats_app_start_time

    logger.info(f"Requesting block statistics.")

    # if True:
    #
    #     return await render_template('feature_not_available.html')

    if not read_db_connected:
        logger.error("Database connection is not live.")

        return await render_template('feature_not_available.html')

    if utils.block_stats_status.is_set():
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

        return await render_template('please_wait.html', remaining_time=remaining_time)

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
    total_users = utils.total_users_cache.get("total_users")

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

        return await render_template('please_wait.html', remaining_time=remaining_time)

    # total_users = await utils.get_user_count(get_active=False)

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
# ============================================= API Endpoints ==========================================================
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
        if database_handler.redis_connection:
            matching_handles = await database_handler.retrieve_autocomplete_handles(query_without_at)  # Use redis, failover db
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


@app.route('/blocklist')
async def blocklist_redirect():
    if request.method == 'GET':

        return redirect('/', code=302)


@app.route('/blocklist/<identifier>')
async def blocklist(identifier):
    if not identifier:
        logger.warning(f"Page request failed, no identifier present. | {str(session_ip)} > " + str(*session.values()))

        return redirect('/', code=302)

    # Check if the 'from' parameter is present in the query string
    request_from = request.args.get('from')

    logger.info(f"{request_from} page request for: {identifier}")

    if request_from == 'next' or request_from == 'previous':
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

        return await render_template('blocklist.html', blocklist=blocklist, count=formatted_count,
                                     more_data_available=more_data_available, page=page, identifier=identifier, user=handle_identifier)
    else:
        logger.warning(f"Page request failed, not from organic search. | {str(session_ip)} > " + str(*session.values()))

        return redirect('/', code=302)


@app.route('/single_blocklist')
async def single_blocklist_redirect():
    if request.method == 'GET':

        return redirect('/', code=302)


@app.route('/single_blocklist/<identifier>')
async def single_blocklist(identifier):
    if not identifier:
        logger.warning(f"Page request failed, no identifier present. | {str(session_ip)} > " + str(*session.values()))

        return redirect('/', code=302)

    # Check if the 'from' parameter is present in the query string
    request_from = request.args.get('from')

    logger.info(f"{request_from} page request for: {identifier}")

    if request_from == 'next' or request_from == 'previous':
        # Get pagination parameters from the request (e.g., page number)
        page = request.args.get('page', default=1, type=int)
        items_per_page = 100
        offset = (page - 1) * items_per_page

        blocklist, count = await utils.get_single_user_blocks(identifier, limit=items_per_page, offset=offset)

        formatted_count = '{:,}'.format(count)
        if utils.is_did(identifier):
            handle_identifier = await utils.use_handle(identifier)

        more_data_available = (offset + len(blocklist)) < count

        if offset + items_per_page > count:
            more_data_available = False

        return await render_template('single_blocklist.html', blocklist=blocklist, count=formatted_count,
                                     more_data_available=more_data_available, page=page, identifier=identifier, user=handle_identifier)
    else:
        logger.warning(f"Page request failed, not from organic search. | {str(session_ip)} > " + str(*session.values()))

        return redirect('/')


@app.route('/process_status', methods=['GET'])
async def update_block_stats():
    logger.info("System status requested.")

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
        "read db status": read_db_status
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
    global db_connected
    global read_db_connected, write_db_connected
    global db_pool_acquired

    read_db_connected = await database_handler.create_connection_pool("read")  # Creates connection pool for db if connection made
    write_db_connected = await database_handler.create_connection_pool("write")

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

                db_connected = True
                # await database_handler.create_connection_pool()  # Creates connection pool for db
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
    else:
        db_connected = True


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

                break
            else:
                logger.warning("Tables do not exist in db.")
                sys.exit()

        await asyncio.sleep(30)


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


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
