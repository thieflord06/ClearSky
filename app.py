# app.py

import asyncpg
from flask import Flask, render_template, request, session, jsonify
from datetime import datetime
from waitress import serve
import os
import sys
import uuid
import argparse
import re
import asyncio
import database_handler
import utils
import on_wire
import config_helper
from config_helper import logger
# ======================================================================================================================
# ============================= Pre-checks // Set up logging and debugging information =================================
config = config_helper.read_config()

title_name = "ClearSky"
os.system("title " + title_name)
version = "1.6.4"
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
app = Flask(__name__)

# Configure session secret key
app.secret_key = 'your-secret-key'
users_db_folder_path = config.get("database", "users_db_path")
users_db_filename = 'users_cache.db'
users_db_path = users_db_folder_path + users_db_filename

# Get the database configuration
database_config = utils.get_database_config()

# Now you can access the configuration values using dictionary keys
pg_user = database_config["user"]
pg_password = database_config["password"]
pg_host = database_config["host"]
pg_database = database_config["database"]


# ======================================================================================================================
# ================================================== HTML Pages ========================================================
@app.route('/')
def index():
    # Generate a new session number and store it in the session
    if 'session_number' not in session:
        session['session_number'] = generate_session_number()

    return render_template('index.html')


@app.route('/loading')
def loading():

    return render_template('loading.html')


@app.route('/coming_soon')
def coming_soon():
    return render_template('coming_soon.html')


@app.route('/status')
def always_200():

    return "OK", 200


@app.route('/contact')
def contact():

    return render_template('contact.html')


# Handles selection for form
@app.route('/selection_handle', methods=['POST'])
async def selection_handle():
    global session_ip, did_identifier, handle_identifier
    session_ip = get_ip()
    logger.debug(request.form)
    identifier = request.form['identifier'].lower()
    identifier = identifier.strip()
    identifier = identifier.replace('@', '')
    selection = request.form['selection']

    # Check if the flag to skip "Option 5" is present in the form data
    skip_option5 = request.form.get('skipOption5', '').lower()
    if skip_option5 == "true":
        skip_option5 = True
    elif skip_option5 == "false":
        skip_option5 = False

    if selection == "4":
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count requested")
        count = await utils.get_user_count()
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count: " + str(count))

        return jsonify({"count": count})
    if is_did(identifier) or is_handle(identifier):
        # Check if did or handle exists before processing
        if is_did(identifier):
            handle_identifier = await on_wire.resolve_did(identifier)
            did_identifier = "place_holder"
        if is_handle(identifier):
            did_identifier = await on_wire.resolve_handle(identifier)
            handle_identifier = "place_holder"
        if not handle_identifier or not did_identifier:
            return render_template('no_longer_exists.html', content_type='text/html')
        if selection != "4":
            if not identifier:
                return render_template('error.html')
            if selection == "1":
                logger.info(str(session_ip) + " > " + str(*session.values()) + ": " + "DID resolve request made for: " + identifier)
                result = did_identifier
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + result)

                return jsonify({"result": result})
            elif selection == "2":
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Handle resolve request made for: " + identifier)
                result = handle_identifier
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + str(result))

                return jsonify({"result": result})
            elif selection == "3":
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Block list requested for: " + identifier)
                blocklist, count = await get_user_block_list_html(identifier)

                if "did" in identifier:
                    identifier = handle_identifier
                return jsonify({"block_list": blocklist, "user": identifier, "count": count})
            elif selection == "5" and not skip_option5:
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Block list requested for: " + identifier)
                blocks, dates, count = await utils.get_single_user_blocks(identifier)
                if "did" in identifier:
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
                # return jsonify({"user": identifier, "block_list": blocks, "count": count})
            elif skip_option5:
                return jsonify({"message": "Option 5 skipped"})
    else:
        return render_template('error.html')


async def get_user_block_list_html(ident):
    blocked_users, timestamps = await utils.get_user_block_list(ident)
    block_list = []

    if not blocked_users:
        total_blocked = 0
        handles = [f"{ident} hasn't blocked anyone."]
        timestamp = datetime.now().date()
        block_list.append({"handle": handles, "timestamp": timestamp})
        return block_list, total_blocked
    else:
        async with asyncpg.create_pool(
            user=pg_user,
            password=pg_password,
            host=pg_host,
            database=pg_database
        ) as new_connection_pool:
            async with new_connection_pool.acquire() as connection:
                records = await connection.fetch(
                    'SELECT handle FROM users WHERE did = ANY($1)',
                    blocked_users
                )

        handles = [record['handle'] for record in records]
        total_blocked = len(handles)

        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Blocklist Request Result: " + ident + " | " + "Total blocked: " + str(total_blocked) + " :: " + str(list(zip(handles, timestamps))))

        for handle, timestamp in zip(handles, timestamps):
            block_list.append({"handle": handle, "timestamp": timestamp})

        return block_list, total_blocked


# ======================================================================================================================
# ============================================= Main functions =========================================================
def generate_session_number():

    return str(uuid.uuid4().hex)


def get_ip():  # Get IP address of session request
    if 'X-Forwarded-For' in request.headers:
        # Get the client's IP address from the X-Forwarded-For header
        ip = request.headers['X-Forwarded-For']
        # The client's IP address may contain multiple comma-separated values
        # Extract the first IP address from the list
        ip = ip.split(',')[0].strip()
    else:
        # Use the remote address if the X-Forwarded-For header is not available
        ip = request.remote_addr
    return ip


def is_did(identifier):
    did_pattern = r'^did:[a-z]+:[a-zA-Z0-9._:%-]*[a-zA-Z0-9._-]$'
    return re.match(did_pattern, identifier) is not None


def is_handle(identifier):
    handle_pattern = r'^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$'
    return re.match(handle_pattern, identifier) is not None


# ======================================================================================================================
# =============================================== Main Logic ===========================================================
ip_address = config.get("server", "ip")
port_address = config.get("server", "port")

# python app.py --update-users-did-handle-db // command to update users db with dids and handles
# python app.py --update-users-did-only-db // command to update users db with dids only
# python app.py --fetch-users-count // command to get current count in db
# python app.py --update-blocklists-db // command to update all users blocklists
# python app.py --truncate-blocklists_table-db // command to update all users blocklists
# python app.py --truncate-users_table-db // command to update all users blocklists
# python app.py --delete-database // command to delete entire database
# python app.py --retrieve-blocklists-db // initial/re-initialize get for blocklists database


async def main():
    parser = argparse.ArgumentParser(description='ClearSky Web Server: ' + version)
    parser.add_argument('--update-users-did-handle-db', action='store_true', help='Update the database with all users')
    parser.add_argument('--update-users-did-only-db', action='store_true', help='Update the database with all users')
    parser.add_argument('--fetch-users-count', action='store_true', help='Fetch the count of users')
    parser.add_argument('--update-blocklists-db', action='store_true', help='Update the blocklists table')
    parser.add_argument('--retrieve-blocklists-db', action='store_true', help='Initial/re-initialize get for blocklists database')
    parser.add_argument('--truncate-blocklists_table-db', action='store_true', help='delete blocklists table')
    parser.add_argument('--truncate-users_table-db', action='store_true', help='delete users table')
    parser.add_argument('--delete-database', action='store_true', help='delete entire database')
    args = parser.parse_args()

    await database_handler.create_connection_pool()  # Creates connection pool for db

    if args.update_users_did_handle_db:
        # Call the function to update the database with all users
        logger.info("Users db update requested.")
        all_dids = await database_handler.get_all_users_db(True, False)
        logger.info("Users db updated dids.")
        logger.info("Update users handles requested.")
        batch_size = 1000
        total_dids = len(all_dids)
        batch_tasks = []
        total_handles_updated = 0

        async with database_handler.connection_pool.acquire() as connection:
            async with connection.transaction():
                # Concurrently process batches and update the handles
                for i in range(0, total_dids, batch_size):
                    logger.info("Getting batch to resolve.")
                    batch_dids = all_dids[i:i + batch_size]

                    # Process the batch asynchronously
                    batch_handles_updated = await database_handler.process_batch(batch_dids)
                    total_handles_updated += batch_handles_updated

                    # Log progress for the current batch
                    logger.info(f"Handles updated: {total_handles_updated}/{total_dids}")
                    logger.info(f"First few DIDs in the batch: {batch_dids[:5]}")

                logger.info("Users db update finished.")
                sys.exit()
    elif args.update_users_did_only_db:
        # Call the function to update the database with all users dids
        logger.info("Users db update requested.")
        await database_handler.get_all_users_db(True, False, init_db_run=True)
        logger.info("Users db updated dids finished.")
        sys.exit()
    elif args.fetch_users_count:
        # Call the function to fetch the count of users
        count = await database_handler.count_users_table()
        logger.info(f"Total users in the database: {count}")
        sys.exit()
    elif args.retrieve_blocklists_db:
        logger.info("Get Blocklists db requested.")
        await database_handler.update_all_blocklists()
        logger.info("Blocklist db fetch finished.")
        sys.exit()
    elif args.update_blocklists_db:
        logger.info("Update Blocklists db requested.")
        database_handler.get_single_users_blocks_db(run_update=False, get_dids=True)
        logger.info("Update Blocklists db finished.")
        sys.exit()
    else:
        logger.info("Web server starting at: " + ip_address + ":" + port_address)
        await serve(app, host=ip_address, port=port_address)

if __name__ == '__main__':
    asyncio.run(main())
