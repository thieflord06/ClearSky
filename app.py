import time
import asyncpg
from flask import Flask, render_template, request, session, jsonify
import requests
import urllib.parse
from datetime import datetime
from waitress import serve
import logging.config
import logging
import configparser
import os
import sys
import uuid
import platform
import argparse
from tqdm import tqdm
import re
import asyncio
import hypercorn
import httpx
from httpx import Timeout, HTTPStatusError
# ======================================================================================================================
# ============================= Pre-checks // Set up logging and debugging information =================================
# Checks if .ini file exits locally and exits if it doesn't
if not os.path.exists('config.ini'):
    print("No Config file", "Config.ini file does not exist\nPlace config.ini in: " + str(os.getcwd()) + "\nRe-run program")
    sys.exit()

# Read log directory from .ini and if directory structure doesn't, exist create it.
config = configparser.ConfigParser()
config.read("config.ini")

# Check OS and assigns log location based on file OS file system
try:
    log_dir = None
    current_os = platform.platform()
    if "Windows" in current_os:
        windows_args = config.get("windows", "args")
        log_dir = config.get("windows", "logdir")
        log_name = config.get("windows", "log_name")
        linux_users_db = config.get("windows", "users_db_path")
        config.set("handler_fileHandler", "args", str(windows_args))
        config.set("handler_fileHandler", "logdir", str(log_dir))
        config.set("handler_fileHandler", "log_name", str(log_name))
        config.set("database", "users_db_path", str(linux_users_db))
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
            configfile.close()
    else:
        linux_users_db = config.get("linux", "users_db_path")
        linux_args = config.get("linux", "args")
        log_dir = config.get("linux", "logdir")
        log_name = config.get("linux", "log_name")
        config.set("handler_fileHandler", "args", str(linux_args))
        config.set("handler_fileHandler", "logdir", str(log_dir))
        config.set("handler_fileHandler", "log_name", str(log_name))
        config.set("database", "users_db_path", str(linux_users_db))
        with open('config.ini', 'w') as configfile:
            config.write(configfile)
            configfile.close()
except (configparser.NoOptionError, configparser.NoSectionError, configparser.MissingSectionHeaderError):
    sys.exit()
try:
    if os.path.exists(log_dir) is False:
        os.makedirs(log_dir)
        os.makedirs(linux_users_db)
except PermissionError:
    print("Cannot create log directory\nChange 'agrs' and 'logdir' in config.ini path to a path with permissions")
    sys.exit()

logging.config.fileConfig('config.ini')
logger = logging.getLogger()

title_name = "ClearSky"
os.system("title " + title_name)
version = "1.1.0"
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
    global session_ip
    session_ip = get_ip()
    logger.debug(request.form)
    identifier = request.form['identifier'].lower()
    identifier = identifier.strip()
    identifier = identifier.replace('@', '')
    selection = request.form['selection']

    if selection == "4":
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count requested")
        count = await get_all_users_db(get_count=True)
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count: " + str(count))

        return jsonify({"count": count})
    if is_did(identifier) or is_handle(identifier):
        if selection != "4":
            if not identifier:
                return render_template('error.html')
            if selection == "1":
                logger.info(str(session_ip) + " > " + str(*session.values()) + ": " + "DID resolve request made for: " + identifier)
                result = await resolve_handle(identifier)
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + result)

                return jsonify({"result": result})
            elif selection == "2":
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Handle resolve request made for: " + identifier)
                result = await resolve_did(identifier)
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + str(result))

                return jsonify({"result": result})
            elif selection == "3":
                if "did" in identifier:
                    identifier = await resolve_did(identifier)
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Block list requested for: " + identifier)
                blocklist, count = await get_user_block_list_html(identifier)

                return jsonify({"block_list": blocklist, "user": identifier, "count": count})
            elif selection == "5":
                if "did" not in identifier:
                    identifier = await resolve_handle(identifier)
                logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Block list requested for: " + identifier)
                blocks, dates, count = await get_single_user_blocks(identifier)
                if "did" in identifier:
                    identifier = await resolve_did(identifier)

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
    else:
        return render_template('error.html')


async def get_user_block_list_html(ident):
    blocked_users, timestamps = await get_user_block_list(ident)
    handles = []
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
# ============================================= On-Wire requests =======================================================
async def resolve_handle(info):  # Take Handle and get DID
    base_url = "https://bsky.social/xrpc/"
    url = urllib.parse.urljoin(base_url, "com.atproto.identity.resolveHandle")
    params = {
        "handle": info
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"
    logger.debug(full_url)

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(full_url)
                response.raise_for_status()

                response_json = response.json()
                logger.debug("response: " + str(response_json))

                result = list(response_json.values())[0]

                return result
        except (httpx.RequestError, HTTPStatusError) as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {e}")
            await asyncio.sleep(2)
            continue

    logger.warning("Resolve error for: " + info + " after multiple retries.")
    return "error"


async def resolve_did(did):  # Take DID and get handle
    handle = did
    base_url = "https://bsky.social/xrpc/"
    url = urllib.parse.urljoin(base_url, "com.atproto.repo.describeRepo")
    params = {
        "repo": handle
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"
    logger.debug(full_url)

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(full_url)
                response.raise_for_status()

                response_json = response.json()
                logger.debug("response: " + str(response_json))

                if response.status_code == 200:
                    records = response_json.get("handle", "")
                    return records
                else:
                    error_message = response_json.get("message", "")
                    logger.debug(error_message)

                    if "could not find user" in error_message.lower():
                        logger.warning("User not found. Skipping...")
                        return
                    else:
                        retry_count += 1
                        logger.warning("Error:" + str(response.status_code))
                        logger.warning("Retrying: " + str(full_url))
                        await asyncio.sleep(10)
        except httpx.RequestError as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {e}")
            await asyncio.sleep(2)
            continue
        except httpx.JSONDecodeError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {e}")
            await asyncio.sleep(2)
            continue
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {e}")
            await asyncio.sleep(2)
            continue

    logger.warning("Failed to resolve: " + str(did) + " after multiple retries.")
    return "Error"


def process_did_list_to_handle(did_list):
    handle_list = []
    for item in did_list:
        handle_list.append(resolve_did(item))

    return handle_list


# ======================================================================================================================
# ============================================= Features functions =====================================================
def get_all_users():
    base_url = "https://bsky.social/xrpc/"
    limit = 1000
    cursor = None
    records = []

    logger.info("Getting all dids.")

    while True:
        url = urllib.parse.urljoin(base_url, "com.atproto.sync.listRepos")
        params = {
            "limit": limit,
        }
        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)
        response = requests.get(full_url)

        if response.status_code == 200:
            response_json = response.json()
            repos = response_json.get("repos", [])
            for repo in repos:
                records.append((repo["did"],))

            cursor = response_json.get("cursor")
            if not cursor:
                break
        else:
            logger.warning("Response status code: " + str(response.status_code))
            pass
    return records


async def get_user_handle(did):
    async with asyncpg.create_pool(
        user=pg_user,
        password=pg_password,
        host=pg_host,
        database=pg_database
    ) as new_connection_pool:
        async with new_connection_pool.acquire() as connection:
            handle = await connection.fetchval('SELECT handle FROM users WHERE did = $1', did)
        return handle


async def get_all_users_count():
    users = await get_all_users_db(False, False, True)
    if not isinstance(users, int):
        return len(users)
    # formatted_count = "{:,}".format(users)
    return users
    # return formatted_count


async def get_single_user_blocks(ident):
    # Execute the SQL query to get all the user_dids that have the specified did in their blocklist
    async with connection_pool.acquire() as connection:
        result = await connection.fetch('SELECT user_did, block_date FROM blocklists WHERE blocked_did = $1', ident)

    # # Execute the SQL query to get all the user_dids that have the specified did in their blocklist
    # connection_pool.execute('SELECT user_did, block_date FROM blocklists WHERE blocked_did = ?', (ident,))
    # result = connection_pool.fetchall()

    if result:
        # Extract the user_dids from the query result
        user_dids = [item[0] for item in result]
        block_dates = [item[1] for item in result]
        count = len(user_dids)

        resolved_handles = []

        for user_did in user_dids:
            handle = await get_user_handle(user_did)
            resolved_handles.append(handle)

        return resolved_handles, block_dates, count
    else:
        # ident = resolve_handle(ident)
        no_blocks = ident + ": has not been blocked by anyone."
        date = datetime.now().date()
        return no_blocks, date, 0


async def get_user_block_list(ident):
    base_url = "https://bsky.social/xrpc/"
    collection = "app.bsky.graph.block"
    limit = 100
    blocked_users = []
    created_dates = []
    cursor = None
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
        params = {
            "repo": ident,
            "limit": limit,
            "collection": collection,
            # "cursor": cursor
        }

        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(full_url, timeout=10)  # Set an appropriate timeout value (in seconds)
                response.raise_for_status()  # Raise an exception for any HTTP error status codes
        except httpx.ReadTimeout as e:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(5)
            continue
        except httpx.RequestException as e:
            logger.warning("Error during API call: %s", e)
            retry_count += 1
            await asyncio.sleep(5)
            continue

        if response.status_code == 200:
            response_json = response.json()
            records = response_json.get("records", [])

            for record in records:
                value = record.get("value", {})
                subject = value.get("subject")
                created_at_value = value.get("createdAt")
                if subject:
                    blocked_users.append(subject)
                if created_at_value:
                    try:
                        created_date = datetime.strptime(created_at_value, "%Y-%m-%dT%H:%M:%S.%fZ").date()
                    except ValueError:
                        created_date = None
                    created_dates.append(created_date)

            cursor = response_json.get("cursor")
            if not cursor:
                break
        else:
            retry_count += 1
            logger.warning("Error during API call. Status code: %s", response.status_code)
            await asyncio.sleep(5)
            continue

    if retry_count == max_retries:
        logger.warning("Could not get block list for: " + ident)
        return ["error"], [str(datetime.now().date())]
    if not blocked_users and retry_count != max_retries:
        return [], []

    return blocked_users, created_dates


async def fetch_handles_batch(batch_dids):
    handles = []
    for did in batch_dids:
        # Apply strip('\'\",') to remove leading/trailing quotes or commas
        did = did[0].strip('\',')
        logger.info(str(did))
        handle = await resolve_did(did)
        handles.append((did, handle))
    return handles
    # tasks = [resolve_did(did.strip('\'\",')) for did in batch_dids]
    # return await asyncio.gather(*tasks)

# ======================================================================================================================
# ========================================= database handling functions ================================================
pg_user = config.get("database", "pg_user")
pg_password = config.get("database", "pg_password")
pg_host = config.get("database", "pg_host")
pg_database = config.get("database", "pg_database")

connection_pool = None
db_lock = asyncio.Lock()


async def create_connection_pool():
    global connection_pool

    # Acquire the lock before creating the connection pool
    async with db_lock:
        if connection_pool is None:
            connection_pool = await asyncpg.create_pool(
                user=pg_user,
                password=pg_password,
                host=pg_host,
                database=pg_database
            )


async def count_users_table():
    async with connection_pool.acquire() as connection:
        # Execute the SQL query to count the rows in the "users" table
        return await connection.fetchval('SELECT COUNT(*) FROM users')


def get_single_users_blocks_db(run_update=False, get_dids=False):
    all_dids = get_all_users_db(run_update=run_update, get_dids=get_dids)

    for i, ident in enumerate(tqdm(all_dids, desc="Updating blocklists", unit="DID", ncols=100)):
        user_did = ident[0]
        update_blocklist_table(user_did)

        # Sleep for 60 seconds every 5 minutes
        if (i + 1) % (300000 // 100) == 0:  # Assuming you have 100 dids in all_dids
            logger.info("Pausing...")
            time.sleep(60)


async def update_user_handle(ident, handle):
    async with connection_pool.acquire() as connection:
        await connection.execute('UPDATE users SET handle = $1 WHERE did = $2', handle, ident)


async def get_all_users_db(run_update=False, get_dids=False, get_count=False):
    batch_size = 10000
    if get_count:
        # Fetch the total count of users in the "users" table
        async with connection_pool.acquire() as connection:
            return await connection.fetchval('SELECT COUNT(*) FROM users')
    if not run_update:
        async with connection_pool.acquire() as connection:
            if get_dids:
                # Return the user_dids from the "users" table
                return await connection.fetch('SELECT did FROM users')
    else:
        # Get all DIDs
        records = get_all_users()

        # Transform the records into a list of tuples with the correct format for insertion
        formatted_records = [(record[0],) for record in records]

        logger.info(f"Total DIDs: {len(formatted_records)}")

        async with connection_pool.acquire() as connection:
            logger.info("Connected to db.")
            async with connection.transaction():
                # Insert data in batches
                for i in range(0, len(formatted_records), batch_size):
                    batch_data = records[i: i + batch_size]
                    try:
                        await connection.executemany('INSERT INTO users (did) VALUES ($1) ON CONFLICT DO NOTHING',
                                                     batch_data)
                        logger.info(
                            f"Inserted batch {i // batch_size + 1} of {len(formatted_records) // batch_size + 1} batches.")
                    except Exception as e:
                        logger.error(f"Error inserting batch {i // batch_size + 1}: {str(e)}")
                    # await connection.executemany('INSERT INTO users (did) VALUES ($1) ON CONFLICT DO NOTHING', batch_data)

        # Return the records when run_update is false and get_count is called
        return records


async def update_blocklist_table(ident):
    blocked_by_list, block_date = get_user_block_list(ident)

    if not blocked_by_list:
        return

    async with connection_pool.acquire() as connection:
        logger.info("Connected to db.")
        async with connection.transaction():
            # Retrieve the existing blocklist entries for the specified ident
            existing_records = await connection.fetch(
                'SELECT blocked_did, block_date FROM blocklists WHERE user_did = $1', ident
            )
            existing_blocklist_entries = {(record['blocked_did'], record['block_date']) for record in existing_records}

            # Prepare the data to be inserted into the database
            data = [(ident, blocked_did, date) for blocked_did, date in zip(blocked_by_list, block_date)]

            # Convert the new blocklist entries to a set for comparison
            new_blocklist_entries = {(record['blocked_did'], record['block_date']) for record in data}

            # Check if there are differences between the existing and new blocklist entries
            if existing_blocklist_entries != new_blocklist_entries:
                # Delete existing blocklist entries for the specified ident
                await connection.execute('DELETE FROM blocklists WHERE user_did = $1', ident)

                # Insert the new blocklist entries
                await connection.executemany(
                    'INSERT INTO blocklists (user_did, blocked_did, block_date) VALUES ($1, $2, $3)', data
                )
# ======================================================================================================================
# =============================================== Main Logic ===========================================================
ip_address = config.get("server", "ip")
port_address = config.get("server", "port")

# python app.py --update-users-db // command to update users db
# python app.py --fetch-users-count // command to get current count in db
# python app.py --update-blocklists-db // command to update all users blocklists
# python app.py --truncate-blocklists_table-db // command to update all users blocklists
# python app.py --truncate-users_table-db // command to update all users blocklists
# python app.py --delete-database // command to delete entire database
# python app.py --retrieve-blocklists-db // initial/re-initialize get for blocklists database


async def main():
    parser = argparse.ArgumentParser(description='ClearSky Web Server: ' + version)
    parser.add_argument('--update-users-db', action='store_true', help='Update the database with all users')
    parser.add_argument('--fetch-users-count', action='store_true', help='Fetch the count of users')
    parser.add_argument('--update-blocklists-db', action='store_true', help='Update the blocklists table')
    parser.add_argument('--retrieve-blocklists-db', action='store_true', help='Initial/re-initialize get for blocklists database')
    parser.add_argument('--truncate-blocklists_table-db', action='store_true', help='delete blocklists table')
    parser.add_argument('--truncate-users_table-db', action='store_true', help='delete users table')
    parser.add_argument('--delete-database', action='store_true', help='delete entire database')
    args = parser.parse_args()
    total_handles_updated = 0

    await create_connection_pool()  # Creates connection pool for db

    if args.update_users_db:
        # Call the function to update the database with all users
        logger.info("Users db update requested.")
        all_dids = await get_all_users_db(True, False)
        logger.info("Users db updated dids.")
        logger.info("Update users handles requested.")
        batch_size = 1000
        total_dids = len(all_dids)

        for i in range(0, total_dids, batch_size):
            batch_dids = all_dids[i:i + batch_size]
            batch_handles_and_dids = await fetch_handles_batch(batch_dids)
            logger.info((str(batch_handles_and_dids)))

            # Update the database with the batch of handles
            for did, handle in batch_handles_and_dids:
                await update_user_handle(did, handle)

            # Update the total_handles_updated counter and log the progress
            total_handles_updated += len(batch_handles_and_dids)
            logger.info(f"Handles updated: {total_handles_updated}/{total_dids}")

            # Log the first few handles in the batch for verification
            logger.info(f"First few handles in the batch: {[handle for _, handle in batch_handles_and_dids[:5]]}")
        # for did in all_dids:
        #     str_did = did[0].strip('\'\",')
        #     handle = resolve_did(str_did)
        #     await update_user_handle(str(str_did), handle)
        logger.info("Users db update finished.")
        sys.exit()
    elif args.fetch_users_count:
        # Call the function to fetch the count of users
        count = await count_users_table()
        logger.info(f"Total users in the database: {count}")
        sys.exit()
    elif args.retrieve_blocklists_db:
        logger.info("Get Blocklists db requested.")
        # truncate_blocklists_table()
        # truncate_users_table()
        get_single_users_blocks_db(run_update=True, get_dids=False)
        logger.info("Blocklist db fetch finished.")
        sys.exit()
    elif args.update_blocklists_db:
        logger.info("Update Blocklists db requested.")
        get_single_users_blocks_db(run_update=False, get_dids=True)
        logger.info("Update Blocklists db finished.")
        sys.exit()
    else:
        logger.info("Web server starting at: " + ip_address + ":" + port_address)
        await serve(app, host=ip_address, port=port_address)

if __name__ == '__main__':
    asyncio.run(main())
