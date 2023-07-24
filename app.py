from flask import Flask, render_template, request, session, jsonify
import requests
import urllib.parse
from datetime import datetime
from waitress import serve
import logging.config
import configparser
import os
import sys
import uuid
import platform
import sqlite3
import argparse
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
version = "0.2.6"
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
def selection_handle():
    global session_ip
    session_ip = get_ip()
    logger.debug(request.form)
    identifier = request.form['identifier'].lower()
    identifier = identifier.replace('@', '')
    selection = request.form['selection']
    if selection != "4":
        if not identifier:
            return render_template('error.html')
    if selection == "1":
        logger.info(str(session_ip) + " > " + str(*session.values()) + ": " + "DID resolve request made for: " + identifier)
        result = resolve_handle(identifier)
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + result)
        # logger.debug(jsonify({"result": result}))

        # return render_template('result.html', result=result)
        return jsonify({"result": result})
    elif selection == "2":
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Handle resolve request made for: " + identifier)
        result = resolve_did(identifier)
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Request Result: " + identifier + " | " + str(result))

        # return render_template('result.html', result=result)
        return jsonify({"result": result})
    elif selection == "3":
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Block list requested for: " + identifier)
        blocklist, count = get_user_block_list_html(identifier)

        # return render_template('blocklist.html', block_list=blocklist, user=identifier, count=count)
        return jsonify({"block_list": blocklist, "user": identifier, "count": count})
    elif selection == "4":
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count requested")
        count = get_all_users_count()
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Total User count: " + str(count))

        # return render_template('total_users.html', count=count)
        return jsonify({"count": count})
    elif selection == "5":
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Block list requested for: " + identifier)
        blocks, dates, count = get_single_user_blocks(identifier)
        logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Single Blocklist Request Result: " + identifier + " | " + "Block by: " + str(blocks) + " :: " + "Total count: " + str(count))
        # count = len(blocks)
        # blocks = None
        # count = 1
        # return render_template('blocklist.html', user=identifier, block_list=blocks, count=count)
        return jsonify({"user": identifier, "block_list": blocks, "count": count})


def get_user_block_list_html(ident):
    blocked_users, timestamps = get_user_block_list(ident)
    handles = []
    if blocked_users:
        for handle in process_did_list_to_handle(blocked_users):
            handles.append(handle)
    else:
        handles = [f"{ident} hasn't blocked anyone."]

    if not blocked_users:
        total_blocked = len(handles) - 1
    else:
        total_blocked = len(handles)
    logger.info(str(session_ip) + " > " + str(*session.values()) + " | " + "Blocklist Request Result: " + ident + " | " + "Total blocked: " + str(total_blocked) + " :: " + str(list(zip(handles, timestamps))))

    # Return the list of dictionaries instead of the zip object
    block_list = []
    for handle, timestamp in zip(handles, timestamps):
        block_list.append({"handle": handle, "timestamp": timestamp})

    # total_blocked = len(block_list)
    # logger.debug(block_list)
    return block_list, total_blocked
    # return zip(handles, timestamps), total_blocked


# ======================================================================================================================
# ======================================================= Logic ========================================================
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


def resolve_handle(info):  # Take Handle and get DID
    base_url = "https://bsky.social/xrpc/"
    url = urllib.parse.urljoin(base_url, "com.atproto.identity.resolveHandle")
    params = {
        "handle": info
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"
    logger.debug(full_url)
    get_response = requests.get(full_url)
    response = get_response.json().values()

    return list(response)[0]


def resolve_did(did):  # Take DID and get handle
    handle = did
    base_url = "https://bsky.social/xrpc/"
    url = urllib.parse.urljoin(base_url, "com.atproto.repo.describeRepo")
    params = {
        "repo": handle
    }

    encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
    full_url = f"{url}?{encoded_params}"
    logger.debug(full_url)
    get_response = requests.get(full_url)

    if get_response.status_code == 200:
        response_json = get_response.json()
        records = response_json["handle"]

        return records
    else:
        print("Error:", get_response.status_code)


def get_all_users():
    base_url = "https://bsky.social/xrpc/"
    limit = 1000
    cursor = None
    records = []

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
    return records


def get_all_users_db(run_update=False, get_dids=False):
    if not run_update:
        if os.path.exists(users_db_path):
            # Attempt to fetch data from the cache (SQLite database)
            conn = sqlite3.connect(users_db_path)
            cursor = conn.cursor()

            cursor.execute('SELECT did FROM users')
            cached_users = cursor.fetchall()
            conn.close()  # Left off at logic for getting all users and then adding it to db
        if get_dids:
            return cached_users
        elif cached_users:
            records = count_users_table()
            return records
            # If data is found in the cache, return it directly

    records = get_all_users()

    # Clear the existing data by truncating the table
    if run_update:
        truncate_users_table()

    # Store the fetched users data in the cache (SQLite database)
    conn = sqlite3.connect(users_db_path)
    cursor = conn.cursor()

    cursor.executemany('INSERT INTO users (did) VALUES (?)', records)
    conn.commit()
    conn.close()

    logger.debug(str(records))
    return len(records)


def get_all_users_count():
    users = get_all_users_db()
    if not isinstance(users, int):
        return users
    # formatted_count = "{:,}".format(users)
    return users
    # return formatted_count


def get_single_user_blocks(ident):
    if os.path.exists(users_db_path):
        # Connect to the SQLite database
        conn = sqlite3.connect(users_db_path)
        cursor = conn.cursor()

        # Execute the SQL query to get all the user_dids that have the specified did in their blocklist
        cursor.execute('SELECT user_did, block_date FROM blocklists WHERE blocked_did = ?', (ident,))
        result = cursor.fetchall()

        # Close the connection to the database
        conn.close()

        if result:
            # Extract the user_dids from the query result
            user_dids = [item[0] for item in result]
            block_dates = [item[1] for item in result]
            count = len(user_dids)

            resolved_handles = []

            for user_did in user_dids:
                handle = resolve_did(user_did)
                resolved_handles.append(handle)

            return resolved_handles, block_dates, count
        else:
            error_text = "error"
            logger.warning("Blocklist db empty.")
            return error_text, error_text, 0
    else:
        logger.error("No db to get data.")
        error_text = "error"
        return error_text, error_text, 0


def update_blocklist_table(ident):
    blocked_by_list, block_date = get_user_block_list(ident)
    # all_dids = get_all_users_db(get_dids=get_dids)

    if not blocked_by_list:
        return

    # resolve_handles = []
    # for  user_did in blocked_by_list:
    #     handle = resolve_did(user_did)
    #     resolve_handles.append(handle)

    # Connect to the SQLite database
    conn = sqlite3.connect(users_db_path)
    cursor = conn.cursor()

    # Create the blocklists table if it doesn't exist
    # create_blocklist_table()

    # for ident in all_dids:
    #     blocked_by_list, block_date = get_user_block_list(ident)

    # Prepare the data to be inserted into the database
    data = []
    for blocked_did, date in zip(blocked_by_list, block_date):
        data.append((ident, blocked_did, date))

    # Check if each record already exists in the blocklists table
    existing_records = set()
    cursor.execute('SELECT user_did, blocked_did FROM blocklists')
    for row in cursor.fetchall():
        existing_records.add((row[0], row[1]))

    # Insert only the records that do not already exist in the table
    data_to_insert = []
    for record in data:
        if record not in existing_records:
            data_to_insert.append(record)

    # Insert the data into the blocklists table
    cursor.executemany('INSERT INTO blocklists (user_did, blocked_did, block_date) VALUES (?, ?, ?)', data_to_insert)

    # Commit the changes and close the connection to the database
    conn.commit()
    conn.close()


def get_single_users_blocks_db(get_dids=False):
    truncate_blocklists_table()
    all_dids = get_all_users_db(get_dids=get_dids)
    create_blocklist_table()

    for ident in all_dids:
        user_did = ident[0]
        update_blocklist_table(user_did)


def get_user_block_list(ident):
    base_url = "https://bsky.social/xrpc/"
    collection = "app.bsky.graph.block"
    limit = 100
    blocked_users = []
    created_dates = []
    cursor = None

    while True:
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
        response = requests.get(full_url)

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
            break
    # cursor = response_json.get("cursor")
    if not blocked_users:
        return [], [str(datetime.now().date())]

    # Return the blocked users and created_at timestamps if needed
    return blocked_users, created_dates


def process_did_list_to_handle(did_list):
    handle_list = []
    for item in did_list:
        handle_list.append(resolve_did(item))

    return handle_list


def create_user_cache_database():
    logger.debug(users_db_path)

    # Check if the database file exists
    if not os.path.exists(users_db_path):
        try:
            if not os.path.exists(users_db_folder_path):
                os.makedirs(users_db_folder_path)
        except PermissionError:
            logger.warning("Cannot create log directory\nChange 'db_path' in config.ini path to a path with permissions")
            sys.exit()

        logger.info("Creating database.")
        conn = sqlite3.connect(users_db_path)
        cursor = conn.cursor()

        # Create the users table if it doesn't exist
        cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    did TEXT UNIQUE
                )
            ''')

        conn.commit()
        conn.close()
    else:
        logger.warning(f"Database '{users_db_filename}' already exists. Skipping creation.")


def create_blocklist_table():
    if os.path.exists(users_db_path):
        conn = sqlite3.connect(users_db_path)
        cursor = conn.cursor()

        # Check if the "blocklists" table already exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='blocklists'")
        table_exists = cursor.fetchone()

    if not table_exists:
        logger.info("Creating blocklist table.")
        # Connect to the SQLite database
        conn = sqlite3.connect(users_db_path)
        cursor = conn.cursor()

        # Define the schema for the new table
        schema = '''
            CREATE TABLE IF NOT EXISTS blocklists (
                user_did TEXT,
                blocked_did TEXT,
                block_date TEXT,
                PRIMARY KEY (user_did, blocked_did)
            )
        '''

        cursor.execute(schema)

        # Create an index on the user_did column
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_did ON blocklists (user_did)')

        # Create an index on the blocked_did column
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_blocked_did ON blocklists (blocked_did)')

        # Execute the CREATE TABLE query to create the new table

        conn.commit()
        conn.close()
    else:
        logger.info("'Blocklist' table already exists. Skipping creation.")


def truncate_users_table():
    conn = sqlite3.connect(users_db_path)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM users')
    conn.commit()
    conn.close()


def truncate_blocklists_table():
    conn = sqlite3.connect(users_db_path)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM blocklists')
    conn.commit()
    conn.close()


def count_users_table():
    # Connect to the SQLite database
    conn = sqlite3.connect(users_db_path)
    cursor = conn.cursor()

    # Execute the SQL query to count the rows in the "users" table
    cursor.execute('SELECT COUNT(*) FROM users')
    count = cursor.fetchone()[0]

    # Close the connection to the database
    conn.close()

    return count


ip_address = config.get("server", "ip")
port_address = config.get("server", "port")

# python app.py --update-users-db // command to update users db
# python app.py --fetch-users-count // command to get current count in db
# python app.py --update-blocklists-db // command to update all users blocklists

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ClearSky Web Server')
    parser.add_argument('--update-users-db', action='store_true', help='Update the database with all users')
    parser.add_argument('--fetch-users-count', action='store_true', help='Fetch the count of users')
    parser.add_argument('--update-blocklists-db', action='store_true', help='Fetch the count of users')
    args = parser.parse_args()

    if os.path.exists(users_db_path):
        create_user_cache_database()
        create_blocklist_table()

    if args.update_users_db:
        # Call the function to update the database with all users
        logger.info("Users db update requested.")
        get_all_users_db(True)
        logger.info("Users db update finished.")
        sys.exit()
    elif args.fetch_users_count:
        # Call the function to fetch the count of users
        count = count_users_table()
        logger.info(f"Total users in the database: {count}")
        sys.exit()
    elif args.update_blocklists_db:
        logger.info("Blocklists db update requested.")
        get_single_users_blocks_db(True)
        logger.info("Blocklist db update finished.")
    else:
        logger.info("Web server starting at: " + ip_address + ":" + port_address)
        serve(app, host=ip_address, port=port_address)
