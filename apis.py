# apis.py

from datetime import timedelta
from quart_rate_limiter import rate_limit
from helpers import get_ip, generate_session_number
from quart import Blueprint, jsonify, send_file, render_template, session, send_from_directory
from errors import BadRequest, NotFound, DatabaseConnectionError, NoFileProvided, FileNameExists, ExceedsFileSizeLimit
from core import logger, request, get_blocklist, get_single_blocklist, get_in_common_blocklist, \
    get_in_common_blocked, convert_uri_to_url, get_total_users, get_did_info, get_handle_info, get_handle_history_info, \
    get_list_info, get_moderation_lists, get_blocked_search, get_blocking_search, fun_facts, funer_facts, \
    block_stats, autocomplete, get_internal_status, check_api_keys, retrieve_dids_per_pds, \
    retrieve_subscribe_blocks_blocklist, retrieve_subscribe_blocks_single_blocklist, verify_handle, store_data, \
    retrieve_csv_data, retrieve_csv_files_info, api_key_required, cursor_recall_status, time_behind

api_blueprint = Blueprint('api', __name__)


# ======================================================================================================================
# ================================================== Static Pages ======================================================
@api_blueprint.route('/', methods=['GET'])
async def index():
    # Generate a new session number and store it in the session
    if 'session_number' not in session:
        session['session_number'] = generate_session_number()

    return await render_template('index.html')


@api_blueprint.route('/fediverse', methods=['GET'])
async def fediverse():
    # Generate a new session number and store it in the session
    if 'session_number' not in session:
        session['session_number'] = generate_session_number()

    return await render_template('data-transfer.html')


@api_blueprint.route('/fedi-delete-request', methods=['GET'])
async def fedi_delete_request():
    # Generate a new session number and store it in the session
    if 'session_number' not in session:
        session['session_number'] = generate_session_number()

    return await render_template('fedi-delete-request.html')


@api_blueprint.route('/images/favicon.png', methods=['GET'])
async def favicon1():
    return await send_from_directory('images', 'favicon.png')


@api_blueprint.route('/images/apple-touch-icon.png', methods=['GET'])
async def favicon2():
    return await send_from_directory('images', 'apple-touch-icon.png')


@api_blueprint.route('/images/apple-touch-icon-120x120.png', methods=['GET'])
async def favicon3():
    return await send_from_directory('images', 'apple-touch-icon-120x120.png')


@api_blueprint.route('/images/apple-touch-icon-152x152.png', methods=['GET'])
async def favicon4():
    return await send_from_directory('images', 'apple-touch-icon-152x152.png')


@api_blueprint.route('/images/CleardayLarge.png', methods=['GET'])
async def logo():
    return await send_from_directory('images', 'CleardayLarge.png')


@api_blueprint.route('/frequently_asked', methods=['GET'])
async def faq():
    session_ip = await get_ip()

    logger.info(f"{session_ip} - FAQ requested.")

    return await render_template('coming_soon.html')


@api_blueprint.route('/coming_soon', methods=['GET'])
async def coming_soon():
    session_ip = await get_ip()

    logger.info(f"{session_ip} - Coming soon requested.")

    return await render_template('coming_soon.html')


@api_blueprint.route('/status', methods=['GET'])
async def always_200():
    return "OK", 200


@api_blueprint.route('/contact', methods=['GET'])
async def contact():
    session_ip = await get_ip()

    logger.info(f"{session_ip} - Contact requested.")

    return await render_template('contact.html')


@api_blueprint.route('/api/v1/anon/images/logo', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_logo():
    return await send_from_directory('images', 'Clearskylogo.png')


@api_blueprint.route('/api/v1/anon/images/icon', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_icon():
    return await send_from_directory('images', 'favicon32.png')


@api_blueprint.route('/api/v1/auth/images/logo', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def auth_get_logo():
    return await send_from_directory('images', 'Clearskylogo.png')


@api_blueprint.route('/api/v1/auth/images/icon', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def auth_get_icon():
    return await send_from_directory('images', 'favicon32.png')


# ======================================================================================================================
# ===================================================== APIs ===========================================================

# ======================================================================================================================
# ===================================================== V1 =============================================================

# ======================================================================================================================
# ============================================= Authenticated API Endpoints ============================================
@api_blueprint.route('/api/v1/auth/blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/auth/blocklist/<client_identifier>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_blocklist(client_identifier, page) -> jsonify:
    try:
        return await get_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/single-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/auth/single-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_single_blocklist(client_identifier, page) -> jsonify:
    try:
        return await get_single_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_single_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/in-common-blocklist/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_in_common_blocklist(client_identifier) -> jsonify:
    try:
        return await get_in_common_blocklist(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_in_common_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/in-common-blocked-by/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_in_common_blocked_by(client_identifier) -> jsonify:
    try:
        return await get_in_common_blocked(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_in_common_blocked_by: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/at-uri/<path:uri>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_convert_uri_to_url(uri) -> jsonify:
    try:
        return await convert_uri_to_url(uri)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_convert_uri_to_url: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/total-users', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_total_users() -> jsonify:
    try:
        return await get_total_users()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_total_users: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/get-did/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_did_info(client_identifier) -> jsonify:
    try:
        return await get_did_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_did_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/get-handle/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_handle_info(client_identifier) -> jsonify:
    try:
        return await get_handle_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_handle_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/get-handle-history/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_handle_history_info(client_identifier) -> jsonify:
    try:
        return await get_handle_history_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_handle_history_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/get-list/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_list_info(client_identifier) -> jsonify:
    try:
        return await get_list_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_list_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/get-moderation-list/<string:input_name>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/auth/get-moderation-list/<string:input_name>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_moderation_lists(input_name, page) -> jsonify:
    try:
        return await get_moderation_lists(input_name, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_moderation_lists: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/blocklist-search-blocked/<client_identifier>/<search_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_blocked_search(client_identifier, search_identifier) -> jsonify:
    try:
        return await get_blocked_search(client_identifier, search_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_blocked_search: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/blocklist-search-blocking/<client_identifier>/<search_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_blocking_search(client_identifier, search_identifier) -> jsonify:
    try:
        return await get_blocking_search(client_identifier, search_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_blocking_search: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/lists/fun-facts', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_fun_facts() -> jsonify:
    try:
        return await fun_facts()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_fun_facts: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/lists/funer-facts', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_funer_facts() -> jsonify:
    try:
        return await funer_facts()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_funer_facts: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/lists/block-stats', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_block_stats() -> jsonify:
    try:
        return await block_stats()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_block_stats: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/base/autocomplete/<client_identifier>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_autocomplete(client_identifier) -> jsonify:
    try:
        return await autocomplete(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_autocomplete: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/base/internal/status/process-status', methods=['GET'])
@api_key_required("INTERNALSERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_get_internal_status() -> jsonify:
    try:
        return await get_internal_status()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_get_internal_status: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/base/internal/api-check', methods=['GET'])
@api_key_required("INTERNALSERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_check_api_keys() -> jsonify:
    try:
        return await check_api_keys()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_check_api_keys: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/lists/dids-per-pds', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_dids_per_pds() -> jsonify:
    try:
        return await retrieve_dids_per_pds()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_dids_per_pds: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/subscribe-blocks-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/auth/subscribe-blocks-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_subscribe_blocks_blocklist(client_identifier, page) -> jsonify:
    try:
        return await retrieve_subscribe_blocks_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_subscribe_blocks_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/subscribe-blocks-single-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/auth/subscribe-blocks-single-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@api_key_required("SERVER")
@rate_limit(30, timedelta(seconds=1))
async def auth_subscribe_blocks_single_blocklist(client_identifier, page) -> jsonify:
    try:
        return await retrieve_subscribe_blocks_single_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_subscribe_blocks_single_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/validation/validate-handle/<client_identifier>', methods=['GET'])
@rate_limit(30, timedelta(seconds=1))
async def auth_validate_handle(client_identifier) -> jsonify:
    try:
        return await verify_handle(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_validate_handle: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/data-transaction/receive', methods=['POST'])
@rate_limit(1, timedelta(seconds=2))
async def auth_receive_data() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"data list file upload request: {session_ip} - {api_key}")

    return jsonify({"error": "Not Implemented"}), 501

    try:
        file_name = await request.form
        file_name = file_name.get('filename')

        # Retrieve additional fields
        author = await request.form
        author = author.get('author')

        description = await request.form
        description = description.get('description')

        appeal = await request.form
        appeal = appeal.get('appealsProcess')

        list_type = await request.form
        list_type = list_type.get('listType')

        if file_name is None:
            file_name = request.args.get('filename')
        if author is None:
            author = request.args.get('author')
        if description is None:
            description = request.args.get('description')
        if appeal is None:
            appeal = request.args.get('appealsProcess')
        if list_type is None:
            list_type = request.args.get('listType')

        if not list_type.lower().strip() == 'user' or 'domain':
            raise BadRequest

        if len(author) > 100 or len(description) > 300 or len(appeal) > 500:
            logger.warning(f"Data too long: Author: {len(author)}, Description: {len(description)}, Appeal: {len(appeal)}")
            raise BadRequest

        # Check if the request contains a file
        if not file_name:
            raise BadRequest

        # Check if files were sent in the request
        files = await request.files
        if 'file' not in files:
            raise BadRequest("No file provided.")

        # Get the file from the request
        file_storage = files['file']

        if file_name != file_storage.filename:
            raise BadRequest()

        try:
            # Read the content of the file
            file_content = file_storage.read()
        except Exception as e:
            logger.error(f"Error reading file content, probably not a csv: {file_name} {e}")
            raise BadRequest()

        await store_data(file_content, file_name, author, description, appeal, list_type)

        return jsonify({"message": "File received and processed successfully"}), 200
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except NoFileProvided:
        return jsonify({"error": "No file provided"}), 400
    except FileNameExists:
        return jsonify({"error": "File name already exists"}), 409
    except ExceedsFileSizeLimit:
        return jsonify({"error": "File size limit exceeded."}), 413
    except Exception as e:
        logger.error(f"Error in receive_data: {e}")

        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/data-transaction/retrieve', methods=['GET'])
@rate_limit(1, timedelta(seconds=2))
async def auth_retrieve_data() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"data list file request: {session_ip} - {api_key}")

    return jsonify({"error": "Not Implemented"}), 501

    try:
        retrieve_lists = request.args.get('retrieveLists')
        file_name = request.args.get('file')  # need to validate the file name
    except AttributeError:
        return jsonify({"error": "Invalid request"}), 400

    try:
        if retrieve_lists == "true" and file_name is not None:
            # Assuming retrieve_csv_data() returns the file path of the CSV file
            file_content = await retrieve_csv_data(file_name)

            if file_content is None:
                return jsonify({"error": "Not found"}), 404

            logger.info(f"Sending file: {file_name}")

            return await send_file(file_content,
                                   mimetype='text/csv',
                                   as_attachment=True,
                                   attachment_filename=file_name)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_retrieve_data: {e}")

        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/data-transaction/query', methods=['GET'])
@rate_limit(1, timedelta(seconds=2))
async def auth_query_data() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"data list query request: {session_ip} - {api_key}")

    return jsonify({"error": "Not Implemented"}), 501

    try:
        get_list = request.args.get('list')
    except AttributeError:
        return jsonify({"error": "Invalid request"}), 400

    try:
        return await retrieve_csv_files_info(get_list)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in retrieve_csv_files_info: {e}")

        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/auth/status/time-behind', methods=['GET'])
@rate_limit(30, timedelta(seconds=2))
async def auth_time_behind() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"time behind request: {session_ip} - {api_key}")

    try:
        return await time_behind()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in time_behind: {e}")
        return jsonify({"error": "Internal error"}), 500


# ======================================================================================================================
# ========================================== Unauthenticated API Endpoints =============================================
@api_blueprint.route('/api/v1/anon/blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/anon/blocklist/<client_identifier>/<int:page>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_blocklist(client_identifier, page) -> jsonify:
    try:
        return await get_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/single-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/anon/single-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_single_blocklist(client_identifier, page) -> jsonify:
    try:
        return await get_single_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_single_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/in-common-blocklist/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_in_common_blocklist(client_identifier) -> jsonify:
    try:
        return await get_in_common_blocklist(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_in_common_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/in-common-blocked-by/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_in_common_blocked_by(client_identifier) -> jsonify:
    try:
        return await get_in_common_blocked(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_in_common_blocked_by: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/at-uri/<path:uri>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_convert_uri_to_url(uri) -> jsonify:
    try:
        return await convert_uri_to_url(uri)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_convert_uri_to_url: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/total-users', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_total_users() -> jsonify:
    try:
        return await get_total_users()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_total_users: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/get-did/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_did_info(client_identifier) -> jsonify:
    try:
        return await get_did_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_did_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/get-handle/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_handle_info(client_identifier) -> jsonify:
    try:
        return await get_handle_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_handle_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/get-handle-history/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_handle_history_info(client_identifier) -> jsonify:
    try:
        return await get_handle_history_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_handle_history_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/get-list/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_list_info(client_identifier) -> jsonify:
    try:
        return await get_list_info(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_list_info: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/get-moderation-list/<string:input_name>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/anon/get-moderation-list/<string:input_name>/<int:page>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_moderation_lists(input_name, page) -> jsonify:
    try:
        return await get_moderation_lists(input_name, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_moderation_lists: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/blocklist-search-blocked/<client_identifier>/<search_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_blocked_search(client_identifier, search_identifier) -> jsonify:
    try:
        return await get_blocked_search(client_identifier, search_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_blocked_search: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/blocklist-search-blocking/<client_identifier>/<search_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_blocking_search(client_identifier, search_identifier) -> jsonify:
    try:
        return await get_blocking_search(client_identifier, search_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_blocking_search: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/lists/fun-facts', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_fun_facts() -> jsonify:
    try:
        return await fun_facts()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_fun_facts: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/lists/funer-facts', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_funer_facts() -> jsonify:
    try:
        return await funer_facts()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_funer_facts: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/lists/block-stats', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_block_stats() -> jsonify:
    try:
        return await block_stats()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_block_stats: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/base/autocomplete/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_autocomplete(client_identifier) -> jsonify:
    try:
        return await autocomplete(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_autocomplete: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/base/internal/status/process-status', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_get_internal_status() -> jsonify:
    try:
        return await get_internal_status()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_get_internal_status: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/lists/dids-per-pds', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_dids_per_pds() -> jsonify:
    try:
        return await retrieve_dids_per_pds()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_dids_per_pds: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/subscribe-blocks-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/anon/subscribe-blocks-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_subscribe_blocks_blocklist(client_identifier: str, page: int) -> jsonify:
    try:
        return await retrieve_subscribe_blocks_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_subscribe_blocks_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/subscribe-blocks-single-blocklist/<client_identifier>', defaults={'page': 1}, methods=['GET'])
@api_blueprint.route('/api/v1/anon/subscribe-blocks-single-blocklist/<client_identifier>/<int:page>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_subscribe_blocks_single_blocklist(client_identifier, page) -> jsonify:
    try:
        return await retrieve_subscribe_blocks_single_blocklist(client_identifier, page)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_subscribe_blocks_single_blocklist: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/validation/validate-handle/<client_identifier>', methods=['GET'])
@rate_limit(5, timedelta(seconds=1))
async def anon_validate_handle(client_identifier) -> jsonify:
    try:
        return await verify_handle(client_identifier)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in anon_validate_handle: {e}")
        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/data-transaction/receive', methods=['POST'])
@rate_limit(1, timedelta(seconds=2))
async def anon_receive_data() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"data list file upload request: {session_ip} - {api_key}")

    return jsonify({"error": "Not Implemented"}), 501

    try:
        file_name = await request.form
        file_name = file_name.get('filename')

        # Retrieve additional fields
        author = await request.form
        author = author.get('author')

        description = await request.form
        description = description.get('description')

        appeal = await request.form
        appeal = appeal.get('appealsProcess')

        list_type = await request.form
        list_type = list_type.get('listType')

        if file_name is None:
            file_name = request.args.get('filename')
        if author is None:
            author = request.args.get('author')
        if description is None:
            description = request.args.get('description')
        if appeal is None:
            appeal = request.args.get('appealsProcess')
        if list_type is None:
            list_type = request.args.get('listType')

        if list_type.lower().strip() not in ['user', 'domain']:
            raise BadRequest

        if len(author) > 100 or len(description) > 300 or len(appeal) > 500:
            logger.warning(f"Data too long: Author: {len(author)}, Description: {len(description)}, Appeal: {len(appeal)}")
            raise BadRequest

        # Check if the request contains a file
        if not file_name:
            raise BadRequest

        # Check if files were sent in the request
        files = await request.files
        if 'file' not in files:
            raise BadRequest("No file provided.")

        # Get the file from the request
        file_storage = files['file']

        if file_name != file_storage.filename:
            raise BadRequest()

        try:
            # Read the content of the file
            file_content = file_storage.read()
        except Exception as e:
            logger.error(f"Error reading file content, probably not a csv: {file_name} {e}")
            raise BadRequest()

        await store_data(file_content, file_name, author, description, appeal, list_type)

        return jsonify({"message": "File received and processed successfully"}), 200
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except NoFileProvided:
        return jsonify({"error": "No file provided"}), 400
    except FileNameExists:
        return jsonify({"error": "File name already exists"}), 409
    except ExceedsFileSizeLimit:
        return jsonify({"error": "File size limit exceeded."}), 413
    except Exception as e:
        logger.error(f"Error in receive_data: {e}")

        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/data-transaction/retrieve', methods=['GET'])
@rate_limit(1, timedelta(seconds=2))
async def anon_retrieve_data() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"data list file request: {session_ip} - {api_key}")

    return jsonify({"error": "Not Implemented"}), 501

    try:
        retrieve_lists = request.args.get('retrieveLists')
        file_name = request.args.get('file')  # need to validate the file name
    except AttributeError:
        return jsonify({"error": "Invalid request"}), 400

    try:
        if retrieve_lists == "true" and file_name is not None:
            # Assuming retrieve_csv_data() returns the file path of the CSV file
            file_content = await retrieve_csv_data(file_name)

            if file_content is None:
                return jsonify({"error": "Not found"}), 404

            logger.info(f"Sending file: {file_name}")

            return await send_file(file_content,
                                   mimetype='text/csv',
                                   as_attachment=True,
                                   attachment_filename=file_name)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in auth_retrieve_data: {e}")

        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/data-transaction/query', methods=['GET'])
@rate_limit(1, timedelta(seconds=2))
async def anon_query_data() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"data list query request: {session_ip} - {api_key}")

    return jsonify({"error": "Not Implemented"}), 501

    try:
        get_list = request.args.get('list')
    except AttributeError:
        return jsonify({"error": "Invalid request"}), 400

    try:
        return await retrieve_csv_files_info(get_list)
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in retrieve_csv_files_info: {e}")

        return jsonify({"error": "Internal error"}), 500


@api_blueprint.route('/api/v1/anon/cursor-recall/status', methods=['GET'])
@rate_limit(5, timedelta(seconds=2))
async def anon_cursor_recall() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"cursor recall request: {session_ip} - {api_key}")

    try:
        return await cursor_recall_status()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in cursor_recall_status: {e}")
        return jsonify({"error": "Internal error"})


@api_blueprint.route('/api/v1/anon/status/time-behind', methods=['GET'])
@rate_limit(5, timedelta(seconds=2))
async def anon_time_behind() -> jsonify:
    session_ip = await get_ip()
    api_key = request.headers.get('X-API-Key')

    logger.info(f"time behind request: {session_ip} - {api_key}")

    try:
        return await time_behind()
    except DatabaseConnectionError:
        logger.error("Database connection error")
        return jsonify({"error": "Connection error"}), 503
    except BadRequest:
        return jsonify({"error": "Invalid request"}), 400
    except NotFound:
        return jsonify({"error": "Not found"}), 404
    except Exception as e:
        logger.error(f"Error in time_behind: {e}")
        return jsonify({"error": "Internal error"}), 500
# ======================================================================================================================
# ===================================================== V2 =============================================================
