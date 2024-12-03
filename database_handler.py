# database_handler.py

import asyncio
import functools
import itertools
import math
import os
from datetime import datetime, timezone

import asyncpg
from cachetools import TTLCache

import config_helper
import utils
from config_helper import check_override, logger
from errors import DatabaseConnectionError, InternalServerError, NotFound

# ======================================================================================================================
# ===================================================  global variables ================================================

connection_pools = {}
db_lock = asyncio.Lock()
once = None
no_tables = asyncio.Event()

all_blocks_cache = TTLCache(maxsize=5000000, ttl=86400)  # every 24 hours

blocklist_updater_status = asyncio.Event()
blocklist_24_updater_status = asyncio.Event()
block_cache_status = asyncio.Event()

last_update_top_block = None
last_update_top_24_block = None
all_blocks_process_time = None
all_blocks_last_update = None
top_blocks_start_time = None
top_24_blocks_start_time = None
top_blocks_process_time = None
top_24_blocks_process_time = None
top_blocked_as_of_time = None
top_24_blocked_as_of_time = None
total_users_as_of_time = None

users_table = "users"
blocklist_table = "blocklists"
top_blocks_table = "top_block"
top_24_blocks_table = "top_twentyfour_hour_block"
mute_lists_table = "mutelists"
mute_lists_users_table = "mutelists_users"
last_created_table = "last_did_created_date"

database_config = None


# ======================================================================================================================
# ========================================= database handling functions ================================================
def get_connection_pool(db_type="read"):
    override = config_helper.check_override()
    if os.getenv("CLEAR_SKY") and override is False:
        if db_type == "read":
            return next(read_db_iterator)
        elif db_type == "cursor":
            for db, _configg in database_config.items():
                if "cursor" in db.lower():
                    return db
        else:
            for db, _configg in database_config.items():
                use_local_db = database_config.get("use_local_db")

                if use_local_db is None:
                    continue
                if "use_local_db" in db.lower() and "none" in use_local_db.lower():
                    continue
                if db.lower() == "write_keyword":
                    continue
                if db.startswith("CLEARSKY_DATABASE") and database_config.get("write_keyword") in db.lower():
                    write = db

                    return write

                continue

    if not os.getenv("CLEAR_SKY") or override is True:
        if db_type == "read":
            return next(read_db_iterator)
        elif db_type == "cursor":
            for db, _configg in database_config.items():
                if "cursor" in db.lower():
                    return db
        else:
            for db, _configg in database_config.items():
                use_local_db = database_config.get("use_local_db")

                if use_local_db is None:
                    continue
                if "use_local_db" in db.lower() and "none" in use_local_db.lower():
                    continue
                if db.lower() == "write_keyword":
                    continue
                if "write" in db.lower():
                    return db

                continue

        logger.error("No write db found.")


async def create_connection_pools(database_configg):
    global connection_pools

    async with db_lock:
        for db, configg in database_configg.items():
            if "clearsky_database" in db.lower() and "db" not in db.lower():
                continue
            if "database" in db.lower() and db not in connection_pools:
                try:
                    connection_pool = await asyncpg.create_pool(
                        user=configg["user"],
                        password=configg["password"],
                        host=configg.get("host", "localhost"),
                        port=configg.get("port", "5432"),
                        database=configg["database"],
                        min_size=20,
                        max_size=100,
                    )
                    connection_pools[db] = connection_pool
                    logger.info(f"Connection pool created for {db}")
                except OSError:
                    logger.error(f"Network connection issue. db connection not established for {db}.")
                except (
                    asyncpg.exceptions.InvalidAuthorizationSpecificationError,
                    asyncpg.exceptions.CannotConnectNowError,
                ):
                    logger.error(f"db connection issue for {db}.")
                except asyncpg.InvalidAuthorizationSpecificationError:
                    logger.error(f"db connection issue for {db}.")

    return connection_pools


async def check_database_connection(db):
    if db in connection_pools:
        pool = connection_pools[db]
        if pool is None:
            logger.error(f"Database connection pool for {db} does not exist.")

            return False
        try:
            async with pool.acquire() as connection:
                try:
                    # Perform a simple query to check if the connection is alive
                    await connection.fetchval("SELECT 1")
                    return True
                except Exception as e:
                    logger.error(f"Error checking database connection for {db}: {e}")
                    return False
        except Exception as e:
            logger.error(f"Error checking database connection for {db}: {e}")
            return False
    else:
        logger.error(f"Database connection pool for {db} does not exist.")
        return False


def check_db_connection(*dbs):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for db in dbs:
                if not await check_database_connection(
                    db
                ):  # Implement your function to check the database connection here
                    raise DatabaseConnectionError("Database connection not available")

            return await func(*args, **kwargs)

        return wrapper

    return decorator


async def find_handles(value):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            logger.debug(f"{value}")

            query_text1 = """SELECT handle
                            FROM users
                            WHERE handle LIKE $1 || '%'
                            ORDER BY handle ASC
                            LIMIT 5"""

            result = await asyncio.wait_for(connection.fetch(query_text1, value), timeout=5.0)

            logger.debug("autocomplete fulfilled.")

            if not result:
                return None

            # Extract matching handles from the database query result
            matching_handles = [row["handle"] for row in result]

            return matching_handles
    except asyncpg.ConnectionDoesNotExistError:
        logger.error("db connection issue.")

        return None
    except Exception as e:
        logger.error(f"Error retrieving autocomplete handles: {e}")

        return None


async def get_blocklist(ident, limit=100, offset=0):
    block_list = []

    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query = """SELECT DISTINCT blocked_did, block_date
            FROM blocklists
            WHERE user_did = $1
            ORDER BY block_date DESC
            LIMIT $2
            OFFSET $3"""
            result = await connection.fetch(query, ident, limit, offset)

            if result:
                # Iterate over blocked_users and extract handle and status
                for blocked_did, block_date in result:
                    block_list.append(
                        {
                            "did": blocked_did,
                            "blocked_date": block_date.isoformat(),
                        }
                    )

                return block_list
            else:
                block_list = []

                return block_list
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving blocklist for {ident}: {e} {type(e)}")
        raise InternalServerError


async def get_single_user_blocks_count(did_identifier):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query = """SELECT COUNT(DISTINCT user_did)
                        FROM blocklists
                        WHERE blocked_did = $1"""

            result = await connection.fetchval(query, did_identifier)

            return result
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving blocklist count for {did_identifier}: {e} {type(e)}")
        raise InternalServerError


async def get_user_blocks_count(did_identifier):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query = """SELECT COUNT(DISTINCT blocked_did)
                        FROM blocklists
                        WHERE user_did = $1"""

            result = await connection.fetchval(query, did_identifier)

            return result
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving blocklist count for {did_identifier}: {e} {type(e)}")
        raise InternalServerError


async def get_subscribe_blocks(ident, limit=100, offset=0):
    data_list = []

    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query_1 = """SELECT mu.subject_did, mu.owner_did, s.date_added, s.list_uri, m.url
                            FROM subscribe_blocklists AS s
                            INNER JOIN mutelists_users AS mu ON s.list_uri = mu.list_uri
                            INNER JOIN mutelists AS m ON s.list_uri = m.uri
                            WHERE s.did = $1
                            ORDER BY s.date_added DESC
                            LIMIT $2
                            OFFSET $3"""

            sub_list = await connection.fetch(query_1, ident, limit, offset)

            if not sub_list:
                return None, 0

            for record in sub_list:
                list_dict = {
                    "did": record["ident"],
                    "subject_did": record["subject_did"],
                    "date_added": record["date_added"].isoformat(),
                    "list owner": record["owner_did"],
                    "list_uri": record["list_uri"],
                    "list_url": record["url"],
                }

                data_list.append(list_dict)

            return data_list
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving subscribe blocklist for {ident}: {e} {type(e)}")
        raise InternalServerError


async def get_subscribe_blocks_count(ident):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query = """SELECT COUNT(DISTINCT list_uri)
                        FROM subscribe_blocklists
                        WHERE did = $1"""

            result = await connection.fetchval(query, ident)

            return result
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving subscribe blocklist count for {ident}: {e} {type(e)}")
        raise InternalServerError


async def get_subscribe_blocks_single(ident, limit=100, offset=0):
    data_list = []

    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query_1 = """SELECT s.did, s.date_added, s.list_uri, m.url, m.did as list owner, count(s.did) as count
                            FROM subscribe_blocklists AS s
                            INNER JOIN mutelists AS m ON s.list_uri = m.uri
                            INNER JOIN mutelists_users AS mu ON s.list_uri = mu.list_uri
                            WHERE mu.subject_did = $1
                            ORDER BY s.date_added DESC
                            LIMIT $2
                            OFFSET $3"""

            data = await connection.fetch(query_1, ident, limit, offset)

            for record in data:
                list_dict = {
                    "did": record["did"],
                    "list owner": record["list owner"],
                    "date_added": record["date_added"].isoformat(),
                    "list_uri": record["list_uri"],
                    "list_url": record["url"],
                    "count": record["count"],
                }

                data_list.append(list_dict)

            return data_list
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving subscribe blocklist for {ident}: {e} {type(e)}")
        raise InternalServerError


async def get_listitem_url(uri):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query = "SELECT list_uri FROM mutelist_users WHERE listitem_uri = $1"
            list_uri = await connection.fetchval(query, uri)

            url = await utils.list_uri_to_url(list_uri)

            return url
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving URL {uri}: {e} {type(e)}")
        raise InternalServerError


async def get_moderation_list(name, limit=100, offset=0):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            search_string = f"%{name}%"

            name_query = """SELECT ml.url, ml.did, ml.name, ml.description, ml.created_date, mc.user_count
            FROM mutelists AS ml
            LEFT mutelists_user_count AS mc ON ml.uri = mc.list_uri
            WHERE ml.name ILIKE $1
            ORDER BY ml.created_date DESC
            LIMIT $2
            OFFSET $3"""

            name_mod_lists = await connection.fetch(name_query, search_string, limit, offset)

            description_query = """SELECT ml.url, ml.did, ml.name, ml.description, ml.created_date,
            mc.user_count
            FROM mutelists AS ml
            LEFT mutelists_user_count AS mc ON ml.uri = mc.list_uri
            WHERE ml.description ILIKE $1
            ORDER BY ml.created_date DESC
            LIMIT $2
            OFFSET $3"""

            description_mod_lists = await connection.fetch(description_query, search_string, limit, offset)

            name_count_query = """SELECT COUNT(*) FROM mutelists WHERE name ILIKE $1"""

            description_count_query = """SELECT COUNT(*) FROM mutelists WHERE description ILIKE $1"""

            name_count = await connection.fetchval(name_count_query, search_string)

            description_count = await connection.fetchval(description_count_query, search_string)
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving results for {name}: {e} {type(e)}")
        raise InternalServerError

    count = name_count + description_count

    if count > 0:
        pages = count / 100

        pages = math.ceil(pages)
    else:
        pages = 0

    lists = []

    if name_mod_lists or description_mod_lists:
        for record in name_mod_lists:
            data = {
                "url": record["url"],
                "did": record["did"],
                "name": record["name"],
                "description": record["description"],
                "created_date": record["created_date"].isoformat(),
                "list count": record["user_count"],
            }
            lists.append(data)

        for record in description_mod_lists:
            data = {
                "url": record["url"],
                "did": record["did"],
                "name": record["name"],
                "description": record["description"],
                "created_date": record["created_date"].isoformat(),
                "list count": record["user_count"],
            }
            lists.append(data)
    else:
        lists = None

    return lists, pages


async def get_listblock_url(uri):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query = "SELECT list_uri FROM subscribe_blocklists WHERE uri = $1"
            list_uri = await connection.fetchval(query, uri)

            url = await utils.list_uri_to_url(list_uri)

            return url
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error retrieving URL {uri}: {e} {type(e)}")
        raise InternalServerError


async def blocklist_search(search_list, lookup, switch):
    pool_name = get_connection_pool("write")
    async with connection_pools[pool_name].acquire() as connection, connection.transaction():
        try:
            blocking = """SELECT b.user_did, b.blocked_did, b.block_date
                                FROM blocklists AS b
                                WHERE user_did = $1
                                  AND blocked_did = $2"""

            blocked = """SELECT b.user_did, b.blocked_did, b.block_date
                                FROM blocklists AS b
                                WHERE user_did = $2
                                  AND blocked_did = $1"""

            if "blocking" in switch:
                query = blocking
            elif "blocked" in switch:
                query = blocked
            else:
                result = None

                return result

            result = await connection.fetch(query, search_list, lookup)

            if result:
                resultslist = {}

                if "blocking" in switch:
                    for record in result:
                        block_date = record["block_date"]
                        did = record["blocked_did"]

                    results = {
                        "blocked_date": block_date.isoformat(),
                        "did": did,
                    }
                elif "blocked" in switch:
                    for record in result:
                        block_date = record["block_date"]
                        did = record["user_did"]

                    results = {
                        "blocked_date": block_date.isoformat(),
                        "did": did,
                    }
                else:
                    results = None

                resultslist.update(results)

                return resultslist
            else:
                resultslist = None

                return resultslist
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError


async def update_24_hour_block_list_table(entries, list_type):
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            data = [(did, count, list_type) for did, count in entries]
            # Insert the new row with the given last_processed_did
            query = "INSERT INTO top_twentyfour_hour_block (did, count, list_type) VALUES ($1, $2, $3)"
            await connection.executemany(query, data)
            logger.info("Updated top 24 block table.")
    except asyncpg.exceptions.UniqueViolationError:
        logger.warning("Attempted to insert duplicate entry into top block table")
        raise InternalServerError
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
        raise InternalServerError
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def truncate_top_blocks_table():
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            # Delete the existing rows if it exists
            await connection.execute("TRUNCATE top_block")
            logger.info("Truncated block table.")
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def truncate_top24_blocks_table():
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            # Delete the existing row if it exists
            await connection.execute("TRUNCATE top_twentyfour_hour_block")
            logger.info("Truncated top 24 block table.")
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def update_top_block_list_table(entries, list_type):
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            data = [(did, count, list_type) for did, count in entries]
            # Insert the new row with the given last_processed_did
            query = "INSERT INTO top_block (did, count, list_type) VALUES ($1, $2, $3)"
            await connection.executemany(query, data)
            logger.info("Updated top block table")
    except asyncpg.exceptions.UniqueViolationError:
        logger.warning("Attempted to insert duplicate entry into top block table")
        raise InternalServerError
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
        raise InternalServerError
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_top_blocks_list() -> tuple[list[tuple[str, int]], list[tuple[str, int]]]:
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            query1 = "SELECT distinct did, count FROM top_block WHERE list_type = 'blocked'"
            query2 = "SELECT distinct did, count FROM top_block WHERE list_type = 'blocker'"
            blocked_rows = await connection.fetch(query1)
            blocker_rows = await connection.fetch(query2)

            return blocked_rows, blocker_rows
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_24_hour_block_list():
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            query1 = "SELECT distinct did, count FROM top_twentyfour_hour_block WHERE list_type = 'blocked'"
            query2 = "SELECT distinct did, count FROM top_twentyfour_hour_block WHERE list_type = 'blocker'"
            blocked_rows = await connection.fetch(query1)
            blocker_rows = await connection.fetch(query2)

            return blocked_rows, blocker_rows
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_top_blocks():
    blocked_results = []
    blockers_results = []

    logger.info("Getting top blocks from db.")
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            # Insert the new row with the given last_processed_did
            blocked_query = """SELECT b.blocked_did, COUNT(*) AS block_count
                                    FROM blocklists AS b
                                    WHERE b.blocked_did IN (
                                    SELECT u.did
                                    FROM users AS u
                                    WHERE u.status = TRUE
                                )
                                    GROUP BY b.blocked_did
                                    ORDER BY block_count DESC
                                    LIMIT 25"""

            blocked_data = await connection.fetch(blocked_query)
            blocked_results.append(blocked_data)

            blockers_query = """SELECT b.user_did, COUNT(*) AS block_count
                                    FROM blocklists AS b
                                    WHERE b.user_did IN (
                                    SELECT u.did
                                    FROM users AS u
                                    WHERE u.status = TRUE
                                )
                                    GROUP BY b.user_did
                                    ORDER BY block_count DESC
                                    LIMIT 25"""

            blockers_data = await connection.fetch(blockers_query)
            blockers_results.append(blockers_data)

            return blocked_data, blockers_data
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
        raise InternalServerError
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_block_stats():
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            logger.info("Getting block statistics.")

            query_1 = """SELECT COUNT(blocked_did) from blocklists"""
            query_2 = """select count(distinct blocked_did) from blocklists"""
            query_3 = """select count(distinct user_did) from blocklists"""
            query_4 = """SELECT COUNT(*) AS user_count
                                FROM (
                                SELECT user_did
                                FROM blocklists
                                GROUP BY user_did
                                HAVING COUNT(DISTINCT blocked_did) = 1
                            ) AS subquery"""
            query_5 = """SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) BETWEEN 2 AND 100
                                ) AS subquery"""
            query_6 = """SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) BETWEEN 101 AND 1000
                                ) AS subquery"""
            query_7 = """SELECT COUNT(DISTINCT user_did) AS user_count
                                FROM (
                                    SELECT user_did
                                    FROM blocklists
                                    GROUP BY user_did
                                    HAVING COUNT(DISTINCT blocked_did) > 1000
                                ) AS subquery"""
            query_8 = """SELECT AVG(block_count) AS mean_blocks
                                FROM (
                                    SELECT user_did, COUNT(DISTINCT blocked_did) AS block_count
                                    FROM blocklists
                                    GROUP BY user_did
                                ) AS subquery"""
            query_9 = """SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) = 1
                                ) AS subquery"""
            query_10 = """SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) BETWEEN 2 AND 100
                                ) AS subquery"""
            query_11 = """SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) BETWEEN 101 AND 1000
                                ) AS subquery"""
            query_12 = """SELECT COUNT(*) AS user_count
                                FROM (
                                    SELECT blocked_did
                                    FROM blocklists
                                    GROUP BY blocked_did
                                    HAVING COUNT(DISTINCT user_did) > 1000
                                ) AS subquery"""
            query_13 = """SELECT AVG(block_count) AS mean_blocks
                                FROM (
                                    SELECT blocked_did, COUNT(DISTINCT user_did) AS block_count
                                    FROM blocklists
                                    GROUP BY blocked_did
                                ) AS subquery"""

            number_of_total_blocks = await connection.fetchval(query_1)
            logger.info("Completed query 1")
            number_of_unique_users_blocked = await connection.fetchval(query_2)
            logger.info("Completed query 2")
            number_of_unique_users_blocking = await connection.fetchval(query_3)
            logger.info("Completed query 3")
            number_block_1 = await connection.fetchval(query_4)
            logger.info("Completed query 4")
            number_blocking_2_and_100 = await connection.fetchval(query_5)
            logger.info("Completed query 5")
            number_blocking_101_and_1000 = await connection.fetchval(query_6)
            logger.info("Completed query 6")
            number_blocking_greater_than_1000 = await connection.fetchval(query_7)
            logger.info("Completed query 7")
            average_number_of_blocks = await connection.fetchval(query_8)
            logger.info("Completed query 8")
            number_blocked_1 = await connection.fetchval(query_9)
            logger.info("Completed query 9")
            number_blocked_2_and_100 = await connection.fetchval(query_10)
            logger.info("Completed query 10")
            number_blocked_101_and_1000 = await connection.fetchval(query_11)
            logger.info("Completed query 11")
            number_blocked_greater_than_1000 = await connection.fetchval(query_12)
            logger.info("Completed query 12")
            average_number_blocked = await connection.fetchval(query_13)
            logger.info("Completed query 13")
            total_users = await get_user_count(get_active=False)
            logger.info("Completed query 14")

            logger.info("All blocklist queries complete.")

            return (
                number_of_total_blocks,
                number_of_unique_users_blocked,
                number_of_unique_users_blocking,
                number_block_1,
                number_blocking_2_and_100,
                number_blocking_101_and_1000,
                number_blocking_greater_than_1000,
                average_number_of_blocks,
                number_blocked_1,
                number_blocked_2_and_100,
                number_blocked_101_and_1000,
                number_blocked_greater_than_1000,
                average_number_blocked,
                total_users,
            )
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_top24_blocks():
    blocked_results = []
    blockers_results = []

    logger.info("Getting top 24 blocks from db.")
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            # Insert the new row with the given last_processed_did
            blocked_query = """SELECT b.blocked_did, COUNT(*) AS block_count
                                    FROM blocklists AS b
                                    JOIN users AS u ON b.blocked_did = u.did AND u.status = TRUE
                                    WHERE b.block_date >= now() - INTERVAL '1 day'
                                    GROUP BY b.blocked_did
                                    ORDER BY block_count DESC
                                    LIMIT 25"""

            blocked_data = await connection.fetch(blocked_query)
            blocked_results.append(blocked_data)

            blockers_query = """SELECT b.user_did, COUNT(*) AS block_count
                                    FROM blocklists as b
                                    JOIN users AS u ON b.user_did = u.did AND u.status = TRUE
                                    WHERE b.block_date >= now() - INTERVAL '1 day'
                                    GROUP BY user_did
                                    ORDER BY block_count DESC
                                    LIMIT 25"""

            blockers_data = await connection.fetch(blockers_query)
            blockers_results.append(blockers_data)

            return blocked_data, blockers_data
    except asyncpg.exceptions.UndefinedTableError:
        logger.warning("table doesn't exist")
        raise InternalServerError
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_similar_blocked_by(user_did):
    global all_blocks_cache

    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        blocked_by_users = await connection.fetch("SELECT user_did FROM blocklists WHERE blocked_did = $1", user_did)

    # Extract the values from the records
    blocked_by_users_ids = [record["user_did"] for record in blocked_by_users]

    if not all_blocks_cache:
        logger.info("Caching all blocklists.")

        block_cache_status.set()

        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            # Fetch all blocklists except for the specified user's blocklist
            all_blocklists_rows = await connection.fetch("SELECT user_did, blocked_did FROM blocklists")
            all_blocks_cache = all_blocklists_rows

            block_cache_status.clear()
    else:
        all_blocklists_rows = all_blocks_cache

    # Create a dictionary to store blocklists as sets
    blocklists = {}
    for row in all_blocklists_rows:
        user_id = row["user_did"]
        blocked_id = row["blocked_did"]
        if user_id not in blocklists:
            blocklists[user_id] = set()
        blocklists[user_id].add(blocked_id)

    # Calculate match percentages for each person's block list
    user_match_percentages = {}
    for blocked_by_user_id in blocked_by_users_ids:
        if blocked_by_user_id != user_did:
            common_blocked = set(blocklists.get(blocked_by_user_id, set())) & set(blocklists.get(user_did, set()))
            common_count = len(common_blocked)
            person_blocked_count = len(blocklists.get(blocked_by_user_id, set()))

            # Check for division by zero
            if person_blocked_count == 0:
                continue  # Skip this comparison

            match_percentage = (common_count / person_blocked_count) * 100

            # Only include matches with a match_percentage greater than 1
            if match_percentage > 1:
                user_match_percentages[blocked_by_user_id] = match_percentage

    # Sort users by match percentage
    sorted_users = sorted(user_match_percentages.items(), key=lambda x: x[1], reverse=True)

    # Select top 20 users
    top_similar_users = sorted_users[:20]

    logger.info(top_similar_users)

    users = [user for user, percentage in top_similar_users]
    percentages = [percentage for user, percentage in top_similar_users]
    status_list = []
    for user, _percentage in top_similar_users:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            status = await connection.fetch("SELECT status FROM users WHERE did = $1", user)
            status_list.append(status)

    # Return the sorted list of users and their match percentages
    return users, percentages, status_list


async def get_similar_users(user_did):
    global all_blocks_cache
    global all_blocks_process_time
    global all_blocks_last_update

    all_blocks = all_blocks_cache.get("blocks")

    if not all_blocks:
        logger.info("Caching all blocklists.")
        start_time = datetime.now(timezone.utc)

        block_cache_status.set()

        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            # Fetch all blocklists except for the specified user's blocklist
            all_blocklists_rows = await connection.fetch("SELECT user_did, blocked_did FROM blocklists")
            all_blocks_cache["blocks"] = all_blocklists_rows

            block_cache_status.clear()
            end_time = datetime.now(timezone.utc)
            if start_time is not None:
                all_blocks_process_time = end_time - start_time
            all_blocks_last_update = end_time
    else:
        all_blocklists_rows = all_blocks

    # Create a dictionary to store blocklists as sets
    blocklists = {}
    specific_user_blocklist = set()

    for row in all_blocklists_rows:
        user_id = row["user_did"]
        blocked_id = row["blocked_did"]
        if user_id != user_did:
            if user_id not in blocklists:
                blocklists[user_id] = set()
            blocklists[user_id].add(blocked_id)
        else:
            # Get the specific user's blocklist
            specific_user_blocklist.add(blocked_id)

    if not len(specific_user_blocklist):
        users = "no blocks"
        percentages = 0
        status = None

        return users, percentages, status

    # Calculate match percentage for each user
    user_match_percentages = {}

    for other_user_did, other_user_blocklist in blocklists.items():
        common_blocked_users = specific_user_blocklist & other_user_blocklist
        common_count = len(common_blocked_users)
        specific_count = len(specific_user_blocklist)
        other_count = len(other_user_blocklist)

        # Check for division by zero
        if specific_count == 0 or other_count == 0:
            continue  # Skip this comparison

        match_percentage = (common_count / specific_count) * 100

        if match_percentage > 1:  # set threshold for results
            user_match_percentages[other_user_did] = match_percentage

    # Sort users by match percentage
    sorted_users = sorted(user_match_percentages.items(), key=lambda x: x[1], reverse=True)

    # Select top 20 users
    top_similar_users = sorted_users[:20]

    logger.info(f"Similar blocks: {await get_user_handle(user_did)} | {top_similar_users}")

    users = [user for user, percentage in top_similar_users]
    percentages = [percentage for user, percentage in top_similar_users]
    status_list = []
    for user, _percentage in top_similar_users:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            status = await connection.fetchval("SELECT status FROM users WHERE did = $1", user)
            status_list.append(status)
    logger.info(status_list)
    return users, percentages, status_list


async def blocklists_updater():
    global last_update_top_block
    global blocklist_updater_status
    global top_blocks_start_time
    global top_blocks_process_time
    global top_blocked_as_of_time

    blocked_list = "blocked"
    blocker_list = "blocker"

    top_blocks_start_time = datetime.now(timezone.utc)
    blocklist_updater_status.set()

    logger.info("Updating top blocks lists requested.")

    try:
        await truncate_top_blocks_table()
    except Exception as e:
        logger.error(f"Error truncating top blocks table: {e}")
        return

    blocked_results, blockers_results = await get_top_blocks()  # Get blocks for db
    logger.debug(f"blocked count: {len(blocked_results)} blockers count: {len(blockers_results)}")

    # Update blocked entries
    await update_top_block_list_table(blocked_results, blocked_list)  # add blocked to top blocks table
    logger.info("Updated top blocked db.")

    await update_top_block_list_table(blockers_results, blocker_list)  # add blocker to top blocks table
    logger.info("Updated top blockers db.")

    top_blocked, top_blockers, blocked_aid, blocker_aid = await utils.resolve_top_block_lists()
    logger.info("Top blocks lists page updated.")

    blocklist_updater_status.clear()

    last_update_top_block = datetime.now(timezone.utc)
    end_time = datetime.now(timezone.utc)

    if top_blocks_start_time is not None:
        top_blocks_process_time = end_time - top_blocks_start_time

    top_blocks_start_time = None

    top_blocked_as_of_time = datetime.now(timezone.utc).isoformat()

    return top_blocked, top_blockers, blocked_aid, blocker_aid


async def top_24blocklists_updater():
    global last_update_top_24_block
    global blocklist_24_updater_status
    global top_24_blocks_start_time
    global top_24_blocks_process_time
    global top_24_blocked_as_of_time

    blocked_list_24 = "blocked"
    blocker_list_24 = "blocker"

    top_24_blocks_start_time = datetime.now(timezone.utc)
    blocklist_24_updater_status.set()

    logger.info("Updating top 24 blocks lists requested.")

    try:
        await truncate_top24_blocks_table()
    except Exception as e:
        logger.error(f"Error truncating top 24 blocks table: {e}")
        return

    blocked_results_24, blockers_results_24 = await get_top24_blocks()  # Get blocks for db

    logger.debug(f"blocked count: {len(blocked_results_24)} blockers count: {len(blockers_results_24)}")

    # Update blocked entries
    await update_24_hour_block_list_table(blocked_results_24, blocked_list_24)  # add blocked to top blocks table

    logger.info("Updated top blocked db.")

    await update_24_hour_block_list_table(blockers_results_24, blocker_list_24)  # add blocker to top blocks table

    logger.info("Updated top blockers db.")

    top_blocked_24, top_blockers_24, blocked_aid_24, blocker_aid_24 = await utils.resolve_top24_block_lists()

    logger.info("Top 24 hour blocks lists page updated.")

    blocklist_24_updater_status.clear()

    last_update_top_24_block = datetime.now(timezone.utc)
    end_time = datetime.now(timezone.utc)

    if top_24_blocks_start_time is not None:
        top_24_blocks_process_time = end_time - top_24_blocks_start_time

    top_24_blocks_start_time = None

    top_24_blocked_as_of_time = datetime.now(timezone.utc).isoformat()

    return top_blocked_24, top_blockers_24, blocked_aid_24, blocker_aid_24


async def get_mutelists(ident, limit=100, offset=0):
    count = 0
    pages = 0

    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        query = """
        SELECT ml.url, ml.name, ml.did, ml.description, ml.created_date, mu.date_added, mc.user_count
        FROM mutelists AS ml
        INNER JOIN mutelists_users AS mu ON ml.uri = mu.list_uri
        LEFT JOIN mutelists_user_count AS mc ON ml.uri = mc.list_uri
        WHERE mu.subject_did = $1
        ORDER BY mu.date_added DESC
        LIMIT $2 OFFSET $3
        """
        try:
            mute_lists = await connection.fetch(query, ident, limit, offset)
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError

        lists = []
        for record in mute_lists:
            data = {
                "url": record["url"],
                "did": record["did"],
                "name": record["name"],
                "description": record["description"],
                "created_date": record["created_date"].isoformat(),
                "date_added": record["date_added"].isoformat(),
                "list user count": record["user_count"],
            }
            lists.append(data)

        return lists, count, pages


async def get_mutelist_count(ident):
    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        query = """
        SELECT COUNT(*) FROM mutelists_users WHERE subject_did = $1
        """
        try:
            count = await connection.fetchval(query, ident)
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError

        return count


async def check_api_key(api_environment, key_type, key_value) -> bool:
    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        try:
            query = """SELECT valid FROM API WHERE key = $3 environment = $1 AND access_type LIKE '%' || $2 || '%'"""

            status = await connection.fetchval(query, api_environment, key_type, key_value)

            return status
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError


async def tables_exists() -> bool:
    pool_name = get_connection_pool("write")

    async with connection_pools[pool_name].acquire() as connection, connection.transaction():
        try:
            query1 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
            query2 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
            query3 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""
            query4 = """SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)"""

            users_exist = await connection.fetchval(query1, users_table)
            blocklists_exist = await connection.fetchval(query2, blocklist_table)
            top_blocks_exist = await connection.fetchval(query3, top_blocks_table)
            top_24_exist = await connection.fetchval(query4, top_24_blocks_table)

            values = (users_exist, blocklists_exist, top_blocks_exist, top_24_exist)

            return not any(value is False for value in values)
        except asyncpg.ConnectionDoesNotExistError:
            raise DatabaseConnectionError
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError


async def get_api_keys(environment, key_type, key) -> dict | None:
    if not key and not key_type and not environment:
        logger.error("Missing required parameters for API verification.")

        return None

    pool_name = get_connection_pool("write")
    async with connection_pools[pool_name].acquire() as connection, connection.transaction():
        try:
            query = """SELECT a.key as key, a.valid, aa.*
                            FROM api AS a
                            INNER JOIN api_access AS aa ON a.key = aa.key
                            WHERE a.key = $2 AND a.environment = $1 AND a.valid is TRUE"""

            results = await connection.fetch(query, environment, key)

            for item in results:
                data = {
                    "key": item["key"],
                    "valid": item["valid"],
                    key_type: item[key_type.lower()],
                }

            return data
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError


async def get_dids_per_pds() -> dict | None:
    data_dict = {}

    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            query = """SELECT users.pds, COUNT(did) AS did_count, pds.status
                        FROM users
                        join pds on users.pds = pds.pds
                        WHERE users.pds IS NOT NULL
                        GROUP BY users.pds, pds.status
                        ORDER BY did_count desc"""

            results = await connection.fetch(query)
            for record in results:
                data_dict[record["pds"]] = {
                    "did_count": record["did_count"],
                    "status": record["status"],
                }

            return data_dict
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_block_row(uri) -> dict | None:
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            query = """SELECT user_did, blocked_did, block_date, cid, uri
                            FROM blocklists
                            WHERE uri = $1"""

            record = await connection.fetch(query, uri)

            if record:
                result = record[0]
                response = {
                    "user did": result["user_did"],
                    "blocked did": result["blocked_did"],
                    "block date": result["block_date"],
                    "cid": result["cid"],
                    "uri": result["uri"],
                }

                return response
            else:
                raise NotFound
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def identifier_exists_in_db(identifier):
    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        if utils.is_did(identifier):
            results = await connection.fetch("SELECT did, status FROM users WHERE did = $1", identifier)

            true_record = None

            for result in results:
                # ident = result['did']
                status = result["status"]

                if status:
                    ident = True
                    true_record = (ident, status)
                    break

            if true_record:
                ident, status = true_record
            else:
                ident = False
                status = False
        elif utils.is_handle(identifier):
            results = await connection.fetch("SELECT handle, status FROM users WHERE handle = $1", identifier)

            true_record = None

            for result in results:
                # ident = result['handle']
                status = result["status"]

                if status:
                    ident = True
                    true_record = (ident, status)
                    break

            if true_record:
                ident, status = true_record
            else:
                ident = False
                status = False
        else:
            ident = False
            status = False

    return ident, status


async def get_user_did(handle) -> str | None:
    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        try:
            did = await connection.fetchval("SELECT did FROM users WHERE handle = $1 AND status is True", handle)
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError

    return did


async def get_user_handle(did) -> str | None:
    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        try:
            handle = await connection.fetchval("SELECT handle FROM users WHERE did = $1", did)
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError

    return handle


async def get_user_count(get_active=True) -> int:
    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        try:
            if get_active:
                count = await connection.fetchval(
                    """SELECT COUNT(*)
                                                    FROM users
                                                    JOIN pds ON users.pds = pds.pds
                                                    WHERE users.status IS TRUE AND pds.status IS TRUE"""
                )

                # count = await connection.fetchval("""SELECT COUNT(*)
                #                                     FROM users
                #                                     WHERE users.status IS TRUE""")
            else:
                count = await connection.fetchval(
                    """SELECT COUNT(*) FROM users JOIN pds ON users.pds = pds.pds WHERE pds.status is TRUE"""
                )
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError

        return count


async def get_deleted_users_count() -> int:
    pool_name = get_connection_pool("read")
    async with connection_pools[pool_name].acquire() as connection:
        try:
            count = await connection.fetchval(
                "SELECT COUNT(*) FROM USERS JOIN pds ON users.pds = pds.pds WHERE pds.status is TRUE AND "
                "users.status is FALSE"
            )
        except asyncpg.PostgresError as e:
            logger.error(f"Postgres error: {e}")
            raise DatabaseConnectionError
        except asyncpg.InterfaceError as e:
            logger.error(f"interface error: {e}")
            raise DatabaseConnectionError
        except AttributeError:
            logger.error("db connection issue.")
            raise DatabaseConnectionError
        except Exception as e:
            logger.error(f"Error: {e}")
            raise InternalServerError

        return count


async def get_single_user_blocks(ident, limit=100, offset=0):
    try:
        # Execute the SQL query to get all the user_dids that have the specified did/ident in their blocklist
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            result = await connection.fetch(
                """SELECT DISTINCT user_did, block_date
                                                FROM blocklists
                                                WHERE blocked_did = $1
                                                ORDER BY block_date DESC
                                                LIMIT $2
                                                OFFSET $3""",
                ident,
                limit,
                offset,
            )

            block_list = []

            if result:
                # Iterate over blocked_users and extract handle and status
                for user_did, block_date in result:
                    block_list.append(
                        {
                            "did": user_did,
                            "blocked_date": block_date.isoformat(),
                        }
                    )

                return block_list
            else:
                block_list = []

                return block_list
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_did_web_handle_history(identifier) -> list | None:
    handle_history = []
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            history = await connection.fetch(
                "SELECT handle, pds, timestamp FROM did_web_history WHERE did = $1",
                identifier,
            )

            if history is None:
                return None

            for record in history:
                timestamp = None if record["timestamp"] is None else record["timestamp"].isoformat()

                handle_history.append((record["handle"], timestamp, record["pds"]))

            handle_history.sort(key=lambda x: x[2])

            return handle_history
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_starter_packs(ident, limit=100, offset=0) -> list:
    starter_packs = []
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            result = await connection.fetch(
                """SELECT name, description, did, created_date, url
                                                FROM starter_packs
                                                WHERE did = $1
                                                ORDER BY created_date DESC
                                                LIMIT $2
                                                OFFSET $3""",
                ident,
                limit,
                offset,
            )

            if result:
                for record in result:
                    starter_packs.append(
                        {
                            "name": record["name"],
                            "description": record["description"],
                            "did": record["did"],
                            "created_date": record["created_date"].isoformat(),
                            "url": record["url"],
                        }
                    )

                return starter_packs
            else:
                return starter_packs
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_starter_packs_count(ident) -> int:
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            count = await connection.fetchval("SELECT COUNT(*) FROM starter_packs WHERE did = $1", ident)

            return count
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_single_starter_packs(ident, limit=100, offset=0):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            result = await connection.fetch(
                """SELECT s.name, s.description, s.did, s.created_date, s.url
                                                FROM starter_packs AS s
                                                JOIN mutelists_users AS mu ON s.list_uri = mu.list_uri
                                                WHERE mu.subject_did = $1
                                                ORDER BY s.created_date DESC
                                                LIMIT $2
                                                OFFSET $3""",
                ident,
                limit,
                offset,
            )

            starter_packs = []

            if result:
                for record in result:
                    starter_packs.append(
                        {
                            "name": record["name"],
                            "description": record["description"],
                            "did": record["did"],
                            "created_date": record["created_date"].isoformat(),
                            "url": record["url"],
                        }
                    )

                return starter_packs
            else:
                return starter_packs
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_single_starter_packs_count(ident):
    try:
        pool_name = get_connection_pool("read")
        async with connection_pools[pool_name].acquire() as connection:
            count = await connection.fetchval(
                """SELECT COUNT(*) FROM starter_packs
                                                JOIN mutelists_users AS mu ON starter_packs.list_uri as s = mu.list_uri
                                                WHERE mu.subject_did = $1""",
                ident,
            )

            return count
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def deactivate_user(user) -> None:
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            await connection.execute("UPDATE users SET status = FALSE WHERE did = $1", user)
    except Exception as e:
        logger.error(f"Error deactivating user: {e}")


async def get_cursor_recall():
    try:
        async with connection_pools["cursor"].acquire() as connection:
            query = """SELECT * FROM cursor_storage WHERE service = 'clearsky.cursors'"""

            records = await connection.fetch(query)

            return records
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


async def get_cursor_time():
    try:
        pool_name = get_connection_pool("write")
        async with connection_pools[pool_name].acquire() as connection, connection.transaction():
            query = """SELECT timestamp FROM subscriptionstate WHERE service = 'firehose.clearsky.services'"""

            records = await connection.fetchval(query)

            query2 = """SELECT response FROM subscriptionstate WHERE service = 'firehose.clearsky.services.override'"""

            records2 = await connection.fetchval(query2)

            if not records2:
                records2 = None

            return records, records2
    except asyncpg.PostgresError as e:
        logger.error(f"Postgres error: {e}")
        raise DatabaseConnectionError
    except asyncpg.InterfaceError as e:
        logger.error(f"interface error: {e}")
        raise DatabaseConnectionError
    except AttributeError:
        logger.error("db connection issue.")
        raise DatabaseConnectionError
    except Exception as e:
        logger.error(f"Error: {e}")
        raise InternalServerError


# ======================================================================================================================
# ============================================ get database credentials ================================================
def get_database_config(ovride=False) -> dict:
    db_config = {}

    try:
        if not os.getenv("CLEAR_SKY") or ovride:
            logger.info("Database connection: Using config.ini.")

            use_local_db = config.get("database", "use_local", fallback="None")

            if not use_local_db:
                use_local_db = "None"

            db_config["use_local_db"] = use_local_db

            read_keyword = config.get("environment", "read_keyword", fallback="read")

            if not read_keyword:
                read_keyword = "read"

            db_config["read_keyword"] = read_keyword

            write_keyword = config.get("environment", "write_keyword", fallback="write")

            if not write_keyword:
                write_keyword = "write"

            db_config["write_keyword"] = write_keyword

            for section in config.sections():
                if section.startswith("database."):
                    db_type = section
                    db_config[db_type] = {
                        "user": config.get(
                            section,
                            "CLEARSKY_DATABASE_U",
                            fallback=os.getenv(f"CLEARSKY_DATABASE_U_{db_type.upper()}"),
                        ),
                        "password": config.get(
                            section,
                            "CLEARSKY_DATABASE_P",
                            fallback=os.getenv(f"CLEARSKY_DATABASE_P_{db_type.upper()}"),
                        ),
                        "host": config.get(
                            section,
                            "CLEARSKY_DATABASE_H",
                            fallback=os.getenv(f"CLEARSKY_DATABASE_H_{db_type.upper()}"),
                        ),
                        "port": config.get(
                            section,
                            "CLEARSKY_DATABASE_PORT",
                            fallback=os.getenv(f"CLEARSKY_DATABASE_PORT_{db_type.upper()}"),
                        ),
                        "database": config.get(
                            section,
                            "CLEARSKY_DATABASE_DB",
                            fallback=os.getenv(f"CLEARSKY_DATABASE_DB_{db_type.upper()}"),
                        ),
                    }
        else:
            logger.info("Database connection: Using environment variables.")

            use_local_db = os.environ.get("USE_LOCAL_DB")

            if not use_local_db:
                use_local_db = "None"

            db_config["use_local_db"] = use_local_db

            read_keyword = os.environ.get("READ_KEYWORD")

            if not read_keyword:
                read_keyword = "read"

            db_config["read_keyword"] = read_keyword

            write_keyword = os.environ.get("WRITE_KEYWORD")

            if not write_keyword:
                write_keyword = "write"

            db_config["write_keyword"] = write_keyword

            for key, _value in os.environ.items():
                if key.startswith("CLEARSKY_DATABASE"):
                    db_type = key if key.startswith("CLEARSKY_DATABASE_DB") else None

                    name_parts = key.split("_")
                    name = name_parts[3]
                    if db_type:
                        db_config[db_type] = {
                            "user": os.environ.get(f"CLEARSKY_DATABASE_USR_{name}"),
                            "password": os.environ.get(f"CLEARSKY_DATABASE_PW_{name}"),
                            "host": os.environ.get(f"CLEARSKY_DATABASE_H_{name}"),
                            "database": os.environ.get(f"CLEARSKY_DATABASE_DB_{name}"),
                        }

        return db_config
    except Exception as e:
        logger.error("Database connection information not present: Set environment variables or config.ini")
        logger.error(f"Error: {e}")


config = config_helper.read_config()

override = check_override()

# Get the database configuration
database_config = get_database_config(True) if override else get_database_config()

# Initialize a round-robin iterator for read databases
read_keyword = database_config["read_keyword"]

# Extract database names from the config file
config_db_names = [db for db in database_config if f"database_{read_keyword}" in db.lower()]

# Extract database names from the environment variables
env_db_names = [
    key for key in os.environ if key.startswith("CLEARSKY_DATABASE") and f"db_{read_keyword}" in key.lower()
]

read_dbs = config_db_names + env_db_names

read_db_iterator = itertools.cycle(read_dbs)


async def local_db() -> bool:
    if database_config["use_local_db"].lower() == "none":
        return False
    else:
        logger.warning("Using local db.")

        return True
