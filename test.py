# test.py
import asyncio
import sys
import urllib
import urllib.parse
import httpx
from datetime import datetime
import app
import utils
from config_helper import logger, limiter
import database_handler
import random
import string
import datetime
import dns.resolver
from on_wire import resolve_handle
from on_wire import resolve_did


async def generate_random_user_data():
    try:
        async with database_handler.connection_pools['read'].acquire() as connection:
            async with connection.transaction():
                # Generate and insert random user data into the 'users' table
                user_data = []
                for _ in range(50000):
                    prefix = 'did:plc:'
                    alphanumeric = string.ascii_letters + string.digits
                    suffix = ''.join(random.choices(alphanumeric + '._:%-', k=24))

                    did = prefix + suffix
                    random_string = ''.join(random.choices(string.ascii_letters, k=10))
                    handle = random_string + '.bsky.social'
                    status = random.choice([True, False])  # Random status

                    user_data.append((did, handle, status))

                await connection.executemany(
                    "INSERT INTO users (did, handle, status) VALUES ($1, $2, $3)",
                    user_data
                )

                logger.info("user data created.")

                return user_data
    except Exception as e:
        logger.error(f"Error generating random data: {e}")


async def generate_random_block_data(user_data):
    try:
        async with database_handler.connection_pools['read'].acquire() as connection:
            async with connection.transaction():
                # Generate and insert random blocklist data into the 'blocklists' table

                blocklists_data = []

                for user in user_data:
                    user_did = user[0]  # User DID
                    num_blocked_dids = random.randint(0, 150)  # Random number of blocked_dids (adjust the range as needed)

                    blocked_dids = random.sample([user[0] for user in user_data if user[0] != user_did], num_blocked_dids)
                    block_dates = [generate_random_date() for _ in range(num_blocked_dids)]

                    # Create blocklist entries for this user
                    for blocked_did, block_date in zip(blocked_dids, block_dates):
                        blocklists_data.append((user_did, blocked_did, block_date))

                await connection.executemany(
                    "INSERT INTO blocklists (user_did, blocked_did, block_date) VALUES ($1, $2, $3)",
                    blocklists_data
                )

                logger.info("blocklist data created.")

    except Exception as e:
        logger.error(f"Error generating random data: {e}")


# Function to generate a random date within a specified range
def generate_random_date():
    current_date = datetime.date.today()
    start_date = datetime.date(2022, 1, 1)  # Change this to your desired start date
    end_date = current_date  # Change this to your desired end date
    random_date = start_date + datetime.timedelta(days=random.randint(0, (end_date - start_date).days))

    return random_date.isoformat()


def resolve_handle_dns(domain):
    at_proto_lookup = "_atproto."

    lookup = f"{at_proto_lookup}{domain}"

    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = 5
        resolver.lifetime = 5
        result = resolver.resolve(lookup, "TXT")

        result = str(result[0])
        result = result.strip('"')

        return result
    except Exception as e:
        logger.error(f"Error resolving DNS: {e}")

        return None


async def verify_handle(identity):
    handle1 = None
    handle2 = None
    handle3 = None

    async def resolve_handle_wellknown_atproto(ident):
        url = f"https://{urllib.parse.unquote(ident)}/.well-known/atproto-did"

        max_retries = 5
        retry_count = 0

        while retry_count < max_retries:
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(url)
                    if response.status_code == 200:
                        response_json = response.json()

                        return response_json
                    if response.status_code == 400:
                        logger.debug(f"response 400: {response.json()}")

                        return None

                    return result
            except Exception as e:
                retry_count += 1
                logger.error(f"Error occurred while making the API call: {e}")
                return None

        logger.warning("Resolve error for: " + ident + " after multiple retries.")

        return None

    at_proto_lookup = "_atproto."

    lookup = f"{at_proto_lookup}{identity}"

    at_proto_result = await resolve_handle_wellknown_atproto(identity)

    bsky_result = await resolve_handle(identity)

    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = 5
        resolver.lifetime = 5
        result = resolver.resolve(lookup, "TXT")

        if result:
            result = str(result[0])
            dns_result = result.strip('"')
            dns_result = dns_result.replace('did=', '')
        else:
            dns_result = None

        # return result
    except Exception as e:
        logger.error(f"Error resolving DNS: {e}")

        dns_result = None

    if (at_proto_result is not None and "did:plc" in at_proto_result) or (bsky_result is not None and "did:plc" in bsky_result) or (dns_result is not None and "did:plc" in dns_result):
        if "bsky.social" in identity:
            if at_proto_result and bsky_result:
                if at_proto_result == bsky_result:
                    return True
                else:
                    return False
        else:
            if dns_result and bsky_result:
                if dns_result == bsky_result:
                    return True
                else:
                    return False
            elif bsky_result and at_proto_result:
                if bsky_result == at_proto_result:
                    return True
                else:
                    return False
            else:
                logger.error(
                    f"validitiy case didnt match for {identity} | at_proto: {at_proto_result} | bsky: {bsky_result} | dns: {dns_result}")

                return False
    elif (at_proto_result is not None and "did:web" in at_proto_result) or (bsky_result is not None and "did:web" in bsky_result) or (dns_result is not None and "did:web" in dns_result):
        if at_proto_result is not None and "did:web" in at_proto_result:
            handle1 = await resolve_did(at_proto_result)
        elif bsky_result is not None and "did:web" in bsky_result:
            handle2 = await resolve_did(bsky_result)
        elif dns_result is not None and "did:web" in dns_result:
            handle3 = await resolve_did(dns_result)

        if identity == handle1[0] or identity == handle2[0] or identity == handle3[0]:
            if dns_result and bsky_result:
                if dns_result == bsky_result:
                    return True
                else:
                    return False
            elif bsky_result and at_proto_result:
                if bsky_result == at_proto_result:
                    return True
                else:
                    return False
            else:
                logger.error(
                    f"validity case didn't match for {identity} | at_proto: {at_proto_result} | bsky: {bsky_result} | dns: {dns_result}")

                return False
    else:
        return False


async def describe_pds(pds):
    url = f"{pds}/xrpc/com.atproto.server.describeServer"

    logger.debug(url)

    try:
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(url)
            except Exception:
                return False

            if response.status_code == 200:
                response_json = response.json()
                available_user_domains = str(response_json["availableUserDomains"]).strip("[]")
                logger.info(f"available_user_domains: {available_user_domains}")
                if available_user_domains is not None:
                    return True
                else:
                    return False
            else:
                return False
    except Exception:
        logger.warning(f"Failed to describe PDS: {pds}")
        return False


async def get_user_block_list(ident, pds):
    base_url = f"{pds}/xrpc/"
    collection = "app.bsky.graph.block"
    limit = 100
    blocked_data = []
    cursor = None
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        url = urllib.parse.urljoin(base_url, "com.atproto.repo.listRecords")
        params = {
            "repo": ident,
            "limit": limit,
            "collection": collection,
        }

        if cursor:
            params["cursor"] = cursor

        encoded_params = urllib.parse.urlencode(params, quote_via=urllib.parse.quote)
        full_url = f"{url}?{encoded_params}"
        logger.debug(full_url)

        try:
            async with limiter:
                async with httpx.AsyncClient() as client:
                    response = await client.get(full_url, timeout=10,
                                                follow_redirects=True)  # Set an appropriate timeout value (in seconds)

                ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))
                if ratelimit_remaining < 100:
                    logger.warning(
                        f"Blocklist Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                    # Sleep until the rate limit resets
                    current_time = datetime.now()
                    sleep_time = ratelimit_reset - current_time.timestamp() + 1
                    logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                    await asyncio.sleep(sleep_time)
        except httpx.ReadTimeout:
            logger.warning("Request timed out. Retrying... Retry count: %d", retry_count)
            retry_count += 1
            await asyncio.sleep(10)
            continue
        except httpx.RequestError as e:
            logger.warning(f"Error during API call: {e} : {full_url}")
            retry_count += 1
            logger.info("sleeping 5")
            await asyncio.sleep(5)
            continue

        if response.status_code == 200:
            response_json = response.json()
            records = response_json.get("records", [])

            for record in records:
                value = record.get("value", {})
                subject = value.get("subject")
                created_at_value = value.get("createdAt")
                timestamp = created_at_value
                # timestamp = datetime.fromisoformat(created_at_value)
                uri = record.get("uri")
                cid = record.get("cid")

                logger.debug(f"subject: {subject} created: {timestamp} uri: {uri} cid: {cid}")

                if subject and timestamp and uri and cid:
                    blocked_data.append((subject, timestamp, uri, cid))
                else:
                    if not timestamp:
                        timestamp = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.error(f"{full_url}: missing timestamp")
                    elif not uri:
                        uri = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.error(f"{full_url}: Missing uri")
                    elif not cid:
                        cid = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.error(f"{full_url}: missing cid")
                    elif not subject:
                        subject = None
                        blocked_data.append((subject, timestamp, uri, cid))
                        logger.error(f"{full_url}: missing subject")
                    else:
                        logger.error(f"{full_url}: missing data")
                        return None

            cursor = response_json.get("cursor")

        else:
            if response.status_code == 429:
                logger.warning("Received 429 Too Many Requests. Retrying after 60 seconds...")
                await asyncio.sleep(60)  # Retry after 60 seconds
            elif response.status_code == 400:
                retry_count += 1
                try:
                    error_message = response.json()["error"]
                    message = response.json()["message"]
                    if error_message == "InvalidRequest" and "Could not find repo" in message:
                        logger.debug("Could not find repo: " + str(ident))

                        return None
                except KeyError:
                    return None
            else:
                retry_count += 1
                logger.warning("Error during API call. Status code: %s", response.status_code)
                await asyncio.sleep(5)
                continue

        if not cursor:
            break

    logger.debug(blocked_data)

    if retry_count == max_retries:
        logger.warning("Could not get block list for: " + ident)

        return None

    return blocked_data


async def main():
    try:
        await database_handler.create_connection_pool("read")
        await database_handler.create_connection_pool("write")
    except Exception as e:
        logger.error(f"Error creating connection pool: {str(e)}")
        sys.exit()

    # answer = await describe_pds('https://zaluka.yartsa.xyz')
    # logger.info(f"pds valid: {answer}")
    # await database_handler.create_connection_pool("write")

    # await database_handler.process_delete_queue()

    # await database_handler.update_did_webs()

    # logger.info("Update did pds service information.")
    # last_value = "2024-03-21 01:07:33.497000+00:00"
    # if last_value:
    #     logger.info(f"last value retrieved, starting from: {last_value}")
    # else:
    #     last_value = None
    #     logger.info(f"No last value retrieved, starting from beginning.")
    # await utils.get_all_did_records(last_value)

    # record = await get_user_block_list('did:plc:mystu6bxz4df3vlxydlc4ekr', 'https://amanita.us-east.host.bsky.network')
    #
    # print(record)
    # print(len(record))
    #
    # await database_handler.update_blocklist_table('did:plc:mystu6bxz4df3vlxydlc4ekr', record)

    # logger.info("Getting label information.")
    # labelers = await database_handler.get_labelers()
    #
    # logger.info("Updating labeler data.")
    # await database_handler.update_labeler_data(labelers)

    # result = await app.get_handle_history_info("genco.me")

    # logger.info(result)
    # await database_handler.update_did_webs()

    # await utils.get_resolution_queue_info()
    # #
    # logger.info("Getting label information.")
    # labelers = await database_handler.get_labelers()
    # #
    # logger.info("Updating labeler data.")
    # await database_handler.update_labeler_data(labelers)

    # result = await resolve_did("did:plc:qwopp46kocaqyadn2yz7gh6t", did_web_pds=False)
    #
    # print(result[0])

    # await verify_handle("thieflord.dev")

    await database_handler.get_block_row('at://did:plc:vmdqzixzv3o7zw6bn233gb4s/app.bsky.graph.block/3kc7atxkmsp2u')
if __name__ == '__main__':
    asyncio.run(main())
