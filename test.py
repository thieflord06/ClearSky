# test.py
import asyncio
import urllib

import httpx

from config_helper import logger
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

        if identity == handle1 or identity == handle2 or identity == handle3:
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


async def main():
    # answer = await describe_pds('https://zaluka.yartsa.xyz')
    # logger.info(f"pds valid: {answer}")
    await database_handler.create_connection_pool("write")

    await database_handler.process_delete_queue()

if __name__ == '__main__':
    asyncio.run(main())
