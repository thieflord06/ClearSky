# on_wire.py

import urllib.parse
import asyncio
import httpx
from httpx import HTTPStatusError
from config_helper import logger, limiter


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
                response_json = response.json()

                if response.status_code == 400:
                    try:
                        error_message = response.json()["error"]
                        message = response.json()["message"]
                        if error_message == "InvalidRequest" and "Unable to resolve handle" in message:
                            logger.warning("Could not find repo: " + str(info))

                            return None
                    except KeyError:
                        logger.error(f"Error getting response: key error")

                        return None
                logger.debug("response: " + str(response_json))

                result = list(response_json.values())[0]

                return result
        except (httpx.RequestError, HTTPStatusError) as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {e}")
            # await asyncio.sleep(30)
            return None

    logger.warning("Resolve error for: " + info + " after multiple retries.")

    return None


async def resolve_did(did, did_web_pds=False):  # Take DID and get handle
    base_url = "https://plc.directory/"
    url = f"{base_url}{did}"

    max_retries = 5
    retry_count = 0

    if "did:web" in did:
        logger.info("Resolving did:web")
        short_did = did[len("did:web:"):]
        url = f"https://{urllib.parse.unquote(short_did)}/.well-known/did.json"

    logger.debug(url)

    while retry_count < max_retries:
        try:
            async with limiter:
                async with httpx.AsyncClient() as client:
                    response = await client.get(url)

                # ratelimit_limit = int(response.headers.get('Ratelimit-Limit', 0))
                # ratelimit_remaining = int(response.headers.get('Ratelimit-Remaining', 0))
                # ratelimit_reset = int(response.headers.get('Ratelimit-Reset', 0))
                #
                # if ratelimit_remaining < 100:
                #     logger.warning(f"Resolve Rate limit low: {ratelimit_remaining} \n Rate limit: {ratelimit_limit} Rate limit reset: {ratelimit_reset}")
                #     # Sleep until the rate limit resets
                #     sleep_time = 15
                #     logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")
                #     await asyncio.sleep(sleep_time)

                response_json = response.json()
                logger.debug("response: " + str(response_json))

                if response.status_code == 200:

                    if "did:web" in did:
                        try:
                            record = response_json["alsoKnownAs"][0]
                        except Exception as e:
                            logger.error(f"Error getting did:web handle: {e} | did: {did} | {url}")

                            record = None
                    else:
                        record = response_json.get("alsoKnownAs", "")

                    record = str(record)
                    stripped_record = record.replace("at://", "")
                    stripped_record = stripped_record.strip("[]").replace("'", "")

                    if did_web_pds:
                        logger.info(f"Getting PDS for {did}")

                        try:
                            endpoint = response_json["service"][0]["serviceEndpoint"]

                            logger.info(f"Endpoint retrieved for {did}: {endpoint}")

                            return endpoint
                        except Exception as e:
                            logger.error(f"Error getting did:web PDS: {e} | did: {did} | PDS: {endpoint} | {url}")

                            return None

                    if "RateLimit Exceeded" in stripped_record:
                        retry_count += 1
                        sleep_time = 15

                        logger.warning(f"Approaching Rate limit waiting for {sleep_time} seconds")

                        await asyncio.sleep(sleep_time)
                        # stripped_record = did
                        #
                        # return stripped_record

                    return stripped_record
                elif response.status_code == 429:
                    logger.warning("Too many requests, pausing.")
                    await asyncio.sleep(10)
                elif response.status_code == 404:
                    logger.warning(f"404 not found: {did}")

                    return None
                else:
                    error_message = response_json.get("message", "")
                    logger.debug(error_message)

                    if "DID not registered" in error_message.lower():
                        logger.warning("User not found. Skipping...")

                        return None
                    elif "DID not available" in error_message.lower():
                        logger.warning("User not found. Skipping...")

                        return None
                    else:
                        retry_count += 1
                        logger.warning("Error:" + str(response.status_code))
                        logger.warning("Retrying: " + str(url))
                        await asyncio.sleep(5)
        except httpx.DecodingError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {e}")
            # await asyncio.sleep(5)

            return None
        except httpx.RequestError as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {str(e)} {url}")
            # await asyncio.sleep(5)

            return None
        except httpx.HTTPStatusError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {str(e)} {url}")
            # await asyncio.sleep(5)

            return None
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {str(e)} {url}")
            # await asyncio.sleep(5)

            return None

    logger.warning("Failed to resolve: " + str(did) + " after multiple retries.")

    return None


async def get_avatar_id(did):
    handle = did
    base_url = "https://bsky.social/xrpc/"
    collection = "app.bsky.actor.profile"
    rkey = "self"
    url = urllib.parse.urljoin(base_url, "com.atproto.repo.getRecord")
    params = {
        "repo": handle,
        "collection": collection,
        "rkey": rkey
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
                response_json = response.json()
                logger.debug("response: " + str(response_json))

                if response.status_code == 200:
                    avatar_info = response_json.get("value", {}).get("avatar", {})
                    avatar_link = avatar_info.get("ref", {}).get("$link", "")
                    avatar_mimetype = avatar_info.get("mimeType", "")
                    if avatar_link and "jpeg" in avatar_mimetype:

                        return avatar_link
                    elif avatar_link:
                        logger.warning(f"No picture format or incorrect format: {avatar_mimetype}")
                        return avatar_link
                    elif not avatar_link:
                        avatar_cid = avatar_info.get("cid", "")

                        return avatar_cid
                    else:
                        retry_count += 1
                        logger.warning("Error:" + "status code: " + str(response.status_code))
                        logger.error("Could not find avatar ID or picture format")
                        logger.warning("Retrying: " + str(full_url))
                        await asyncio.sleep(10)
                elif response.status_code == 400:
                    logger.warning("User not found. Skipping...")

                    return None
                else:
                    error_message = response_json.get("message", "")
                    logger.debug(error_message)

                    break
        except httpx.DecodingError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {e}")
            # await asyncio.sleep(30)

            return None
        except httpx.RequestError as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {str(e)} {full_url}")
            # await asyncio.sleep(30)

            return None
        except httpx.HTTPStatusError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {str(e)} {full_url}")
            # await asyncio.sleep(30)

            return None
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {str(e)} {full_url}")
            # await asyncio.sleep(30)

            return None

    logger.warning("Failed to resolve: " + str(did) + " after multiple retries.")

    return "Error"


async def get_profile_picture(did, avatar_id):
    handle = did
    base_url = "https://av-cdn.bsky.app/img/feed_fullsize/plain/"
    url = urllib.parse.urljoin(base_url)
    params = {
        "did": handle,
        "/": avatar_id
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
                response_json = response.json()
                logger.debug("response: " + str(response_json))

                if response.status_code == 200:
                    avatar_info = response_json.get("value", {}).get("avatar", {})
                    avatar_link = avatar_info.get("ref", {}).get("$link", "")
                    avatar_mimetype = avatar_info.get("mimeType", "")
                    if avatar_link and "jpeg" in avatar_mimetype:

                        return avatar_link
                else:
                    error_message = response_json.get("message", "")
                    logger.debug(error_message)

                    if "could not find user" in error_message.lower():
                        logger.warning("User not found. Skipping...")

                        return None
                    else:
                        retry_count += 1
                        logger.warning("Error:" + str(response.status_code))
                        logger.warning("Retrying: " + str(full_url))
                        await asyncio.sleep(10)
        except httpx.DecodingError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {e}")
            # await asyncio.sleep(30)

            return None
        except httpx.RequestError as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {str(e)} {full_url}")
            # await asyncio.sleep(30)

            return None
        except httpx.HTTPStatusError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {str(e)} {full_url}")
            # await asyncio.sleep(30)

            return None
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {str(e)} {full_url}")
            # await asyncio.sleep(30)

            return None

    logger.warning("Failed to resolve: " + str(did) + " after multiple retries.")

    return "Error"
