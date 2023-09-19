# on_wire.py

import urllib.parse
import asyncio
import httpx
from httpx import HTTPStatusError
from config_helper import logger


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
                            return "Could not find, there may be a typo."
                    except KeyError:
                        pass
                logger.debug("response: " + str(response_json))

                result = list(response_json.values())[0]

                return result
        except (httpx.RequestError, HTTPStatusError) as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {e}")
            await asyncio.sleep(30)
            continue

    logger.warning("Resolve error for: " + info + " after multiple retries.")

    return "error"


async def resolve_did(did):  # Take DID and get handle
    base_url = "https://plc.directory/"
    url = f"{base_url}{did}"

    logger.debug(url)

    max_retries = 5
    retry_count = 0

    while retry_count < max_retries:
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response_json = response.json()
                logger.debug("response: " + str(response_json))

                if response.status_code == 200:
                    record = response_json.get("alsoKnownAs", "")
                    record = str(record)
                    stripped_record = record.replace("at://", "")
                    stripped_record = stripped_record.strip("[]").replace("'", "")

                    return stripped_record
                elif response.status_code == 429:
                    logger.warning("Too many requests, pausing.")
                    await asyncio.sleep(60)
                else:
                    error_message = response_json.get("message", "")
                    logger.debug(error_message)

                    if "DID not registered" in error_message.lower():
                        logger.warning("User not found. Skipping...")

                        return None
                    else:
                        retry_count += 1
                        logger.warning("Error:" + str(response.status_code))
                        logger.warning("Retrying: " + str(url))
                        await asyncio.sleep(25)
        except httpx.DecodingError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {e}")
            await asyncio.sleep(30)

            continue
        except httpx.RequestError as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {str(e)} {url}")
            await asyncio.sleep(30)

            continue
        except httpx.HTTPStatusError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {str(e)} {url}")
            await asyncio.sleep(30)

            continue
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {str(e)} {url}")
            await asyncio.sleep(30)

            continue

    logger.warning("Failed to resolve: " + str(did) + " after multiple retries.")

    return "Error"


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
            await asyncio.sleep(30)

            continue
        except httpx.RequestError as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {str(e)} {full_url}")
            await asyncio.sleep(30)

            continue
        except httpx.HTTPStatusError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {str(e)} {full_url}")
            await asyncio.sleep(30)

            continue
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {str(e)} {full_url}")
            await asyncio.sleep(30)

            continue

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
            await asyncio.sleep(30)

            continue
        except httpx.RequestError as e:
            retry_count += 1
            logger.error(f"Error occurred while making the API call: {str(e)} {full_url}")
            await asyncio.sleep(30)

            continue
        except httpx.HTTPStatusError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {str(e)} {full_url}")
            await asyncio.sleep(30)

            continue
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {str(e)} {full_url}")
            await asyncio.sleep(30)

            continue

    logger.warning("Failed to resolve: " + str(did) + " after multiple retries.")

    return "Error"
