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
            logger.error(f"Error occurred while making the API call: {e}")
            await asyncio.sleep(30)
            continue
        except httpx.HTTPStatusError as e:
            retry_count += 1
            logger.error(f"Error occurred while parsing JSON response: {e}")
            await asyncio.sleep(30)
            continue
        except Exception as e:
            retry_count += 1
            logger.error(f"An unexpected error occurred: {e}")
            await asyncio.sleep(30)
            continue

    logger.warning("Failed to resolve: " + str(did) + " after multiple retries.")
    return "Error"
