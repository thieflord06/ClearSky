# helpers.py

import os
import uuid
import asyncio
from quart import request
from datetime import datetime
from config_helper import logger, config

# ======================================================================================================================
# ============================================== global variables ======================================================
blocklist_24_failed = asyncio.Event()
blocklist_failed = asyncio.Event()
runtime = datetime.now()
version = "3.33.5"
default_push_server = "https://ui.staging.clearsky.app"


# ======================================================================================================================
# ================================================ Main functions ======================================================
async def get_ip() -> str:  # Get IP address of session request
    if 'X-Forwarded-For' in request.headers:
        # Get the client's IP address from the X-Forwarded-For header
        ip = request.headers.get('X-Forwarded-For')
        # The client's IP address may contain multiple comma-separated values
        # Extract the first IP address from the list
        ip = ip.split(',')[0].strip()
    else:
        # Use the remote address if the X-Forwarded-For header is not available
        ip = request.remote_addr

    return ip


async def get_time_since(time) -> str:
    if time is None:
        return "Not initialized"
    time_difference = datetime.now() - time

    minutes = int((time_difference.total_seconds() / 60))
    hours = minutes // 60
    remaining_minutes = minutes % 60

    if hours > 0 and remaining_minutes > 0:
        if hours == 1:
            elapsed_time = f"{int(hours)} hour {int(remaining_minutes)} minutes ago"
        else:
            elapsed_time = f"{int(hours)} hours {int(remaining_minutes)} minutes ago"
    elif hours > 0:
        if hours == 1:
            elapsed_time = f"{int(hours)} hour ago"
        else:
            elapsed_time = f"{int(hours)} hours ago"
    elif minutes > 0:
        if minutes == 1:
            elapsed_time = f"{int(minutes)} minute ago"
        else:
            elapsed_time = f"{int(minutes)} minutes ago"
    else:
        elapsed_time = "less than a minute ago"

    return elapsed_time


async def get_ip_address():
    if not os.environ.get('CLEAR_SKY'):
        logger.info("IP connection: Using config.ini")
        ip_address = config.get("server", "ip")
        port_address = config.get("server", "port")

        return ip_address, port_address
    else:
        logger.info("IP connection: Using environment variables.")
        ip_address = os.environ.get('CLEAR_SKY_IP')
        port_address = os.environ.get('CLEAR_SKY_PORT')

        return ip_address, port_address


async def get_var_info() -> dict[str, str]:
    config_api_key = config.get("environment", "api_key")
    config_self_server = config.get("environment", "self_server")

    if not os.getenv('CLEAR_SKY'):
        push_server = config.get("environment", "push_server")
        api_key = config.get("environment", "api_key")
        self_server = config.get("environment", "self_server")
    else:
        push_server = os.environ.get("CLEARSKY_PUSH_SERVER")
        api_key = os.environ.get("CLEARSKY_API_KEY")
        self_server = os.environ.get("CLEARSKY_SELF_SERVER")

    if not api_key:
        logger.error(f"No API key configured, attempting to use config file: {config_api_key}")
        api_key = config_api_key

    if not push_server:
        logger.error(f"No push server configured, using default push server: {default_push_server}")
        push_server = default_push_server

    if not self_server:
        logger.error(f"No self server configured, attempting to use config file: {config_self_server}")
        self_server = config_self_server

    values = {
        "api_key": api_key,
        "push_server": push_server,
        "self_server": self_server
    }

    return values


def generate_session_number() -> str:
    return str(uuid.uuid4().hex)
