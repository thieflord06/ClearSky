# environment.py

from config_helper import config, logger
import os


def get_api_var():
    if not os.getenv('CLEAR_SKY'):
        api_environment = config.get("environment", "api")
        if not api_environment:
            logger.warning("Using default environment.")
            api_environment = "prod"
    else:
        api_environment = os.environ.get("CLEARSKY_ENVIRONMENT")
        if not api_environment:
            logger.warning("Using default environment.")
            api_environment = "prod"

    return api_environment
