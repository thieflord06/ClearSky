# environment.py

from config_helper import config
import os

if not os.getenv('CLEAR_SKY'):
    api_environment = config.get("environment", "api")
    if not api_environment:
        api_environment = "prod"
else:
    api_environment = os.environ.get("CLEARSKY_ENVIRONMENT")
    if not api_environment:
        api_environment = "prod"
