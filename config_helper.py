# config_helper.py

import os
import platform
import configparser
import logging.config
import sys
from aiolimiter import AsyncLimiter

ini_file = "config.ini"
upload_limit_mb = 1
rate_limit = 2500  # Requests per minute
time_interval = 300  # 60 seconds = 1 minute
limiter = AsyncLimiter(rate_limit, time_interval)


def remove_file_handler_from_config(config_file_path: str) -> None:
    configure = configparser.ConfigParser()
    configure.read(config_file_path)

    # Check if 'fileHandler' exists in the [handlers] section
    if 'fileHandler' in configure['handlers']['keys']:
        # Remove 'fileHandler' from the list of handlers
        handlers = configure['handlers']['keys'].split(',')
        if 'fileHandler' in handlers:
            handlers.remove('fileHandler')
        configure['handlers']['keys'] = ','.join(handlers)

    if 'fileHandler' in configure['logger_root']['handlers']:
        # Remove 'fileHandler' from the list of handlers
        handlers = configure['logger_root']['handlers'].split(',')
        if 'fileHandler' in handlers:
            handlers.remove('fileHandler')
        configure['logger_root']['handlers'] = ','.join(handlers)

    if 'fileHandler' in configure['logger_httpxLogger']['handlers']:
        # Remove 'fileHandler' from the list of handlers
        handlers = configure['logger_httpxLogger']['handlers'].split(',')
        if 'fileHandler' in handlers:
            handlers.remove('fileHandler')
        configure['logger_httpxLogger']['handlers'] = ','.join(handlers)

    # Save the modified config to the same file
    with open(config_file_path, 'w') as config_file:
        configure.write(config_file)

    print("removed file handler.")
    print("Console logging only.")


def read_config() -> configparser.ConfigParser:
    config = configparser.ConfigParser()

    if os.path.exists(ini_file):
        config.read(ini_file)
    else:
        print(f"Config.ini file does not exist\nPlace config.ini in: {str(os.getcwd())} \nRe-run program")
        sys.exit()

    return config


def update_config_based_on_os(config: configparser.ConfigParser, temp: bool = False) -> str:
    args = None
    log_dir = None
    log_name = None

    try:
        current_os = platform.platform()

        if temp:
            if "Windows" not in current_os:
                log_dir = config.get('temp', 'logdir')
                if not os.path.exists(log_dir):
                    os.makedirs(log_dir)

                args = config.get("temp", "args")
                log_dir = config.get("temp", "logdir")
                log_name = config.get("temp", "log_name")
        elif "Windows" in current_os:
            args = config.get("windows", "args")
            log_dir = config.get("windows", "logdir")
            log_name = config.get("windows", "log_name")
        else:
            args = config.get("linux", "args")
            log_dir = config.get("linux", "logdir")
            log_name = config.get("linux", "log_name")

        if args and log_dir and log_name:
            config.set("handler_fileHandler", "args", str(args))
            config.set("handler_fileHandler", "logdir", str(log_dir))
            config.set("handler_fileHandler", "log_name", str(log_name))

        with open(ini_file, 'w') as configfile:
            config.write(configfile)
            configfile.close()

        return log_dir

    except (configparser.NoOptionError, configparser.NoSectionError, configparser.MissingSectionHeaderError):
        raise


def create_log_directory(log_dir: str, configer: configparser.ConfigParser) -> None:
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
    except PermissionError:
        print("Cannot create log directory")

        # Remove 'fileHandler' from the 'handlers' key value
        handlers_value = configer['logger_root']['handlers']
        updated_handlers = [handler.strip() for handler in handlers_value.split(',') if
                            handler.strip() != 'fileHandler']
        configer['logger_root']['handlers'] = ','.join(updated_handlers)

        handlers_key_value = configer['handlers']['keys']
        updated_handlers_key = [handler.strip() for handler in handlers_key_value.split(',') if
                                handler.strip() != 'fileHandler']
        configer['handlers']['keys'] = ','.join(updated_handlers_key)

        httpx_handlers_value = configer['logger_httpxLogger']['handlers']
        updated_httpx_handlers = [handler.strip() for handler in httpx_handlers_value.split(',') if
                                  handler.strip() != 'fileHandler']
        configer['logger_httpxLogger']['handlers'] = ','.join(updated_httpx_handlers)

        # Save the updated config to the file
        with open(ini_file, 'w') as configfile:
            configer.write(configfile)

        print("PermissionError: Logging to file disabled due to lack of write permission.")
    except OSError:
        config = read_config()
        current_os = platform.platform()
        if "Windows" not in current_os:
            update_config_based_on_os(read_config(), True)
            log_dir = config.get('temp', 'logdir')
            print("Using temp for logging.")
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)


def configure_logging() -> logging.Logger:
    try:
        logging.config.fileConfig(ini_file)
        logger = logging.getLogger()

        # Set log level for httpx logger
        httpx_logger = logging.getLogger('httpx')
        httpx_level = logging.getLevelName(config.get('logger_httpxLogger', 'level'))
        httpx_logger.setLevel(httpx_level)

        return logger

    except Exception as e:
        logging.error(f"An error occurred while configuring logging: {e} {type(e)}")
        raise


def check_override():
    config_file = read_config()
    try:
        override = config_file.get("override", "override").lower()
        if override == "true":
            override = True
            print(f"Override is set, using config file variables.")
        else:
            print("Override not set.")
            override = False
    except Exception:
        override = False

    return override


# Set up log files and directories for entire project from config.ini
config = read_config()
log_dir = update_config_based_on_os(config)

if "True" in read_config().get("log_option", "console_only"):
    remove_file_handler_from_config(ini_file)
else:
    create_log_directory(log_dir, config)

# Create and configure the logger instance
logger = configure_logging()
