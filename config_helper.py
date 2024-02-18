# config_helper.py

import os
import platform
import configparser
import logging.config
import sys
from aiolimiter import AsyncLimiter

ini_file = "config.ini"

rate_limit = 2500  # Requests per minute
time_interval = 300  # 60 seconds = 1 minute
limiter = AsyncLimiter(rate_limit, time_interval)


def remove_file_handler_from_config(config_file_path):
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

    # Save the modified config to the same file
    with open(config_file_path, 'w') as config_file:
        configure.write(config_file)

    print("removed file handler.")
    print("Console logging only.")


def read_config():
    config = configparser.ConfigParser()

    if os.path.exists(ini_file):
        config.read(ini_file)
    else:
        print(f"Config.ini file does not exist\nPlace config.ini in: {str(os.getcwd())} \nRe-run program")
        sys.exit()

    return config


def update_config_based_on_os(config, temp=False):
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

        config.set("handler_fileHandler", "args", str(args))
        config.set("handler_fileHandler", "logdir", str(log_dir))
        config.set("handler_fileHandler", "log_name", str(log_name))

        with open(ini_file, 'w') as configfile:
            config.write(configfile)
            configfile.close()

        return log_dir

    except (configparser.NoOptionError, configparser.NoSectionError, configparser.MissingSectionHeaderError):
        raise


def create_log_directory(log_dir, configer):
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


def configure_logging():
    try:
        logging.config.fileConfig(ini_file)
        logger = logging.getLogger()

        return logger

    except Exception as e:
        logging.error(f"An error occurred while configuring logging: {e} {type(e)}")
        raise


# Set up log files and directories for entire project from config.ini
config = read_config()
log_dir = update_config_based_on_os(config)

if "True" in read_config().get("log_option", "console_only"):
    remove_file_handler_from_config(ini_file)
else:
    create_log_directory(log_dir, config)

# Create and configure the logger instance
logger = configure_logging()
