# config_helper.py

import os
import platform
import configparser
import logging.config


def read_config():
    config = configparser.ConfigParser()
    if os.path.exists('config.ini'):
        config.read("config.ini")
    else:
        raise FileNotFoundError("Config.ini file does not exist")
    return config


def update_config_based_on_os(config):
    try:
        current_os = platform.platform()

        if "Windows" in current_os:
            args = config.get("windows", "args")
            log_dir = config.get("windows", "logdir")
            log_name = config.get("windows", "log_name")
            users_db = config.get("windows", "users_db_path")
        else:
            args = config.get("linux", "args")
            log_dir = config.get("linux", "logdir")
            log_name = config.get("linux", "log_name")
            users_db = config.get("linux", "users_db_path")

        config.set("handler_fileHandler", "args", str(args))
        config.set("handler_fileHandler", "logdir", str(log_dir))
        config.set("handler_fileHandler", "log_name", str(log_name))
        # config.set("handler_fileHandler", "users_db_path", str(users_db))

        with open('config.ini', 'w') as configfile:
            config.write(configfile)

        configfile.close()
        return log_dir, users_db

    except (configparser.NoOptionError, configparser.NoSectionError, configparser.MissingSectionHeaderError):
        raise


def create_log_directory(log_dir, users_db_path):
    try:
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
        if not os.path.exists(users_db_path):
            os.makedirs(users_db_path)
    except PermissionError:
        raise PermissionError("Cannot create log directory")


def configure_logging():
    try:
        logging.config.fileConfig('config.ini')
        logger = logging.getLogger()

    except Exception as e:
        logging.error(f"An error occurred while configuring logging: {e}")
        raise

    return logger


# Create and configure the logger instance
logger = configure_logging()
