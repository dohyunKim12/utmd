import logging
import os
import yaml

class Config:
    GTM_SERVER_IP = None
    GTM_SERVER_PORT = None
    KAFKA_ADDRESS = None
    TOPIC_NAME = None
    HOME_DIR = None
    PACKAGE_DIR = None
    LOG_LEVEL = None
    SOCKET_PATH = None

    @classmethod
    def load_config(cls, config_path="config.yaml"):
        if os.path.exists(config_path):
            with open(config_path, "r") as file:
                config = yaml.safe_load(file)

            for key, value in config.get("env_vars", {}).items():
                if isinstance(value, str):
                    value = value.format(username=os.getenv("USER", "default_user"))
                os.environ[key] = str(value)

        cls.GTM_SERVER_IP = os.getenv("GTM_SERVER_IP", "default")
        cls.GTM_SERVER_PORT = os.getenv("GTM_SERVER_PORT", "8023")
        cls.KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", "localhost:9092")
        user = os.getenv("USER", "dohyun.kim")
        cls.TOPIC_NAME = "utm-" + user
        cls.HOME_DIR = os.getenv("HOME", "/home/" + user)
        cls.PACKAGE_DIR = os.path.join(cls.HOME_DIR, "utmd")
        cls.LOG_LEVEL = os.getenv("UTMD_LOG_LEVEL", logging.INFO)
        cls.SOCKET_PATH = os.getenv("SOCKET_PATH", '/tmp/utm_uds_' + user)

