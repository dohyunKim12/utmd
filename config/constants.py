import os

_USER = os.getenv("USER", "default_user")
# _HOME_DIR = os.getenv("HOME", f"/home/{_USER}")
_HOME_DIR = os.getenv("HOME", f"/home/root")
_LOG_LEVEL = os.getenv("UTMD_LOG_LEVEL", "INFO")
# _SOCKET_PATH = os.getenv("SOCKET_PATH", f"/tmp/utm_uds_{_USER}")
_SOCKET_PATH = os.getenv("SOCKET_PATH", f"/tmp/tm_uds")

_GTM_SERVER_IP = os.getenv("GTM_SERVER_IP", "127.0.0.1")
_GTM_SERVER_PORT = os.getenv("GTM_SERVER_PORT", "8023")
_KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", "localhost:9092")
# _TOPIC_NAME = f"utm-{_USER}"
_TOPIC_NAME = "tm"

# Constants var
HOME_DIR = _HOME_DIR
PACKAGE_DIR = os.path.join(HOME_DIR, "utmd")
SOCKET_PATH = _SOCKET_PATH
LOG_LEVEL = _LOG_LEVEL

GTM_SERVER_IP = _GTM_SERVER_IP
GTM_SERVER_PORT = _GTM_SERVER_PORT
KAFKA_ADDRESS = _KAFKA_ADDRESS
TOPIC_NAME = _TOPIC_NAME

