import requests

from config.config import Config
from config.logger import get_logger

logger = get_logger()


def http_post_request(url: str, data: dict, headers: dict = None):
    if headers is None:
        headers = {"Content-Type": "application/json"}

    try:
        response = requests.post(url, json=data, headers=headers)
        response.raise_for_status()

        logger.info(f"POST request success: {url}, response: {response.json()}")
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"POST request failed: {url}, error: {e}")
        return None


def send_add_request(payload):
    url = f"http://{Config.GTM_SERVER_IP}:{Config.GTM_SERVER_PORT}/api/task/add"
    headers = {"Content-Type": "application/json"}
    data = payload.to_dict()
    http_post_request(url, data, headers)


def send_finish_request(task_id, user, state):
    logger.info(f"Send finish request for task: {task_id}, state: {state}")
    url = f"http://{Config.GTM_SERVER_IP}:{Config.GTM_SERVER_PORT}/api/task/finish"
    headers = {"Content-Type": "application/json"}
    data = {
        "task_id": task_id,
        "user": user,
        "state": state
    }
    http_post_request(url, data, headers)


def send_set_job_id_request(task_id, user, job_id, short_cmd):
    url = f"http://{Config.GTM_SERVER_IP}:{Config.GTM_SERVER_PORT}/api/task/set_job_id"
    headers = {"Content-Type": "application/json"}
    data = {
        "task_id": task_id,
        "user": user,
        "job_id": job_id,
        "short_cmd": short_cmd
    }
    http_post_request(url, data, headers)