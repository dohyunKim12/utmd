# ENV GTM_SERVER_IP, GTM_SERVER_PORT, KAFKA_ADDRESS, TOPIC_NAME, HOME needed
import os
import subprocess
import sys
import json
import signal
import logging
import threading
from datetime import datetime, timezone
import time

import requests
import yaml
from kafka import KafkaConsumer
from dotenv import dotenv_values

from taskobject import TaskObject

logger = None
srun_task_dict = {}

class Config:
    GTM_SERVER_IP = None
    GTM_SERVER_PORT = None
    KAFKA_ADDRESS = None
    TOPIC_NAME = None
    HOME_DIR = None
    PACKAGE_DIR = None
    LOG_LEVEL = None

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
        cls.TOPIC_NAME = os.getenv("TOPIC_NAME", "default")
        cls.HOME_DIR = os.getenv("HOME", "/home/default")
        cls.PACKAGE_DIR = os.path.join(cls.HOME_DIR, "utmd")
        cls.LOG_LEVEL = os.getenv("UTMD_LOG_LEVEL", logging.INFO)

def initialize():
    global logger

    Config.load_config()

    log_dir = os.path.join(Config.PACKAGE_DIR, "log")
    os.makedirs(log_dir, exist_ok=True)

    # Setup logger
    log_file = os.path.join(log_dir, "utmd.log")
    logger = logging.getLogger()
    logger.setLevel(Config.LOG_LEVEL)

    # Remove previous handlers
    while logger.hasHandlers():
        logger.removeHandler(logger.handlers[0])

    formatter = logging.Formatter(
        "[%(asctime)s|%(levelname)-7s][%(name)s %(lineno)d %(funcName)s] %(message)s ",
        datefmt='%Y-%m-%d %H:%M:%S')

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.info("Logger initialized.")
    logger.info("Utmd initialized and started successfully.")

def signal_handler(signum):
    logger.info(f"Received signal {signum}. Shutting down.")
    sys.exit(0)

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

def send_complete_request(task_id, user):
    url = f"http://{Config.GTM_SERVER_IP}:{Config.GTM_SERVER_PORT}/api/task/complete"
    headers = {"Content-Type": "application/json"}
    data = {
        "task_id": task_id,
        "user": user
    }
    http_post_request(url, data, headers)

def get_job_info(comment_value, max_retries, interval):
    for attempt in range(max_retries):
        result = subprocess.run(["scontrol", "show", "job", "--json"], capture_output=True, text=True) # sync call
        try:
            jobs = json.loads(result.stdout)
            for job in jobs["jobs"]:
                if job.get("comment") == comment_value:
                    job_id = str(job["job_id"])
                    short_cmd = job["name"]
                    return job_id, short_cmd

        except (json.JSONDecodeError, KeyError):
            logger.error("Failed to parse Slurm JSON output")

        logger.info(f"[{attempt+1}/{max_retries}] Cannot find job info. Retry after {interval} sec...")
        time.sleep(interval)

    logger.warn(f"Cannot find job in slurm for comment: {comment_value}")
    return None, None

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

def execute_srun(payload):
    try:
        # Change Directory
        if not os.path.exists(payload.directory):
            logger.error(f"Directory does not exists: {payload.directory}")
            return
        os.chdir(payload.directory)
        logger.info(f"Executing command: `{payload.command}` in directory: {payload.directory}")

        srun_log_file_path = os.path.join(Config.PACKAGE_DIR, "commands", payload.date_str , payload.uuid, "srun.log")
        os.makedirs(os.path.dirname(srun_log_file_path), exist_ok=True)

        with open(srun_log_file_path, "a") as srun_log_file:
            srun_log_file.write(f"Starting command: {payload.command}\n")
            srun_log_file.flush()

            srun_process = subprocess.Popen(
                payload.command,
                shell=True,
                stdout=srun_log_file,
                stderr=srun_log_file,
                stdin=subprocess.DEVNULL,
                text=True,
                env=payload.env,
                start_new_session=True
            )
            logger.info(f"Started background process with PID: {srun_process.pid}")
            srun_task_dict[payload.task_id] = srun_process

        try:
            # execute scontrol command for get job_id, short_cmd by comment (utm-uuid)
            interval = 0.3
            time.sleep(interval)
            job_id, short_cmd = get_job_info("utm-" + payload.uuid, 10, interval)

            # call GTM service call(set_job_id) to register job_id & short cmd
            send_set_job_id_request(payload.task_id, payload.user, job_id, short_cmd)

            if srun_process.wait() == 0:
                logger.info(f"Process with PID {srun_process.pid} completed successfully.")
            else:
                logger.error(f"Process with PID {srun_process.pid} terminated with errors.")
        except Exception as e:
            logger.error(f"Unexpected error in get_set_job_id: {e}")
        finally:
            with open(srun_log_file_path, "a") as srun_log_file:
                srun_log_file.write("===EOF===\n")
                srun_log_file.flush()
            send_complete_request(payload.task_id, payload.user)
    except Exception as e:
        logger.error(f"Error occurred in srun execute: {e}")

def terminate_task(task_id):
    process = srun_task_dict.get(task_id)
    if process:
        logger.info(f"Terminating task with ID: {task_id}")
        process.terminate()
        srun_task_dict.pop(task_id, None)
    else:
        logger.warning(f"No running task found with ID: {task_id}")

def validate_message(message):
    try:
        data = json.loads(message)

        required_fields = ["id", "user", "directory", "uuid", "command", "action"]
        if not all(field in data for field in required_fields):
            logger.error(f"Missing required fields in message: {message}")
            return None
        task_id = data.get("id")
        user = data.get("user")
        directory = data.get("directory")
        uuid = data.get("uuid")
        command = data.get("command")
        action = data.get("action")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message: {message}, Error: {e}")
        return None

    if action == "cancel" :
        logger.info(f"Terminating task_id: {task_id}, command: {command}")
        terminate_task(task_id)
        return 0
    elif action == "add" :
        logger.info(f"Validating message: task_id={task_id}, directory={directory}, uuid={uuid}, command={command}")

        # Read env file
        timestamp = int(uuid.split("_")[0])
        dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        local_tz = datetime.now().astimezone().tzinfo
        dt_local = dt_utc.astimezone(local_tz)
        date_str = dt_local.strftime("%Y-%m-%d")
        env_path = f"{Config.PACKAGE_DIR}/commands/{date_str}/{uuid}/.env"

        if os.path.exists(env_path):
            env = dotenv_values(env_path)
        else :
            logger.error(f".env file not found at: {env_path}")
            return None
        return TaskObject(task_id, user, command, uuid, directory, env, date_str)
    else :
        logger.error(f"Invalid action type: {action}")
        return None

def kafka_consumer():
    logger.info("Starting Kafka consumer...")
    consumer = KafkaConsumer(
        Config.TOPIC_NAME,
        bootstrap_servers=Config.KAFKA_ADDRESS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="utmd",
        value_deserializer=lambda x: x.decode("utf-8"),
    )
    for message in consumer:
        if message and message.value:
            logger.info(f"Received message: {message.value}")
            payload = validate_message(message.value)

            if payload is None:
                logger.error("Received an invalid message: Payload is None.")
                continue

            if isinstance(payload, TaskObject):
                thread = threading.Thread(target=execute_srun, args=(payload,))
                thread.start()
            else:
                logger.error(f"Error occurred in validate_message: {payload}")

def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    initialize()

    kafka_consumer()

if __name__ == "__main__":
    logging.info("Running in background mode.")
    main()
