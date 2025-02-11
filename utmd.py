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
from kafka import KafkaConsumer
from dotenv import dotenv_values

from taskobject import TaskObject

GTM_SERVER_IP = None
GTM_SERVER_PORT = None
KAFKA_ADDRESS = None
TOPIC_NAME = None
HOME_DIR = None
PACKAGE_DIR = None
LOG_LEVEL = None
srun_task_dict = {}
logger = None

def initialize():
    global logger
    global GTM_SERVER_IP, GTM_SERVER_PORT, KAFKA_ADDRESS, TOPIC_NAME, HOME_DIR, PACKAGE_DIR, LOG_LEVEL

    GTM_SERVER_IP = os.getenv("GTM_SERVER_IP", "default")
    GTM_SERVER_PORT = os.getenv("GTM_SERVER_PORT", "8023")
    KAFKA_ADDRESS = os.getenv("KAFKA_ADDRESS", "localhost:9092")
    TOPIC_NAME = os.getenv("TOPIC_NAME", "default")
    HOME_DIR = os.getenv("HOME")
    LOG_LEVEL = os.getenv("UTMD_LOG_LEVEL", logging.INFO)
    PACKAGE_DIR = HOME_DIR + "/utmd"

    log_dir = os.path.join(PACKAGE_DIR, "log")
    os.makedirs(log_dir, exist_ok=True)

    # Setup logger
    log_file = os.path.join(log_dir, "utmd.log")
    logger = logging.getLogger()
    logger.setLevel(LOG_LEVEL)

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


def signal_handler(signum, frame):
    logger.info(f"Received signal {signum}. Shutting down.")
    sys.exit(0)

def send_complete_request(task_id, user):
    url = f"http://{GTM_SERVER_IP}:{GTM_SERVER_PORT}/api/task/complete"
    headers = {"Content-Type": "application/json"}
    data = {
        "task_id": task_id,
        "user": user
    }
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Complete request successfully send: {response.json()}")
        else:
            logger.error(f"Complete request failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error occurred in send complete request: {e}")

def execute_srun(payload):
    try:
        # Change Directory
        os.chdir(payload.directory)
        logger.info(f"Executing command: `{payload.command}` in directory: {payload.directory}")

        srun_log_file_path = os.path.join(PACKAGE_DIR, "commands", payload.date_str , payload.uuid, "srun.log")
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
            set_job_id(payload.task_id, payload.user, job_id, short_cmd)

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
            # Send HTTP complete request
            send_complete_request(payload.task_id, payload.user)
    except Exception as e:
        logger.error(f"Error occurred in srun execute: {e}")


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

def set_job_id(task_id, user, job_id, short_cmd):
    url = f"http://{GTM_SERVER_IP}:{GTM_SERVER_PORT}/api/task/set_job_id"
    headers = {"Content-Type": "application/json"}
    data = {
        "task_id": task_id,
        "user": user,
        "job_id": job_id,
        "short_cmd": short_cmd
    }
    try:
        response = requests.post(url, json=data, headers=headers)
        if response.status_code >= 200 and response.status_code < 300:
            logger.info(f"Set job_id request successfully send: {response.json()}")
        else:
            logger.error(f"Set job_id request failed: {response.status_code}, {response.text}")
    except requests.exceptions.RequestException as e:
        logger.error(f"Error occurred in send complete request: {e}")

def terminate_task(task_id):
    process = srun_task_dict.get(task_id)
    if process:
        logger.info(f"Terminating task with ID: {task_id}")
        process.terminate()
        srun_task_dict.pop(task_id, None)
    else:
        logger.warning(f"No running task found with ID: {task_id}")

def kill_task(task_id):
    process = srun_task_dict.get(task_id)
    if process:
        logger.info(f"Killing task with ID: {task_id}")
        process.kill()
        srun_task_dict.pop(task_id, None)
    else:
        logger.warning(f"No running task found with ID: {task_id}")

def validate_message(message):
    try:
        data = json.loads(message)
        task_id = data.get("id")
        user = data.get("user")
        directory = data.get("directory")
        uuid = data.get("uuid")
        command = data.get("command")
        action = data.get("action")
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse message: {message}, Error: {e}")

    if action and task_id and directory and uuid and command:
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
            env_path = f"{PACKAGE_DIR}/commands/{date_str}/{uuid}/.env"

            if os.path.exists(env_path):
                env = dotenv_values(env_path)
            else :
                logger.error(f".env file not found at: {env_path}")
                return 1
            return TaskObject(task_id, user, command, uuid, directory, env, date_str)
        else :
            raise Exception("Parsing Exception in action")
    else:
        logger.error("Message validation failed. Unknown action/id/directory/uuid/command")
        return 1

def kafka_consumer():
    logger.info("Starting Kafka consumer...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=KAFKA_ADDRESS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="my-group",
        value_deserializer=lambda x: x.decode("utf-8"),
    )
    for message in consumer:
        logger.info(f"Received message: {message.value}")
        payload = validate_message(message.value)
        if isinstance(payload, TaskObject):
            thread = threading.Thread(target=execute_srun, args=(payload,))
            thread.start()
        else:
            if not payload == 0:
                logger.error("Error occurred in validate_message")

def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    initialize()

    kafka_consumer()

if __name__ == "__main__":
    logging.info("Running in background mode.")
    main()
