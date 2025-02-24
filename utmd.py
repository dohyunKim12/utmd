# ENV GTM_SERVER_IP, GTM_SERVER_PORT, KAFKA_ADDRESS, TOPIC_NAME, HOME needed
import copy
import shutil
import struct
import subprocess
import json
import signal
import logging
import socket
import os
import sys
import threading
import uuid
from datetime import datetime, timezone
import time

import requests
import yaml
import re
from kafka import KafkaConsumer
from dotenv import dotenv_values

from taskobject import TaskObject

logger = None
running_flag = True
srun_task_dict = {}

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

def signal_handler(signum, frame):
    global running_flag
    logger.info(f"Received signal {signum}. Shutting down.")
    for task_id in list(srun_task_dict.keys()):
        terminate_task(task_id)
    running_flag = False
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

def regenerate_uuid(payload):
    # re-gen uuid
    new_timestamp = str(int(time.time()))
    new_uuid = new_timestamp + "_" + uuid.uuid4().hex[:8]

    # create env file (copy env from old_env_file_path)
    timestamp = int(payload.uuid.split("_")[0])
    dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    local_tz = datetime.now().astimezone().tzinfo
    dt_local = dt_utc.astimezone(local_tz)
    date_str = dt_local.strftime("%Y-%m-%d")
    old_env_path = f"{Config.PACKAGE_DIR}/commands/{date_str}/{payload.uuid}/.env"

    new_date_str = datetime.now(tz=local_tz).strftime("%Y-%m-%d")
    new_env_path = f"{Config.PACKAGE_DIR}/commands/{new_date_str}/{new_uuid}/.env"

    try:
        new_env_dir = os.path.dirname(new_env_path)
        os.makedirs(new_env_dir, exist_ok=True)
        shutil.copy(old_env_path, new_env_path)
        logger.info(f"Copied {old_env_path} → {new_env_path}")
    except FileNotFoundError:
        logger.error(f"Error: generate new_payload failed. old_env_path {old_env_path} not found.")
        return

    # re-gen comment in command
    new_comment = f"utm-{new_uuid}"
    pattern = r"--comment\s*=\s*'[^']*'"
    if re.search(pattern, payload.command):
        # replace --comment field (old comment to new comment)
        new_command = re.sub(pattern, f"--comment='{new_comment}'", payload.command)
    else:
        # add new comment if --comment not exists
        new_command = re.sub(r"(\bsrun\b)", r"\1 --comment='{}'".format(new_comment), payload.command, count=1)

    # generate new_payload
    new_payload = copy.deepcopy(payload)
    new_payload.uuid = new_uuid
    new_payload.task_id = None
    new_payload.job_id = None
    new_payload.command = new_command
    logger.info(f"TaskId {payload.task_id}'s uuid updated to {new_payload.uuid}")
    return new_payload

def send_add_request(payload):
    url = f"http://{Config.GTM_SERVER_IP}:{Config.GTM_SERVER_PORT}/api/task/add"
    headers = {"Content-Type": "application/json"}
    data = payload.to_dict()
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

def get_job_state(job_id, max_wait=30, interval=1):
    try:
        waited = 0
        while waited < max_wait:
             result = subprocess.run(
                 ["sacct", "-X", "-j", str(job_id), "--format=State,ExitCode", "--noheader"],
                 capture_output=True, text=True, check=True
             )
             output_list = result.stdout.split()
             job_state = re.sub(r'\+$', '', output_list[0])
             exit_code = int(output_list[1].split(":")[0])
             if job_state not in ["RUNNING", "PENDING", None]:
                 return job_state, exit_code
             time.sleep(interval)
             waited += interval
        return None, None
    except subprocess.CalledProcessError as e:
        logger.error(f"Error getting job state: {e}")
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

def execute_srun(payload: TaskObject):
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
            payload.job_id = job_id

            # call GTM service call(set_job_id) to register job_id & short cmd
            send_set_job_id_request(payload.task_id, payload.user, job_id, short_cmd)

            if srun_process.wait() == 0:
                logger.info(f"Task {payload.task_id}, JobId {payload.job_id} completed successfully.")
            else:
                logger.error(f"Task {payload.task_id}, JobId {payload.job_id} terminated with errors.")
        except Exception as e:
            logger.error(f"Unexpected error in get_job_info: {e}")
        finally:
            with open(srun_log_file_path, "a") as srun_log_file:
                srun_log_file.write("===EOF===\n")
                srun_log_file.flush()

            # execute sacct & get status, call GTM addTask if preempt
            status, exit_code = get_job_state(payload.job_id)
            if status == "COMPLETED" :
                send_finish_request(payload.task_id, payload.user, "completed")
            elif status == "PREEMPTED" :
                new_payload = regenerate_uuid(payload)
                send_finish_request(payload.task_id, payload.user, "preempted")
                send_add_request(new_payload)
            elif status == "CANCELLED" or (status == "FAILED" and exit_code == 0):
                send_finish_request(payload.task_id, payload.user, "cancelled")
            elif status == "FAILED" and exit_code != 0 :
                send_finish_request(payload.task_id, payload.user, "failed")
            else :
                logger.warn(f"Unreachable state {status} in task_id {payload.task_id}")
    except Exception as e:
        logger.error(f"Error occurred in srun execute: {e}")

def terminate_task(task_id):
    process = srun_task_dict.get(task_id)
    if process:
        logger.info(f"Terminating task with ID: {task_id}")
        process.terminate()
        try:
            process.wait(timeout=5)
            logger.info(f"Task {task_id} terminated gracefully.")
        except subprocess.TimeoutExpired:
            logger.warning(f"Task {task_id} did not terminate in time. Forcing kill.")
            process.kill()
            process.wait()
        finally:
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
        job_id = data.get("job_id")
        user = data.get("user")
        directory = data.get("directory")
        uuid = data.get("uuid")
        command = data.get("command")
        action = data.get("action")
        license_type = data.get("license_type")
        license_count = data.get("license_count")
        timelimit = data.get("timelimit")
        requested_cpu = data.get("requested_cpu")
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
        return TaskObject(task_id, job_id, user, command, uuid, directory, env, date_str, license_type, license_count, timelimit, requested_cpu)
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
    try:
        while running_flag:
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
    finally:
        logger.info("Closing kafka consumer")
        consumer.close()


def process_uds_request(request):
    logger.info(f"Received request: {request}")
    if request == "liveness":
        return {"status_code": 200, "message": "Task executed successfully", "data": {"result": True}}
    elif request == "shutdown":
        signal_handler(signal.SIGTERM, None)
        return {"status_code": 200, "message": "Utmd shutdown successfully", "data": {"result": True}}
    elif request == "count_running":
        # count srun_task_dict.keys and save into result
        result = len(srun_task_dict.keys())
        return {"status_code": 200, "message": "Task executed successfully", "data": {"result": result}}
    else:
        return {"status_code": 400, "message": "Invalid request", "data": {"request": request}}


def recv_all(sock, length):
    data = b""
    while len(data) < length:
        more = sock.recv(length - len(data))
        if not more:
            raise EOFError(f"Connection closed while receiving data. {len(data)} byte received.")
        data += more
    return data


def send_response(sock, response_dict):
    response_json = json.dumps(response_dict).encode('utf-8')
    length_prefix = struct.pack('!I', len(response_json))
    sock.sendall(length_prefix + response_json)


def handle_client(conn):
    try:
        raw_length = recv_all(conn, 4)
        request_length = struct.unpack('!I', raw_length)[0]
        request = recv_all(conn, request_length).decode("utf-8")

        logger.info(f"Received client request: {request}")
        response = process_uds_request(request)
        send_response(conn, response)
        logger.info("Send response success.")
    except Exception as e:
        logger.error(f"Error occurred while handling client request: {e}")
    finally:
        conn.close()
        logger.info("Client connection close.")


def bind_socket():
    global running_flag
    logger.info(f"Binding socket to {Config.SOCKET_PATH}")
    if os.path.exists(Config.SOCKET_PATH):
        os.unlink(Config.SOCKET_PATH)

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(Config.SOCKET_PATH)
        server.listen(5)
        logger.info(f"UDS server listening on {Config.SOCKET_PATH}")

        threads = []

        try:
            while running_flag:
                try:
                    client, _ = server.accept()
                    logger.info(f"Client connection accepted")
                    thread = threading.Thread(target=handle_client, args=(client,))
                    thread.start()
                    threads.append(thread)
                except socket.error as e:
                    if running_flag:
                        logger.error(f"Socket error occurred: {e}")
                    else:
                        logger.info("Sigterm detected")
                        break
        finally:
            logger.info("Server terminating: All uds connections waiting for terminate")
            for thread in threads:
                thread.join()
            logger.info("All server & client threads terminated.")


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    initialize()

    consumer_thread = threading.Thread(target=kafka_consumer, daemon=True)
    consumer_thread.start()

    bind_socket()

    consumer_thread.join()

if __name__ == "__main__":
    logging.info("Running in background mode.")
    main()
