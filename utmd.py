# ENV GTM_SERVER_IP, GTM_SERVER_PORT, KAFKA_ADDRESS, TOPIC_NAME, HOME needed
import copy
import shutil
import subprocess
import json
import signal
import logging
import os
import sys
import threading
import uuid
from datetime import datetime, timezone
import time
import re
from object.taskobject import TaskObject
from config.globals import Globals
from config.logger import initialize_logger

initialize_logger()
logger = Globals.logger

# Must be imported after Config.load_config()
from util.http_utils import *
from kafka_consumer import kafka_consumer
from uds_server import bind_socket


def signal_handler(signum, frame):
    global running_flag
    logger.info(f"Received signal {signum}. Shutting down.")
    for task_id in list(Globals.srun_task_dict.keys()):
        logger.info(f"Terminating task with ID: {task_id}")
        terminate_task(task_id)
        send_finish_request(task_id, 'root', "cancelled")
    running_flag = False
    sys.exit(0)


def regenerate_payload(payload):
    # re-gen uuid
    new_timestamp = str(int(time.time()))
    new_uuid = new_timestamp + "_" + uuid.uuid4().hex[:8]

    # create env file (copy env from old_env_file_path)
    timestamp = int(payload.uuid.split("_")[0])
    dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    local_tz = datetime.now().astimezone().tzinfo
    dt_local = dt_utc.astimezone(local_tz)
    date_str = dt_local.strftime("%Y-%m-%d")
    old_env_path = f"{constants.PACKAGE_DIR}/commands/{date_str}/{payload.uuid}/.env"

    new_date_str = datetime.now(tz=local_tz).strftime("%Y-%m-%d")
    new_env_path = f"{constants.PACKAGE_DIR}/commands/{new_date_str}/{new_uuid}/.env"

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


def get_job_info(comment_value, max_retries, interval):
    logger.info(f"Searching for job with comment: {comment_value}")
    for attempt in range(max_retries):
        result = subprocess.run(["scontrol", "show", "job", "--json"], capture_output=True, text=True) # sync call
        try:
            jobs = json.loads(result.stdout)
            for job in jobs["jobs"]:
                if job.get("comment") == comment_value:
                    job_id = str(job["job_id"])
                    short_cmd = job["name"]
                    logger.info(f"Found job with ID: {job_id}, short_cmd: {short_cmd}, comment: {comment_value}")
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


def execute_srun(payload: TaskObject):
    try:
        # Change Directory
        if not os.path.exists(payload.directory):
            logger.error(f"Directory does not exists: {payload.directory}")
            return
        os.chdir(payload.directory)
        logger.info(f"Executing command: `{payload.command}` in directory: {payload.directory}")

        srun_log_file_path = os.path.join(constants.PACKAGE_DIR, "commands", payload.date_str, payload.uuid, "srun.log")
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
            Globals.srun_task_dict[payload.task_id] = srun_process

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
            logger.error(f"Unexpected error in get_job_info & send_set_job_id: {e}")
        finally:
            with open(srun_log_file_path, "a") as srun_log_file:
                srun_log_file.write("===EOF===\n")
                srun_log_file.flush()
            Globals.srun_task_dict.pop(payload.task_id, None)

            # execute sacct & get status, call GTM addTask if preempt
            status, exit_code = get_job_state(payload.job_id)
            if status == "COMPLETED" :
                send_finish_request(payload.task_id, payload.user, "completed")
            elif status == "PREEMPTED" :
                new_payload = regenerate_payload(payload)
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
    process = Globals.srun_task_dict.get(task_id)
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
            Globals.srun_task_dict.pop(task_id, None)
    else:
        logger.warning(f"No running task found with ID: {task_id}")


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    consumer_thread = threading.Thread(target=kafka_consumer, daemon=True)
    consumer_thread.start()

    bind_socket()

    consumer_thread.join()

if __name__ == "__main__":
    logging.info("Running in background mode.")
    main()
