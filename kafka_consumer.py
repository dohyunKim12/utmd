import json
import os
import threading
from datetime import datetime, timezone

from dotenv import dotenv_values
from kafka import KafkaConsumer

from config import constants
from config.globals import Globals

from object.taskobject import TaskObject


logger = Globals.logger


def kafka_consumer():
    import utmd
    logger.info("Starting Kafka consumer...")
    consumer = KafkaConsumer(
        constants.TOPIC_NAME,
        bootstrap_servers=constants.KAFKA_ADDRESS,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="utmd",
        value_deserializer=lambda x: x.decode("utf-8"),
    )
    try:
        while Globals.running_flag:
            for message in consumer:
                if message and message.value:
                    logger.info(f"Received message: {message.value}")
                    payload = validate_message(message.value)

                    if payload is None:
                        logger.error("Received an invalid message: Payload is None.")
                        continue

                    if isinstance(payload, TaskObject):
                        thread = threading.Thread(target=utmd.execute_srun, args=(payload,))
                        thread.start()
                    else:
                        logger.error(f"Error occurred in validate_message: {payload}")
    finally:
        logger.info("Closing kafka consumer")
        consumer.close()


def validate_message(message):
    import utmd
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
        utmd.terminate_task(task_id)
        return 0
    elif action == "add" :
        logger.info(f"Validating message: task_id={task_id}, directory={directory}, uuid={uuid}, command={command}")

        # Read env file
        timestamp = int(uuid.split("_")[0])
        dt_utc = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        local_tz = datetime.now().astimezone().tzinfo
        dt_local = dt_utc.astimezone(local_tz)
        date_str = dt_local.strftime("%Y-%m-%d")
        env_path = f"{constants.PACKAGE_DIR}/commands/{date_str}/{uuid}/.env"

        if os.path.exists(env_path):
            env = dotenv_values(env_path)
        else :
            logger.error(f".env file not found at: {env_path}")
            return None
        return TaskObject(task_id, job_id, user, command, uuid, directory, env, date_str, license_type, license_count, timelimit, requested_cpu)
    else :
        logger.error(f"Invalid action type: {action}")
        return None
