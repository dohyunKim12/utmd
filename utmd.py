import os
import subprocess
import sys
import json
import signal
import logging
from kafka import KafkaConsumer
from dotenv import dotenv_values

PID_FILE = "/tmp/utmd.pid"
LOG_FILE = "/tmp/utmd.log"
TOPIC_NAME = None
KAFKA_SERVER = None
FOREGROUND = False

def initialize():
    global TOPIC_NAME, KAFKA_SERVER, FOREGROUND

    TOPIC_NAME = os.getenv("TOPIC_NAME", "default")
    KAFKA_SERVER = os.getenv("KAFKA_SERVER", "default")
    FOREGROUND= os.getenv("FOREGROUND", "false").lower() == "true"

    logging.info(f"Foreground mode: {FOREGROUND}")

    # Logging config
    logging.basicConfig(
        filename=LOG_FILE,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def daemonize():
    if os.fork() > 0:
        sys.exit(0)
    os.setsid()
    if os.fork() > 0:
        sys.exit(0)
    sys.stdout.flush()
    sys.stderr.flush()
    with open("/dev/null", "wb", 0) as dev_null:
        os.dup2(dev_null.fileno(), sys.stdin.fileno())
        os.dup2(dev_null.fileno(), sys.stdout.fileno())
        os.dup2(dev_null.fileno(), sys.stderr.fileno())

def write_pid_file():
    if os.path.exists(PID_FILE):
        logging.error("Daemon is already running.")
        sys.exit(1)
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

def remove_pid_file():
    if os.path.exists(PID_FILE):
        os.remove(PID_FILE)

def signal_handler(signum, frame):
    logging.info(f"Received signal {signum}. Shutting down.")
    remove_pid_file()
    sys.exit(0)

def process_message(message):
    global FOREGROUND
    try:
        data = json.loads(message)
        directory = data.get("directory")
        uuid = data.get("uuid")
        command = data.get("command")

        if directory and uuid and command:
            logging.info(f"Processing message: directory={directory}, uuid={uuid}, command={command}")

            # Change Directory
            os.chdir(directory)

            # Read env file
            env_path = f"{directory}/{uuid}.env"
            srun_log_file = f"{directory}/{uuid}.log"
            if os.path.exists(env_path):
                env_vars = dotenv_values(env_path)
            else:
                logging.error(f".env file not found at: {env_path}")
                sys.exit(0)

            logging.info(f"Executing command: {command}")

            if FOREGROUND:
                srun_process = subprocess.Popen(
                    command,
                    shell=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env_vars
                )
                for line in iter(srun_process.stdout.readline, ""):
                    print(line, end="")
                for line in iter(srun_process.stderr.readline, ""):
                    print(line, end="", file=sys.stderr)

                exit_code = srun_process.wait()
                print(f"srun completed with exit code: {exit_code}")
                return exit_code
            else:
                with open(srun_log_file, "a") as log_file:
                    log_file.write(f"Starting command: {command}\n")

                    srun_process = subprocess.Popen(
                        command,
                        shell=True,
                        stdout=log_file,
                        stderr=log_file,
                        text=True,
                        env=env_vars,
                        start_new_session=True
                    )
                    logging.info(f"Started background process with PID: {srun_process.pid}")
                    log_file.write(f"Process started with PID: {srun_process.pid}\n")
                    return srun_process.pid

        else:
            logging.warning(f"Missing required fields in message: {message}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse message: {message}, Error: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

def kafka_consumer():
    logging.info("Starting Kafka consumer...")
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers=[KAFKA_SERVER],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="my-group",
        value_deserializer=lambda x: x.decode("utf-8"),
    )

    for message in consumer:
        logging.info(f"Received message: {message.value}")
        process_message(message.value)

def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    initialize()

    write_pid_file()

    try:
        kafka_consumer()
    finally:
        remove_pid_file()
if __name__ == "__main__":
    if os.getenv("FOREGROUND", "false").lower() == "true":
        logging.info("Running in foreground mode.")
        main()
    else:
        daemonize()
        logging.info("Running in background mode.")
        main()
