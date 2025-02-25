import json
import os
import signal
import struct
import threading
import socket

from config.config import Config
from config.logger import get_logger


logger = get_logger()


def process_uds_request(request):
    import utmd
    logger.info(f"Received request: {request}")
    if request == "liveness":
        return {"status_code": 200, "message": "Task executed successfully", "data": {"result": True}}
    elif request == "shutdown":
        utmd.signal_handler(signal.SIGTERM, None)
        return {"status_code": 200, "message": "Utmd shutdown successfully", "data": {"result": True}}
    elif request == "count_running":
        # count srun_task_dict.keys and save into result
        result = len(utmd.get_srun_task_dict().keys())
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
    import utmd
    logger.info(f"Binding socket to {Config.SOCKET_PATH}")
    if os.path.exists(Config.SOCKET_PATH):
        os.unlink(Config.SOCKET_PATH)

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(Config.SOCKET_PATH)
        server.listen(5)
        logger.info(f"UDS server listening on {Config.SOCKET_PATH}")

        threads = []

        try:
            while utmd.get_running_flag():
                try:
                    client, _ = server.accept()
                    logger.info(f"Client connection accepted")
                    thread = threading.Thread(target=handle_client, args=(client,))
                    thread.start()
                    threads.append(thread)
                except socket.error as e:
                    if utmd.get_running_flag():
                        logger.error(f"Socket error occurred: {e}")
                    else:
                        logger.info("Sigterm detected")
                        break
        finally:
            logger.info("Server terminating: All uds connections waiting for terminate")
            for thread in threads:
                thread.join()
            logger.info("All server & client threads terminated.")