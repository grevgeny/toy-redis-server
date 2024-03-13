import socket
import time

import pytest


def test_redis_server_port_binding(master_redis_server):  # type: ignore
    host, port = master_redis_server  # type: ignore
    connected = False
    retries = 0

    while not connected and retries < 5:
        try:
            with socket.create_connection((host, port), timeout=1):  # type: ignore
                print("Connection successful")
                connected = True
        except (ConnectionRefusedError, socket.timeout):
            if retries >= 2:
                print(f"Failed to connect to {host}:{port}, retrying in 1s")
            time.sleep(1)
            retries += 1

    if not connected:
        pytest.fail(f"Unable to connect to {host}:{port} after {retries} retries")
