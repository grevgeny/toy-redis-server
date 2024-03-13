import subprocess
import time

import pytest


@pytest.fixture(scope="package")
def master_redis_server():
    server_process = subprocess.Popen(["python", "-m", "toy_redis_server.main"])

    time.sleep(5)

    yield "localhost", 6379

    server_process.terminate()
    server_process.wait()
