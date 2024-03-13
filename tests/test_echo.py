import random
import string

import pytest
from redis import ConnectionError, Redis


def generate_random_word(length: int = 10):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))


def test_echo(master_redis_server):
    host, port = master_redis_server

    try:
        redis_client = Redis()
    except ConnectionError:
        pytest.fail(f"Failed to connect to Redis server at {host}:{port}")

    random_word = generate_random_word()
    response = redis_client.echo(random_word)
    assert (
        response.decode() == random_word
    ), "The ECHO response does not match the sent word"

    redis_client.close()
