import asyncio
import datetime
from typing import Any

HOST, PORT = "", 6379

PONG = b"+PONG\r\n"
OK = b"+OK\r\n"
NULL = b"$-1\r\n"

DATABASE: dict[str, tuple[Any, datetime.datetime | None]] = {}


async def extract_command(data: bytes) -> list[str]:
    decoded_data: str = data.decode()
    command = [arg for arg in decoded_data.split("\r\n")[:-1] if arg[0] not in "*$"]
    return command


async def handle_connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info("peername")
    print("Connected by", addr)
    while True:
        # Receive
        try:
            data = await reader.read(1024)
        except ConnectionError:
            print(f"Client suddenly closed while receiving from {addr}")
            break
        if not data:
            break

        # Parse the data and extract command
        command = await extract_command(data)

        # Respond based on command recieved
        match command:
            case ["ping"]:
                response = PONG
            case ["echo", data]:
                response = f"+{data}\r\n".encode()
            case ["set", key, value]:
                DATABASE[key] = (value, None)
                response = OK
            case ["set", key, value, _, time]:
                expiry = datetime.datetime.now(datetime.UTC) + datetime.timedelta(
                    milliseconds=int(time)
                )
                DATABASE[key] = (value, expiry)
                response = OK
            case ["get", key]:
                value, expiry = DATABASE.get(key, (None, None))
                if expiry and expiry < datetime.datetime.now(datetime.UTC):
                    DATABASE.pop(key)
                    response = NULL
                else:
                    response = f"+{value}\r\n".encode() if value else NULL
            case _:
                response = f"-ERR unknown command '{command[0]}'\r\n".encode()

        try:
            writer.write(response)
        except ConnectionError:
            print("Client suddenly closed, cannot send")
            break
    writer.close()
    print("Disconnected by", addr)


async def main(host, port):
    server = await asyncio.start_server(handle_connection, host, port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main(HOST, PORT))
