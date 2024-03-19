from typing import Any


class RESPEncoder:
    @staticmethod
    def encode_simple_string(data: str) -> bytes:
        return f"+{data}\r\n".encode()

    @staticmethod
    def encode_integer(data: int) -> bytes:
        return f":{data}\r\n".encode()

    @staticmethod
    def encode_bulk_string(data: str) -> bytes:
        return f"${len(data)}\r\n{data}\r\n".encode()

    @staticmethod
    def encode_null() -> bytes:
        return b"$-1\r\n"

    @staticmethod
    def encode_array(*elements: str | list[Any]) -> bytes:
        encoded_array = f"*{len(elements)}\r\n".encode()

        for element in elements:
            if isinstance(element, list):
                encoded_array += RESPEncoder.encode_array(*element)
            else:
                encoded_array += RESPEncoder.encode_bulk_string(element)

        return encoded_array

    @staticmethod
    def encode_error(error: str) -> bytes:
        return f"-ERR {error}\r\n".encode()
