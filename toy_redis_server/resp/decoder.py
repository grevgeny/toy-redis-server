from typing import Any


class RESPDecoder:
    @staticmethod
    def decode_simple_string(data: bytes) -> str:
        return data.decode("utf-8").rstrip("\r\n")

    @staticmethod
    def decode_bulk_string(data: bytes) -> str | None:
        if data == b"$-1\r\n":
            return None
        _, value, _ = data.split(b"\r\n", 2)
        return value.decode("utf-8")

    @staticmethod
    def decode_array(data: bytes) -> tuple[list[str], Any]:
        n, rest = data.split(b"\r\n", 1)
        num_elements = int(n)
        elements: list[str] = []

        for _ in range(num_elements):
            next_data, rest = RESPDecoder._split_next(rest)
            elements.append(RESPDecoder.decode(next_data))

        return elements, *RESPDecoder.decode(rest)

    @staticmethod
    def decode(raw_data: bytes) -> Any:
        if raw_data == b"":
            return ""

        datatype, encoded_data = raw_data[0:1], raw_data[1:]

        if datatype == b"+":
            return RESPDecoder.decode_simple_string(encoded_data)
        elif datatype == b"$":
            return RESPDecoder.decode_bulk_string(encoded_data)
        elif datatype == b"*":
            return RESPDecoder.decode_array(encoded_data)
        else:
            raise ValueError("Unsupported data format")

    @staticmethod
    def _split_next(data: bytes) -> tuple[bytes, bytes]:
        prefix = data[0:1]
        if prefix == b"+":
            split_point = data.find(b"\r\n") + 2
            return data[:split_point], data[split_point:]
        elif prefix == b"$":
            length_end = data.find(b"\r\n") + 2
            length = int(data[1 : length_end - 2])
            split_point = length_end + length + 2
            return data[:split_point], data[split_point:]
        else:
            raise ValueError("Unsupported data format")
