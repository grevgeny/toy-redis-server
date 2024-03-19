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
    def encode_array(*elements: str) -> bytes:
        encoded_elements = [f"${len(element)}\r\n{element}\r\n" for element in elements]
        array_length = len(elements)
        encoded_array = f"*{array_length}\r\n{''.join(encoded_elements)}".encode()

        return encoded_array

    @staticmethod
    def encode_error(error: str) -> bytes:
        return f"-ERR {error}\r\n".encode()
