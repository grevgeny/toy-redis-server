import datetime
import struct
from typing import BinaryIO

from app.rdb.constants import (
    FORMAT_MAPPING,
    LengthEncoding,
    OpCode,
    StringEncoding,
    Type,
)


# Helper functions
def read(file: BinaryIO, data_type: str):
    fmt = FORMAT_MAPPING.get(data_type)
    if not fmt:
        raise ValueError(f"Unsupported data type: {data_type}")
    size = struct.calcsize(fmt)
    return struct.unpack(fmt, file.read(size))[0]


def to_datetime(usecs_since_epoch: int) -> datetime.datetime:
    epoch = datetime.datetime.utcfromtimestamp(0)
    return epoch + datetime.timedelta(microseconds=usecs_since_epoch)


class RDBParser:
    def __init__(self) -> None:
        self.data = {}

    @classmethod
    def load_from_file(cls, filepath: str) -> dict:
        with open(filepath, "rb") as file:
            parser = cls()
            parser.parse(file)

        return parser.data

    def parse(self, file: BinaryIO) -> None:
        self.parse_magic_string(file)
        self.parse_version(file)
        self.parse_contents(file)

    def parse_magic_string(self, file: BinaryIO) -> None:
        magic_string = file.read(5)
        if magic_string != b"REDIS":
            raise ValueError("Invalid RDB file format")

    def parse_version(self, file: BinaryIO) -> None:
        version_bytes = file.read(4)
        _ = int(version_bytes.decode())

    def parse_contents(self, file: BinaryIO) -> None:
        while True:
            op_code = read(file, "BYTE")
            if op_code == OpCode.EOF:
                break
            self.handle_op_code(file, op_code)

    def handle_op_code(self, file: BinaryIO, op_code: int):
        match op_code:
            case OpCode.SELECTDB:
                self.read_length(file)
            case OpCode.RESIZEDB:
                self.read_length(file)
                self.read_length(file)
            case OpCode.AUX:
                _, _ = self.parse_auxiliary(file)
            case OpCode.EXPIRETIME:
                _ = self.parse_expirytime(file)
            case OpCode.EXPIRETIME_MS:
                _ = self.parse_expirytime_ms(file)
            case _:
                self.parse_key_value(file, op_code)

    def read_length(self, file: BinaryIO) -> int:
        length, _ = self.parse_length_with_encoding(file)
        return length

    def parse_length_with_encoding(self, file: BinaryIO) -> tuple[int, bool]:
        byte = read(file, "BYTE")
        encoding = byte >> 6  # Get the two most significant bits for encoding type

        if encoding == LengthEncoding.ENCVAL:
            # Special encoded value (e.g., small integers)
            return byte & 0x3F, True
        elif encoding == LengthEncoding.BIT_6:
            # Plain 6-bit length
            return byte & 0x3F, False
        elif encoding == LengthEncoding.BIT_14:
            # 14-bit length stored in two bytes
            extra = read(file, "byte")
            return ((byte & 0x3F) << 8) | extra, False
        else:
            # 32-bit length
            return read(file, "UINT64"), False

    def parse_auxiliary(self, file: BinaryIO) -> tuple[bytes, bytes]:
        key = self.parse_string(file)
        value = self.parse_string(file)
        return key, value

    def parse_string(self, file: BinaryIO) -> bytes:
        length, is_encoded = self.parse_length_with_encoding(file)
        if is_encoded:
            return self.read_encoded_value(file, length)
        else:
            return file.read(length)

    def read_encoded_value(self, file: BinaryIO, encoding: int) -> bytes:
        if encoding == StringEncoding.INT8:
            return read(file, "INT8")
        elif encoding == StringEncoding.INT16:
            return read(file, "INT16")
        elif encoding == StringEncoding.INT32:
            return read(file, "INT32")
        raise ValueError(f"Unsupported encoding type: {encoding}")

    def parse_expirytime(self, file: BinaryIO) -> datetime.datetime:
        expiry = read(file, "UINT32")
        return to_datetime(expiry * 1_000_000)

    def parse_expirytime_ms(self, file: BinaryIO) -> datetime.datetime:
        expiry_ms = read(file, "UINT64")
        return to_datetime(expiry_ms * 1_000)

    def parse_key_value(self, file: BinaryIO, op_code: int):
        key = self.parse_string(file)

        if op_code == Type.STRING:
            value = self.parse_string(file)
            self.data[key.decode("utf-8")] = (value.decode("utf-8"), None)
        else:
            raise NotImplementedError(
                f"Value type {op_code} parsing is not implemented."
            )
