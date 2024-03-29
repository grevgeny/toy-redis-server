import datetime
import io
import struct
from typing import BinaryIO

from toy_redis_server.data_types import Data, String
from toy_redis_server.rdb.constants import (
    DataType,
    LengthEncoding,
    OpCode,
    StringEncoding,
    Type,
)

FORMAT_MAPPING = {
    DataType.SIGNED_CHAR: "b",
    DataType.UNSIGNED_CHAR: "B",
    DataType.SIGNED_SHORT: "h",
    DataType.UNSIGNED_SHORT: "H",
    DataType.SIGNED_INT: "i",
    DataType.UNSIGNED_INT: "I",
    DataType.UNSIGNED_INT_BE: ">I",
    DataType.SIGNED_LONG: "q",
    DataType.UNSIGNED_LONG: "Q",
    DataType.UNSIGNED_LONG_BE: ">Q",
}


def read_bytes(file: BinaryIO, length: int) -> bytes:
    return file.read(length)


def unpack_data(file: BinaryIO, data_type: DataType) -> int:
    fmt = FORMAT_MAPPING.get(data_type)
    if not fmt:
        raise ValueError(f"Unsupported data type: {data_type}")

    data_length = struct.calcsize(fmt)
    data = read_bytes(file, data_length)

    return struct.unpack(fmt, data)[0]


class RDBParser:
    def __init__(self) -> None:
        self.data: Data = {}

        self._expiry_dt: datetime.datetime | None = None

    @classmethod
    def load_from_file(cls, filepath: str) -> Data:
        with open(filepath, "rb") as file:
            parser = cls()
            parser.parse(file)

        return parser.data

    @classmethod
    def load_from_bytes(cls, data: bytes) -> Data:
        with io.BytesIO(data) as data_stream:
            parser = cls()
            parser.parse(data_stream)

        return parser.data

    def parse(self, file: BinaryIO) -> None:
        self.parse_magic_string(file)
        self.parse_version(file)
        self.parse_contents(file)

    def parse_magic_string(self, file: BinaryIO) -> None:
        if read_bytes(file, 5) != b"REDIS":
            raise ValueError("Invalid RDB file format")

    def parse_version(self, file: BinaryIO) -> None:
        read_bytes(file, 4)

    def parse_contents(self, file: BinaryIO) -> None:
        while True:
            op_code = unpack_data(file, DataType.UNSIGNED_CHAR)
            if op_code == OpCode.EOF:
                break
            self.handle_op_code(file, op_code)

    def handle_op_code(self, file: BinaryIO, op_code: int) -> None:
        match op_code:
            case OpCode.AUX:
                self.parse_string(file)
                self.parse_string(file)

            case OpCode.SELECTDB:
                self.read_length(file)

            case OpCode.RESIZEDB:
                self.read_length(file)
                self.read_length(file)

            case OpCode.EXPIRETIME:
                expiry_dt = self.parse_expirytime(file)
                self._expiry_dt = expiry_dt

            case OpCode.EXPIRETIME_MS:
                expiry_dt = self.parse_expirytime_ms(file)
                self._expiry_dt = expiry_dt

            case value_type:
                entry = self.parse_key_value(file, value_type)
                entry.expiry, self._expiry_dt = self._expiry_dt, None
                self.data[entry.key] = entry

    def parse_length_with_encoding(self, file: BinaryIO) -> tuple[int, bool]:
        length: int
        is_encoded: bool = False

        enc_type = unpack_data(file, DataType.UNSIGNED_CHAR)

        match (enc_type & 0xC0) >> 6:
            case LengthEncoding.ENCVAL:
                is_encoded = True
                length = enc_type & 0x3F
            case LengthEncoding.BIT_6:
                length = enc_type & 0x3F
            case LengthEncoding.BIT_14:
                next_byte = unpack_data(file, DataType.UNSIGNED_CHAR)
                length = ((enc_type & 0x3F) << 8) | next_byte
            case LengthEncoding.BIT_32:
                length = unpack_data(file, DataType.UNSIGNED_INT_BE)
            case LengthEncoding.BIT_64:
                length = unpack_data(file, DataType.UNSIGNED_LONG_BE)
            case _:
                raise ValueError(f"Unknown length encoding: {enc_type}")

        return length, is_encoded

    def read_length(self, file: BinaryIO) -> int:
        length, _ = self.parse_length_with_encoding(file)
        return length

    def parse_string(self, file: BinaryIO) -> int | bytes:
        result: int | bytes

        length, is_encoded = self.parse_length_with_encoding(file)

        if is_encoded:
            match length:
                case StringEncoding.INT8:
                    result = unpack_data(file, DataType.SIGNED_CHAR)
                case StringEncoding.INT16:
                    result = unpack_data(file, DataType.SIGNED_SHORT)
                case StringEncoding.INT32:
                    result = unpack_data(file, DataType.SIGNED_INT)
                case _:
                    raise ValueError(f"Unsupported encoding type: {length}")
        else:
            result = read_bytes(file, length)

        return result

    def parse_expirytime(self, file: BinaryIO) -> datetime.datetime:
        timestamp = unpack_data(file, DataType.UNSIGNED_INT)
        return datetime.datetime.fromtimestamp(timestamp, tz=datetime.UTC)

    def parse_expirytime_ms(self, file: BinaryIO) -> datetime.datetime:
        timestamp = unpack_data(file, DataType.UNSIGNED_LONG)
        return datetime.datetime.fromtimestamp(timestamp / 1000.0, tz=datetime.UTC)

    def parse_key_value(self, file: BinaryIO, value_type: int) -> String:
        key = self.parse_string(file)

        if isinstance(key, bytes):
            decoded_key = key.decode()
        else:
            decoded_key = str(key)

        match value_type:
            case Type.STRING:
                value = self.parse_string(file)
                if isinstance(value, bytes):
                    decoded_value = value.decode()
                else:
                    decoded_value = str(value)
                return String(decoded_key, decoded_value)
            case _:
                raise NotImplementedError(
                    f"Value type {value_type} parsing is not implemented."
                )
