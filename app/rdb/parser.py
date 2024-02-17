import datetime
import struct
from typing import Any, BinaryIO

from app.rdb.constants import (
    FORMAT_MAPPING,
    DataType,
    LengthEncoding,
    OpCode,
    StringEncoding,
    Type,
)


# Helper functions
def unpack_data(file: BinaryIO, data_type: DataType, length: int | None = None) -> Any:
    if length:
        return file.read(length)

    fmt = FORMAT_MAPPING.get(data_type)
    if not fmt:
        raise ValueError(f"Unsupported data type: {data_type}")

    data = file.read(struct.calcsize(fmt))
    return struct.unpack(fmt, data)[0]


class RDBParser:
    def __init__(self) -> None:
        self.data: dict[str, tuple[str, datetime.datetime | None]] = {}

        self._expiry_dt: datetime.datetime | None = None

    @classmethod
    def load_from_file(
        cls, filepath: str
    ) -> dict[str, tuple[str, datetime.datetime | None]]:
        with open(filepath, "rb") as file:
            parser = cls()
            parser.parse(file)

        return parser.data

    def parse(self, file: BinaryIO) -> None:
        self.parse_magic_string(file)
        self.parse_version(file)
        self.parse_contents(file)

    def parse_magic_string(self, file: BinaryIO) -> None:
        if unpack_data(file, DataType.UNSIGNED_CHAR, 5) != b"REDIS":
            raise ValueError("Invalid RDB file format")

    def parse_version(self, file: BinaryIO) -> None:
        _ = unpack_data(file, DataType.UNSIGNED_CHAR, 4)

    def parse_contents(self, file: BinaryIO) -> None:
        while True:
            op_code = unpack_data(file, DataType.UNSIGNED_CHAR)
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
                expiry_dt = self.parse_expirytime(file)
                self._expiry_dt = expiry_dt
            case OpCode.EXPIRETIME_MS:
                expiry_dt = self.parse_expirytime_ms(file)
                self._expiry_dt = expiry_dt
            case value_type:
                key, value = self.parse_key_value(file, value_type)
                self.data[key.decode()] = (value.decode(), self._expiry_dt)
                self._expiry_dt = None

    def read_length(self, file: BinaryIO) -> int:
        length, _ = self.parse_length_with_encoding(file)
        return length

    def parse_length_with_encoding(self, file: BinaryIO) -> tuple[int, bool]:
        byte = unpack_data(file, DataType.UNSIGNED_CHAR)
        encoding = byte >> 6  # Get the two most significant bits for encoding type

        if encoding == LengthEncoding.ENCVAL:
            return byte & 0x3F, True
        elif encoding == LengthEncoding.BIT_6:
            return byte & 0x3F, False
        elif encoding == LengthEncoding.BIT_14:
            extra = unpack_data(file, DataType.UNSIGNED_CHAR)
            return ((byte & 0x3F) << 8) | extra, False
        else:
            raise NotImplementedError("encoding")

    def parse_auxiliary(self, file: BinaryIO) -> tuple[bytes, bytes]:
        key = self.parse_string(file)
        value = self.parse_string(file)
        return key, value

    def parse_string(self, file: BinaryIO) -> bytes:
        length, is_encoded = self.parse_length_with_encoding(file)
        if is_encoded:
            return self.read_encoded_value(file, length)
        else:
            return unpack_data(file, DataType.UNSIGNED_CHAR, length)

    def read_encoded_value(self, file: BinaryIO, encoding: int) -> bytes:
        if encoding == StringEncoding.INT8:
            return unpack_data(file, DataType.SIGNED_CHAR)
        elif encoding == StringEncoding.INT16:
            return unpack_data(file, DataType.SIGNED_SHORT)
        elif encoding == StringEncoding.INT32:
            return unpack_data(file, DataType.SIGNED_INT)
        else:
            raise ValueError(f"Unsupported encoding type: {encoding}")

    def parse_expirytime(self, file: BinaryIO) -> datetime.datetime:
        timestamp = unpack_data(file, DataType.UNSIGNED_INT)
        return datetime.datetime.fromtimestamp(timestamp, tz=datetime.UTC)

    def parse_expirytime_ms(self, file: BinaryIO) -> datetime.datetime:
        timestamp = unpack_data(file, DataType.UNSIGNED_LONG)
        return datetime.datetime.fromtimestamp(timestamp / 1000.0, tz=datetime.UTC)

    def parse_key_value(self, file: BinaryIO, value_type: int) -> tuple[Any, Any]:
        key = self.parse_string(file)

        if value_type == Type.STRING:
            value = self.parse_string(file)
        else:
            raise NotImplementedError(
                f"Value type {value_type} parsing is not implemented."
            )

        return key, value
