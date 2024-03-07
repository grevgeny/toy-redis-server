import datetime
import io
from typing import Any, BinaryIO

from app.rdb.constants import (
    DataType,
    LengthEncoding,
    OpCode,
    StringEncoding,
    Type,
)
from app.rdb.helpers import read_bytes, unpack_data

Data = dict[str, tuple[str, datetime.datetime | None]]


class RDBParser:
    def __init__(self) -> None:
        self.data: Data = {}

        self._expiry_dt: datetime.datetime | None = None

    @classmethod
    def load_from_file(
        cls, filepath: str
    ) -> dict[str, tuple[str, datetime.datetime | None]]:
        with open(filepath, "rb") as file:
            parser = cls()
            parser.parse(file)

        return parser.data

    @classmethod
    def load_from_bytes(
        cls, data: bytes
    ) -> dict[str, tuple[str, datetime.datetime | None]]:
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
                key, value = self.parse_key_value(file, value_type)
                self.data[key.decode()] = (value.decode(), self._expiry_dt)
                self._expiry_dt = None

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

    def parse_key_value(self, file: BinaryIO, value_type: int) -> tuple[Any, Any]:
        key = self.parse_string(file)

        match value_type:
            case Type.STRING:
                value = self.parse_string(file)
            case _:
                raise NotImplementedError(
                    f"Value type {value_type} parsing is not implemented."
                )

        return key, value
