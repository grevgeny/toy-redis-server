import base64
import struct
from typing import BinaryIO

from app.rdb.constants import DataType

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


def get_empty_rdb():
    EMPTY_RDS_BASE64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    return base64.b64decode(EMPTY_RDS_BASE64)
