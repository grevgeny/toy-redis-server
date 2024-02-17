from enum import Enum, auto


class DataType(Enum):
    SIGNED_CHAR = auto()
    UNSIGNED_CHAR = auto()
    SIGNED_SHORT = auto()
    UNSIGNED_SHORT = auto()
    SIGNED_INT = auto()
    UNSIGNED_INT = auto()
    UNSIGNED_INT_BE = auto()
    SIGNED_LONG = auto()
    UNSIGNED_LONG = auto()
    UNSIGNED_LONG_BE = auto()
    BINARY_DOUBLE = auto()
    BINARY_FLOAT = auto()


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
    DataType.BINARY_DOUBLE: "d",
    DataType.BINARY_FLOAT: "f",
}


class LengthEncoding:
    """Defines for encoding the length of keys and values in the RDB file."""

    BIT_6 = 0
    BIT_14 = 1
    BIT_32 = 0x80
    ENCVAL = 3


class StringEncoding:
    """Special encodings for string objects stored in RDB files."""

    INT8 = 0
    INT16 = 1
    INT32 = 2


class Type:
    """Object types in RDB files. Maps memory storage types to RDB types."""

    STRING = 0


class OpCode:
    """Special opcodes used in RDB files for various metadata and control purposes."""

    AUX = 250
    RESIZEDB = 251
    EXPIRETIME_MS = 252
    EXPIRETIME = 253
    SELECTDB = 254
    EOF = 255
