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


class LengthEncoding:
    BIT_6 = 0
    BIT_14 = 1
    ENCVAL = 3
    BIT_32 = 128
    BIT_64 = 129


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
