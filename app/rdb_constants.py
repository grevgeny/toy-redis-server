FORMAT_MAPPING = {
    "BYTE": "B",  # Unsigned byte
    "INT8": "b",  # Signed byte
    "INT16": "h",  # Signed short
    "INT32": "i",  # Signed int
    "INT64": "q",  # Signed long long
    "UINT32": ">I",  # Unsigned int (big endian)
    "UINT64": ">Q",  # Unsigned long long (big endian)
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
