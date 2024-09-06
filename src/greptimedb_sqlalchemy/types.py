import logging

# Configure the logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


from sqlalchemy.types import (
    String as SAString,
    LargeBinary,
    SmallInteger,
    Integer,
    BigInteger,
    Float,
    DateTime,
    Date,
    Boolean,
    Numeric,
)


class String(SAString):
    __visit_name__ = "String"


class Binary(LargeBinary):
    __visit_name__ = "Binary"


class Int8(SmallInteger):
    __visit_name__ = "Int8"


class Int16(SmallInteger):
    __visit_name__ = "Int16"


class Int32(Integer):
    __visit_name__ = "Int32"


class Int64(BigInteger):
    __visit_name__ = "Int64"


class UInt8(Integer):
    __visit_name__ = "UInt8"


class UInt16(Integer):
    __visit_name__ = "UInt16"


class UInt32(BigInteger):
    __visit_name__ = "UInt32"


class UInt64(BigInteger):
    __visit_name__ = "UInt64"


class Float32(Float):
    __visit_name__ = "Float32"


class Float64(Float):
    __visit_name__ = "Float64"


class TimestampSecond(DateTime):
    __visit_name__ = "TimestampSecond"


class TimestampMillisecond(DateTime):
    __visit_name__ = "TimestampMillisecond"


class TimestampMicrosecond(DateTime):
    __visit_name__ = "TimestampMicrosecond"


class TimestampNanosecond(DateTime):
    __visit_name__ = "TimestampNanosecond"


class Decimal(Numeric):
    __visit_name__ = "Decimal"


class Date(Date):
    __visit_name__ = "Date"


class DateTime(DateTime):
    __visit_name__ = "DateTime"


class Boolean(Boolean):
    __visit_name__ = "Boolean"


TYPE_MAP = {
    "String": String,
    "Binary": Binary,
    "Int8": Int8,
    "Int16": Int16,
    "Int32": Int32,
    "Int64": Int64,
    "UInt8": UInt8,
    "UInt16": UInt16,
    "UInt32": UInt32,
    "UInt64": UInt64,
    "Float32": Float32,
    "Float64": Float64,
    "TimestampSecond": TimestampSecond,
    "TimestampMillisecond": TimestampMillisecond,
    "TimestampMicrosecond": TimestampMicrosecond,
    "TimestampNanosecond": TimestampNanosecond,
    "Decimal": Decimal,
    "Date": Date,
    "DateTime": DateTime,
    "Boolean": Boolean,
}


def resolve_data_type(name):
    result = TYPE_MAP.get(name)
    return result
