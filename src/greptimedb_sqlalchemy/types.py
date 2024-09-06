import logging

# Configure the logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

from sqlalchemy.types import (
    TypeDecorator,
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


class GreptimeType(TypeDecorator):
    impl = None
    cache_ok = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def process_bind_param(self, value, dialect):
        return value

    def process_result_value(self, value, dialect):
        return value


class String(GreptimeType):
    impl = SAString
    __visit_name__ = "String"


class Binary(GreptimeType):
    impl = LargeBinary
    __visit_name__ = "Binary"


class Int8(GreptimeType):
    impl = SmallInteger
    __visit_name__ = "Int8"


class Int16(GreptimeType):
    impl = SmallInteger
    __visit_name__ = "Int16"


class Int32(GreptimeType):
    impl = Integer
    __visit_name__ = "Int32"


class Int64(GreptimeType):
    impl = BigInteger
    __visit_name__ = "Int64"


class UInt8(GreptimeType):
    impl = Integer
    __visit_name__ = "UInt8"


class UInt16(GreptimeType):
    impl = Integer
    __visit_name__ = "UInt16"


class UInt32(GreptimeType):
    impl = BigInteger
    __visit_name__ = "UInt32"


class UInt64(GreptimeType):
    impl = BigInteger
    __visit_name__ = "UInt64"


class Float32(GreptimeType):
    impl = Float
    __visit_name__ = "Float32"


class Float64(GreptimeType):
    impl = Float
    __visit_name__ = "Float64"


class TimestampSecond(GreptimeType):
    impl = DateTime
    __visit_name__ = "TimestampSecond"


class TimestampMillisecond(GreptimeType):
    impl = DateTime
    __visit_name__ = "TimestampMillisecond"


class TimestampMicrosecond(GreptimeType):
    impl = DateTime
    __visit_name__ = "TimestampMicrosecond"


class TimestampNanosecond(GreptimeType):
    impl = DateTime
    __visit_name__ = "TimestampNanosecond"


class Decimal(GreptimeType):
    impl = Numeric
    __visit_name__ = "Decimal"


class Date(GreptimeType):
    impl = Date
    __visit_name__ = "Date"


class DateTime(GreptimeType):
    impl = DateTime
    __visit_name__ = "DateTime"


class Boolean(GreptimeType):
    impl = Boolean
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
