from __future__ import annotations

import re
from typing import Any
import logging

# Configure the logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

from sqlalchemy.engine.reflection import Inspector
from superset.constants import TimeGrain
from superset.db_engine_specs.base import BasicParametersType, BaseEngineSpec
from sqlalchemy.types import TypeEngine
from superset.utils.core import GenericDataType, ColumnSpec, ColumnTypeSource

from . import types


class GreptimeDBEngineSpec(BaseEngineSpec):
    engine = "greptimedb"
    engine_aliases = {"greptime"}
    engine_name = "GreptimeDB"
    default_driver = "psycopg2"
    encryption_parameters = {"sslmode": "prefer"}
    sqlalchemy_uri_placeholder = "greptimedb://username:password@host:port/database"
    time_groupby_inline = False
    allows_hidden_cc_in_orderby = True
    time_secondary_columns = True
    try_remove_schema_from_table_name = False
    max_column_name_length = 63
    top_keywords: set[str] = set({})

    supports_dynamic_schema = False
    supports_catalog = False
    supports_dynamic_catalog = False

    _time_grain_expressions = {
        None: "{col}",
        TimeGrain.SECOND: "DATE_TRUNC('second', {col})",
        TimeGrain.MINUTE: "DATE_TRUNC('minute', {col})",
        TimeGrain.HOUR: "DATE_TRUNC('hour', {col})",
        TimeGrain.DAY: "DATE_TRUNC('day', {col})",
        TimeGrain.WEEK: "DATE_TRUNC('week', {col})",
        TimeGrain.MONTH: "DATE_TRUNC('month', {col})",
        TimeGrain.QUARTER: "DATE_TRUNC('quarter', {col})",
        TimeGrain.YEAR: "DATE_TRUNC('year', {col})",
    }

    column_type_mappings = (
        (
            re.compile("^BOOLEAN$", re.IGNORECASE),
            types.Boolean,
            GenericDataType.BOOLEAN,
        ),
        (
            re.compile("^BINARY$", re.IGNORECASE),
            types.Binary,
            GenericDataType.STRING,
        ),
        (
            re.compile("^STRING$", re.IGNORECASE),
            types.String,
            GenericDataType.STRING,
        ),
        (
            re.compile("^INT8$", re.IGNORECASE),
            types.Int8,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^INT16$", re.IGNORECASE),
            types.Int16,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^INT32$", re.IGNORECASE),
            types.Int32,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^INT64$", re.IGNORECASE),
            types.Int64,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^UINT8$", re.IGNORECASE),
            types.UInt8,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^UINT16$", re.IGNORECASE),
            types.UInt16,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^UINT32$", re.IGNORECASE),
            types.UInt32,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^UINT64$", re.IGNORECASE),
            types.UInt64,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^DECIMAL", re.IGNORECASE),
            types.Decimal,
            GenericDataType.NUMERIC,
        ),
        (
            re.compile("^DATE$", re.IGNORECASE),
            types.Date,
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile("^DATETIME$", re.IGNORECASE),
            types.DateTime,
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile("^TIMESTAMPSECOND$", re.IGNORECASE),
            types.TimestampSecond,
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile("^TIMESTAMPMILLISECOND$", re.IGNORECASE),
            types.TimestampMillisecond,
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile("^TIMESTAMPMICROSECOND$", re.IGNORECASE),
            types.TimestampMicrosecond,
            GenericDataType.TEMPORAL,
        ),
        (
            re.compile("^TIMESTAMPNANOSECOND$", re.IGNORECASE),
            types.TimestampNanosecond,
            GenericDataType.TEMPORAL,
        ),
    )

    @classmethod
    def build_sqlalchemy_uri(
        cls,
        parameters: BasicParametersType,
        encrypted_extra: dict[str, str] | None = None,
    ) -> str:
        host = parameters.get("host")
        port = parameters.get("port")
        username = parameters.get("username")
        password = parameters.get("password")
        database = parameters.get("database")
        return f"greptimedb://{username}:{password}@{host}:{port}/{database}"

    @classmethod
    def get_default_schema_for_query(cls, database, query) -> str | None:
        return None

    @classmethod
    def get_allow_cost_estimate(cls, extra: dict[str, Any]) -> bool:
        return False

    @classmethod
    def get_view_names(
        cls,
        database,
        inspector: Inspector,
        schema: str | None,
    ) -> set[str]:
        return set()

    @classmethod
    def get_indexes(
        cls,
        database,
        inspector,
        table,
    ) -> list[dict[str, Any]]:
        return []

    @classmethod
    def get_table_names(cls, database, inspector, schema: str | None) -> set[str]:
        return set(inspector.get_table_names(schema))

    @classmethod
    def get_column_types(
        cls,
        column_type: str | None,
    ) -> tuple[TypeEngine, GenericDataType] | None:
        if not column_type:
            return None

        for regex, sqla_type, generic_type in (
            cls.column_type_mappings + cls._default_column_type_mappings
        ):
            match = regex.match(column_type)

            if not match:
                continue

            logger.info(f"getting column type {column_type}, {sqla_type}")
            if callable(sqla_type):
                return sqla_type(match), generic_type
            return sqla_type, generic_type
        return None

    @classmethod
    def get_column_spec(
        cls,
        native_type: str | None,
        db_extra: dict[str, Any] | None = None,
        source: ColumnTypeSource = ColumnTypeSource.GET_TABLE,
    ) -> ColumnSpec | None:
        spec = GreptimeDBEngineSpec.get_column_types(native_type)
        if spec is not None:
            sqla_type, generic_type = spec
            return ColumnSpec(
                sqla_type,
                generic_type,
                generic_type == GenericDataType.TEMPORAL,
            )
        else:
            return None

    @classmethod
    def column_datatype_to_string(cls, sqla_column_type: TypeEngine, dialect) -> str:
        return sqla_column_type.__visit_name__
