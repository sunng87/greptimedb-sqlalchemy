from __future__ import annotations

from typing import Any

from sqlalchemy.engine.reflection import Inspector
import logging

# Configure the logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

from superset.db_engine_specs.base import BasicParametersType
from superset.db_engine_specs.postgres import PostgresEngineSpec

from superset.constants import TimeGrain


class GreptimeDBEngineSpec(PostgresEngineSpec):
    engine = "greptimedb"
    engine_aliases = {"greptime"}
    engine_name = "GreptimeDB"
    default_driver = "psycopg2"
    encryption_parameters = {"sslmode": "prefer"}
    sqlalchemy_uri_placeholder = "greptimedb://username:password@host:port/database"
    time_groupby_inline = False
    allows_hidden_cc_in_orderby = True
    time_secondary_columns = True
    try_remove_schema_from_table_name = True
    max_column_name_length = 63
    top_keywords: set[str] = set({})

    supports_dynamic_schema = True
    supports_catalog = True
    supports_dynamic_catalog = True

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
        """Return the default schema for a given query."""
        return "public"

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
