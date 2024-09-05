import abc

import sqlalchemy
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2


def connection_uri(
    host: str, port: str, username: str, password: str, database: str = "public"
):
    return f"greptimedb://{username}:{password}@{host}:{port}/{database}"


def create_engine(
    host: str, port: str, username: str, password: str, database: str = "public"
):
    return sqlalchemy.create_engine(
        connection_uri(host, port, username, password, database),
        future=True,
        hide_parameters=False,
        implicit_returning=False,
        isolation_level="REPEATABLE READ",
    )


class GreptimeDBDialect(PGDialect_psycopg2, abc.ABC):
    name = "greptimedb"
    psycopg2_version = (2, 9)
    default_schema_name = "public"
    supports_schemas = False
    supports_statement_cache = False
    supports_server_side_cursors = False
    supports_native_boolean = True
    supports_views = False
    supports_empty_insert = False
    supports_multivalues_insert = True
    supports_comments = True
    postfetch_lastrowid = False
    _has_native_hstore = False

    @classmethod
    def dbapi(cls):
        import greptimedb_sqlalchemy.dbapi as dbapi

        return dbapi

    def get_schema_names(self, conn, **kw):
        return [row[0] for row in self._exec(conn, "SHOW DATABASES")]

    def get_table_names(self, conn, schema=None, **kw):
        return [row[0] for row in self._exec(conn, "SHOW TABLES")]

    def has_table(self, conn, table_name, schema=None):
        return table_name in set(self.get_table_names(conn, schema))

    @sqlalchemy.engine.reflection.cache
    def get_columns(self, conn, table_name, schema=None, **kw):
        schema_name = schema or "public"
        columns = self._exec(
            conn,
            f"SELECT column_name, column_type, is_nullable FROM information_schema.columns WHERE table_schema = '{schema_name}' AND 'table_name' = '{table_name}'",
        )
        return [{"name": row[0], "type": row[1], "nullable": row[2]} for row in columns]

    def get_pk_constraint(self, conn, table_name, schema=None, **kw):
        return []

    def get_foreign_keys(
        self,
        conn,
        table_name,
        schema=None,
        postgresql_ignore_search_path=False,
        **kw,
    ):
        return []

    def get_temp_table_names(self, conn, **kw):
        return []

    def get_view_names(self, conn, schema=None, **kw):
        return []

    def get_temp_view_names(self, conn, schema=None, **kw):
        return []

    def get_view_definition(self, conn, view_name, schema=None, **kw):
        pass

    def get_indexes(self, conn, table_name, schema=None, **kw):
        return []

    def get_unique_constraints(self, conn, table_name, schema=None, **kw):
        return []

    def get_check_constraints(self, conn, table_name, schema=None, **kw):
        return []

    def has_sequence(self, conn, sequence_name, schema=None, **_kw):
        return False

    def do_begin_twophase(self, conn, xid):
        raise NotImplementedError

    def do_prepare_twophase(self, conn, xid):
        raise NotImplementedError

    def do_rollback_twophase(self, conn, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_commit_twophase(self, conn, xid, is_prepared=True, recover=False):
        raise NotImplementedError

    def do_recover_twophase(self, conn):
        raise NotImplementedError

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        return None

    def _exec(self, conn, sql_query):
        return conn.execute(sqlalchemy.text(sql_query))
