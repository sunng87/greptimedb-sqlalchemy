import psycopg2

apilevel = "2.0"
threadsafety = 2
paramstyle = "pyformat"


class Error(Exception):
    pass


def connect(**kwargs):
    host = kwargs.get("host") or "127.0.0.1"
    port = kwargs.get("port") or 4003
    user = kwargs.get("user") or ""
    password = kwargs.get("password") or ""
    database = kwargs.get("database") or "public"
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
    )
    conn.autocommit = True
    return conn
