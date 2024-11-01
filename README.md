# greptimedb-sqlalchemy

![PyPI - Version](https://img.shields.io/pypi/v/greptimedb-sqlalchemy)

SQLAlchemy and Superset connector for
[GreptimeDB](https://github.com/greptimeteam/greptimedb). This
connector is developed against Superset master branch. It is expected
to work with upcoming 4.1 release.

![screenshot](screenshot.png)

## Superset

If you are using docker to run superset, add this library to
`docker/requirements-local.txt`

```bash
echo "greptimedb-sqlalchemy" > docker/requirements-local.txt
```

Start superset using `docker compose -f docker-compose-non-dev.yml up` you will
be able to add GreptimeDB as a database.
