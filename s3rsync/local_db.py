from contextlib import contextmanager

import peewee  # type: ignore
from dynaconf import settings  # type: ignore


database = peewee.DatabaseProxy()


@contextmanager
def open_database(path=settings.LOCAL_DB):
    database.initialize(peewee.SqliteDatabase(path, pragmas={"foreign_keys": 1}))
    try:
        database.connect()
        yield database
    finally:
        database.close()
