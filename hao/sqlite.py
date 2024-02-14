# -*- coding: utf-8 -*-
"""

####################################################
###########         config.yml          ############
####################################################
sqlite:
  default:
    path: data/sqlite.db
  test:
    path: data/sqlite-test.db


####################################################
###########          usage              ############
####################################################
from hao.sqlite import SQLite
with SQLite() as db:
    records = db.execute('select * from t_dummy_table').fetchall()

"""

import sqlite3
from collections import namedtuple
from typing import Literal, Optional, Union

from . import config, logs, paths

LOGGER = logs.get_logger(__name__)


def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def namedtuple_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    cls = namedtuple("Row", fields)
    return cls._make(row)


class SQLite:

    _CURSORS = {
        'tuple': None,
        'dict': dict_factory,
        'namedtuple': namedtuple_factory,
    }

    def __init__(self,
                 profile: str | None = 'default',
                 *,
                 path: str | None = None,
                 cursor: Literal['tuple', 'dict', 'namedtuple'] = 'tuple') -> None:
        self.profile = profile
        self.path = path
        self._path = paths.get(path or config.get(f"sqlite.{profile}.path"))
        self.conn: sqlite3.Connection = None
        self._row_factory = self._CURSORS.get(cursor)

    def connect(self):
        self.conn = sqlite3.connect(self._path)
        self.conn.row_factory = self._row_factory
        return self

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, _type, _value, _trace):
        try:
            self.conn.close()
        except Exception:
            pass

    def execute(self, sql, params: Optional[Union[list, tuple]] = None, *, commit: bool = False) -> sqlite3.Cursor:
        cursor = self.conn.execute(sql, params or ())
        if commit:
            self.commit()
        return cursor

    def executemany(self, sql, params: Optional[Union[list, tuple]] = None, *, commit: bool = False) -> sqlite3.Cursor:
        cursor = self.conn.executemany(sql, params or ())
        if commit:
            self.commit()
        return cursor

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()

    def list_tables(self):
        return self.conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table'").fetchall()

    def show_table(self, table):
        return self.conn.execute(f"pragma table_info('{table}')").fetchall()
