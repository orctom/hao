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

from . import config, logs, paths

LOGGER = logs.get_logger(__name__)

class SQLite:
    def __init__(self, profile='default') -> None:
        self.profile = profile
        self._path = paths.get(config.get(f"sqlite.{profile}.path"))
        self.conn: sqlite3.Connection = None

    def connect(self):
        self.conn = sqlite3.connect(self._path)
        return self

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, _type, _value, _trace):
        self.conn.close()

    def execute(self, sql, *args) -> sqlite3.Cursor:
        return self.conn.execute(sql, *args)

    def executemany(self, sql, *args) -> sqlite3.Cursor:
        return self.conn.executemany(sql, *args)

    def commit(self):
        return self.conn.commit()

    def rollback(self):
        return self.conn.rollback()

    def list_tables(self):
        return self.conn.execute("SELECT name, sql FROM sqlite_master WHERE type='table'").fetchall()

    def show_table(self, table):
        return self.conn.execute(f"pragma table_info('{table}')").fetchall()
