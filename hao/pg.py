# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install "psycopg[binary]" dbutils>=3.0.0

####################################################
###########         config.yml          ############
####################################################
pg:
  default:
    host: default-host
    port: 5432
    user: username
    password: password
    dbname: default_db_name
  some-other:
    host: some-other-host
    port: 5432
    user: username
    password: password
    dbname: default_db_name


####################################################
###########          usage              ############
####################################################
from hao.pg import PG
with PG() as db:
    records = db.fetchall('select * from t_dummy_table')

with PG('profile-name', cursor='dict') as db:
    ...
"""
import secrets
from typing import Literal, Optional, Union

import psycopg
from dbutils.pooled_db import PooledDB
from psycopg import Connection, Cursor
from psycopg.rows import dict_row, namedtuple_row, tuple_row

from . import config, logs, strings

LOGGER = logs.get_logger(__name__)


class PG:
    _POOLS = {}
    _CURSORS = {
        'tuple': tuple_row,
        'dict': dict_row,
        'namedtuple': namedtuple_row,
    }

    def __init__(self, profile='default', cursor: Literal['tuple', 'dict', 'namedtuple'] = 'tuple') -> None:
        super().__init__()
        self.profile = profile
        conf_profile = config.get(f"pg.{self.profile}", {})
        assert len(conf_profile) > 0, f'pg profile not configured: pg.{self.profile}'
        self.__conf = {
            'mincached': 1,
            'maxcached': 2,
            'maxshared': 2,
            'maxconnections': 20,
            **conf_profile
        }
        self._row_factory = self._CURSORS.get(cursor)
        self._ensure_pool()

    def _ensure_pool(self):
        if self.profile in PG._POOLS:
            return

        conf = {**self.__conf}
        LOGGER.debug(f"connecting [{self.profile}], host: {conf.get('host')}, db: {conf.get('db')}")

        pool = PooledDB(
            psycopg,
            mincached=conf.pop('mincached', 1),
            maxcached=conf.pop('maxcached', 2),
            maxshared=conf.pop('maxshared', 2),
            maxconnections=conf.pop('maxconnections', 20),
            blocking=conf.pop('blocking', False),
            maxusage=conf.pop('maxusage', None),
            setsession=conf.pop('setsession', None),
            reset=conf.pop('reset', True),
            failures=conf.pop('failures', None),
            ping=conf.pop('ping', 1),
            autocommit=conf.pop('autocommit', False),
            **conf
        )
        PG._POOLS[self.profile] = pool

    def __str__(self) -> str:
        return f"profile: [{self.profile}], host: {self.__conf.get('host')}, db: {self.__conf.get('db')}"

    def __repr__(self) -> str:
        return self.__str__()

    def __enter__(self):
        self._conn = self.connect()
        self._cursor = self._conn.cursor(row_factory=self._row_factory)
        return self

    def connect(self) -> Connection:
        return self._POOLS.get(self.profile).connection()

    def cursor(self, cursor: Literal['tuple', 'dict', 'namedtuple'] = 'tuple') -> Cursor:
        return self._conn.cursor(row_factory=self._CURSORS.get(cursor))

    def execute(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False) -> Cursor:
        self._cursor.execute(sql, params)
        if commit:
            self.commit()
        return self._cursor

    def executemany(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False) -> Cursor:
        self._cursor.executemany(sql, params)
        if commit:
            self.commit()
        return self._cursor

    def fetchone(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False):
        self._cursor.execute(sql, params)
        if commit:
            self.commit()
        return self._cursor.fetchone()

    def fetchall(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False):
        self._cursor.execute(sql, params)
        if commit:
            self.commit()
        return self._cursor.fetchall()

    def fetch(self, sql: str, params: Optional[Union[list, tuple]] = None, batch=2000, *, commit: bool = False):
        name = f"{strings.sha256(sql)}-{hash(','.join(params)) if params else 0}-{secrets.token_hex()}"
        cursor = self._conn.cursor(name=name, row_factory=self._row_factory)
        try:
            cursor.execute(sql, params)
            if commit:
                self.commit()
            while True:
                records = cursor.fetchmany(size=batch)
                if not records:
                    break
                for record in records:
                    yield record
        finally:
            cursor.close()

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def __exit__(self, _type, _value, _trace):
        self._cursor.close()
        self._conn.close()
