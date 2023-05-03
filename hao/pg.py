# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install DBUtils

pip install psycopg psycopg-binary

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
    db.cursor.execute('select * from t_dummy_table')
    records = db.cursor.fetchall()

with PG('some-other', cursor_class='dict') as db:
    ...
"""
from typing import Optional, Union

from . import config, logs

try:
    from dbutils.pooled_db import PooledDB
except ImportError:
    from DBUtils.PooledDB import PooledDB

try:
    import psycopg as client
except ImportError:
    import psycopg2 as client

LOGGER = logs.get_logger(__name__)


class PG:
    _POOLS = {}

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self._ensure_pool()

    def _ensure_pool(self):
        if self.profile in PG._POOLS:
            return
        conf_profile = config.get(f"pg.{self.profile}", {})
        if len(conf_profile) == 0:
            raise ValueError(f'pg profile not configured: {self.profile}')
        conf = {
            'mincached': 1,
            'maxcached': 2,
            'maxshared': 2,
            'maxconnections': 20,
        }
        conf.update(conf_profile)
        LOGGER.debug(f"connecting [{self.profile}], host: {conf.get('host')}, db: {conf.get('db')}")

        pool = PooledDB(
            client,
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
            **conf
        )
        PG._POOLS[self.profile] = pool

    def __enter__(self):
        self.conn = self.connect()
        self.cursor = self.conn.cursor()
        return self

    def connect(self):
        return self._POOLS.get(self.profile).connection()

    def execute(self, sql: str, params: Optional[Union[list, tuple]] = None):
        self.cursor.execute(sql, params)
        return self.cursor

    def executemany(self, sql: str, params: Optional[Union[list, tuple]] = None):
        self.cursor.executemany(sql, params)
        return self.cursor

    def fetchone(self, sql: str, params: Optional[Union[list, tuple]] = None):
        self.cursor.execute(sql, params)
        return self.cursor.fetchone()

    def fetchall(self, sql: str, params: Optional[Union[list, tuple]] = None):
        self.cursor.execute(sql, params)
        return self.cursor.fetchall()

    def fetchmany(self, sql: str, params: Optional[Union[list, tuple]] = None):
        self.cursor.execute(sql, params)
        return self.cursor.fetchmany()

    def commit(self, sql: str, params: Optional[Union[list, tuple]] = None):
        n_rows = self.cursor.execute(sql, params)
        self.conn.commit()
        return n_rows

    def rollback(self):
        self.conn.rollback()

    def __exit__(self, _type, _value, _trace):
        self.cursor.close()
        self.conn.close()
