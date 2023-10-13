# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
# Option 1: requires mysql client installed
pip install mysqlclient dbutils>=3.0.0

# Option 2: green pure python client, slower than `mysqlclient`
pip install pymysql dbutils>=3.0.0

####################################################
###########         config.yml          ############
####################################################
mysql:
  default:
    host: default-host
    port: 3306
    user: username
    password: password
    db: default_db_name
  some-other:
    host: some-other-host
    port: 3306
    user: username
    password: password
    db: default_db_name


####################################################
###########          usage              ############
####################################################
from hao.mysql import MySQL
with MySQL() as db:
    records = db.fetchall('select * from t_dummy_table')

with MySQL('profile-name', cursor='dict') as db:
    ...
"""

from typing import Literal, Optional, Union

from dbutils.pooled_db import PooledDB

from . import config, logs

try:
    import pymysql as mysqlclient
    from pymysql.connections import Connection
    from pymysql.cursors import Cursor, DictCursor, SSCursor, SSDictCursor
except ImportError:
    import MySQLdb as mysqlclient
    from MySQLdb.connections import Connection
    from MySQLdb.cursors import Cursor, DictCursor, SSCursor, SSDictCursor


LOGGER = logs.get_logger(__name__)


class MySQL:
    _POOLS = {}
    _CURSORS = {
        'default': Cursor,
        'ss': SSCursor,
        'dict': DictCursor,
        'ss-dict': SSDictCursor,
    }

    def __init__(self, profile='default', cursor: Literal['default', 'ss', 'dict', 'ss-dict'] = 'default') -> None:
        super().__init__()
        self.profile = profile
        self._cursor_class = self._CURSORS.get(cursor, Cursor)
        self._ensure_pool()

    def _ensure_pool(self):
        if self.profile in MySQL._POOLS:
            return
        conf_profile = config.get(f"mysql.{self.profile}", {})
        if len(conf_profile) == 0:
            raise ValueError(f'mysql profile not configured: {self.profile}')
        conf = {
            'mincached': 1,
            'maxcached': 2,
            'maxshared': 2,
            'maxconnections': 20,
            'use_unicode': True,
            'charset': "utf8",
        }
        conf.update(conf_profile)
        LOGGER.debug(f"connecting [{self.profile}], host: {conf.get('host')}, db: {conf.get('db')}")

        pool = PooledDB(
            mysqlclient,
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
        MySQL._POOLS[self.profile] = pool

    def __enter__(self):
        self._conn = self.connect()
        self._cursor = self._conn.cursor(self._cursor_class)
        return self

    def connect(self) -> Connection:
        return self._POOLS.get(self.profile).connection()

    def cursor(self, cursor: Literal['default', 'ss', 'dict', 'ss-dict'] = 'default') -> Cursor:
        self._conn.ping()
        return self._conn.cursor(self._CURSORS.get(cursor))

    def execute(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False) -> Cursor:
        self._conn.ping()
        self._cursor.execute(sql, params)
        if commit:
            self.commit()
        return self._cursor

    def executemany(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False) -> Cursor:
        self._conn.ping()
        self._cursor.executemany(sql, params)
        if commit:
            self.commit()
        return self._cursor

    def fetchone(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False):
        self._conn.ping()
        self._cursor.execute(sql, params)
        if commit:
            self.commit()
        return self._cursor.fetchone()

    def fetchall(self, sql: str, params: Optional[Union[list, tuple]] = None, *, commit: bool = False):
        self._conn.ping()
        self._cursor.execute(sql, params)
        if commit:
            self.commit()
        return self._cursor.fetchall()

    def fetch(self, sql: str, params: Optional[Union[list, tuple]] = None, batch=2000, *, commit: bool = False):
        self._conn.ping()
        self._cursor.execute(sql, params)
        if commit:
            self.commit()
        while True:
            records = self._cursor.fetchmany(size=batch)
            if not records:
                break
            for record in records:
                yield record

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def __exit__(self, _type, _value, _trace):
        self._cursor.close()
        self._conn.close()
