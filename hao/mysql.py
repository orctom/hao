# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
# Option 1: asyncmy (recommended)
pip install asyncmy

# Option 2: aiomysql (fallback)
pip install aiomysql

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
async with MySQL() as db:
    records = await db.fetchall('select * from t_dummy_table')

async with MySQL('profile-name', cursor='dict') as db:
    ...
"""

from typing import Literal

from . import config, logs

LOGGER = logs.get_logger(__name__)

try:
    from asyncmy import Connection
    from asyncmy.cursors import Cursor, DictCursor, SSCursor, SSDictCursor
    from asyncmy.pool import Pool
    _MYSQL_CLIENT = 'asyncmy'
except ImportError:
    try:
        from aiomysql import Connection
        from aiomysql.cursors import Cursor, DictCursor, SSCursor, SSDictCursor
        from aiomysql.pool import Pool
        _MYSQL_CLIENT = 'aiomysql'
    except ImportError:
        raise ImportError("Either asyncmy or aiomysql is required")


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
        conf_profile = config.get(f"mysql.{self.profile}", {})
        assert len(conf_profile) > 0, f'mysql profile not configured: mysql.{self.profile}'
        self.__conf = {
            'use_unicode': True,
            'charset': "utf8",
            **conf_profile
        }
        self._cursor_class = self._CURSORS.get(cursor, Cursor)
        self._ensure_pool()

    def _ensure_pool(self):
        if self.profile in MySQL._POOLS:
            return
        conf = {**self.__conf}
        LOGGER.debug(f"connecting [{self.profile}], host: {conf.get('host')}, db: {conf.get('db')}")

        db = conf.pop('db', None)
        if db:
            conf['db'] = db

        pool = Pool(
            minsize=conf.pop('mincached', 1),
            maxsize=conf.pop('maxcached', 20),
            **conf
        )
        MySQL._POOLS[self.profile] = pool

    def __str__(self) -> str:
        return f"profile: [{self.profile}], host: {self.__conf.get('host')}, db: {self.__conf.get('db')}"

    def __repr__(self) -> str:
        return self.__str__()

    async def __aenter__(self):
        self._conn = await self.connect()
        self._cursor = await self._conn.cursor(self._cursor_class)
        return self

    async def connect(self) -> Connection:
        return await MySQL._POOLS.get(self.profile).acquire()

    async def cursor(self, cursor: Literal['default', 'ss', 'dict', 'ss-dict'] = 'default'):
        await self._conn.ping()
        return await self._conn.cursor(self._CURSORS.get(cursor))

    async def execute(self, sql: str, params: list | tuple | None = None, *, commit: bool = False) -> Cursor:
        await self._conn.ping()
        await self._cursor.execute(sql, params)
        if commit:
            await self.commit()
        return self._cursor

    async def executemany(self, sql: str, params: list | tuple | None = None, *, commit: bool = False) -> Cursor:
        await self._conn.ping()
        await self._cursor.executemany(sql, params)
        if commit:
            await self.commit()
        return self._cursor

    async def fetchone(self, sql: str, params: list | tuple | None = None, *, commit: bool = False):
        await self._conn.ping()
        await self._cursor.execute(sql, params)
        if commit:
            await self.commit()
        return await self._cursor.fetchone()

    async def fetchall(self, sql: str, params: list | tuple | None = None, *, commit: bool = False):
        await self._conn.ping()
        await self._cursor.execute(sql, params)
        if commit:
            await self.commit()
        return await self._cursor.fetchall()

    async def fetch(self, sql: str, params: list | tuple | None = None, batch=2000, *, commit: bool = False):
        await self._conn.ping()
        await self._cursor.execute(sql, params)
        if commit:
            await self.commit()
        while True:
            records = await self._cursor.fetchmany(size=batch)
            if not records:
                break
            for record in records:
                yield record

    async def commit(self):
        await self._conn.commit()

    async def rollback(self):
        await self._conn.rollback()

    async def __aexit__(self, _type, _value, _trace):
        await self._cursor.close()
        await MySQL._POOLS.get(self.profile).release(self._conn)
