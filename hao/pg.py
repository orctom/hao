# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install "psycopg[pool]"

####################################################
###########         config.yml          ############
####################################################
pg:
  default:
    host: default-host
    port: 5432
    user: username
    password: password
    db: default_db_name
  some-other:
    host: some-other-host
    port: 5432
    user: username
    password: password
    db: default_db_name


####################################################
###########          usage              ############
####################################################
from hao.pg import PG
async with PG() as db:
    records = await db.fetchall('select * from t_dummy_table')

async with PG('profile-name', cursor='dict') as db:
    ...
"""
import secrets
from typing import Literal

import psycopg_pool
from psycopg import AsyncConnection, AsyncCursor
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
        self._conf = {
            'min_size': 1,
            'max_size': 2,
            'max_idle': 30,
            'max_lifetime': 300,
            **conf_profile
        }
        self._row_factory = self._CURSORS.get(cursor)
        self._ensure_pool()

    def _ensure_pool(self):
        if self.profile in PG._POOLS:
            return

        conf = {**self._conf}
        LOGGER.debug(f"connecting [{self.profile}], host: {conf.get('host')}, db: {conf.get('db')}")

        min_size = conf.pop('min_size', 1)
        max_size = conf.pop('max_size', 2)
        max_idle = conf.pop('max_idle', 30)
        max_lifetime = conf.pop('max_lifetime', 300)

        pool = psycopg_pool.AsyncConnectionPool(
            conninfo="",
            min_size=min_size,
            max_size=max_size,
            max_idle=max_idle,
            max_lifetime=max_lifetime,
            **conf
        )
        PG._POOLS[self.profile] = pool

    def __str__(self) -> str:
        return f"profile: [{self.profile}], host: {self._conf.get('host')}, db: {self._conf.get('db')}"

    def __repr__(self) -> str:
        return self.__str__()

    async def __aenter__(self):
        self._conn = await self.connect()
        self._cursor = self._conn.cursor(row_factory=self._row_factory)
        return self

    async def connect(self) -> AsyncConnection:
        return await PG._POOLS.get(self.profile).getconn()

    def cursor(self, cursor: Literal['tuple', 'dict', 'namedtuple'] = 'tuple') -> AsyncCursor:
        return self._conn.cursor(row_factory=self._CURSORS.get(cursor))

    async def execute(self, sql: str, params: list | tuple | None = None, *, commit: bool = False) -> AsyncCursor:
        await self._cursor.execute(sql, params)
        if commit:
            await self.commit()
        return self._cursor

    async def executemany(self, sql: str, params: list | tuple | None = None, *, commit: bool = False) -> AsyncCursor:
        await self._cursor.executemany(sql, params)
        if commit:
            await self.commit()
        return self._cursor

    async def fetchone(self, sql: str, params: list | tuple | None = None, *, commit: bool = False):
        await self._cursor.execute(sql, params)
        if commit:
            await self.commit()
        return await self._cursor.fetchone()

    async def fetchall(self, sql: str, params: list | tuple | None = None, *, commit: bool = False):
        await self._cursor.execute(sql, params)
        if commit:
            await self.commit()
        return await self._cursor.fetchall()

    async def fetch(self, sql: str, params: list | tuple | None = None, batch=2000, *, commit: bool = False):
        name = f"{strings.sha256(sql)}-{hash(','.join(params)) if params else 0}-{secrets.token_hex()}"
        cursor = self._conn.cursor(name=name, row_factory=self._row_factory)
        try:
            await cursor.execute(sql, params)
            if commit:
                await self.commit()
            while True:
                records = await cursor.fetchmany(size=batch)
                if not records:
                    break
                for record in records:
                    yield record
        finally:
            await cursor.close()

    async def commit(self):
        await self._conn.commit()

    async def rollback(self):
        await self._conn.rollback()

    async def __aexit__(self, _type, _value, _trace):
        await self._cursor.close()
        await PG._POOLS.get(self.profile).putconn(self._conn)
