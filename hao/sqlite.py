# -*- coding: utf-8 -*-
"""

####################################################
###########         dependency          ############
####################################################
pip install aiosqlite

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
async with SQLite() as db:
    records = await db.fetchall('select * from t_dummy_table')

"""

from collections import namedtuple
from typing import Literal

import aiosqlite

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
                 profile: str = 'default',
                 *,
                 path: str | None = None,
                 cursor: Literal['tuple', 'dict', 'namedtuple'] = 'tuple') -> None:
        self.profile = profile
        self.path = path
        self._path = paths.get(path or config.get(f"sqlite.{profile}.path"))
        self.conn: aiosqlite.Connection = None
        self._row_factory = self._CURSORS.get(cursor)

    async def connect(self):
        self.conn = await aiosqlite.connect(self._path)
        self.conn.row_factory = self._row_factory
        return self

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, _type, _value, _trace):
        try:
            await self.conn.close()
        except Exception:
            pass

    async def execute(self, sql, params: list | tuple | None = None, *, commit: bool = False):
        cursor = await self.conn.execute(sql, params or ())
        try:
            if commit:
                await self.commit()
            return cursor.rowcount
        finally:
            await cursor.close()

    async def executemany(self, sql, params: list | tuple | None = None, *, commit: bool = False):
        cursor = await self.conn.executemany(sql, params or ())
        try:
            if commit:
                await self.commit()
            return cursor.rowcount
        finally:
            await cursor.close()

    async def fetchone(self, sql: str, params: list | tuple | None = None, *, commit: bool = False):
        cursor = await self.conn.execute(sql, params or ())
        try:
            if commit:
                await self.commit()
            return await cursor.fetchone()
        finally:
            await cursor.close()

    async def fetchall(self, sql: str, params: list | tuple | None = None, *, commit: bool = False):
        cursor = await self.conn.execute(sql, params or ())
        try:
            if commit:
                await self.commit()
            return await cursor.fetchall()
        finally:
            await cursor.close()

    async def fetch(self, sql: str, params: list | tuple | None = None, batch=2000, *, commit: bool = False):
        cursor = await self.conn.execute(sql, params or ())
        try:
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
        return await self.conn.commit()

    async def rollback(self):
        return await self.conn.rollback()

    async def list_tables(self):
        sql = "SELECT name, sql FROM sqlite_master WHERE type='table'"
        return await self.fetchall(sql)

    async def show_table(self, table):
        sql = f"pragma table_info('{table}')"
        return await self.fetchall(sql)
