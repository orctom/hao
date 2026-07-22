# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install "sqlalchemy[asyncio]"

####################################################
###########         config.yml          ############
####################################################
# https://docs.sqlalchemy.org/en/14/core/engines.html
db:
  default:
    url: postgresql+asyncpg://user:password@host:port/db
    hide_parameters: false
    echo: true
  db2:
    url: mysql+asyncmy://scott:tiger@hostname/dbname
    pool_size: 100


####################################################
###########          usage              ############
####################################################
from hao.db import DB

async with DB().ctx_session() as session:
    session.add(some_object)
    session.add(some_other_object)
# commits transaction, closes session
    ...

session = await DB().session()
try:
    session.add(some_object)
    session.add(some_other_object)
    await session.commit()
finally:
    await session.close()
"""
import asyncio
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_engine_from_config,
    async_scoped_session,
    async_sessionmaker,
)

from . import config, jsons, singleton


class DB(metaclass=singleton.Multiton):
    def __init__(self, profile='default') -> None:
        self.profile = profile
        conf_profile = config.get(f"db.{self.profile}", {})
        assert len(conf_profile) > 0, f'db profile not configured: db.{self.profile}'
        self.__conf = {
            'hide_parameters': True,
            'json_serializer': jsons.dumps,
            'pool_pre_ping': True,
            'pool_size': 5,
            'pool_recycle': 3500 * 6,
            **conf_profile
        }
        self.engine = self._create_engine()
        self._session = self._create_session()

    def __str__(self) -> str:
        return f"url: {self.engine.url}"

    def __repr__(self) -> str:
        return self.__str__()

    def _create_engine(self) -> AsyncEngine:
        return async_engine_from_config(self.__conf, prefix='')

    def _create_session(self):
        return async_sessionmaker(self.engine, expire_on_commit=False)

    async def session(self) -> AsyncSession:
        return self._session()

    @asynccontextmanager
    async def ctx_session(self):
        async with self._session.begin() as session:
            yield session

    async def scoped_session(self) -> AsyncSession:
        return async_scoped_session(self._session, scopefunc=lambda: id(asyncio.get_running_loop()))

    async def connection(self) -> AsyncConnection:
        return await self.engine.connect()
