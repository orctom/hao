# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install sqlalchemy

####################################################
###########         config.yml          ############
####################################################
# https://docs.sqlalchemy.org/en/14/core/engines.html
db:
  default:
    url: postgresql://user:password@host:port/db
    hide_parameters: false
    echo: true
  db2:
    url: mysql://scott:tiger@hostname/dbname
    pool_size: 100


####################################################
###########          usage              ############
####################################################
from hao.pg import PG
session = DB().session()
try:
    session.add(some_object)
    session.add(some_other_object)
    session.commit()
finally:
    session.close()

with DB('db2').session(ctx=True) as session:
    session.add(some_object)
    session.add(some_other_object)
# commits transaction, closes session
    ...
"""
from sqlalchemy import Connection, engine_from_config
from sqlalchemy.orm import Session, scoped_session, sessionmaker

from . import config, jsons, singleton


class DB(metaclass=singleton.Multiton):
    def __init__(self, profile='default') -> None:
        self.profile = profile
        self.engine = self._create_engine()
        self._session = self._create_session()

    def _create_engine(self):
        params_default = {
            'hide_parameters': True,
            'json_serializer': jsons.dumps,
            'pool_pre_ping': True,
            'pool_size': 5,
            'pool_recycle': 3500 * 6,
        }
        cfg = {**config.get(f"db.{self.profile}"), **params_default}
        return engine_from_config(cfg, prefix='')

    def _create_session(self):
        return sessionmaker(self.engine)

    def session(self) -> Session:
        return self._session()

    def ctx_session(self) -> Session:
        return self._session.begin()

    def scoped_session(self) -> Session:
        return scoped_session(self._session)

    def connection(self) -> Connection:
        return self.engine.connect()
