# -*- coding: utf-8 -*-
# https://python-driver-for-arangodb.readthedocs.io/en/master/specs.html#standardcollection
"""
####################################################
###########         dependency          ############
####################################################
pip install python-arango

####################################################
###########         config.yml          ############
####################################################
arango:
  default:
    hosts: http://host:port
    username: username
    password: password
    db: db_name


####################################################
###########          usage              ############
####################################################
from hao.arango import Arango
arango = Arango()
arango.execute(aql, bind_vars={...})
"""

from multiprocessing import Lock

from arango import ArangoClient

from . import config, logs

LOGGER = logs.get_logger(__name__)


class Arango:
    __slots__ = ['profile', '__conf', '_lock', '_lock_graph', '_client', '_db', '_graph']

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self.__conf = config.get(f"arango.{profile}", {})
        assert len(self.__conf) > 0, f'arango profile not configured: arango.{self.profile}'
        LOGGER.debug(f"connecting to profile: {profile}")
        self._lock = Lock()
        self._lock_graph = Lock()
        self._client = ArangoClient(hosts=self.__conf.get('hosts'))
        self._db = None
        self._graph = None

    def __str__(self) -> str:
        return f"hosts: {self.__conf.get('hosts')}"

    def __repr__(self) -> str:
        return self.__str__()

    def db(self):
        if self._db is None:
            with self._lock:
                if self._db is None:
                    db = self.__conf.get('db')
                    username = self.__conf.get('username')
                    password = self.__conf.get('password')
                    if db is None or username is None or password is None:
                        raise ValueError(f'missing config for '
                                         f'`arango.{self.profile}.db` or '
                                         f'`arango.{self.profile}.username` or '
                                         f'`arango.{self.profile}.password`')
                    self._db = self._client.db(db, username=username, password=password)
        return self._db

    def graph(self):
        if self._graph is None:
            with self._lock_graph:
                if self._graph is None:
                    graph = self.__conf.get('graph')
                    if graph is None:
                        raise ValueError(f'missing config for `arango.{self.profile}.graph`')
                    self._graph = self.db().graph(graph)
        return self._graph

    def collection(self, collection_name, create_if_missing=True):
        db = self.db()
        if db.has_collection(collection_name):
            return db.collection(collection_name)
        elif create_if_missing:
            return db.create_collection(collection_name)
        else:
            return None

    def vertex_collection(self, collection_name, create_if_missing=True):
        graph = self.graph()
        if graph.has_vertex_collection(collection_name):
            return graph.vertex_collection(collection_name)
        elif create_if_missing:
            return graph.create_vertex_collection(collection_name)
        else:
            return None

    def execute(self,
                query,
                count=False,
                batch_size=None,
                ttl=None,
                bind_vars=None,
                full_count=None,
                max_plans=None,
                optimizer_rules=None,
                cache=None,
                memory_limit=0,
                fail_on_warning=None,
                profile=None,
                max_transaction_size=None,
                max_warning_count=None,
                intermediate_commit_count=None,
                intermediate_commit_size=None,
                satellite_sync_wait=None,
                read_collections=None,
                write_collections=None,
                stream=None,
                skip_inaccessible_cols=None):
        return self.db().aql.execute(
            query,
            count=count,
            batch_size=batch_size,
            ttl=ttl,
            bind_vars=bind_vars,
            full_count=full_count,
            max_plans=max_plans,
            optimizer_rules=optimizer_rules,
            cache=cache,
            memory_limit=memory_limit,
            fail_on_warning=fail_on_warning,
            profile=profile,
            max_transaction_size=max_transaction_size,
            max_warning_count=max_warning_count,
            intermediate_commit_count=intermediate_commit_count,
            intermediate_commit_size=intermediate_commit_size,
            satellite_sync_wait=satellite_sync_wait,
            read_collections=read_collections,
            write_collections=write_collections,
            stream=stream,
            skip_inaccessible_cols=skip_inaccessible_cols)
