# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install pymongo

####################################################
###########         config.yml          ############
####################################################
mongo:
  default:  # profile name
    host: localhost
    port: 27017
    username: username
    password: password
    db: db_name
  some-other:  # profile name
    host: 10.0.0.1
    port: 27017
    username: username
    password: password
    db: db_name

####################################################
###########          usage              ############
####################################################
from hao.mongo import Mongo
mongo = Mongo()
mongo_other = Mongo('some-other')
item1 = mongo.find_by_id('col_name', _id)
item2 = mongo.find_one('col_name', {'field': 'val'})
"""
from typing import List, Optional, Union

from bson import ObjectId
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.common import (
    INTERNAL_URI_OPTION_NAME_MAP,
    KW_VALIDATORS,
    NONSPEC_OPTIONS_VALIDATOR_MAP,
    TIMEOUT_OPTIONS,
    URI_OPTIONS_ALIAS_MAP,
    URI_OPTIONS_DEPRECATION_MAP,
    URI_OPTIONS_VALIDATOR_MAP,
    WRITE_CONCERN_OPTIONS,
)
from pymongo.database import Database

from . import config, singleton

PARAMS = list(KW_VALIDATORS) + list(URI_OPTIONS_VALIDATOR_MAP) + list(NONSPEC_OPTIONS_VALIDATOR_MAP) + list(URI_OPTIONS_ALIAS_MAP) + \
         list(INTERNAL_URI_OPTION_NAME_MAP) + list(URI_OPTIONS_DEPRECATION_MAP) + list(TIMEOUT_OPTIONS) + list(WRITE_CONCERN_OPTIONS)


UPDATE_OPS = ('$currentDate', '$inc', '$min', '$max', '$mul', '$rename', '$set', '$setOnInsert', '$unset')


def connect(host=None,
            port=None,
            document_class=dict,
            tz_aware=None,
            connect_now=None,
            type_registry=None,
            **kwargs):
    params = {'authSource': kwargs.get('db', 'admin')}
    kwargs = {k: v for k, v in kwargs.items() if k in PARAMS}
    params.update(kwargs)
    return MongoClient(host, port, document_class, tz_aware, connect_now, type_registry, **params)


def ensure_id_type(query: Union[str, ObjectId, dict]):
    if query is None:
        return None
    if isinstance(query, ObjectId):
        return query
    if isinstance(query, str):
        return ObjectId(query)
    if isinstance(query, dict):
        _id = query.get('_id')
        if _id:
            query['_id'] = ObjectId(_id)
        return query
    return query


class Mongo(object, metaclass=singleton.Multiton):

    def __init__(self, profile='default', db_name=None) -> None:
        super().__init__()
        self.profile = profile
        self._conf = config.get(f"mongo.{profile}")
        if self._conf is None:
            raise ValueError(f'no config found for mongodb, expecting: `mongo.{profile}')
        self.client = connect(**self._conf)
        self.db = self.get_db(db_name)

    def __str__(self) -> str:
        return f"{self.client.address} [{self.db.name}]"

    def __repr__(self):
        return self.__str__()

    def switch_db(self, name=None):
        self.db = self.get_db(name)
        return self

    def get_db(self, name=None) -> Database:
        if name is None:
            name = self._conf.get('db')
        return self.client[name]

    def col(self, name: str) -> Collection:
        return self.db[name]

    def is_collection_exist(self, collection_name):
        return len(self.db.list_collection_names(filter={"name": collection_name})) > 0

    def count(self, col_name: str, query: Optional[dict] = None):
        query = ensure_id_type(query)
        return self.col(col_name).count_documents(query or {})

    def find_by_id(self, col_name: str, _id: Union[str, ObjectId]):
        _id = ensure_id_type(_id)
        return self.col(col_name).find_one({'_id': _id})

    def find_one(self, col_name: str, query: Optional[dict] = None, projection: Optional[dict] = None, **kwargs):
        query = ensure_id_type(query)
        return self.col(col_name).find_one(query or {}, projection, **kwargs)

    def find(self, col_name: str, query: Optional[dict] = None, projection: Optional[dict] = None, **kwargs):
        query = ensure_id_type(query)
        return self.col(col_name).find(query or {}, projection, **kwargs)

    def save(self, col_name: str, data: dict):
        _id = data.pop('_id', None)
        if _id is None:
            return self.col(col_name).insert_one(data)
        else:
            _id = ensure_id_type(_id)
            rt = self.col(col_name).update_one({'_id': _id}, {"$set": data})
            if rt.matched_count == 0:
                return self.col(col_name).insert_one(data)
            else:
                return rt

    def update_one(self, col_name: str, query: dict, data: dict):
        query = ensure_id_type(query)
        if not any(op in data for op in UPDATE_OPS):
            data = {'$set': data}
        return self.col(col_name).update_one(query, data)

    def update(self, col_name: str, query: dict, data: dict):
        query = ensure_id_type(query)
        if not any(op in data for op in UPDATE_OPS):
            data = {'$set': data}
        return self.col(col_name).update_many(query, data)

    def delete_by_id(self, col_name: str, _id: Union[str, ObjectId]):
        _id = ensure_id_type(_id)
        return self.col(col_name).delete_one({'_id': _id})

    def delete_one(self, col_name: str, query: dict):
        query = ensure_id_type(query)
        return self.col(col_name).delete_one(query)

    def delete(self, col_name: str, query: dict):
        query = ensure_id_type(query)
        return self.col(col_name).delete_many(query)

    def bulk(self, col_name: str, batch: list, ordered=True, bypass_document_validation=False):
        return self.col(col_name).bulk_write(batch, ordered=ordered, bypass_document_validation=bypass_document_validation)

    def agg(self, col_name: str, pipeline: dict):
        return self.col(col_name).aggregate(pipeline)

    def drop(self, col_name):
        return self.col(col_name).drop()

    def copy_col(self, col_name_src: str, col_name_tgt: str, query: Optional[dict] = None):
        assert col_name_src is not None
        assert col_name_tgt is not None
        assert col_name_src != col_name_tgt
        query = query or {}
        pipeline = [{"$match": query}, {"$out": col_name_tgt}]
        self.col(col_name_src).aggregate(pipeline)
        return self.count(col_name_tgt)

    def find_one_and_update(self, col_name: str, query: dict, update: dict, return_document=ReturnDocument.AFTER, **kwargs):
        return self.col(col_name).find_one_and_update(query, update, return_document=return_document, **kwargs)

    def find_one_and_replace(self, col_name: str, query: dict, replacement: dict, return_document=ReturnDocument.AFTER, **kwargs):
        return self.col(col_name).find_one_and_replace(query, replacement, return_document=return_document, **kwargs)

    def find_one_and_delete(self, col_name: str, query: dict, projection: Optional[dict] = None, **kwargs):
        return self.col(col_name).find_one_and_delete(query, projection=projection, **kwargs)

    def list_collections(self, filter=None, count=False, session=None, **kwargs):
        if filter and isinstance(filter, str):
            filter = {"name": {"$regex": filter}}
        col_names = self.db.list_collection_names(filter=filter, session=session, **kwargs)
        return {col: self.count(col) for col in col_names} if count else col_names

    def get_collections_size(self):
        total = 0
        sizes = {}
        with self.client.start_session() as session:
            collection_names = list(sorted(self.db.list_collection_names()))
            print(f"collections:")
            for col_name in collection_names:
                size = self.db.command({"collstats": col_name, 'scale': 1024 * 1024}, session=session).get('size')
                sizes[col_name] = f"{size} MB"
                total += size
        sizes['total'] = f"{total} MB"
        return sizes

    def print_collections_size(self):
        sizes = self.get_collections_size()
        pad_size = max([len(col_name) for col_name in sizes]) + 1
        for col_name, size in sizes.items():
            print(f"{col_name: <{pad_size}}: {size}")

    def create_user(self,
                    username: str,
                    password: str,
                    db: str,
                    roles: Optional[List[str]] = None):
        """
        db.createUser(
          {
            user: "{username}",
            pwd:  "{password}",
            roles: [
                { role: "readWrite", db: "{db}" },
                { role: "dbAdmin", db: "{db}" }
            ]
          }
        )
        db.getUsers()
        """
        if roles is None:
            roles = ['readWrite', 'dbAdmin']
        roles = [{'role': r, 'db': db} for r in roles]
        res = self.get_db(db).command(
            'createUser',
            username,
            pwd=password,
            roles=roles
        )
        return res.get('ok', 0) > 0
