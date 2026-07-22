# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install motor

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
item1 = await mongo.find_by_id('col_name', _id)
item2 = await mongo.find_one('col_name', {'field': 'val'})
"""
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from . import config, singleton

UPDATE_OPS = ('$currentDate', '$inc', '$min', '$max', '$mul', '$rename', '$set', '$setOnInsert', '$unset')


def ensure_id_type(query: str | ObjectId | dict):
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
        self._conf = config.get(f"mongo.{self.profile}", {})
        assert len(self._conf) > 0, f'mongo profile not configured: mongo.{self.profile}'
        self.client = self._connect()
        self.db = self.get_db(db_name)

    def __str__(self) -> str:
        return repr(self.client)

    def __repr__(self):
        return self.__str__()

    def _connect(self):
        kwargs = {k: v for k, v in self._conf.items() if k != 'db'}
        return AsyncIOMotorClient(**kwargs)

    def switch_db(self, name=None):
        self.db = self.get_db(name)
        return self

    def get_db(self, name=None) -> AsyncIOMotorDatabase:
        if name is None:
            name = self._conf.get('db')
        return self.client[name]

    def col(self, name: str) -> AsyncIOMotorCollection:
        return self.db[name]

    async def is_collection_exist(self, collection_name):
        return len(await self.db.list_collection_names(filter={"name": collection_name})) > 0

    async def count(self, col_name: str, query: dict | None = None):
        query = ensure_id_type(query)
        return await self.col(col_name).count_documents(query or {})

    async def find_by_id(self, col_name: str, _id: str | ObjectId):
        _id = ensure_id_type(_id)
        return await self.col(col_name).find_one({'_id': _id})

    async def find_one(self, col_name: str, query: dict | None = None, projection: dict | None = None, **kwargs):
        query = ensure_id_type(query)
        return await self.col(col_name).find_one(query or {}, projection, **kwargs)

    async def find(self, col_name: str, query: dict | None = None, projection: dict | None = None, **kwargs):
        query = ensure_id_type(query)
        cursor = self.col(col_name).find(query or {}, projection, **kwargs)
        return await cursor.to_list(length=None)

    async def save(self, col_name: str, data: dict):
        _id = data.pop('_id', None)
        if _id is None:
            return await self.col(col_name).insert_one(data)
        else:
            _id = ensure_id_type(_id)
            rt = await self.col(col_name).update_one({'_id': _id}, {"$set": data})
            if rt.matched_count == 0:
                return await self.col(col_name).insert_one(data)
            else:
                return rt

    async def update_one(self, col_name: str, query: dict, data: dict):
        query = ensure_id_type(query)
        if not any(op in data for op in UPDATE_OPS):
            data = {'$set': data}
        return await self.col(col_name).update_one(query, data)

    async def update(self, col_name: str, query: dict, data: dict):
        query = ensure_id_type(query)
        if not any(op in data for op in UPDATE_OPS):
            data = {'$set': data}
        return await self.col(col_name).update_many(query, data)

    async def delete_by_id(self, col_name: str, _id: str | ObjectId):
        _id = ensure_id_type(_id)
        return await self.col(col_name).delete_one({'_id': _id})

    async def delete_one(self, col_name: str, query: dict):
        query = ensure_id_type(query)
        return await self.col(col_name).delete_one(query)

    async def delete(self, col_name: str, query: dict):
        query = ensure_id_type(query)
        return await self.col(col_name).delete_many(query)

    async def bulk(self, col_name: str, batch: list, ordered=True, bypass_document_validation=False):
        return await self.col(col_name).bulk_write(batch, ordered=ordered, bypass_document_validation=bypass_document_validation)

    async def agg(self, col_name: str, pipeline: dict):
        cursor = self.col(col_name).aggregate(pipeline)
        return await cursor.to_list(length=None)

    async def drop(self, col_name):
        return await self.col(col_name).drop()

    async def copy_col(self, col_name_src: str, col_name_tgt: str, query: dict | None = None):
        assert col_name_src is not None
        assert col_name_tgt is not None
        assert col_name_src != col_name_tgt
        query = query or {}
        pipeline = [{"$match": query}, {"$out": col_name_tgt}]
        cursor = self.col(col_name_src).aggregate(pipeline)
        await cursor.to_list(length=None)
        return await self.count(col_name_tgt)

    async def find_one_and_update(self, col_name: str, query: dict, update: dict, **kwargs):
        return await self.col(col_name).find_one_and_update(query, update, return_document=True, **kwargs)

    async def find_one_and_replace(self, col_name: str, query: dict, replacement: dict, **kwargs):
        return await self.col(col_name).find_one_and_replace(query, replacement, return_document=True, **kwargs)

    async def find_one_and_delete(self, col_name: str, query: dict, projection: dict | None = None, **kwargs):
        return await self.col(col_name).find_one_and_delete(query, projection=projection, **kwargs)

    async def list_collections(self, filter=None, count=False, session=None, **kwargs):
        if filter and isinstance(filter, str):
            filter = {"name": {"$regex": filter}}
        col_names = await self.db.list_collection_names(filter=filter, session=session, **kwargs)
        if count:
            result = {}
            for col in col_names:
                result[col] = await self.count(col)
            return result
        return col_names

    async def get_collections_size(self):
        total = 0
        sizes = {}
        async with await self.client.start_session() as session:
            collection_names = list(sorted(await self.db.list_collection_names()))
            print("collections:")
            for col_name in collection_names:
                size = (await self.db.command({"collstats": col_name, 'scale': 1024 * 1024}, session=session)).get('size')
                sizes[col_name] = f"{size} MB"
                total += size
        sizes['total'] = f"{total} MB"
        return sizes

    async def print_collections_size(self):
        sizes = await self.get_collections_size()
        pad_size = max([len(col_name) for col_name in sizes]) + 1
        for col_name, size in sizes.items():
            print(f"{col_name: <{pad_size}}: {size}")

    def create_user(self,
                    username: str,
                    password: str,
                    db: str,
                    roles: list[str] | None = None):
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
        res = await self.get_db(db).command(
            'createUser',
            username,
            pwd=password,
            roles=roles
        )
        return res.get('ok', 0) > 0
