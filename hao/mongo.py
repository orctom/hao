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
from bson import ObjectId
mongo = Mongo()
mongo = Mongo('some-other')
item = mongo.db.collection_name.find_one({'_id': ObjectId(_id)})

collection = mongo.db['collection_name']

"""
from pymongo import MongoClient
from pymongo.common import KW_VALIDATORS, URI_OPTIONS_VALIDATOR_MAP, NONSPEC_OPTIONS_VALIDATOR_MAP, URI_OPTIONS_ALIAS_MAP, \
    INTERNAL_URI_OPTION_NAME_MAP, URI_OPTIONS_DEPRECATION_MAP, TIMEOUT_OPTIONS, WRITE_CONCERN_OPTIONS

from . import logs, config

LOGGER = logs.get_logger(__name__)

PARAMS = list(KW_VALIDATORS) + list(URI_OPTIONS_VALIDATOR_MAP) + list(NONSPEC_OPTIONS_VALIDATOR_MAP) + list(URI_OPTIONS_ALIAS_MAP) + \
         list(INTERNAL_URI_OPTION_NAME_MAP) + list(URI_OPTIONS_DEPRECATION_MAP) + list(TIMEOUT_OPTIONS) + list(WRITE_CONCERN_OPTIONS)


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


class Mongo(object):

    def __init__(self, profile='default') -> None:
        super().__init__()
        self.profile = profile
        self._conf = config.get(f"mongo.{profile}")
        if self._conf is None:
            raise ValueError(f'no config found for mongodb, expecting: `mongo.{profile}')
        self.client = connect(**self._conf)
        self.db = self.db()

    def db(self, name=None):
        if name is None:
            name = self._conf.get('db')
        return self.client[name]

    def is_collection_exist(self, collection_name):
        return len(self.db.list_collection_names(filter={"name": collection_name})) > 0
