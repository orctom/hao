# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install redis

####################################################
###########         config.yml          ############
####################################################
redis:
  default:
    host: localhost
    port: 6379
  gpu1:
    host: gpu1
    port: 26379
  gpu-lx:
    host: gpu-lx
    port: 26379

####################################################
###########          usage              ############
####################################################
from hao.redis import Redis
redis = Redis()
print(redis.scard('bidding-non-product-set'))

redis_gpu1 = Redis('gpu1')
print(redis_gpu1.scard('bidding-non-product-set'))
"""
import redis as r

from . import config


class Redis(r.Redis):
    _POOLS = {}

    def __init__(self, profile='default') -> None:
        self.profile = profile
        self._ensure_pool()
        super().__init__(connection_pool=self._POOLS.get(self.profile))

    def _ensure_pool(self):
        if self.profile in Redis._POOLS:
            return
        self._conf = config.get(f"redis.{self.profile}")
        if self._conf is None:
            raise ValueError(f'no config found for mongodb, expecting: `redis.{self.profile}`')

        pool = r.ConnectionPool(
            host=self._conf.get('host', '127.0.0.1'),
            port=self._conf.get('port', 6379),
            password=self._conf.get('password'),
            socket_connect_timeout=self._conf.get('timeout', 7),
            decode_responses=self._conf.get('decode_responses', True)
        )
        Redis._POOLS[self.profile] = pool

    def bf_add(self, key, *values):
        self.execute_command('BF.MADD', key, *values)

    def bf_exists(self, key, *values):
        results = list(map(lambda i: i == 1, self.execute_command('BF.MEXISTS', key, *values)))
        return results if len(results) > 1 else results[0]

    @staticmethod
    def connect(host: str,
                port: int = 6379,
                db: int = 0,
                decode_responses=True) -> r.Redis:
        return r.Redis(host=host, port=port, db=db, decode_responses=decode_responses)
