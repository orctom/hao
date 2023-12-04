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
        self.__conf = config.get(f"redis.{self.profile}", {})
        assert len(self.__conf) > 0, f'redis profile not configured: redis.{self.profile}'
        self._ensure_pool()
        super().__init__(connection_pool=self._POOLS.get(self.profile))

    def _ensure_pool(self):
        if self.profile in Redis._POOLS:
            return

        pool = r.ConnectionPool(
            host=self.__conf.get('host', '127.0.0.1'),
            port=self.__conf.get('port', 6379),
            password=self.__conf.get('password'),
            socket_connect_timeout=self.__conf.get('timeout', 7),
            decode_responses=self.__conf.get('decode_responses', True)
        )
        Redis._POOLS[self.profile] = pool

    def __str__(self) -> str:
        return f"host: {self.__conf.get('host', '127.0.0.1')}, port:{self.__conf.get('port', 6379)}"

    def __repr__(self) -> str:
        return self.__str__()

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
