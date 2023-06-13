# -*- coding: utf-8 -*-
"""
# option 1: ~/.s3config
[default]
endpoint = host:port
access_key = vNr2WOwm06lMGmpcUo9p
secret_key = IBvOcYjjPYB3PqGBpbryfqvxy4NWvX8oqSncYlwY

# option 2: config.yml
s3.{profile}
  endpoint: host:port
  access_key: vNr2WOwm06lMGmpcUo9p
  secret_key: IBvOcYjjPYB3PqGBpbryfqvxy4NWvX8oqSncYlwY
"""
import configparser
import logging
import math
import os
import random
import signal
from typing import Optional

from minio import Minio
from minio.error import MinioException
from tqdm import tqdm

from . import config, paths, strings
from .stopwatch import Stopwatch

LOGGER = logging.getLogger(__name__)

IGNORED = ('.DS_Store', 'Thumbs.db', 'Thumbs.db:encryptable', 'ehthumbs.db', 'ehthumbs_vista.db')


class S3:
    def __init__(self,
                 profile: str = 'default',
                 *,
                 endpoint: Optional[str] = None,
                 access_key: Optional[str] = None,
                 secret_key: Optional[str] = None,
                 secure: bool = False) -> None:
        if endpoint is None or access_key is None or secret_key is None:
            conf = config.get(f"s3.{profile}")
            if conf is None or len(conf) == 0:
                raise ValueError('Empty config')
            endpoint = conf.get('endpoint')
            access_key = conf.get('access_key')
            secret_key = conf.get('secret_key')
            secure = conf.get('secure', secure)
            if endpoint is None or access_key is None or secret_key is None:
                raise ValueError(f"`endpoint`, `access_key` and `secret_key` are required, but not satisfied (s3.{profile})")
        self.client = Minio(endpoint, access_key, secret_key, secure=secure)

    @classmethod
    def from_s3config(cls):
        config_file = '~/.s3config'
        config_path = paths.get(config_file)
        if not os.path.exists(config_path):
            raise ValueError(f"Config file not found: {config_file}")
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        if 'default' in cfg:
            cfg = cfg['default']
        endpoint = cfg.get('endpoint')
        access_key = cfg.get('access_key')
        secret_key = cfg.get('secret_key')
        secure = strings.boolean(cfg.get('secure'), False)
        if endpoint is None or access_key is None or secret_key is None:
            raise ValueError(f"`endpoint`, `access_key` and `secret_key` are required, but not satisfied ({config_file})")
        return S3(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

    def download(self, path_s3: str, path_local: str, overwrite: bool = False):
        assert path_s3 is not None
        assert path_local is not None
        path_local = paths.get(path_local)
        if not overwrite and os.path.exists(path_local):
            return

        response = None
        stop = False
        def handle_stop(signum, frame):
            nonlocal stop
            stop = True

        try:
            signal.signal(signal.SIGINT, handle_stop)
            signal.signal(signal.SIGTERM, handle_stop)
            splits = path_s3.split(os.path.sep)
            bucket_name = splits[0]
            object_name = os.path.sep.join(splits[1:])
            response = self.client.get_object(bucket_name, object_name)
            total_length = int(response.headers.get('content-length'))
            filename = os.path.basename(object_name)
            filesize_kb = total_length / 1024
            desc = f'{filename} -> ({filesize_kb / 1024:.2f} m)'
            color = random.choice(['cyan', 'blue', 'green', 'red', 'magenta', 'yellow'])
            bar = tqdm(total=math.ceil(filesize_kb), unit='k', ascii=' ―', colour=color, desc=desc)
            paths.make_parent_dirs(path_local)
            with open(path_local, 'wb') as f:
                for d in response.stream(1024 * 1024):
                    if stop:
                        raise KeyboardInterrupt()
                    size = f.write(d)
                    bar.update(size // 1024)
            bar.close()
        except KeyboardInterrupt:
            paths.delete(path_local)
            return
        except MinioException as e:
            paths.delete(path_local)
            raise e
        finally:
            if response:
                response.close()
                response.release_conn()


def init(key='s3.init', overwrite: bool = False):
    """
    Download from s3 to local according to `s3.init` in config yml, which should be
        - a dict
        - a string pointing to a dict
        - a list of dict
        - a list of strings, each pointing to a dict

    the dict each contains:
        - s3: the path in (including bucket name)
        - local: the local path where to download to

    Optional access_keys to set s3 access key and secret key

    e.g.
    ::
        model:
          general:
            s3: bidding/lx_20200414_all_BertBiRNNAttnCRF_149_f1_0.954.bin
            local: data/model/lx_20200414_all_BertBiRNNAttnCRF_149_f1_0.954.bin
          product:
            s3: bidding/lx_20200413_mn_BertBiRNNAttnCRF_200_f1_0.869.bin
            local: data/model/lx_20200413_mn_BertBiRNNAttnCRF_200_f1_0.869.bin

        s3:
          init:
            - model.general
            - model.product

        python:
        hao.s3.init()                   # same as spanner.s3.init('s3.init')
        hao.s3.init('s3.init')          # download `model.general` and `model.product`
        hao.s3.init('model.general')    # download `model.general`
    """

    s3 = S3.from_s3config()
    LOGGER.debug(f"[s3] init key: {key}")
    items = config.get(key)
    if items is None or len(items) == 0:
        LOGGER.error(f"[s3] '{key}' not found in config yml, skipped.")
        return
    sw = Stopwatch()

    if isinstance(items, str):
        items = config.get(items)

    if isinstance(items, list):
        for item in items:
            item = config.get(item)
            s3.download(item.get('s3'), item.get('local'), overwrite)
    elif isinstance(items, dict):
        s3.download(items.get('s3'), items.get('local'), overwrite)
    else:
        LOGGER.error(f"[s3] '{key}' not supported type")

    LOGGER.info(f'[s3] init key: {key} finished, took: {sw.took()}')
