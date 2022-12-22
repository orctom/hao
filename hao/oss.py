# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install oss2
"""
import collections
import configparser
import logging
import os
import random
import sys
from typing import List, Optional, Union

import oss2
import requests
from tqdm import tqdm

from . import logs, paths, regexes
from .config import Config, get_config
from .stopwatch import Stopwatch

LOGGER = logs.get_logger(__name__, level=logging.INFO)

P_IGNORED = regexes.re_compile([
    r'\.DS_Store',
    r'Thumbs\.db',
    r'.+\.db',
])


class ProgressBar(object):

    def __init__(self, bar: tqdm, consumed=0) -> None:
        super().__init__()
        self.bar = bar
        self.consumed = consumed


class OSS(object):

    def __init__(self, access_id: str, access_secret: str, endpoint: str = 'http://oss-cn-beijing.aliyuncs.com') -> None:
        super().__init__()
        self.access_id = access_id
        self.access_secret = access_secret
        self.endpoint = endpoint
        self.progress_bars = {}

    @classmethod
    def from_ossutilconfig(cls):
        config_path = paths.get('~/.ossutilconfig')
        if not os.path.exists(config_path):
            LOGGER.error(f"[oss] Local config file not exist: {config_path}")
            return None
        cfg = configparser.ConfigParser()
        cfg.read(config_path)
        if 'Credentials' not in cfg:
            LOGGER.error(f"[oss] 'Credentials' not found in local config file: {config_path}")
            return None
        conf = cfg['Credentials']
        access_id = conf.get('accessKeyID')
        access_secret = conf.get('accessKeySecret')
        if access_id is None or access_secret is None:
            LOGGER.error(f"[oss] missing 'accessKeyID' or 'accessKeySecret' in local config file: {config_path}")
            return None
        endpoint = conf.get('endpoint', 'http://oss-cn-beijing.aliyuncs.com')
        return OSS(access_id, access_secret, endpoint)

    def _get_auth(self):
        return oss2.Auth(self.access_id, self.access_secret)

    def get_bucket(self, bucket_name: str) -> oss2.Bucket:
        auth = self._get_auth()
        return oss2.Bucket(auth, self.endpoint, bucket_name)

    def create_bucket(self, bucket_name: str):
        try:
            bucket = self.get_bucket(bucket_name)
            bucket.create_bucket()
            return bucket
        except oss2.exceptions.ServerError as e:
            if e.code == 'BucketAlreadyExists':
                LOGGER.info(f"[oss] bucket already exist: {bucket_name}")
                return self.get_bucket(bucket_name)
            raise e

    def delete_bucket(self, bucket_name: str):
        bucket = self.get_bucket(bucket_name)
        bucket.delete_bucket()

    def _update_progress_bar(self, file_name: str, total_bytes, consumed_bytes=0, mode='upload'):
        progress_bar = self.progress_bars.get(file_name)
        if progress_bar is None:
            if mode == 'upload':
                desc = f'{file_name} -> ({total_bytes / 1_000_000:.2f} m)'
            else:
                desc = f'({total_bytes / 1_000_000:.2f} m) -> {file_name}'

            colors = ['white', 'cyan', 'blue', 'green', 'red', 'magenta', 'yellow']
            bar = tqdm(total=total_bytes, unit=' bytes', desc=desc, ascii=' ━', colour=random.choice(colors))
            progress_bar = ProgressBar(bar, 0)
            self.progress_bars[file_name] = progress_bar
        if consumed_bytes > 0:
            delta = consumed_bytes - progress_bar.consumed
            progress_bar.bar.update(delta)
            progress_bar.consumed = consumed_bytes
        if consumed_bytes == total_bytes:
            progress_bar.bar.close()
            del self.progress_bars[file_name]

    def _finish_progress_bar(self, key: str):
        if key not in self.progress_bars:
            return
        try:
            progress_bar, _ = self.progress_bars[key]
            progress_bar.update(progress_bar.total - progress_bar.consumed)
            progress_bar.close()
            del self.progress_bars[key]
        except Exception as e:
            LOGGER.warning(e)

    @staticmethod
    def _validate_oss_path(path):
        if path is None or not path.startswith('oss://'):
            raise ValueError(f"Invalid oss path: {path}")

    def _split_oss_path(self, oss_path):
        self._validate_oss_path(oss_path)
        chunks = oss_path[6:].split('/')
        return chunks[0], '/'.join(chunks[1:])

    def upload(self, path_local: str, path_oss: str, progress_bar: bool = True):
        def show_progress(consumed_bytes, total_bytes):
            if progress_bar:
                self._update_progress_bar(file_name, total_bytes, consumed_bytes, mode='upload')

        path_local = paths.get_path(path_local)
        if not os.path.exists(path_local):
            raise ValueError(f"Local rile not found: {path_local}")

        bucket_name, path_oss = self._split_oss_path(path_oss)

        bucket = self.get_bucket(bucket_name)

        LOGGER.info(f"[oss] {path_local} -> oss://{bucket_name}/{path_oss}")
        if os.path.isfile(path_local):
            file_name = os.path.basename(path_local)
            oss2.resumable_upload(bucket, path_oss, path_local, progress_callback=show_progress, num_threads=1)
            if progress_bar:
                self._finish_progress_bar(path_local)
        else:
            for root, sub_dirs, files in os.walk(path_local):
                for file in files:
                    if P_IGNORED.search(file) is not None:
                        continue
                    f_local = os.path.join(root, file)
                    f_remote = os.path.join(path_oss, self._remove_starting_slash(root.replace(path_local, '')), file)
                    file_name = self._remove_starting_slash(os.path.join(root, file).replace(path_local, ''))
                    oss2.resumable_upload(bucket, f_remote, f_local, progress_callback=show_progress, num_threads=1)
                    if progress_bar:
                        self._finish_progress_bar(f_local)

    def delete(self, path_oss: str):
        bucket_name, path_oss = self._split_oss_path(path_oss)
        bucket = self.get_bucket(bucket_name)
        LOGGER.info(f"[oss] deleting: oss://{bucket_name}/{path_oss}")
        if path_oss.endswith('/'):
            files = [obj.key for obj in oss2.ObjectIterator(bucket, prefix=path_oss)]
            if len(files) > 0:
                bucket.batch_delete_objects(files)
        else:
            bucket.delete_object(path_oss)

    def delete_files(self, oss_paths: List[str]):
        batch = collections.defaultdict(list)
        for path_oss in oss_paths:
            bucket_name, path_oss = self._split_oss_path(path_oss)
            batch[bucket_name].append(path_oss)
        for bucket_name, files in batch.items():
            bucket = self.get_bucket(bucket_name)
            bucket.batch_delete_objects(files)

    def list_dir(self, path_oss: str):
        bucket_name, path_oss = self._split_oss_path(path_oss)
        bucket = self.get_bucket(bucket_name)
        if not path_oss.endswith('/'):
            path_oss += '/'
        for obj in oss2.ObjectIterator(bucket, prefix=path_oss):
            yield obj.key

    def is_exist(self, path_oss: str):
        bucket_name, path_oss = self._split_oss_path(path_oss)
        bucket = self.get_bucket(bucket_name)
        if bucket.object_exists(path_oss):
            return True
        try:
            if not path_oss.endswith('/'):
                path_oss += '/'
            if next(oss2.ObjectIterator(bucket, prefix=path_oss)):
                return True
        except StopIteration:
            return False
        return False

    @staticmethod
    def _remove_starting_slash(file_remote: str):
        return file_remote[1:] if len(file_remote) > 0 and file_remote[0] == '/' else file_remote

    def download(self, path_oss: str, path_local: str, progress_bar: bool = True, overwrite: bool = False):
        def show_progress(consumed_bytes, total_bytes):
            if progress_bar:
                self._update_progress_bar(file_name, total_bytes, consumed_bytes, mode='download')

        def should_skip_download(_file_remote, _file_local):
            if not os.path.exists(_file_local):
                return False, None
            if not overwrite:
                return True, 'overwrite=False'
            try:
                crc_remote = bucket.head_object(_file_remote).headers.get('x-oss-hash-crc64ecma')
                crc_local = file_crc64(_file_local)
                return crc_remote == crc_local, 'checksum same'
            except oss2.exceptions.NotFound:
                return True, 'remote file missing'

        bucket_name, path_oss = self._split_oss_path(path_oss)
        path_local = paths.get(path_local)

        bucket = self.get_bucket(bucket_name)

        LOGGER.info(f"[oss] oss://{bucket_name}/{path_oss} -> {path_local}, overwrite: {overwrite}")
        if not path_oss.endswith('/'):
            if not bucket.object_exists(path_oss):
                LOGGER.warning(f"[oss] remote file NOT exist: {path_oss}")
                return
            should_skip, reason = should_skip_download(path_oss, path_local)
            if should_skip:
                LOGGER.debug(f"[oss] oss://{bucket_name}/{path_oss} -> {path_local}, skipped: {reason}")
                return
            paths.make_parent_dirs(path_local)
            if os.path.isdir(path_local):
                path_local = paths.get(path_local, os.path.basename(path_oss))
            file_name = os.path.basename(path_local)
            oss2.resumable_download(bucket, path_oss, path_local, progress_callback=show_progress, num_threads=1)
            self._finish_progress_bar(path_local)
        else:
            n_downloaded = 0
            for obj in oss2.ObjectIterator(bucket, prefix=path_oss):
                f_remote = obj.key
                if f_remote.endswith('/'):
                    continue

                n_downloaded += 1
                file_name = f_remote.replace(path_oss, '')
                f_local = os.path.join(path_local, file_name)
                should_skip, reason = should_skip_download(f_remote, f_local)
                if should_skip:
                    LOGGER.debug(f"[oss] oss://{bucket_name}/{f_remote} -> {f_local}, skipped: {reason}")
                    continue
                paths.make_parent_dirs(f_local)
                oss2.resumable_download(bucket, f_remote, f_local, progress_callback=show_progress, num_threads=1)
                self._finish_progress_bar(f_local)
            if n_downloaded == 0:
                LOGGER.warning(f"[oss] remote file NOT exist: oss://{bucket_name}/{path_oss}")


def file_crc64(file_name, block_size=64 * 1024, init_crc=0):
    with open(file_name, 'rb') as f:
        crc64 = oss2.utils.Crc64(init_crc)
        while True:
            data = f.read(block_size)
            if not data:
                break
            crc64.update(data)

    return str(crc64.crc)


def download():
    args = sys.argv
    if len(args) != 3:
        LOGGER.warn(f'Usage: h-oss-download oss://bucket/your/path local/path')
        return
    path_oss = args[1]
    path_local = args[2]
    OSS.from_ossutilconfig().download(path_oss, path_local)


def upload():
    args = sys.argv
    if len(args) != 3:
        LOGGER.warn(f'Usage: h-oss-upload local/path oss://bucket/your/path')
        return
    path_local = args[1]
    path_oss = args[2]
    OSS.from_ossutilconfig().upload(path_local, path_oss)


def init(key='oss.init',
         overwrite: bool = False,
         oss_keys='oss.keys',
         default: Union[list, dict] = None,
         config: Optional[Union[str, Config]] = None,
         module: Optional[str] = None):
    """
    Download from oss to local according to `oss.init` in config yml, which should be
        - a dict
        - a string pointing to a dict
        - a list of dict
        - a list of strings, each pointing to a dict

    the dict each contains:
        - oss: oss://{bucket}/{path}
        - local: the local path where to download to, optional, default to `~/.oss/{oss_path}`

    Optional oss_keys to set oss access key and access secret

    e.g.
    ::
        # yaml
        model:
          general:
            oss: oss://{bucket}/{path}/{to}/{file_a}
            local: {path_to_local_file_a}
          product:
            oss: oss://{bucket}/{path}/{to}/{file_b}
            local: {path_to_local_file_b}

        oss:
          init:
            - model.general
            - model.product
          keys:
            access_id: {{your_oss_access_key_id}}
            access_secret: {{your_oss_access_key_secret}}
            endpoint: {{optional endpoint}}

        python:
        hao.oss.init()                   # same as hao.oss.init('oss.init')
        hao.oss.init('oss.init')         # download `model.general` and `model.product`
        hao.oss.init('model.general')    # download `model.general`

    """
    def _download(_item):
        if isinstance(_item, str):
            _item = cfg.get(_item)
        if not isinstance(_item, dict):
            LOGGER.info(f"[oss] skipped invalid entry: {_item}")
            return
        path_oss = _item.get('oss')
        if path_oss is None:
            LOGGER.warning(f"[oss] init skipped invalid entry, missing `oss` in: {_item}")
            return
        path_local = paths.get(_item.get('local', paths.get('~/.oss/', path_oss)))
        if not overwrite and os.path.exists(path_local):
            return
        try:
            oss.download(path_oss=path_oss, path_local=path_local, overwrite=overwrite)
        except requests.ConnectionError as e:
            if not os.path.exists(path_local):
                raise e

    LOGGER.debug(f"[oss] init key: {key}")
    cfg = get_config(config, module)
    items = cfg.get(key)
    if items is None or len(items) == 0:
        if default is None:
            LOGGER.info(f"[oss] '{key}' not found in config yml, skipped.")
            return
        items = default
    sw = Stopwatch()

    oss_args = cfg.get(oss_keys)
    if oss_args and 'access_id' in oss_args and 'access_secret' in oss_args:
        oss = OSS(**oss_args)
    else:
        oss = OSS.from_ossutilconfig()

    if isinstance(items, str):
        items = cfg.get(items)

    if isinstance(items, list):
        for item in items:
            _download(item)
    elif isinstance(items, dict):
        _download(items)
    else:
        LOGGER.info(f"[oss] '{key}' not supported type")

    LOGGER.info(f'[oss] init key: {key} finished, took: {sw.took()}')
