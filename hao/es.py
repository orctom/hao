# -*- coding: utf-8 -*-
"""
####################################################
###########         dependency          ############
####################################################
pip install elasticsearch>=8

####################################################
###########         config.yml          ############
####################################################
es:
  default:
    host: host-name
    port: 9200
    user: optional-username
    password: optional-password
    index: default-index
  some-other:
    host: hostname-b
    port: 59200
    index: default-index


####################################################
###########          usage              ############
####################################################
from hao.es import ES
es = ES()
es = ES('profile-name')

await es.delete_by_id(_id, index='optional-index')

await es.save(_id, data, index='optional-index', silent=False)

await es.update(_id, data, index='optional-index', silent=False)

await es.is_exists(_id, index='optional-index')

await es.get_by_id(_id, index='optional-index')

await es.get_by_ids(id_list, index='optional-index')

count = await es.count(query, index='optional-index')

items = [item async for item in await es.search(query, index='optional-index', size=200)]

items_generator = await es.search(query, index='optional-index', size=200, scroll='10m')

await es.delete_by_query(query, index='optional-index', timeout=600)

await es.delete_by_id(id, index='optional-index')

await es.bulk(actions)
"""
import html
from dataclasses import asdict, dataclass, field

from elasticsearch import AsyncElasticsearch, NotFoundError, helpers

from . import config, feishu, invoker, jsons, logs, regexes

LOGGER = logs.get_logger(__name__)


def connect(host, port, user=None, password=None, timeout=60, use_ssl=False, **kwargs) -> AsyncElasticsearch:
    LOGGER.debug(f"[es] connecting to {host}:{port}, use ssl: {use_ssl}")
    hosts = [{'host': host, 'port': port}]
    if user and password:
        return AsyncElasticsearch(hosts, http_auth=(user, password), timeout=timeout, use_ssl=use_ssl, **kwargs)
    return AsyncElasticsearch(hosts, timeout=timeout, use_ssl=use_ssl, **kwargs)


@dataclass
class Highlight:
    fields: dict
    fragmenter: str = field(default='span')
    fragment_size: int = field(default=200)
    number_of_fragments: int = field(default=5)
    pre_tags: list[str] = field(default_factory=lambda: ['<mark>'])
    post_tags: list[str] = field(default_factory=lambda: ['</mark>'])
    order: str = field(default='score')


class ES:

    def __init__(self, profile='default'):
        self.profile = profile
        self.__conf = config.get(f'es.{self.profile}')
        assert len(self.__conf) > 0, f'es profile not configured: es.{self.profile}'
        self.client: AsyncElasticsearch = invoker.invoke(connect, **self.__conf)

    def __str__(self) -> str:
        return f"profile: [{self.profile}], host: {self.__conf.get('host')}, port: {self.__conf.get('port')}"

    def __repr__(self) -> str:
        return self.__str__()

    async def get_by_id(self, _id, index: str, params: dict | None = None, **kwargs):
        assert _id is not None, '_id required'
        assert index is not None, 'index required'

        try:
            return await self.client.get(index=index, id=_id, params=params, **kwargs)
        except NotFoundError:
            return None

    async def find_by_id(self, _id, index: str, **params) -> list:
        query = {"query": {"term": {"_id": _id}}}
        return [item async for item in self.search(query, index, **params)]

    async def get_by_ids(self, _ids, index: str, **params):
        assert _ids is not None and len(_ids) > 0, '_ids required and should not be empty'
        assert index is not None, 'index reequired'

        try:
            result = await self.client.mget(index=index, body={'ids': _ids}, params=params)
            return result.get('docs') if result else None
        except NotFoundError:
            return None

    async def find_by_ids(self, ids: list[str], index: str, **params) -> list:
        query = {"query": {"ids" : {"values": ids}}}
        return [item async for item in self.search(query, index, **params)]

    async def count(self, query: dict, index: str, **params):
        assert query is not None and len(query) > 0, 'query required, and should not be empty'
        assert index is not None, 'index required'

        body = query.copy()
        for name in ['track_total_hits', 'from', 'size', '_source', 'sort', 'highlight']:
            body.pop(name, None)

        data = await self.client.count(
            index=index,
            body=body,
            params=params
        )
        return data['count']

    def search(self,
               query: dict,
               index: str,
               size=500,
               highlight: dict | None = None,
               highlight_fields: dict | list | None = None,
               scroll: str | None = None,
               timeout=60,
               **params):
        assert query is not None and len(query) > 0, 'query required, and should not be empty'
        assert index is not None, 'index required'
        assert (highlight is None and highlight_fields is None) or (scroll is None), 'highlight not supported with scroll'
        if scroll is None or len(scroll) == 0:
            async for item in self._search(query, index, size, highlight, highlight_fields, timeout, **params):
                yield item
        else:
            async for item in self._search_scroll(query, index, size, scroll, timeout, **params):
                yield item

    async def _search(self,
                      query: dict,
                      index: str,
                      size=500,
                      highlight: dict | None = None,
                      highlight_fields: dict | list = None,
                      timeout=60,
                      **params):
        pre_tags, post_tags = None, None
        if highlight:
            query['highlight'] = highlight
            pre_tags, post_tags = highlight.get('pre_tags'), highlight.get('post_tags')
        elif highlight_fields:
            if isinstance(highlight_fields, list):
                highlight_fields = {f: {} for f in highlight_fields}
            highlight = asdict(Highlight(fields=highlight_fields))
            query['highlight'] = highlight
            pre_tags, post_tags = highlight.get('pre_tags'), highlight.get('post_tags')

        data = await self.client.search(index=index, size=size, body=query, request_timeout=timeout, params=params)
        hits = data['hits']['hits']
        do_escape = pre_tags and post_tags and len(pre_tags) == len(post_tags)
        for hit in hits:
            yield self._html_escape(hit, pre_tags, post_tags) if do_escape else hit

    async def _search_scroll(self, query: dict, index: str, size: int, scroll: str, timeout=60, **params):
        data = await self.client.search(index=index, scroll=scroll, size=size, body=query, request_timeout=timeout, params=params)
        sid = data['_scroll_id']
        hits = data['hits']['hits']
        try:
            while sid and hits:
                for hit in hits:
                    yield hit

                data = await self.client.scroll(scroll_id=sid, scroll=scroll)
                sid = data['_scroll_id']
                hits = data['hits']['hits']
        finally:
            try:
                await self.client.clear_scroll(scroll_id=sid, ignore=(404,))
            except Exception:
                pass

    @staticmethod
    def _html_escape(item: dict, pre_tags: list[str], post_tags: list[str]):
        def convert(text):
            for pre_tag in pre_tags:
                text = text.replace(pre_tag, f"lll-{regexes.remove_non_char(pre_tag)}-lll")
            for post_tag in post_tags:
                text = text.replace(post_tag, f"rrr-{regexes.remove_non_char(post_tag)}-rrr")

            text = regexes.remove_html_tags(text)
            text = html.escape(text)

            for pre_tag in pre_tags:
                text = text.replace(f"lll-{regexes.remove_non_char(pre_tag)}-lll", pre_tag)
            for post_tag in post_tags:
                text = text.replace(f"rrr-{regexes.remove_non_char(post_tag)}-rrr", post_tag)

            return text

        highlights = item.get('highlight')
        if highlights:
            item['highlight'] = {field: [convert(entry) for entry in entries] for field, entries in highlights.items()}
        return item

    async def aggs(self, query: dict, index: str, timeout=15, **params):
        assert query is not None and len(query) > 0, 'query required, and should not be empty'
        assert index is not None, 'index required'

        data = await self.client.search( index=index, size=0, body=query, request_timeout=timeout, params=params)
        buckets = {k: v.get('buckets') for k, v in data.get('aggregations').items()}
        total = data['hits']['total']
        return buckets, total

    async def delete_by_id(self, _id, index: str, silent=True, timeout=30, **kwargs) -> bool:
        assert _id is not None, '_id required'
        assert index is not None, 'index required'

        try:
            await self.client.delete(index=index, id=_id, request_timeout=timeout, params=kwargs)
            return True
        except NotFoundError as e:
            if silent:
                return False
            else:
                raise e

    async def delete_by_query(self, query, index: str, silent=True, timeout=30, **params):
        assert query is not None and len(query) > 0, 'query required, and should not be empty'
        assert index is not None, 'index required'

        try:
            return await self.client.delete_by_query(index=index, body=query, request_timeout=timeout, params=params)
        except NotFoundError as e:
            if silent:
                LOGGER.error(f"Failed to delete_by_query: {query}, index: {index}")
                LOGGER.exception(e)
                feishu.notify_exception(e, f"{jsons.dumps(query)}, index: {index}")
            else:
                raise e

    async def save(self, _id, doc, index: str, overwrite=True, silent: bool = True, **params):
        assert _id is not None, '_id required'
        assert index is not None, 'index required'

        if doc is None:
            return
        try:
            index = index or self.index
            if not overwrite and await self.is_exists(_id, index=index):
                return
            await self.client.index(index, doc, id=_id, params=params)
            return _id
        except Exception as e:
            if silent:
                LOGGER.error(f"Failed to process: {doc}")
                LOGGER.exception(e)
                feishu.notify_exception(e, f'{_id}\n{jsons.dumps(doc)}')
            else:
                raise e

    async def update(self, _id, doc, index: str, **params):
        assert _id is not None, '_id required'
        assert index is not None, 'index required'

        if doc is None:
            return
        await self.client.update(index, id=_id, body={'doc': doc}, params=params)

    async def is_exists(self, _id, index: str, source=False):
        assert _id is not None, '_id required'
        assert index is not None, 'index required'

        return await self.client.exists(index=index, id=_id, _source=source)

    async def bulk(self, actions, stats_only=False, *args, **kwargs):
        await helpers.async_bulk(self.client, actions, stats_only=stats_only, *args, **kwargs)
