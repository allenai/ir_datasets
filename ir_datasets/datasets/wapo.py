import io
import json
import tarfile
from typing import NamedTuple, Tuple, Optional
import ir_datasets
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Lazy, DownloadConfig, Migrator
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels, GenericQuery, GenericQrel, TrecQueries, TrecQrels


NAME = 'wapo'


CORE_QREL_DEFS = {
    0: "not relevant",
    1: "relevant",
    2: "highly relevant",
}

BL_QREL_DEFS = {
    0: 'The document provides little or no useful background information.',
    2: 'The document provides some useful background or contextual information that would help the user understand the broader story context of the target article.',
    4: 'The document provides significantly useful background ...',
    8: 'The document provides essential useful background ...',
    16: 'The document _must_ appear in the sidebar otherwise critical context is missing.',
}

RM_TAGS = [' </num>', 'Narrative\n', '</docid>', '</url>']

BL_MAP = {
    ' *<num> Number: ': 'query_id',
    ' *<docid>': 'doc_id',
    ' *<url>': 'url',
}


class WapoDocMedia(NamedTuple):
    type: str
    url: str
    text: str


class WapoDoc(NamedTuple):
    doc_id: str
    url: str
    title: str
    author: str
    published_date: int
    kicker: str
    body: str
    body_paras_html: Tuple[str, ...]
    body_media: Tuple[WapoDocMedia, ...]


class TrecBackgroundLinkingQuery(NamedTuple):
    query_id: str
    doc_id: str
    url: str


class WapoDocs(BaseDocs):
    def __init__(self, dlc, version, count_hint=None, date_mode='field'):
        self._dlc = dlc
        self._version = version
        self._count_hint = count_hint
        self._date_mode = date_mode

    def docs_path(self):
        return self._dlc.path()

    def docs_cls(self):
        return WapoDoc

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._dlc.stream() as stream:
            with tarfile.open(fileobj=stream, mode='r|gz') as tarf:
                for member in tarf:
                    if member.name != f'WashingtonPost.{self._version}/data/TREC_Washington_Post_collection.{self._version}.jl':
                        continue
                    file = tarf.extractfile(member)
                    for line in file:
                        doc_json = json.loads(line)
                        body = ''
                        kicker = ''
                        date = doc_json.get('published_date') if self._date_mode in ('field', 'hybrid') else None
                        body_paras_html = []
                        body_media = []
                        for content in doc_json['contents']:
                            if content is None:
                                continue
                            if content.get('type') == 'kicker':
                                assert content['mime'] == 'text/plain'
                                if content['content'] is not None:
                                    kicker += content['content'] + '\n'
                            elif content.get('type') == 'sanitized_html':
                                if content.get('content') is not None:
                                    body_paras_html.append(content['content'])
                                    if content.get('mime') == 'text/html':
                                        body += ir_datasets.formats.html.textify_sax(content['content']) + '\n'
                                    else:
                                        body += content['content'] + '\n'
                            elif content.get('type') in ['image', 'tweet', 'video', 'gallery']:
                                url = {
                                    'image': lambda: content['imageURL'],
                                    'video': lambda: content['contenturl'],
                                    'gallery': lambda: content['contenturl'],
                                    'tweet': lambda: f"https://twitter.com/{content['content']['user']['screen_name']}/status/{content['content']['id_str']}",
                                }[content['type']]()
                                text = {
                                    'image': lambda: content.get('fullcaption'),
                                    'video': lambda: content.get('blurb'),
                                    'gallery': lambda: content.get('blurb'),
                                    'tweet': lambda: content['content']['text'],
                                }[content['type']]()
                                body_media.append(WapoDocMedia(content['type'], url, text))
                                if text is not None:
                                    body += text + '\n'
                            elif self._date_mode in ('hybrid', 'contents') and content.get('type') == 'date' and isinstance(content['content'], int):
                                # from v3 onward, date can be provided in contents instead
                                date = content['content']
                        yield WapoDoc(
                            doc_json['id'],
                            doc_json['article_url'],
                            doc_json['title'],
                            doc_json['author'],
                            date,
                            kicker.rstrip('\n'),
                            body.rstrip('\n'),
                            tuple(body_paras_html),
                            tuple(body_media))

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        return self.docs_store().count()

    def docs_namespace(self):
        return f'{NAME}/{self._version}'

    def docs_lang(self):
        return 'en'


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection_v2 = WapoDocs(dlc['v2'], version='v2', count_hint=595037, date_mode='field')
    collection_v3 = WapoDocs(dlc['v3'], version='v3', count_hint=671947, date_mode='hybrid')
    collection_v4 = WapoDocs(dlc['v4'], version='v4', count_hint=728626, date_mode='hybrid')

    base = Dataset(documentation('_'))

    subsets['v2'] = Dataset(
        collection_v2,
        documentation('v2'))

    subsets['v3'] = Dataset(
        collection_v3,
        documentation('v3'))

    subsets['v4'] = Dataset(
        collection_v4,
        documentation('v4'))

    subsets['v2/trec-core-2018'] = Dataset(
        collection_v2,
        TrecQueries(dlc['trec-core-2018/queries'], namespace='trec-core-2018', lang='en', remove_tags=RM_TAGS),
        TrecQrels(dlc['trec-core-2018/qrels'], CORE_QREL_DEFS),
        documentation('v2/trec-core-2018'))

    subsets['v2/trec-news-2018'] = Dataset(
        collection_v2,
        TrecQueries(dlc['trec-news-2018/queries'], namespace='trec-news-2018', lang='en', qtype=TrecBackgroundLinkingQuery, qtype_map=BL_MAP, remove_tags=RM_TAGS),
        TrecQrels(dlc['trec-news-2018/qrels'], BL_QREL_DEFS),
        documentation('v2/trec-news-2018'))

    subsets['v2/trec-news-2019'] = Dataset(
        collection_v2,
        TrecQueries(dlc['trec-news-2019/queries'], namespace='trec-news-2019', lang='en', qtype=TrecBackgroundLinkingQuery, qtype_map=BL_MAP, remove_tags=RM_TAGS),
        TrecQrels(dlc['trec-news-2019/qrels'], BL_QREL_DEFS),
        documentation('v2/trec-news-2019'))

    subsets['v3/trec-news-2020'] = Dataset(
        collection_v3,
        TrecQueries(dlc['trec-news-2020/queries'], namespace='trec-news-2020', lang='en', qtype=TrecBackgroundLinkingQuery, qtype_map=BL_MAP, remove_tags=RM_TAGS),
        TrecQrels(dlc['trec-news-2020/qrels'], BL_QREL_DEFS),
        documentation('v3/trec-news-2020'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
