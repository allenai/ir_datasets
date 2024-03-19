import io
import json
import tarfile
from typing import NamedTuple, Tuple
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
    def default_text(self):
        """
        title and body
        """
        return f'{self.title} {self.body}'


class TrecBackgroundLinkingQuery(NamedTuple):
    query_id: str
    doc_id: str
    url: str


class WapoDocs(BaseDocs):
    def __init__(self, dlc, file_name):
        self._dlc = dlc
        self._file_name = file_name

    def docs_path(self, force=True):
        return self._dlc.path(force)

    def docs_cls(self):
        return WapoDoc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        BeautifulSoup = ir_datasets.lazy_libs.bs4().BeautifulSoup
        for doc_json in self.docs_wapo_raw_iter():
            body = ''
            kicker = ''
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
                            body += BeautifulSoup(content['content'], 'lxml-xml').get_text() + '\n'
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
            yield WapoDoc(
                doc_json['id'],
                doc_json['article_url'],
                doc_json['title'],
                doc_json['author'],
                doc_json['published_date'],
                kicker.rstrip('\n'),
                body.rstrip('\n'),
                tuple(body_paras_html),
                tuple(body_media))

    def docs_wapo_raw_iter(self):
        with self._dlc.stream() as stream:
            with tarfile.open(fileobj=stream, mode='r|gz') as tarf:
                for member in tarf:
                    if member.name != self._file_name:
                        continue
                    file = tarf.extractfile(member)
                    for line in file:
                        doc_json = json.loads(line)
                        yield doc_json

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection_v2 = WapoDocs(dlc['v2'], 'WashingtonPost.v2/data/TREC_Washington_Post_collection.v2.jl')
    collection_v4 = WapoDocs(dlc['v4'], 'WashingtonPost.v4/data/TREC_Washington_Post_collection.v4.jl')

    base = Dataset(documentation('_'))

    subsets['v2'] = Dataset(
        collection_v2,
        documentation('v2'))

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
        TrecQueries(dlc['trec-news-2020/queries'], namespace='trec-news-2020', lang='en', qtype=TrecBackgroundLinkingQuery, qtype_map=BL_MAP, remove_tags=RM_TAGS),
        TrecQrels(dlc['trec-news-2020/qrels'], BL_QREL_DEFS),
        documentation('v3/trec-news-2020'))

    subsets['v4'] = Dataset(
        collection_v4,
        documentation('v4'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
