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

RM_TAGS = [' </num>', 'Narrative\n']


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


class WapoDocs(BaseDocs):
    def __init__(self, dlc):
        self._dlc = dlc

    def docs_path(self):
        return self._dlc.path()

    def docs_cls(self):
        return WapoDoc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        BeautifulSoup = ir_datasets.lazy_libs.bs4().BeautifulSoup
        with self._dlc.stream() as stream:
            with tarfile.open(fileobj=stream, mode='r|gz') as tarf:
                for member in tarf:
                    if member.name != 'WashingtonPost.v2/data/TREC_Washington_Post_collection.v2.jl':
                        continue
                    file = tarf.extractfile(member)
                    for line in file:
                        doc_json = json.loads(line)
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

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_count(self):
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

    collection = WapoDocs(dlc['v2'])

    base = Dataset(documentation('_'))

    subsets['v2'] = Dataset(
        collection,
        documentation('v2'))

    subsets['v2/trec-core-2018'] = Dataset(
        collection,
        TrecQueries(dlc['trec-core-2018/queries'], namespace='trec-core-2018', lang='en', remove_tags=RM_TAGS),
        TrecQrels(dlc['trec-core-2018/qrels'], CORE_QREL_DEFS),
        documentation('v2/trec-core-2018'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
