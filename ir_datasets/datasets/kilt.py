import json
import codecs
from typing import NamedTuple, Tuple
import ir_datasets
from ir_datasets.util import TarExtractAll, Cache, RelativePath, Lazy, Migrator
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import BaseDocs
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()


NAME = 'kilt'


class KiltDocAnchor(NamedTuple):
    text: str
    href: str
    paragraph_id: int
    start: int
    end: int


class KiltDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    text_pieces: Tuple[str, ...]
    anchors: Tuple[KiltDocAnchor, ...]
    categories: Tuple[str, ...]
    wikidata_id: str
    history_revid: str
    history_timestamp: str
    history_parentid: str
    history_pageid: str
    history_url: str


def strip_markup(text):
    if text.startswith('Section::::'):
        return text.replace('Section::::', '').replace(':', ' ')
    if text.startswith('BULLET::::-'):
        return text.replace('BULLET::::-', '-')
    return text


class KiltDocs(BaseDocs):
    def __init__(self, streamer, count_hint=None):
        super().__init__()
        self._streamer = streamer
        self._count_hint = count_hint

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._streamer.stream() as stream:
            for doc in stream:
                doc = json.loads(doc)
                yield KiltDoc(
                    doc['wikipedia_id'],
                    doc['wikipedia_title'],
                    ''.join(strip_markup(t) for t in doc['text']),
                    tuple(doc['text']),
                    tuple(KiltDocAnchor(
                        a['text'],
                        a['href'],
                        a['paragraph_id'],
                        a['start'],
                        a['end']) for a in doc['anchors']),
                    tuple(doc['categories'].split(',')),
                    doc.get('wikidata_info', {}).get('wikidata_id', ''),
                    str(doc['history']['revid']),
                    doc['history']['timestamp'],
                    str(doc['history']['parentid']),
                    str(doc['history']['pageid']),
                    doc['history']['url'],
                )

    def docs_cls(self):
        return KiltDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/docs.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(
        KiltDocs(dlc['knowledgesource'], count_hint=5903530),
        documentation('_'))

    subsets = {}

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
