import json
import codecs
from typing import NamedTuple, Tuple
import ir_datasets
from ir_datasets.util import TarExtractAll, Cache, RelativePath, Lazy, Migrator
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQrels
from ir_datasets.formats import BaseDocs, TrecQrels
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.datasets import codec

_logger = ir_datasets.log.easy()


NAME = 'kilt'


CODEC_QREL_DEFS = {
    3: 'Very Valuable. It is absolutely critical to understand what this entity is for understanding this topic.',
    2: 'Somewhat valuable. It is important to understand what this entity is for understanding this topic.',
    1: 'Not Valuable. It is useful to understand what this entity is for understanding this topic.',
    0: 'Not Relevant. This entity is not useful or on topic.',
}


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
    def default_text(self):
        """
        title + text
        """
        return f'{self.title} {self.text}'


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
        for doc in self.docs_kilt_raw_iter():
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

    def docs_kilt_raw_iter(self):
        with self._streamer.stream() as stream:
            for doc in stream:
                yield json.loads(doc)


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    corpus = KiltDocs(dlc['knowledgesource'], count_hint=5903530)

    base = Dataset(
        corpus,
        documentation('_'))

    subsets = {}

    subsets['codec'] = Dataset(
        corpus,
        codec.base.queries_handler(),
        TrecQrels(dlc['codec/qrels'], CODEC_QREL_DEFS),
        documentation('codec'))

    for domain in codec.DOMAINS:
        queries_handler = codec.subsets[domain]
        subsets[f'codec/{domain}'] = Dataset(
            corpus,
            queries_handler,
            FilteredQrels(subsets['codec'].qrels_handler(), codec.filter_qids(domain, queries_handler), mode='include'),
            documentation(f'codec/{domain}'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
