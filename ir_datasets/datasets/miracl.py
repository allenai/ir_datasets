import ir_datasets
from typing import NamedTuple
from ir_datasets.util import DownloadConfig, GzipExtract
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import JsonlDocs, TsvQueries, TrecQrels, TrecScoredDocs

NAME = 'miracl'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    0: 'Not Relevant',
    1: 'Relevant',
}

class MiraclDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    def default_text(self):
        return f'{self.title} {self.text}'


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)

    subsets = {}

    langs = [
        ('ar', 5, {'train', 'dev', 'test-a', 'test-b'}),
        ('bn', 1, {'train', 'dev', 'test-a', 'test-b'}),
        ('de', 32, {'dev', 'test-b'}),
        ('en', 66, {'train', 'dev', 'test-a', 'test-b'}),
        ('es', 21, {'train', 'dev', 'test-b'}),
        ('fa', 5, {'train', 'dev', 'test-b'}),
        ('fi', 4, {'train', 'dev', 'test-a', 'test-b'}),
        ('fr', 30, {'train', 'dev', 'test-b'}),
        ('hi', 2, {'train', 'dev', 'test-b'}),
        ('id', 3, {'train', 'dev', 'test-a', 'test-b'}),
        ('ja', 14, {'train', 'dev', 'test-a', 'test-b'}),
        ('ko', 3, {'train', 'dev', 'test-a', 'test-b'}),
        ('ru', 20, {'train', 'dev', 'test-a', 'test-b'}),
        ('sw', 1, {'train', 'dev', 'test-a', 'test-b'}),
        ('te', 2, {'train', 'dev', 'test-a', 'test-b'}),
        ('th', 2, {'train', 'dev', 'test-a', 'test-b'}),
        ('yo', 1, {'dev', 'test-b'}),
        ('zh', 10, {'train', 'dev', 'test-b'}),
    ]

    for lang, n_doc_files, topic_sets in langs:
        collection = JsonlDocs(
            [GzipExtract(dlc[f'v1.0/{lang}/corpus/{i}']) for i in range(n_doc_files)],
            doc_cls=MiraclDoc,
            mapping={'doc_id': 'docid', 'title': 'title', 'text': 'text'},
            namespace=f'{NAME}/{lang}',
            lang=lang,
            count_hint=ir_datasets.util.count_hint(f'{NAME}/{lang}'),
            docstore_path=base_path/'v1.0'/lang/'docs.pklz4')
        subsets[f'{lang}'] = Dataset(collection, documentation(f'{lang}'))
        if 'train' in topic_sets:
            subsets[f'{lang}/train'] = Dataset(
                collection,
                TsvQueries(dlc[f'v1.0/{lang}/train/topics'], namespace=f'{NAME}/{lang}', lang=lang),
                TrecQrels(dlc[f'v1.0/{lang}/train/qrels'], QRELS_DEFS),
                documentation(f'{lang}/train'))
        if 'dev' in topic_sets:
            subsets[f'{lang}/dev'] = Dataset(
                collection,
                TsvQueries(dlc[f'v1.0/{lang}/dev/topics'], namespace=f'{NAME}/{lang}', lang=lang),
                TrecQrels(dlc[f'v1.0/{lang}/dev/qrels'], QRELS_DEFS),
                documentation(f'{lang}/dev'))
        if 'test-a' in topic_sets:
            subsets[f'{lang}/test-a'] = Dataset(
                collection,
                TsvQueries(dlc[f'v1.0/{lang}/test-a/topics'], namespace=f'{NAME}/{lang}', lang=lang),
                documentation(f'{lang}/test-a'))
        if 'test-b' in topic_sets:
            subsets[f'{lang}/test-b'] = Dataset(
                collection,
                TsvQueries(dlc[f'v1.0/{lang}/test-b/topics'], namespace=f'{NAME}/{lang}', lang=lang),
                documentation(f'{lang}/test-b'))

    ir_datasets.registry.register(NAME, Dataset(documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return collection, subsets


collection, subsets = _init()
