import io
import codecs
import re
import ir_datasets
from ir_datasets.util import DownloadConfig, TarExtract, Cache
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.datasets import msmarco_passage
from ir_datasets.formats import TsvDocs

NAME = 'neumarco'


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)

    subsets = {}

    subsets_from_msmarco = {
        'train': [
            ir_datasets.registry['msmarco-passage/train'].queries_handler(),
            ir_datasets.registry['msmarco-passage/train'].qrels_handler(),
            ir_datasets.registry['msmarco-passage/train'].docpairs_handler(),
        ],
        'train/judged': [
            ir_datasets.registry['msmarco-passage/train/judged'].queries_handler(),
            ir_datasets.registry['msmarco-passage/train/judged'].qrels_handler(),
            ir_datasets.registry['msmarco-passage/train/judged'].docpairs_handler(),
        ],
        'dev': [
            ir_datasets.registry['msmarco-passage/dev'].queries_handler(),
            ir_datasets.registry['msmarco-passage/dev'].qrels_handler(),
        ],
        'dev/small': [
            ir_datasets.registry['msmarco-passage/dev/small'].queries_handler(),
            ir_datasets.registry['msmarco-passage/dev/small'].qrels_handler(),
        ],
        'dev/judged': [
            ir_datasets.registry['msmarco-passage/dev/judged'].queries_handler(),
            ir_datasets.registry['msmarco-passage/dev/judged'].qrels_handler(),
        ]
    }

    base_dlc = dlc['main']

    for lang3, lang2 in [('fas', 'fa'), ('zho', 'zh'), ('rus', 'ru')]:
        corpus_dlc = Cache(TarExtract(base_dlc, f'eng-{lang3}/msmarco.collection.20210731-scale21-sockeye2-tm1.tsv'), base_path/f'{lang2}.tsv')
        collection = TsvDocs(corpus_dlc, namespace=f'{NAME}/{lang2}', lang=lang2, count_hint=ir_datasets.util.count_hint(f'{NAME}/{lang2}'))
        subsets[f'{lang2}'] = Dataset(collection, documentation(f'{lang2}'))
        for s, items in subsets_from_msmarco.items():
            subsets[f'{lang2}/{s}'] = Dataset(
                collection,
                *items,
                documentation(f'{lang2}/{s}'))

    ir_datasets.registry.register(NAME, Dataset(documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return collection, subsets


collection, subsets = _init()
