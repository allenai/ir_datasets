import io
import codecs
import re
import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.datasets import msmarco_passage
from ir_datasets.formats import TsvQueries, TsvDocs

NAME = 'mmarco'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    1: 'Labeled by crowd worker as relevant'
}



def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)

    subsets = {}

    train_qrels = ir_datasets.registry['msmarco-passage/train'].qrels_handler()
    dev_qrels = ir_datasets.registry['msmarco-passage/dev'].qrels_handler()

    for lang in ['es', 'fr', 'pt', 'it', 'id', 'de', 'ru', 'zh']:
        collection = TsvDocs(dlc[f'{lang}/docs'], namespace=f'mmarco/{lang}', lang=lang, count_hint=ir_datasets.util.count_hint(f'{NAME}/{lang}'))
        subsets[f'{lang}'] = Dataset(collection, documentation(f'{lang}'))
        subsets[f'{lang}/train'] = Dataset(
            collection,
            TsvQueries(dlc[f'{lang}/queries/train'], namespace=f'mmarco/{lang}', lang=lang),
            train_qrels,
            documentation(f'{lang}/train'))
        subsets[f'{lang}/dev'] = Dataset(
            collection,
            TsvQueries(dlc[f'{lang}/queries/dev'], namespace=f'mmarco/{lang}', lang=lang),
            dev_qrels,
            documentation(f'{lang}/dev'))

    ir_datasets.registry.register(NAME, Dataset(documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return collection, subsets


collection, subsets = _init()
