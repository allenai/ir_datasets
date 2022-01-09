import io
import codecs
import re
import ir_datasets
from ir_datasets.util import DownloadConfig, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.datasets import msmarco_passage
from ir_datasets.formats import TsvQueries, TsvDocs, TrecQrels, TsvDocPairs, TrecScoredDocs

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
    train_docparis = TsvDocPairs(dlc['train/triples'])
    dev_qrels = TrecQrels(dlc['dev/qrels'], QRELS_DEFS)
    dev_small_qrels = TrecQrels(dlc['dev/qrels-small'], QRELS_DEFS)
    small_dev_qids = Lazy(lambda: {q.query_id for q in dev_small_qrels.qrels_iter()})

    for lang in ['es', 'fr', 'pt', 'it', 'id', 'de', 'ru', 'zh']:
        collection = TsvDocs(dlc[f'{lang}/docs'], namespace=f'mmarco/{lang}', lang=lang, count_hint=ir_datasets.util.count_hint(f'{NAME}/{lang}'))
        subsets[f'{lang}'] = Dataset(collection, documentation(f'{lang}'))
        subsets[f'{lang}/train'] = Dataset(
            collection,
            TsvQueries(dlc[f'{lang}/queries/train'], namespace=f'mmarco/{lang}', lang=lang),
            train_qrels,
            train_docparis,
            documentation(f'{lang}/train'))
        subsets[f'{lang}/dev'] = Dataset(
            collection,
            TsvQueries(dlc[f'{lang}/queries/dev'], namespace=f'mmarco/{lang}', lang=lang),
            dev_qrels,
            documentation(f'{lang}/dev'))
        subsets[f'{lang}/dev/small'] = Dataset(
            collection,
            FilteredQueries(subsets[f'{lang}/dev'].queries_handler(), small_dev_qids, mode='include'),
            dev_small_qrels,
            TrecScoredDocs(dlc[f'{lang}/scoreddocs/dev']) if lang not in ('zh', 'pt') else None,
            documentation(f'{lang}/dev/small'))
        if lang in ('zh', 'pt'):
            subsets[f'{lang}/dev/v1.1'] = Dataset(
                collection,
                TsvQueries(dlc[f'{lang}/queries/dev/v1.1'], namespace=f'mmarco/{lang}', lang=lang),
                dev_qrels,
                documentation(f'{lang}/dev/v1.1'))
            subsets[f'{lang}/dev/small/v1.1'] = Dataset(
                collection,
                FilteredQueries(subsets[f'{lang}/dev/v1.1'].queries_handler(), small_dev_qids, mode='include'),
                dev_small_qrels,
                TrecScoredDocs(dlc[f'{lang}/scoreddocs/dev/v1.1']),
                documentation(f'{lang}/dev/v1.1'))
        if lang in ('pt',):
            subsets[f'{lang}/train/v1.1'] = Dataset(
                collection,
                TsvQueries(dlc[f'{lang}/queries/train/v1.1'], namespace=f'mmarco/{lang}', lang=lang),
                train_qrels,
                train_docparis,
                documentation(f'{lang}/train/v1.1'))

    for lang in ['ar', 'zh', 'dt', 'fr', 'de', 'hi', 'id', 'it', 'ja', 'pt', 'ru', 'es', 'vi']:
        collection = TsvDocs(dlc[f'v2/{lang}/docs'], namespace=f'mmarco/{lang}', lang=lang, count_hint=ir_datasets.util.count_hint(f'{NAME}/v2/{lang}'))
        subsets[f'v2/{lang}'] = Dataset(collection, documentation(f'v2/{lang}'))
        subsets[f'v2/{lang}/train'] = Dataset(
            collection,
            TsvQueries(dlc[f'v2/{lang}/queries/train'], namespace=f'mmarco/v2/{lang}', lang=lang),
            train_qrels,
            train_docparis,
            documentation(f'v2/{lang}/train'))
        subsets[f'v2/{lang}/dev'] = Dataset(
            collection,
            TsvQueries(dlc[f'v2/{lang}/queries/dev'], namespace=f'v2/mmarco/{lang}', lang=lang),
            dev_qrels,
            documentation(f'v2/{lang}/dev'))
        subsets[f'v2/{lang}/dev/small'] = Dataset(
            collection,
            FilteredQueries(subsets[f'v2/{lang}/dev'].queries_handler(), small_dev_qids, mode='include'),
            dev_small_qrels,
            TrecScoredDocs(dlc[f'v2/{lang}/scoreddocs/dev'], negate_score=True),
            documentation(f'v2/{lang}/dev/small'))

    ir_datasets.registry.register(NAME, Dataset(documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return collection, subsets


collection, subsets = _init()
