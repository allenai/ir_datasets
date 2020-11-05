from collections import namedtuple
import ir_datasets
from ir_datasets.util import GzipExtract, DownloadConfig
from ir_datasets.formats import TrecQrels, TrecDocs, TrecQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'trec-mandarin'

TrecMandarinQuery = namedtuple('TrecMandarinQuery', ['query_id', 'title_en', 'title_zh', 'description_en', 'description_zh', 'narrative_en', 'narrative_zh'])

QREL_DEFS = {
    1: 'relevant',
    0: 'not relevant',
}

QTYPE_MAP = {
    '<num> *(Number:)? *CH': 'query_id', # Remove CH prefix from QIDs
    '<E-title> *(Topic:)?': 'title_en',
    '<C-title> *(Topic:)?': 'title_zh',
    '<E-desc> *(Description:)?': 'description_en',
    '<C-desc> *(Description:)?': 'description_zh',
    '<E-narr> *(Narrative:)?': 'narrative_en',
    '<C-narr> *(Narrative:)?': 'narrative_zh',
}


def _init():
    subsets = {}
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.cache_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)

    collection = TrecDocs(dlc['docs'], encoding='GB18030', path_globs=['**/xinhua/x*', '**/peoples-daily/pd*'])

    base = Dataset(collection, documentation('_'))

    subsets['trec5'] = Dataset(
        TrecQueries(GzipExtract(dlc['trec5/queries']), qtype=TrecMandarinQuery, qtype_map=QTYPE_MAP, encoding='GBK'),
        TrecQrels(GzipExtract(dlc['trec5/qrels']), QREL_DEFS),
        collection,
        documentation('trec5'))

    subsets['trec6'] = Dataset(
        TrecQueries(GzipExtract(dlc['trec6/queries']), qtype=TrecMandarinQuery, qtype_map=QTYPE_MAP, encoding='GBK'),
        TrecQrels(GzipExtract(dlc['trec6/qrels']), QREL_DEFS),
        collection,
        documentation('trec6'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
