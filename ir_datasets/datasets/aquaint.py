import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.formats import TrecQrels, TrecDocs, TrecQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'aquaint'

QREL_DEFS = {
    2: 'highly relevant',
    1: 'relevant',
    0: 'not relevant',
}

QTYPE_MAP = {
    '<num> *(Number:)?': 'query_id',
    '<title> *(Topic:)?': 'title',
    '<desc> *(Description:)?': 'description',
    '<narr> *(Narrative:)?': 'narrative'
}


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection = TrecDocs(dlc['docs'], encoding='utf8', path_globs=['aquaint_comp/apw/*/*.gz', 'aquaint_comp/nyt/*/*.gz', 'aquaint_comp/xie/*/*.gz'], namespace=NAME, lang='en')

    base = Dataset(collection, documentation('_'))

    subsets['trec-robust-2005'] = Dataset(
        TrecQueries(dlc['trec-robust-2005/queries'], qtype_map=QTYPE_MAP, namespace='trec-robust', lang='en'),
        TrecQrels(dlc['trec-robust-2005/qrels'], QREL_DEFS),
        collection,
        documentation('trec-robust-2005'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
