import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.formats import TrecQrels, TrecDocs, TrecQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'trec-arabic'

QREL_DEFS = {
    1: 'relevant',
    0: 'not relevant',
}

QTYPE_MAP = {
    '<num> *(Number:)? *AR': 'query_id', # Remove AR prefix from QIDs
    '<title> *(Topic:)?': 'title',
    '<desc> *(Description:)?': 'description',
    '<narr> *(Narrative:)?': 'narrative'
}


def _init():
    subsets = {}
    base_path = ir_datasets.util.cache_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection = TrecDocs(dlc['docs'], encoding='utf8', path_globs=['arabic_newswire_a/transcripts/*/*.sgm.gz'])

    base = Dataset(collection, documentation('_'))

    subsets['ar2001'] = Dataset(
        TrecQueries(dlc['ar2001/queries'], qtype_map=QTYPE_MAP, encoding='ISO-8859-6'),
        TrecQrels(dlc['ar2001/qrels'], QREL_DEFS),
        collection,
        documentation('ar2001'))

    subsets['ar2002'] = Dataset(
        TrecQueries(dlc['ar2002/queries'], qtype_map=QTYPE_MAP, encoding='ISO-8859-6'),
        TrecQrels(dlc['ar2002/qrels'], QREL_DEFS),
        collection,
        documentation('ar2002'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
