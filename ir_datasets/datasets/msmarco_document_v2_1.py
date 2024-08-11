import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import TsvQueries
from ir_datasets.datasets.msmarco_passage import DUA
from ir_datasets.datasets.msmarco_document_v2 import MsMarcoV2Docs

_logger = ir_datasets.log.easy()

NAME = 'msmarco-document-v2.1'

def _init():
    base_path = ir_datasets.util.home_path()/NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    # we can re-use MsMarcoV2Docs, just with a few modifications directly
    collection = MsMarcoV2Docs(dlc['docs'], docid_prefix='msmarco_v2.1_doc_', docstore_size_hint=0, name=NAME)
    subsets = {}

    subsets['trec-rag-2024'] = Dataset(
        collection,
        TsvQueries(dlc['rag-2024-test-topics'], namespace=NAME, lang='en'),
    )

    ir_datasets.registry.register(NAME, Dataset(collection, documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))
    
    return collection, subsets

collection, subsets = _init()
