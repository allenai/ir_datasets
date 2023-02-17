import ir_datasets
from ir_datasets.formats import TsvDocs, TsvQueries, TrecQrels
from ir_datasets.util import DownloadConfig, TarExtract, Cache
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation
from ir_datasets.formats import BaseDocs, TrecXmlQueries, TrecQrels, GenericQuery, GenericQrel
from ir_datasets.indices import PickleLz4FullStore
from typing import NamedTuple, Tuple
import itertools
import io
from ir_datasets.util import GzipExtract, Cache, Lazy, TarExtractAll

import logging


# A unique identifier for this dataset. This should match the file name (with "-" instead of "_")
NAME = "sara"

# What do the relevance levels in qrels mean?
QREL_DEFS = {
    2: 'highly relevant',
    1: 'partially relevant',
    0: 'not relevant',
}

# This message is shown to the user before downloads are started
DUA = 'Please confirm that you agree to the data usage agreement at <https://some-url/>'

def sentinel_splitter(it, sentinel):
    for is_sentinel, group in itertools.groupby(it, lambda l: l == sentinel):
        if not is_sentinel:
            yield list(group)

class SaraDoc(NamedTuple):
    doc_id: str
    text: str

class SaraDocs(BaseDocs):
    def __init__(self,dlc):
        super().__init__()
        self._dlc = dlc
        print("init")

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        print("docs_iter")
        print(self._dlc)
        with self._dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for lines in sentinel_splitter(stream, sentinel='   /\n'):
                print(lines)
                print("123")
                doc_id = lines[0].rstrip('\n')
                doc_text = ''.join(lines[1:])
                yield SaraDoc(doc_id, doc_text)

    # # def docs_count(self):
    #     raise NotImplementedError()
    
    # def docs_path(self, force=True):
    #     return self.docs_dlc.path(force)
    
    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/docs.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(NAME),
        )



# An initialization function is used to keep the namespace clean
def _init():
    # The directory where this dataset's data files will be stored
    base_path = ir_datasets.util.home_path()/NAME
    
    # # Load an object that is used for providing the documentation
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    # A reference to the downloads file, under the key "dummy". (DLC stands for DownLoadable Content)
    dlc = DownloadConfig.context(NAME, base_path)    

    # collection = SaraDocs(dlc.) 
    # How to process the documents. Since they are in a typical TSV format, we'll use TsvDocs.
    # Note that other dataset formats may require you to write a custom docs handler (BaseDocs).
    # Note that this doesn't process the documents now; it just defines how they are processed.
    colle = SaraDocs(Cache(TarExtractAll(dlc, base_path/"docs.txt"), base_path/'docs.txt'))

    print(dlc["docs"].stream())
    stream = io.TextIOWrapper(dlc["docs"].stream())
    

    # @ir_datasets.util.use_docstore
    # def test(dlc):
    #     with dlc["docs"].stream() as stream:
    #         stream = io.TextIOWrapper(stream)
    # test(dlc)
    #print(TarExtractAll(dlc, base_path/"docs.txt").path())
    # How to process the queries. Similar to the documents, you may need to write a custom
    # queries handler (BaseQueries).
    #queries = TsvQueries(dlc['queries'], namespace=NAME, lang='en')

    # Qrels: The qrels file is in the TREC format, so we'll use TrecQrels to process them
    #qrels = TrecQrels(dlc['qrels'], QREL_DEFS)

    # Package the docs, queries, qrels, and documentation into a Dataset object
    #dataset = Dataset(docs, queries, qrels, documentation('_'))

    # Register the dataset in ir_datasets
    #ir_datasets.registry.register(NAME, dataset)

    #return dataset # used for exposing dataset to the namespace
    return -1

_init()