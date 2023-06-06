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
from ir_datasets.util import GzipExtract, Cache, Lazy, TarExtractAll, RelativePath
import os
import re
import logging
import glob
import tarfile


# A unique identifier for this dataset. This should match the file name (with "-" instead of "_")
NAME = "sara"

# What do the relevance levels in qrels mean?
QREL_DEFS = {
    2: 'highly relevant',
    1: 'partially relevant',
    0: 'not relevant',
}

class SaraDoc(NamedTuple):
    doc_id: str
    text: str
    sensitivity: int
    def default_text(self):
        return self.text

class SaraDocs(BaseDocs):
    def __init__(self,dlc):
        super().__init__()
        self._dlc = dlc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        with self._dlc.stream() as stream, \
             tarfile.open(fileobj=stream, mode='r|gz') as tarf:
            it = iter(tarf)
            for record in it:
                if record.name.endswith('.txt') and not record.name.endswith('categories.txt'):
                    dir = os.path.basename(record.name)
                    doc_id = os.path.splitext(dir)[0]
                    with tarf.extractfile(record) as inp:
                         contents = inp.read().decode()
                    record = next(it)
                    assert record.name.endswith(f'{doc_id}.cats')
                    sensitive = 0
                    with tarf.extractfile(record) as file:
                        for line in file:
                            line = line.split()[0]
                            line = line.split(b",")
                            if int(line[0]) == 1 and int(line[1]) in (2, 3):
                                sensitive = 1
                    yield SaraDoc(doc_id, contents, sensitive)

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/docs.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(NAME),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'

    def docs_cls(self):
        return SaraDoc

# An initialization function is used to keep the namespace clean
def _init():
    base_path = ir_datasets.util.home_path()/NAME

    # # Load an object that is used for providing the documentation
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    dlc = DownloadConfig.context(NAME, base_path)

    docs = SaraDocs(dlc["docs"])

    queries = TsvQueries(dlc['queries'], namespace=NAME, lang='en')

    qrels = TrecQrels(dlc['qrels'], QREL_DEFS)

    # Package the docs, queries, qrels, and documentation into a Dataset object
    dataset = Dataset(docs, queries, qrels, documentation('_'))

    # Register the dataset in ir_datasets
    ir_datasets.registry.register(NAME, dataset)

    return dataset # used for exposing dataset to the namespace


_init()
