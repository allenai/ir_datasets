import ir_datasets
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import BaseDocs, TsvQueries, TrecQrels
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import DownloadConfig
from typing import NamedTuple
import csv
import io
import zipfile

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
        max_int = 229739
        csv.field_size_limit(max_int)
        with self._dlc.stream() as stream:
            with zipfile.ZipFile(stream) as zf:
                # Adjust this if the filename inside differs
                with zf.open(zf.namelist()[0]) as f:
                    text_stream = io.TextIOWrapper(
                        f,
                        encoding="utf-8-sig",
                        errors="replace",
                        newline=""
                    )
                    reader = csv.DictReader(text_stream)
                    for row in reader:
                        yield SaraDoc(
                            doc_id=row["docno"],
                            text=row["text"],
                            sensitivity=int(row["sensitivity"])
                        )

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
