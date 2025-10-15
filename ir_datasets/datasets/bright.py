from typing import NamedTuple
import ir_datasets
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import BaseQrels, BaseQueries, GenericDoc, BaseDocs, TrecQrel
from ir_datasets.indices import PickleLz4FullStore


NAME = 'bright'
QRELS_DEFS = {1: 'Relevant', -100: 'Excluded from evaluation'}


def parquet_iter(path):
    pq = ir_datasets.lazy_libs.pyarrow_parquet()
    # https://stackoverflow.com/a/77150113
    batch_size = 64
    with pq.ParquetFile(path) as parquet_file:
        for record_batch in parquet_file.iter_batches(batch_size=batch_size):
            for d in record_batch.to_pylist():
                yield d


class BrightQuery(NamedTuple):
    query_id: str
    text: str
    reasoning: str


class BrightDocs(BaseDocs):
    def __init__(self, name, dlc, count_hint=None):
        self._name = name
        self._dlc = dlc
        self._count_hint = count_hint

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        for d in parquet_iter(self._dlc.path()):
            yield GenericDoc(d['id'], d['content'])

    def docs_path(self, force=True):
        return self._dlc.path(force=force)

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path(force=False)}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_cls(self):
        return GenericDoc

    def docs_namespace(self):
        return NAME

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_lang(self):
        return 'en'


class BrightQueries(BaseQueries):
    def __init__(self, dlc, *, gold_field='gold_ids'):
        self._dlc = dlc
        self._gold_field = gold_field

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        for q in parquet_iter(self._dlc.path()):
            query_id = str(q['id'])
            for doc_id in q[self._gold_field]:
                yield TrecQrel(query_id, str(doc_id), 1, "0")
            for doc_id in q['excluded_ids']:
                if doc_id != 'N/A':
                    yield TrecQrel(query_id, doc_id, -100, "0")

    def queries_iter(self):
        for q in parquet_iter(self._dlc.path()):
            yield BrightQuery(str(q['id']), q['query'], q['reasoning'])

    def queries_cls(self):
        return BrightQuery

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


class BrightQrels(BaseQrels):
    def __init__(self, dlc, *, gold_field='gold_ids'):
        self._dlc = dlc
        self._gold_field = gold_field

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        for q in parquet_iter(self._dlc.path()):
            query_id = str(q['id'])
            for doc_id in q[self._gold_field]:
                yield TrecQrel(query_id, str(doc_id), 1, "0")
            for doc_id in q['excluded_ids']:
                if doc_id != 'N/A':
                    yield TrecQrel(query_id, str(doc_id), -100, "0")

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return QRELS_DEFS


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(documentation('_'))

    subsets = {}

    for subset in ['biology', 'earth-science', 'economics', 'psychology', 'robotics', 'stackoverflow', 'sustainable-living', 'leetcode', 'pony', 'aops', 'theoremqa-theorems', 'theoremqa-questions']:
        subsets[subset] = Dataset(
            BrightDocs(subset, dlc[f'{subset}/docs']),
            BrightQueries(dlc[f'{subset}/queries']),
            BrightQrels(dlc[f'{subset}/queries']),
            documentation(subset),
        )

    # Long docs
    for subset in ['biology', 'earth-science', 'economics', 'psychology', 'robotics', 'stackoverflow', 'sustainable-living', 'pony']:
        subsets[f'{subset}-long'] = Dataset(
            BrightDocs(subset, dlc[f'{subset}-long/docs']),
            BrightQueries(dlc[f'{subset}/queries']),
            BrightQrels(dlc[f'{subset}/queries'], gold_field='gold_ids_long'),
            documentation(f'{subset}-long'),
        )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
