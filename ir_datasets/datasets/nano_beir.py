import ir_datasets
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import (
    BaseDocs,
    BaseQrels,
    BaseQueries,
    GenericDoc,
    GenericQuery,
    TrecQrel,
)
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()


NAME = "nano-beir"


def _map_field(field, data):
    if field in ("doc_id", "query_id"):
        return data["_id"]
    if field == "text":
        return data["text"]
    raise ValueError(f"unknown field: {field}")


def parquet_iter(path):
    pq = ir_datasets.lazy_libs.pyarrow_parquet()
    # https://stackoverflow.com/a/77150113
    batch_size = 64
    with pq.ParquetFile(path) as parquet_file:
        for record_batch in parquet_file.iter_batches(batch_size=batch_size):
            for d in record_batch.to_pylist():
                yield d


class NanoBeirDocs(BaseDocs):
    def __init__(self, name, dlc, doc_type):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._doc_type = doc_type

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        for d in parquet_iter(self._dlc.path()):
            yield self._doc_type(*(_map_field(f, d) for f in self._doc_type._fields))

    def docs_cls(self):
        return self._doc_type

    def docs_store(self, field="doc_id"):
        return PickleLz4FullStore(
            path=f"{ir_datasets.util.home_path()/NAME/self._name}/docs.pklz4",
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=["doc_id"],
            count_hint=ir_datasets.util.count_hint(f"{NAME}/{self._name}"),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return f"{NAME}/{self._name}"

    def docs_lang(self):
        return "en"


class NanoBeirQueries(BaseQueries):
    def __init__(self, name, dlc, query_type):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._query_type = query_type

    def queries_iter(self):
        for d in parquet_iter(self._dlc.path()):
            yield self._query_type(*(_map_field(f, d) for f in self._query_type._fields))

    def queries_cls(self):
        return self._query_type

    def queries_namespace(self):
        return f"{NAME}/{self._name}"

    def queries_lang(self):
        return "en"


class NanoBeirQrels(BaseQrels):
    def __init__(self, qrels_dlc, qrels_defs):
        self._qrels_dlc = qrels_dlc
        self._qrels_defs = qrels_defs

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        for d in parquet_iter(self.qrels_path()):
            yield TrecQrel(d["query-id"], d["corpus-id"], 1, "0")

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._qrels_defs


def _init():
    base_path = ir_datasets.util.home_path() / NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f"docs/{NAME}.yaml")

    base = Dataset(documentation("_"))

    subsets = {}

    benchmarks = [
        "climate-fever",
        "dbpedia-entity",
        "fever",
        "fiqa",
        "hotpotqa",
        "msmarco",
        "nfcorpus",
        "nq",
        "quora",
        "scidocs",
        "arguana",
        "scifact",
        "webis-touche2020",
    ]

    for ds in benchmarks:
        docs = NanoBeirDocs(ds, dlc[f"{ds}/docs"], GenericDoc)
        queries = NanoBeirQueries(ds, dlc[f"{ds}/queries"], GenericQuery)
        qrels = NanoBeirQrels(dlc[f"{ds}/qrels"], qrels_defs={1: 'relevant'})
        subsets[ds] = Dataset(
            docs,
            queries,
            qrels,
            documentation(ds),
        )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f"{NAME}/{s}", subsets[s])

    return base, subsets


base, subsets = _init()
