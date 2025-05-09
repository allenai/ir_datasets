from ir_datasets import registry
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.util.download import RequestsDownload
from ir_datasets.formats.base import BaseDocs
from ir_datasets.indices import Docstore
from ir_datasets.util import ZipExtractCache, home_path, Cache, DownloadConfig
from ir_datasets.formats import BaseDocs, TrecQrels, JsonlQueries
from ir_datasets.indices import PickleLz4FullStore
import os
import gzip
import json
from tqdm import tqdm
from typing import NamedTuple

NAME = "trec-tot"


class JsonlDocumentOffset(NamedTuple):
    doc_id: str
    offset_start: int
    offset_end: int


class TrecToT2025Doc(NamedTuple):
    doc_id: str
    title: str
    url: str
    text: str

    @staticmethod
    def _from_json(json_doc):
        return TrecToT2025Doc(json_doc["id"], json_doc["title"], json_doc["url"], json_doc["text"])

    def default_text(self):
        return self.title + " " + self.text


class JsonlWithOffsetsDocsStore(Docstore):
    def __init__(self, docs, offsets):
       self.__docs = docs
       self.__offsets = offsets
       self._docs_dict = None
       self._id_field = "doc_id"

    def offsets_iter(self):
        with gzip.open(self.__offsets.path(), "rt") as f:
            for i in f:
                i = json.loads(i)
                yield JsonlDocumentOffset(doc_id=i["id"], offset_start=i["offset_start"], offset_end=i["offset_end"])

    def docs_dict(self):
        return PickleLz4FullStore(
            path=str(self.__offsets.path()) + '.pklz4',
            init_iter_fn=self.offsets_iter,
            data_cls=JsonlDocumentOffset,
            lookup_field="doc_id",
            index_fields=("doc_id",)
        )

    def get_many_iter(self, doc_ids):
        offsets = self.docs_dict()

        with open(self.__docs.path(), "rb") as f:
            for doc in doc_ids:
                doc = offsets.get(doc)
                f.seek(doc.offset_start)
                raw_content_bytes = f.read(doc.offset_end - doc.offset_start)
                yield gzip.decompress(raw_content_bytes)
 

class TrecToT2025DocsStore(JsonlWithOffsetsDocsStore):
    def get_many_iter(self, doc_ids):
        for i in super().get_many_iter(doc_ids):
            yield TrecToT2025Doc._from_json(json.loads(i))


class JsonlDocumentsWithOffsets(BaseDocs):
    def __init__(self, docs, offsets):
       self.__docs = docs
       self.__offsets = offsets
    
    def docs_iter(self):
        with gzip.open(self.__docs.path()) as f:
            for l in f:
                yield TrecToT2025Doc._from_json(json.loads(l))

    def docs_cls(self):
        return TrecToT2025Doc

    def docs_store(self, field='doc_id'):
        return TrecToT2025DocsStore(self.__docs, self.__offsets)

    def docs_namespace(self):
        raise ValueError("ToDo: Implement this")

    def docs_count(self):
        return 6407814

    def docs_lang(self):
        return "en"


class TrecToT2025Dataset(Dataset):
    def __init__(self, docs_jsonl_file, offset_jsonl_file, queries=None, qrels=None, documentation=None):
        docs = JsonlDocumentsWithOffsets(docs_jsonl_file, offset_jsonl_file)

        if queries:
            queries = JsonlQueries(queries, lang='en', mapping={"text": "query", "query_id": "query_id"})
        if qrels:
            qrels = TrecQrels(qrels, {0: 'Not Relevant', 1: 'Relevant'})

        super().__init__(docs, queries, qrels, documentation)


def register_dataset():
    if f"{NAME}/2025" in registry:
        return

    dlc = DownloadConfig.context("trec-tot-2025", home_path() / NAME / "2025")

    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    doc_offsets = dlc['trec-tot-2025-offsets.jsonl.gz']
    doc_corpus = dlc['trec-tot-2025-corpus.jsonl.gz']
    registry.register(f"{NAME}/2025", TrecToT2025Dataset(doc_corpus, doc_offsets, documentation=documentation("2025")))
    for i in ["train", "dev1", "dev2", "dev3"]:
        qrels = dlc[i + "-2025-qrel.txt"]
        queries = dlc[i + "-2025-queries.jsonl"]
        registry.register(f"{NAME}/2025/{i}", TrecToT2025Dataset(doc_corpus, doc_offsets, queries, qrels, documentation(f"2025/{i}")))


register_dataset()

