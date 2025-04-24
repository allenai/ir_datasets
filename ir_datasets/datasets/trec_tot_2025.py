from ir_datasets import registry
from ir_datasets.datasets.base import Dataset
from ir_datasets.util.download import RequestsDownload
from ir_datasets.formats.base import BaseDocs
from ir_datasets.indices import Docstore
from ir_datasets.util import ZipExtractCache, home_path, Cache
from ir_datasets.formats import BaseDocs, TrecQrels, JsonlQueries
from ir_datasets.indices import PickleLz4FullStore
import os
import gzip
import json
from tqdm import tqdm
from typing import NamedTuple

NAME = "trec-tot"

def cached_tot_resource(url, md5):
    streamer = RequestsDownload(url)
    return Cache(streamer, home_path() / "trec-tot-2025" / url.split("/")[-1])

class JsonlDocumentOffset(NamedTuple):
    doc_id: str
    offset_start: int
    offset_end: int


class TrecToT2025Doc():
    def __init__(self, json_doc):
        parsed_doc = json.loads(json_doc)
        self.doc_id = parsed_doc["id"]
        self.title = parsed_doc["title"]
        self.url = parsed_doc["url"]
        self.text = parsed_doc["text"]

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
            path=str(self.__offsets.path().absolute().resolve()) + '.pklz4',
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
            yield TrecToT2025Doc(i)


class JsonlDocumentsWithOffsets(BaseDocs):
    def __init__(self, docs, offsets):
       self.__docs = docs
       self.__offsets = offsets
    
    def docs_iter(self):
        with gzip.open(self.__docs.path()) as f:
            for l in f:
                yield TrecToT2025Doc(l)

    def docs_cls(self):
        return self._cls

    def docs_store(self, field='doc_id'):
        return TrecToT2025DocsStore(self.__docs, self.__offsets)

    def docs_namespace(self):
        raise ValueError("ToDo: Implement this")

    def docs_count(self):
        return len(self.docs_dict())

    def docs_lang(self):
        raise ValueError("ToDo: Implement this")


class TrecToT2025Dataset(Dataset):
    def __init__(self, docs_jsonl_file, offset_jsonl_file, queries=None, qrels=None):
        docs = JsonlDocumentsWithOffsets(docs_jsonl_file, offset_jsonl_file)

        if queries:
            queries = JsonlQueries(queries, lang='en', mapping={"text": "query", "query_id": "query_id"})
        if qrels:
            qrels = TrecQrels(qrels, {0: 'Not Relevant', 1: 'Relevant'})

        super().__init__(docs, queries, qrels)


def register_dataset():
    if f"{NAME}/2025" in registry:
        return

    doc_offsets = cached_tot_resource("https://files.webis.de/data-in-progress/trec-tot-2025-offsets.jsonl.gz", "00678e3155d962bb244e034e6401b79b")
    doc_corpus = cached_tot_resource("https://files.webis.de/data-in-progress/trec-tot-2025-corpus.jsonl.gz", "a2c82398aa86df6a68c8706b9b462bf2")
    registry.register(f"{NAME}/2025", TrecToT2025Dataset(doc_corpus, doc_offsets))
    for i in ["train", "dev1", "dev2", "dev3"]:
        qrels = cached_tot_resource("https://files.webis.de/data-in-progress/trec-tot-2025-queries/" + i + "-2025-qrel.txt", "TBD")
        queries = cached_tot_resource("https://files.webis.de/data-in-progress/trec-tot-2025-queries/" + i + "-2025-queries.jsonl", "TBD")
        registry.register(f"{NAME}/2025/{i}", TrecToT2025Dataset(doc_corpus, doc_offsets, queries, qrels))


if __name__ == '__main__':
    register_dataset()
    import ir_datasets
    dataset = ir_datasets.load("trec-tot/2025")

    cnt = 0
    for doc in dataset.docs_iter():
        print(doc.doc_id)
        cnt += 1
        if cnt > 10:
            break

    for doc in ["12", "39", "290", "303", "305", "307", "308", "309"]:
        print(doc, "=>", dataset.docs_store().get(doc).doc_id)

