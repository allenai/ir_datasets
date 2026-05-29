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

NAME = "ntcir-tot"


class NtcirToT2026Doc(NamedTuple):
    doc_id: str
    title: str
    url: str
    text: str

    @staticmethod
    def _from_json(json_doc):
        return NtcirToT2026Doc(json_doc["id"], json_doc["title"], json_doc["url"], json_doc["text"])

    def default_text(self):
        return self.title + " " + self.text


class JsonlDocuments(BaseDocs):
    def __init__(self, docs, lang, docs_count):
       self.__docs = docs
       self.__lang = lang
       self.__docs_count = docs_count
    
    def docs_iter(self):
        with gzip.open(self.__docs.path()) as f:
            for l in f:
                yield NtcirToT2026Doc._from_json(json.loads(l))

    def docs_cls(self):
        return NtcirToT2026Doc

    def docs_namespace(self):
        raise ValueError("ToDo: Implement this")

    def docs_count(self):
        return self.__docs_count

    def docs_lang(self):
        return self.__lang


class NtcirToT2026Dataset(Dataset):
    def __init__(self, docs_jsonl_file, lang, docs_count, queries=None, qrels=None, documentation=None):
        docs = JsonlDocuments(docs_jsonl_file, lang, docs_count)

        if queries:
            queries = JsonlQueries(queries, lang='en', mapping={"text": "query", "query_id": "query_id"})
        if qrels:
            qrels = TrecQrels(qrels, {0: 'Not Relevant', 1: 'Relevant'})

        super().__init__(docs, queries, qrels, documentation)


def register_dataset():
    if f"{NAME}/2026" in registry:
        return

    dlc = DownloadConfig.context("ntcir-tot-2026", home_path() / NAME / "2026")
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    for lang in ['en', 'ja', 'ko', 'zh']:
        doc_corpus = dlc[f"corpus-{lang}.jsonl.gz"]
        registry.register(f"{NAME}/2026/{lang}", NtcirToT2026Dataset(doc_corpus, lang, 0, None, None, documentation(f"2026/{lang}")))
        for split in ["train", "dev", "test"]:

            if split == "test":
                qrels = None
            else:
                qrels = dlc[f"qrels-{split}-{lang}.txt"]

            queries = dlc[f"queries-{split}-{lang}.jsonl"]
            registry.register(f"{NAME}/2026/{lang}/{split}", NtcirToT2026Dataset(doc_corpus, lang, 0, queries, qrels, documentation(f"2026/{lang}/{split}")))

register_dataset()
