import contextlib
import gzip
import io
from pathlib import Path
import json
from typing import NamedTuple, Tuple, List
import tarfile
import ir_datasets
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache, DownloadConfig, GzipExtract, Lazy, Migrator, TarExtractAll, TarExtract
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs, FilteredQrels
from ir_datasets.formats import TsvQueries, TrecQrels, TrecScoredDocs, BaseDocs
from ir_datasets.datasets.msmarco_passage import DUA, DL_HARD_QIDS_BYFOLD, DL_HARD_QIDS
from ir_datasets.datasets.msmarco_document import TREC_DL_QRELS_DEFS
from ir_datasets.datasets.msmarco_document_v2 import MsMarcoV2Docs, MsMarcoV2Document

_logger = ir_datasets.log.easy()

NAME = 'msmarco-document-v2.1'


class MsMarcoV21DocStore(ir_datasets.indices.Docstore):    
    def __init__(self, doc_cls, dlc, base_path):
        super().__init__(doc_cls)
        self.dlc = dlc
        self.cache = None
        self.base_path = base_path

    def built(self):
        return False

    def build(self):
        if self.cache:
            return
        self.cache = Cache(TarExtractAll(self.dlc, "msmarco_v2.1_doc"), self.base_path)

    def get(self, doc_id, field=None):
        (string1, string2, string3, bundlenum, position) = doc_id.split("_")
        assert string1 == "msmarco" and string2 == "v2.1" and string3 == "doc"

        with open(
            f"{self.cache.path()}/msmarco_v2.1_doc_{bundlenum}.json", "rt", encoding="utf8"
        ) as in_fh:
            in_fh.seek(int(position))
            json_string = in_fh.readline()
            document = json.loads(json_string)

            assert document["docid"] == doc_id
            return MsMarcoV2Document(
                document['docid'],
                document['url'],
                document['title'],
                document['headings'],
                document['body'])
        
        # raise KeyError(f'doc_id={doc_id} not found')


class MsMarcoV21Docs(MsMarcoV2Docs):
    _fields = ["doc_id"]
    def __init__(self, dlc):
        super().__init__(dlc)

    def __iter__():
        pass

    def docs_store(self, field='doc_id'):
        ds = MsMarcoV21DocStore(self, self._dlc, 
        ir_datasets.util.home_path() / NAME / "docs")
        ds.build()
        return ds


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    subsets = {}
    
    collection = MsMarcoV21Docs(dlc['docs'])
    for docs in collection.docs_iter():
        print(docs)
        break

    ds = collection.docs_store()
    document = ds.get("msmarco_v2.1_doc_12_0")
    print(document)

if __name__ == "__main__":
    _init()