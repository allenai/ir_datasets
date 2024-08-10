import contextlib
import gzip
import io
import os
import shutil
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


class MsMarcoV21Document(MsMarcoV2Document):
    # Identical to V2 Document
    pass

def ensure_file_is_extracted(file_name):
    if os.path.isfile(file_name):
        return
    import tempfile
    tmp_file = Path(tempfile.mkdtemp()) / file_name.split('/')[-1]

    with gzip.open(file_name + '.gz', 'rb') as f_in:
        with open(tmp_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    shutil.move(tmp_file, file_name)

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
        self.cache = TarExtractAll(self.dlc, self.base_path/"msmarco_v2.1_doc")
        for i in range(0, 59):
            ensure_file_is_extracted(f"{self.cache.path()}/msmarco_v2.1_doc_{i:02d}.json")


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
            return MsMarcoV21Document(
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

    def docs_count(self):
        return 10960555

def _init():
    base_path = ir_datasets.util.home_path()/NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    collection = MsMarcoV21Docs(dlc['docs'])
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
