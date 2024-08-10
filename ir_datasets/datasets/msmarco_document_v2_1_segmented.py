import contextlib
import gzip
import io
from pathlib import Path
import json
from typing import NamedTuple, Tuple, List
import tarfile
import ir_datasets
from ir_datasets.util import Cache, DownloadConfig, GzipExtract, Lazy, Migrator, TarExtractAll, TarExtract
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs, FilteredQrels
from ir_datasets.formats import TsvQueries, TrecQrels, TrecScoredDocs, BaseDocs
from ir_datasets.datasets.msmarco_passage import DUA, DL_HARD_QIDS_BYFOLD, DL_HARD_QIDS
import os.path
import shutil

_logger = ir_datasets.log.easy()

NAME = 'msmarco-document-v2.1'


class MsMarcoV21SegmentedDocument(NamedTuple):
    doc_id: str
    url: str
    title: str
    headings: str
    segment: str
    start_char: int
    end_char: int
    def default_text(self):
        """
        title + headings + segment
        This is consistent with the MsMarcoV21Document that returns the full text alternative of this: title + headings + body
        Please note that Anserini additionaly returns the url. I.e., anserini returns url + title + headings + segment
        E.g., https://github.com/castorini/anserini/blob/b8ce19f56bc4e85056ef703322f76646804ec640/src/main/java/io/anserini/collection/MsMarcoV2DocCollection.java#L169
        """
        return f'{self.title} {self.headings} {self.segment}'


def ensure_file_is_extracted(file_name):
    if os.path.isfile(file_name):
        return
    import tempfile
    tmp_file = Path(tempfile.mkdtemp()) / file_name.split('/')[-1]
    
    with gzip.open(file_name + '.gz', 'rb') as f_in:
        with open(tmp_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    shutil.move(tmp_file, file_name)


class MsMarcoV21SegmentedDocStore(ir_datasets.indices.Docstore):    
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
        self.cache = TarExtractAll(self.dlc, self.base_path/"msmarco_v2.1_doc_segmented")

        for i in range(0, 59):
            ensure_file_is_extracted(f"{self.cache.path()}/msmarco_v2.1_doc_segmented_{i:02d}.json")


    def get(self, doc_id, field=None):
        (string1, string2, string3, bundlenum, doc_position, position) = doc_id.split("_")
        assert string1 == "msmarco" and string2 == "v2.1" and string3 == "doc"

        with open(
            f"{self.cache.path()}/msmarco_v2.1_doc_segmented_{bundlenum}.json", "rt", encoding="utf8"
        ) as in_fh:
            in_fh.seek(int(position))
            json_string = in_fh.readline()
            document = json.loads(json_string)

            assert document["docid"] == doc_id
            return MsMarcoV21SegmentedDocument(
                document['docid'],
                document['url'],
                document['title'],
                document['headings'],
                document['segment'],
                document['start_char'],
                document['end_char']
            )
        

class MsMarcoV21Docs(BaseDocs):
    _fields = ["doc_id"]
    def __init__(self, dlc):
        super().__init__()
        self._dlc = dlc

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._dlc.stream() as stream, \
             tarfile.open(fileobj=stream, mode='r|') as tarf:
            for record in tarf:
                if not record.name.endswith('.gz'):
                    continue
                file = tarf.extractfile(record)
                with gzip.open(file) as file:
                    for line in file:
                        data = json.loads(line)
                        yield MsMarcoV21SegmentedDocument(
                            data['docid'],
                            data['url'],
                            data['title'],
                            data['headings'],
                            data['segment'],
                            data['start_char'],
                            data['end_char'],
                        )

    def docs_cls(self):
        return MsMarcoV21SegmentedDocument

    def __iter__():
        pass

    def docs_store(self, field='doc_id'):
        ds = MsMarcoV21SegmentedDocStore(self, self._dlc, 
        ir_datasets.util.home_path() / NAME / "docs-segmented")
        ds.build()
        return ds

    def docs_count(self):
        return 113520750

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    collection = MsMarcoV21Docs(dlc['docs-segmented'])
    subsets = {}
    subsets['trec-rag-2024'] = Dataset(
        collection,
        TsvQueries(dlc['rag-2024-test-topics'], namespace=NAME, lang='en'),
    )

    ir_datasets.registry.register(NAME + '/segmented', Dataset(collection, documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/segmented/{s}', Dataset(subsets[s], documentation(s)))
    
    return collection, subsets

collection, subsets = _init()
