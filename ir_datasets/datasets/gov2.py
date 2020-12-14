import io
import os
import gzip
from contextlib import contextmanager, ExitStack
from pathlib import Path
from typing import NamedTuple
from glob import glob
import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.formats import TrecQrels, TrecQueries, TrecColonQueries, BaseDocs, GenericQuery
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import Docstore


NAME = 'gov2'


QREL_DEFS = {
    2: 'Highly Relevant',
    1: 'Relevant',
    0: 'Not Relevant',
}

NAMED_PAGE_QREL_DEFS = {
    1: '',
    0: '',
}

NAMED_PAGE_QTYPE_MAP = {
    '<num> *(Number:)? *NP': 'query_id', # Remove NP prefix from QIDs
    '<title> *(Topic:)?': 'text',
}

class Gov2Doc(NamedTuple):
    doc_id: str
    url: str
    http_headers: str
    body: bytes
    body_content_type: str


class Gov2Docs(BaseDocs):
    def __init__(self, docs_dlc):
        super().__init__()
        self.docs_dlc = docs_dlc

    def docs_path(self):
        return self.docs_dlc.path()

    def _docs_iter_source_files(self):
        dirs = sorted((Path(self.docs_dlc.path()) / 'GOV2_data').glob('GX???'))
        for source_dir in dirs:
            for source_file in sorted(source_dir.glob('*.gz')):
                yield source_file

    def docs_iter(self):
        for source_file in self._docs_iter_source_files():
            with self._docs_ctxt_iter_gov2(source_file) as doc_iter:
                yield from doc_iter

    def docs_cls(self):
        return Gov2Doc

    @contextmanager
    def _docs_ctxt_iter_gov2(self, gov2f):
        with ExitStack() as stack:
            if isinstance(gov2f, (str, Path)):
                gov2f = stack.enter_context(gzip.open(gov2f, 'rb'))
            def it():
                doc = None
                for line in gov2f:
                    if line == b'<DOC>\n':
                        assert doc is None
                        doc = line
                    elif line == b'</DOC>\n':
                        doc += line
                        yield self._process_gov2_doc(doc)
                        doc = None
                    elif doc is not None:
                        doc += line
            yield it()

    def _process_gov2_doc(self, raw_doc):
        state = 'DOCNO'
        doc_id = None
        doc_hdr = None
        doc_body = b''
        with io.BytesIO(raw_doc) as f:
            for line in f:
                if state == 'DOCNO':
                    if line.startswith(b'<DOCNO>'):
                        doc_id = line[len(b'<DOCNO>'):-len(b'</DOCNO>')-1].strip().decode()
                        state = 'DOCHDR'
                elif state == 'DOCHDR':
                    if line == b'<DOCHDR>\n':
                        doc_hdr = b''
                    elif line == b'</DOCHDR>\n':
                        state = 'BODY'
                    elif doc_hdr is not None:
                        doc_hdr += line
                elif state == 'BODY':
                    if line == b'</DOC>\n':
                        state = 'DONE'
                    else:
                        doc_body += line
        doc_url, doc_hdr = doc_hdr.decode().split('\n', 1)
        headers = [line.split(':', 1) for line in doc_hdr.split('\n') if ':' in line]
        headers = {k.lower(): v for k, v in headers}
        content_type = 'text/html' # default to text/html
        if 'content-type' in headers:
            content_type = headers['content-type']
        if ';' in content_type:
            content_type, _ = content_type.split(';', 1)
        content_type = content_type.strip()
        return Gov2Doc(doc_id, doc_url, doc_hdr, doc_body, content_type)

    def _docs_id_to_source_file(self, doc_id):
        parts = doc_id.split('-')
        if len(parts) != 3:
            return None
        s_dir, file, doc = parts
        source_file = os.path.join(self.docs_dlc.path(), 'GOV2_data', s_dir, f'{file}.gz')
        return source_file

    def docs_store(self):
        docstore = Gov2Docstore(self)
        return ir_datasets.indices.CacheDocstore(docstore, f'{self.docs_path()}.cache')


class Gov2Docstore(Docstore):
    def __init__(self, gov2_docs):
        super().__init__(gov2_docs.docs_cls(), 'doc_id')
        self.gov2_docs = gov2_docs

    def get_many_iter(self, doc_ids):
        result = {}
        files_to_search = {}
        for doc_id in doc_ids:
            source_file = self.gov2_docs._docs_id_to_source_file(doc_id)
            if source_file is not None:
                if source_file not in files_to_search:
                    files_to_search[source_file] = []
                files_to_search[source_file].append(doc_id)
        for source_file, doc_ids in files_to_search.items():
            doc_ids = sorted(doc_ids)
            with self.gov2_docs._docs_ctxt_iter_gov2(source_file) as doc_it:
                for doc in doc_it:
                    if doc_ids[0] == doc.doc_id:
                        yield doc
                        doc_ids = doc_ids[1:]
                        if not doc_ids:
                            break # file finished


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.cache_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    docs_dlc = dlc['docs']
    collection = Gov2Docs(docs_dlc)
    base = Dataset(collection, documentation('_'))

    subsets['trec-tb-2004'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2004/queries']),
        TrecQrels(dlc['trec-tb-2004/qrels'], QREL_DEFS),
        documentation('trec-tb-2004')
    )
    subsets['trec-tb-2005'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2005/queries']),
        TrecQrels(dlc['trec-tb-2005/qrels'], QREL_DEFS),
        documentation('trec-tb-2005')
    )
    subsets['trec-tb-2005/named-page'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2005/named-page/queries'], qtype=GenericQuery, qtype_map=NAMED_PAGE_QTYPE_MAP),
        TrecQrels(dlc['trec-tb-2005/named-page/qrels'], NAMED_PAGE_QREL_DEFS),
        documentation('trec-tb-2005/named-page')
    )
    subsets['trec-tb-2006'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2006/queries']),
        TrecQrels(dlc['trec-tb-2006/qrels'], QREL_DEFS),
        documentation('trec-tb-2006')
    )
    subsets['trec-tb-2006/named-page'] = Dataset(
        collection,
        TrecQueries(dlc['trec-tb-2006/named-page/queries'], qtype=GenericQuery, qtype_map=NAMED_PAGE_QTYPE_MAP),
        TrecQrels(dlc['trec-tb-2006/named-page/qrels'], NAMED_PAGE_QREL_DEFS),
        documentation('trec-tb-2006/named-page')
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
