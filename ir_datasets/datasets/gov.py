import io
import os
import gzip
import codecs
from collections import Counter
from contextlib import contextmanager, ExitStack
from pathlib import Path
from typing import NamedTuple
from glob import glob
import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract, TarExtract
from ir_datasets.formats import TrecQrels, TrecQueries, TrecColonQueries, BaseDocs, GenericQuery, BaseQrels
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import Docstore, PickleLz4FullStore


_logger = ir_datasets.log.easy()


NAME = 'gov'


QREL_DEFS = {
    1: 'Relevant',
    0: 'Not Relevant',
}

NAMED_PAGE_QREL_DEFS = {
    1: 'Name refers to this page',
}

NAMED_PAGE_QTYPE_MAP = {
    '<num> *(Number:)? *NP': 'query_id', # Remove NP prefix from QIDs
    '<desc> *(Description:)?': 'text',
}

WEB03_QTYPE_MAP = {
    '<num> *(Number:)? *TD': 'query_id', # Remove TD prefix from QIDs
    '<title>': 'title',
    '<desc> *(Description:)?': 'description',
}

WEB04_QTYPE_MAP = {
    '<num> *(Number:)? *WT04-': 'query_id',
    '<title>': 'text',
}

class GovWeb02Query(NamedTuple):
    query_id: str
    title: str
    description: str


class GovDoc(NamedTuple):
    doc_id: str
    url: str
    http_headers: str
    body: bytes
    body_content_type: str


class GovDocs(BaseDocs):
    def __init__(self, docs_dlc):
        super().__init__()
        self.docs_dlc = docs_dlc

    def docs_path(self):
        return self.docs_dlc.path()

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        dirs = sorted(Path(self.docs_dlc.path()).glob('G??'))
        for source_dir in dirs:
            for source_file in sorted(source_dir.glob('*.gz')):
                yield from self._docs_ctxt_iter_gov(source_file)

    def docs_cls(self):
        return GovDoc

    def _docs_ctxt_iter_gov(self, gov2f):
        if isinstance(gov2f, (str, Path)):
            gov2f = gzip.open(gov2f, 'rb')
        doc = None
        for line in gov2f:
            if line == b'<DOC>\n':
                assert doc is None
                doc = line
            elif line == b'</DOC>\n':
                doc += line
                yield self._process_gov_doc(doc)
                doc = None
            elif doc is not None:
                doc += line

    def _process_gov_doc(self, raw_doc):
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

        for encoding in ['utf8', 'ascii', 'latin1']:
            try:
                doc_url, doc_hdr = doc_hdr.decode(encoding).split('\n', 1)
                break
            except UnicodeDecodeError:
                continue
        headers = [line.split(':', 1) for line in doc_hdr.split('\n') if ':' in line]
        headers = {k.lower(): v for k, v in headers}
        content_type = 'text/html' # default to text/html
        if 'content-type' in headers:
            content_type = headers['content-type']
        if ';' in content_type:
            content_type, _ = content_type.split(';', 1)
        content_type = content_type.strip()
        return GovDoc(doc_id, doc_url, doc_hdr, doc_body, content_type)

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_count(self):
        return sum(self._docs_file_counts().values())

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    collection = GovDocs(dlc['docs'])
    base = Dataset(collection, documentation('_'))

    subsets['trec-web-2002'] = Dataset(
        collection,
        TrecQueries(GzipExtract(dlc['trec-web-2002/queries']), namespace='gov/trec-web-2002', lang='en'),
        TrecQrels(GzipExtract(dlc['trec-web-2002/qrels']), QREL_DEFS),
        documentation('trec-web-2002')
    )
    subsets['trec-web-2002/named-page'] = Dataset(
        collection,
        TrecQueries(GzipExtract(dlc['trec-web-2002/named-page/queries']), qtype=GenericQuery, qtype_map=NAMED_PAGE_QTYPE_MAP, namespace='gov/trec-web-2002/named-page', lang='en'),
        TrecQrels(GzipExtract(dlc['trec-web-2002/named-page/qrels']), NAMED_PAGE_QREL_DEFS),
        documentation('trec-web-2002/named-page')
    )
    subsets['trec-web-2003'] = Dataset(
        collection,
        TrecQueries(dlc['trec-web-2003/queries'], qtype=GovWeb02Query, qtype_map=WEB03_QTYPE_MAP, namespace='gov/trec-web-2003', lang='en'),
        TrecQrels(dlc['trec-web-2003/qrels'], QREL_DEFS),
        documentation('trec-web-2003')
    )
    subsets['trec-web-2003/named-page'] = Dataset(
        collection,
        TrecQueries(dlc['trec-web-2003/named-page/queries'], qtype=GenericQuery, qtype_map=NAMED_PAGE_QTYPE_MAP, namespace='gov/trec-web-2003/named-page', lang='en'),
        TrecQrels(dlc['trec-web-2003/named-page/qrels'], NAMED_PAGE_QREL_DEFS),
        documentation('trec-web-2003/named-page')
    )
    subsets['trec-web-2004'] = Dataset(
        collection,
        TrecQueries(dlc['trec-web-2004/queries'], qtype=GenericQuery, qtype_map=WEB04_QTYPE_MAP, namespace='gov/trec-web-2004', lang='en'),
        TrecQrels(dlc['trec-web-2004/qrels'], QREL_DEFS),
        documentation('trec-web-2004')
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
