import re
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
    def default_text(self):
        """
        title
        """
        return self.title


class GovDoc(NamedTuple):
    doc_id: str
    url: str
    http_headers: str
    body: bytes
    body_content_type: str
    def default_text(self):
        return ir_datasets.util.sax_html_parser(self.body, headers=self.http_headers, fields=[{'title', 'body'}])[0]


class GovDocs(BaseDocs):
    def __init__(self, docs_dlc):
        super().__init__()
        self.docs_dlc = docs_dlc

    def docs_path(self, force=True):
        return self.docs_dlc.path(force)

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
        with ExitStack() as stack:
            if isinstance(gov2f, (str, Path)):
                gov2f = stack.enter_context(gzip.open(gov2f, 'rb'))
            inp = bytearray()
            # incrementally read the input file with read1 -- this ends up being more than twice
            # as fast as reading the input line-by-line and searching for <DOC> and </DOC> lines
            inp.extend(gov2f.read1())
            START, END = b'<DOC>\n', b'</DOC>\n'
            while inp != b'':
                inp, next_doc = self._extract_next_block(inp, START, END)
                while next_doc is not None:
                    yield self._process_gov_doc(next_doc)
                    inp, next_doc = self._extract_next_block(inp, START, END)
                inp.extend(gov2f.read1())

    def _process_gov_doc(self, raw_doc):
        # read the file by exploiting the sequence of blocks in the document -- this ends
        # up being several times faster than reading line-by-line
        raw_doc, doc_id = self._extract_next_block(raw_doc, b'<DOCNO>', b'</DOCNO>\n')
        assert doc_id is not None
        doc_id = doc_id.strip().decode()
        doc_body, doc_hdr = self._extract_next_block(raw_doc, b'<DOCHDR>\n', b'</DOCHDR>\n')
        assert doc_hdr is not None
        for encoding in ['utf8', 'ascii', 'latin1']:
            try:
                doc_url, doc_hdr = doc_hdr.decode(encoding).split('\n', 1)
                break
            except UnicodeDecodeError:
                continue
        content_type_match = re.search('^content-type:(.*)$', doc_hdr, re.I|re.M)
        content_type = 'text/html' # default to text/html
        if content_type_match:
            content_type = content_type_match.group(1)
            if ';' in content_type:
                content_type, _ = content_type.split(';', 1)
        content_type = content_type.strip()
        return GovDoc(doc_id, doc_url, doc_hdr, bytes(doc_body), content_type)

    def _extract_next_block(self, inp, START, END):
        # if START and END appear in inp, then return (everything after END in inp, the content between START and END),
        # or if they don't appear, return (inp, None).
        i_start = inp.find(START)
        i_end = inp.find(END)
        if i_start == -1 or i_end == -1:
            return inp, None
        return inp[i_end+len(END):], inp[i_start+len(START):i_end]

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self.docs_path(force=False)}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(NAME),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

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
