import re
import gzip
import indexed_gzip
import codecs
import os
from collections import namedtuple
from contextlib import contextmanager
from glob import glob
import ir_datasets
from ir_datasets.util import GzipExtract, Lazy, DownloadConfig, TarExtract, Cache, Bz2Extract, ZipExtract
from ir_datasets.formats import TrecQrels, TrecDocs, TrecXmlQueries, BaseDocs, GenericDoc, GenericQuery, TrecQrel
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation
from ir_datasets.indices import Docstore, CacheDocstore


NAME = 'clueweb09'


QREL_DEFS = {
    4: 'Nav: This page represents a home page of an entity directly named by the query; the user may be searching for this specific page or site.',
    3: 'Key: This page or site is dedicated to the topic; authoritative and comprehensive, it is worthy of being a top result in a web search engine.',
    2: 'HRel: The content of this page provides substantial information on the topic.',
    1: 'Rel: The content of this page provides some information on the topic, which may be minimal; the relevant information must be on that page, not just promising-looking anchor text pointing to a possibly useful page.',
    0: 'Non: The content of this page does not provide useful information on the topic, but may provide useful information on other topics, including other interpretations of the same query.',
    -2: 'Junk: This page does not appear to be useful for any reasonable purpose; it may be spam or junk',
}

QREL_DEFS_09 = {
    2: 'highly relevant',
    1: 'relevant',
    0: 'not relevant',
}

WarcInfo = namedtuple('WarcInfo', ['doc_id', 'url', 'date', 'http_response'])
TrecWebTrackQuery = namedtuple('TrecWebTrackQuery', ['query_id', 'query', 'description', 'type', 'subtopics'])
TrecPrel = namedtuple('TrecPrel', ['query_id', 'doc_id', 'relevance', 'method', 'iprob'])
WarcHtmlDoc = namedtuple('WarcHtmlDoc', ['doc_id', 'url', 'date', 'html'])


def decode(s, encodings=('iso-8859-1', 'ascii', 'utf8', 'latin1')):
    for encoding in encodings:
        try:
            return s.decode(encoding)
        except UnicodeDecodeError:
            pass
        except LookupError:
            pass


class ClueWebDocStore(Docstore):
    def __init__(self, warc_docs):
        super().__init__(WarcHtmlDoc, 'doc_id')
        self.warc_docs = warc_docs

    def get_many_iter(self, doc_ids):
        warc = ir_datasets.lazy_libs.warc()
        result = {}
        files_to_search = {}
        for doc_id in doc_ids:
            ds, sec, part, doc = doc_id.split('-')
            assert ds == 'clueweb09'
            source_glob = os.path.join(self.warc_docs.docs_dlc.path(), f'ClueWeb09_*', sec, f'{part}.warc.gz')
            source_file = glob(source_glob)
            if source_file:
                source_file = source_file[0]
                if source_file not in files_to_search:
                    files_to_search[source_file] = []
                files_to_search[source_file].append(doc_id)
        for source_file, doc_ids in files_to_search.items():
            doc_ids = sorted(doc_ids)
            with self.warc_docs._iter_warc(source_file) as doc_it:
                for doc in doc_it:
                    if doc_ids[0] == doc.doc_id:
                        yield self.warc_docs._parse_warc(doc)
                        doc_ids = doc_ids[1:]
                        if not doc_ids:
                            break # file finished


class WarcDocs(BaseDocs):
    def __init__(self, id_header):
        self.id_header = id_header

    @contextmanager
    def _iter_warc(self, warcf):
        warc = ir_datasets.lazy_libs.warc_clueweb09()
        with gzip.open(warcf, 'rb') as f:
            with warc.WARCFile(fileobj=f) as f:
                def it():
                    for doc in filter(lambda d: d.type == 'response', f):
                        did = doc[self.id_header]
                        url = doc['WARC-Target-URI']
                        date = doc['WARC-Date']
                        http_response = doc.payload.read()
                        yield WarcInfo(did, url, date, http_response)
                yield it()

    def _parse_warc(self, doc):
        # Doing this here allows for both http headers and <meta http-equiv>
        encoding = re.search(b'charset=([a-zA-Z0-9-_]+)', doc.http_response)
        if encoding:
            encoding = (encoding.group(1).decode(), 'utf8', 'latin1')
        else:
            encoding = ('utf8', 'latin1')
        headers, html = doc.http_response.split(b'\n\n', 1)
        html = decode(html, encoding)
        return WarcHtmlDoc(doc.doc_id, doc.url, doc.date, html)

    def docs_cls(self):
        return WarcHtmlDoc

class ClueWeb09Docs(WarcDocs):
    def __init__(self, docs_dlc):
        super().__init__('WARC-TREC-ID')
        self.docs_dlc = docs_dlc

    def docs_iter(self):
        for source_file in self._iter_sources():
            with self._iter_warc(source_file) as doc_iter:
                yield from map(self._parse_warc, doc_iter)

    def _iter_sources(self):
        files = []
        for lang in ['English', 'Arabic', 'Chinese', 'French', 'German', 'Italian', 'Japanese', 'Korean', 'Portuguese', 'Spanish']:
            files += sorted(glob(os.path.join(self.docs_dlc.path(), f'ClueWeb09_{lang}_*', '*')))
        for source_dir in files:
            for source_file in sorted(glob(os.path.join(source_dir, '*.gz'))):
                yield source_file

    def docs_store(self):
        return CacheDocstore(ClueWebDocStore(self), f'{self.docs_dlc.path()}.cache')


class TrecPrels(TrecQrels):
    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                if line == '\n':
                    continue # ignore blank lines
                cols = line.rstrip().split()
                if len(cols) != 5:
                    raise RuntimeError(f'expected 5 columns, got {len(cols)}')
                qid, did, rel, method, iprob = cols
                yield TrecPrel(qid, did, int(rel), int(method), float(iprob))


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.cache_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    docs_dlc = dlc['docs']
    collection = ClueWeb09Docs(docs_dlc)
    base = Dataset(collection, documentation('_'))

    subsets['trec-web-2009'] = Dataset(
        collection,
        TrecXmlQueries(dlc['trec-web-2009/queries'], qtype=TrecWebTrackQuery),
        TrecPrels(GzipExtract(dlc['trec-web-2009/qrels.adhoc']), QREL_DEFS_09),
        documentation('trec-web-2009'))

    subsets['trec-web-2010'] = Dataset(
        collection,
        TrecXmlQueries(dlc['trec-web-2010/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2010/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2010'))

    subsets['trec-web-2011'] = Dataset(
        collection,
        TrecXmlQueries(dlc['trec-web-2011/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2011/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2011'))

    subsets['trec-web-2012'] = Dataset(
        collection,
        TrecXmlQueries(dlc['trec-web-2012/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2012/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2012'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
