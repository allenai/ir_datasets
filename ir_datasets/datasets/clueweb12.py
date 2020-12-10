import codecs
import os
from collections import namedtuple
from glob import glob
from pathlib import Path
import ir_datasets
from ir_datasets.util import DownloadConfig, TarExtract, TarExtractAll, Cache, Bz2Extract, ZipExtract
from ir_datasets.formats import TrecQrels, TrecDocs, TrecXmlQueries, WarcDocs, GenericDoc, GenericQuery, TrecQrel, NtcirQrels
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation
from ir_datasets.indices import Docstore, CacheDocstore


NAME = 'clueweb12'


QREL_DEFS = {
    4: 'Nav: This page represents a home page of an entity directly named by the query; the user may be searching for this specific page or site.',
    3: 'Key: This page or site is dedicated to the topic; authoritative and comprehensive, it is worthy of being a top result in a web search engine.',
    2: 'HRel: The content of this page provides substantial information on the topic.',
    1: 'Rel: The content of this page provides some information on the topic, which may be minimal; the relevant information must be on that page, not just promising-looking anchor text pointing to a possibly useful page.',
    0: 'Non: The content of this page does not provide useful information on the topic, but may provide useful information on other topics, including other interpretations of the same query.',
    -2: 'Junk: This page does not appear to be useful for any reasonable purpose; it may be spam or junk',
}

NTCIR_QREL_DEFS = {
    0: 'Two annotators rated as non-relevant',
    1: 'One annotator rated as relevant, one as non-relevant',
    2: 'Two annotators rated as relevant, OR one rates as highly relevant and one as non-relevant',
    3: 'One annotator rated as highly relevant, one as relevant',
    4: 'Two annotators rated as highly relevant',
}

MISINFO_QREL_DEFS = {
    0: 'Not relevant',
    1: 'Relevant',
    2: 'Highly relevant',
}

TrecWebTrackQuery = namedtuple('TrecWebTrackQuery', ['query_id', 'query', 'description', 'type', 'subtopics'])
NtcirQuery = namedtuple('NtcirQuery', ['query_id', 'title', 'description'])
ntcir_map = {'qid': 'query_id', 'content': 'title', 'description': 'description'}
MisinfoQuery = namedtuple('MisinfoQuery', ['query_id', 'title', 'cochranedoi', 'description', 'narrative'])
misinfo_map = {'number': 'query_id', 'query': 'title', 'cochranedoi': 'cochranedoi', 'description': 'description', 'narrative': 'narrative'}
MisinfoQrel = namedtuple('MisinfoQrel', ['query_id', 'doc_id', 'relevance', 'effectiveness', 'credibility'])


class MsinfoQrels(TrecQrels):
    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                if line == '\n':
                    continue # ignore blank lines
                cols = line.rstrip().split()
                if len(cols) != 6:
                    raise RuntimeError(f'expected 6 columns, got {len(cols)}')
                qid, it, did, rel, eff, cred = cols
                yield MisinfoQrel(qid, did, int(rel), int(eff), int(cred))

    def qrels_cls(self):
        return MisinfoQrel


class ClueWeb12Docs(WarcDocs):
    def __init__(self, docs_dlc, chk_dlc):
        super().__init__()
        self.docs_dlc = docs_dlc
        self.chk_dlc = chk_dlc

    def docs_path(self):
        return self.docs_dlc.path()

    def _docs_iter_source_files(self):
        for source_dir in sorted(glob(os.path.join(self.docs_dlc.path(), 'ClueWeb12_*', '*'))):
            for source_file in sorted(glob(os.path.join(source_dir, '*.gz'))):
                yield source_file

    def _docs_id_to_source_file(self, doc_id):
        parts = doc_id.split('-')
        if len(parts) != 4:
            return None
        dataset, sec, part, doc = parts
        if dataset != 'clueweb12':
            return None
        return os.path.join(self.docs_dlc.path(), f'ClueWeb12_{sec[:2]}', sec, f'{sec}-{part}.warc.gz')

    def _docs_source_file_to_checkpoint(self, source_file):
        source_prefix = Path(self.docs_dlc.path())
        source_file = Path(source_file)
        index_prefix = Path(self.chk_dlc.path())
        result = index_prefix / source_file.relative_to(source_prefix)
        if result == source_file:
            return None
        return f'{result}.chk.lz2' 


class ClueWeb12b13Docs(ClueWeb12Docs):
    def __init__(self, docs_dlc, chk_dlc, b13_dlc):
        super().__init__(docs_dlc, chk_dlc)
        self.b13_dlc = b13_dlc

    def _iter_b13ids(self):
        with self.b13_dlc.stream() as stream:
            for line in stream:
                line = line.decode()
                did, _ = line.split(',', 1)
                yield did

    def docs_iter(self):
        did_iter = self._iter_b13ids()
        current_did = next(did_iter, None)
        for doc in super().docs_iter():
            if doc.doc_id == current_did:
                yield doc
                next_did = next(did_iter, None)
                advance_file = next_did is None or current_did.split('-')[:-1] != next_did.split('-')[:-1]
                current_did = next_did
                if advance_file:
                    break


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.cache_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    docs_dlc = dlc['docs']
    docs_chk_dlc = TarExtractAll(dlc['docs.chk'], base_path/'corpus.chk')
    b13_dlc = Bz2Extract(Cache(TarExtract(dlc['cw12b-info'], 'ClueWeb12-CreateB13/ClueWeb12_B13_DocID_To_URL.txt.bz2'), base_path/'ClueWeb12_B13_DocID_To_URL.txt.bz2'))

    collection = ClueWeb12Docs(docs_dlc, docs_chk_dlc)
    collection_b13 = ClueWeb12b13Docs(docs_dlc, docs_chk_dlc, b13_dlc)

    base = Dataset(collection, documentation('_'))

    subsets['b13'] = Dataset(collection_b13, documentation('b13'))

    subsets['trec-web-2013'] = Dataset(
        collection,
        TrecXmlQueries(dlc['trec-web-2013/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2013/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2013'))

    subsets['trec-web-2014'] = Dataset(
        collection,
        TrecXmlQueries(dlc['trec-web-2014/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2014/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2014'))

    subsets['b13/ntcir-www-1'] = Dataset(
        collection_b13,
        TrecXmlQueries(Cache(ZipExtract(dlc['ntcir-www-1/queries'], 'eng.queries.xml'), base_path/'ntcir-www-1'/'queries.xml'), qtype=GenericQuery, qtype_map={'qid': 'query_id', 'content': 'text'}),
        NtcirQrels(dlc['ntcir-www-1/qrels'], NTCIR_QREL_DEFS),
        documentation('ntcir-www-1'))

    subsets['b13/ntcir-www-2'] = Dataset(
        collection_b13,
        TrecXmlQueries(Cache(ZipExtract(dlc['ntcir-www-2/queries'], 'qEng.xml'), base_path/'ntcir-www-2'/'queries.xml'), qtype=NtcirQuery, qtype_map=ntcir_map),
        NtcirQrels(dlc['ntcir-www-2/qrels'], NTCIR_QREL_DEFS),
        documentation('ntcir-www-2'))

    subsets['b13/ntcir-www-3'] = Dataset(
        collection_b13,
        TrecXmlQueries(dlc['ntcir-www-3/queries'], qtype=NtcirQuery, qtype_map=ntcir_map),
        documentation('ntcir-www-3'))

    subsets['b13/trec-misinfo-2019'] = Dataset(
        collection_b13,
        TrecXmlQueries(dlc['trec-misinfo-2019/queries'], qtype=MisinfoQuery, qtype_map=misinfo_map),
        MsinfoQrels(dlc['trec-misinfo-2019/qrels'], MISINFO_QREL_DEFS),
        documentation('trec-misinfo-2019'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
