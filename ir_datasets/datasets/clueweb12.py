import codecs
import os
import gzip
import contextlib
from typing import NamedTuple, Tuple
from glob import glob
from pathlib import Path
import ir_datasets
from ir_datasets.util import DownloadConfig, TarExtract, TarExtractAll, Cache, Bz2Extract, ZipExtract
from ir_datasets.formats import TrecQrels, TrecDocs, TrecXmlQueries, WarcDocs, GenericDoc, GenericQuery, TrecQrel, NtcirQrels, TrecSubtopic
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation
from ir_datasets.indices import Docstore, CacheDocstore


_logger = ir_datasets.log.easy()


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


ntcir_map = {'qid': 'query_id', 'content': 'title', 'description': 'description'}
misinfo_map = {'number': 'query_id', 'query': 'title', 'cochranedoi': 'cochranedoi', 'description': 'description', 'narrative': 'narrative'}


class TrecWebTrackQuery(NamedTuple):
    query_id: str
    query: str
    description: str
    type: str
    subtopics: Tuple[TrecSubtopic, ...]


class NtcirQuery(NamedTuple):
    query_id: str
    title: str
    description: str


class MisinfoQuery(NamedTuple):
    query_id: str
    title: str
    cochranedoi: str
    description: str
    narrative: str


class MisinfoQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int
    effectiveness: int
    redibility: int


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
    def __init__(self, docs_dlc, chk_dlc=None):
        super().__init__()
        self.docs_dlc = docs_dlc
        self.chk_dlc = chk_dlc
        self._docs_warc_file_counts_cache = None

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
        if self.chk_dlc is None:
            return None
        source_prefix = Path(self.docs_dlc.path())
        source_file = Path(source_file)
        index_prefix = Path(self.chk_dlc.path())
        result = index_prefix / source_file.relative_to(source_prefix)
        if result == source_file:
            return None
        return f'{result}.chk.lz4'

    def _docs_warc_file_counts(self):
        if self._docs_warc_file_counts_cache is None:
            result = {}
            for counts_file in glob(os.path.join(self.docs_dlc.path(), 'recordcounts', '*.txt')):
                d = os.path.basename(counts_file)[:-len('_counts.txt')]
                with open(counts_file, 'rt') as f:
                    for line in f:
                        file, count = line.strip().split()
                        file = os.path.join(self.docs_dlc.path(), d, file[2:])
                        result[file] = int(count)
            self._docs_warc_file_counts_cache = result
        return self._docs_warc_file_counts_cache

    def docs_count(self):
        return sum(self._docs_warc_file_counts().values())


class ClueWeb12b13Extractor:
    def __init__(self, docs_dlc, extract_jar_dlc):
        self.docs_dlc = docs_dlc
        self.extract_jar_dlc = extract_jar_dlc

    def path(self):
        source_path = self.docs_dlc.path()
        path = f'{source_path}-b13'
        if os.path.exists(path):
            self._create_record_counts_if_needed(path)
            return path
        extract_path = self.extract_jar_dlc.path()
        message = f'''clueweb12-b13 docs not found. Please either:
(1) Link docs to {path} if b13 subset already built, or
(2) Run the following command to build the b13 subset:
java -j {extract_path} {source_path}/ {path}/
'''
        _logger.info(message)
        raise RuntimeError(message)

    def _create_record_counts_if_needed(self, path):
        # The official JAR doesn't build up the recordcounts files that we use for jumping ahead.
        # So we will build them ourselves the first time. Luckily, the header of each WARC file
        # in CW12 contains a warc-number-of-documents header, which we can use (avoids reading)
        # the entire file. It still takes a little time, but not super long.
        rc_dir = os.path.join(path, 'recordcounts')
        if len(os.listdir(rc_dir)) != 0:
            return
        warc = ir_datasets.lazy_libs.warc()
        with contextlib.ExitStack() as stack, _logger.pbar_raw(desc='building b13 document count cache') as pbar:
            for d in glob(os.path.join(path, 'ClueWeb12_??')):
                d = os.path.basename(d)
                out = stack.enter_context(ir_datasets.util.finialized_file(f'{rc_dir}/{d}_counts.txt', 'wt'))
                for file in sorted(glob(os.path.join(path, d, '*', '*.warc.gz'))):
                    shortf = file[-24:]
                    with gzip.open(file, 'rb') as f, warc.WARCFile(fileobj=f) as warcf:
                        num_docs = next(iter(warcf)).header['warc-number-of-documents']
                        out.write(f'./{shortf} {num_docs}\n')
                    pbar.update(1)

    def stream(self):
        raise NotImplementedError


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    docs_dlc = dlc['docs']
    docs_chk_dlc = TarExtractAll(dlc['docs.chk'], base_path/'corpus.chk')
    b13_dlc = Bz2Extract(Cache(TarExtract(dlc['cw12b-info'], 'ClueWeb12-CreateB13/software/CreateClueWeb12B13Dataset.jar'), base_path/'CreateClueWeb12B13Dataset.jar'))

    collection = ClueWeb12Docs(docs_dlc, docs_chk_dlc)
    collection_b13 = ClueWeb12Docs(ClueWeb12b13Extractor(docs_dlc, b13_dlc))

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
