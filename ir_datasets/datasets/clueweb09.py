import os
import codecs
from pathlib import Path
from typing import NamedTuple, Tuple
from glob import glob
import ir_datasets
from ir_datasets.util import GzipExtract, Lazy, DownloadConfig, TarExtract, Cache, Bz2Extract, ZipExtract, TarExtractAll
from ir_datasets.formats import TrecQrels, TrecDocs, TrecXmlQueries, WarcDocs, GenericDoc, GenericQuery, TrecQrel, TrecSubtopic, TrecPrel, TrecPrels, TrecColonQueries
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


class TrecWebTrackQuery(NamedTuple):
    query_id: str
    query: str
    description: str
    type: str
    subtopics: Tuple[TrecSubtopic, ...]


class ClueWeb09Docs(WarcDocs):
    def __init__(self, docs_dlc, chk_dlc, dirs=None, lang=None):
        super().__init__(warc_cw09=True, lang=lang)
        self.docs_dlc = docs_dlc
        self.chk_dlc = chk_dlc
        # All available languages
        self.dirs = dirs or ['ClueWeb09_Arabic_1', 'ClueWeb09_Chinese_1', 'ClueWeb09_Chinese_2', 'ClueWeb09_Chinese_3', 'ClueWeb09_Chinese_4', 'ClueWeb09_English_1', 'ClueWeb09_English_2', 'ClueWeb09_English_3', 'ClueWeb09_English_4', 'ClueWeb09_English_5', 'ClueWeb09_English_6', 'ClueWeb09_English_7', 'ClueWeb09_English_8', 'ClueWeb09_English_9', 'ClueWeb09_English_10', 'ClueWeb09_French_1', 'ClueWeb09_German_1', 'ClueWeb09_Italian_1', 'ClueWeb09_Japanese_1', 'ClueWeb09_Japanese_2', 'ClueWeb09_Korean_1', 'ClueWeb09_Portuguese_1', 'ClueWeb09_Spanish_1', 'ClueWeb09_Spanish_2']
        self._docs_warc_file_counts_cache = None

    def docs_path(self):
        return self.docs_dlc.path()

    def _docs_iter_source_files(self):
        files = []
        for d in self.dirs:
            files += sorted(glob(os.path.join(self.docs_dlc.path(), d, '*')))
        for source_dir in files:
            for source_file in sorted(glob(os.path.join(source_dir, '*.gz'))):
                yield source_file

    def _docs_id_to_source_file(self, doc_id):
        parts = doc_id.split('-')
        if len(parts) != 4:
            return None
        dataset, sec, part, doc = parts
        if dataset != 'clueweb09':
            return None
        source_glob = os.path.join(self.docs_dlc.path(), f'ClueWeb09_*', sec, f'{part}.warc.gz')
        source_file = glob(source_glob)
        if len(source_file) == 0:
            return None
        if len(source_file) > 1:
            raise ValueError(f'doc_id {doc_id} found in multiple files: {source_file}')
        return source_file[0]

    def _docs_source_file_to_checkpoint(self, source_file):
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
            for d in self.dirs:
                counts_file = os.path.join(self.docs_dlc.path(), f'record_counts/{d}_counts.txt')
                with open(counts_file, 'rt') as f:
                    for line in f:
                        file, count = line.strip().split()
                        # Fixing bug in record_counts: en0054 is under ClueWeb09_English_4, not _5
                        if d == 'ClueWeb09_English_5' and 'en0054' in file:
                            file = os.path.join(self.docs_dlc.path(), 'ClueWeb09_English_4', file[3:])
                        else:
                            file = os.path.join(self.docs_dlc.path(), d, file[3:])
                        result[file] = int(count)
            self._docs_warc_file_counts_cache = result
        return self._docs_warc_file_counts_cache

    def docs_namespace(self):
        return NAME


class CatBQrelFilter:
    def __init__(self, qrels_handler):
        self._qrels_handler = qrels_handler

    def __getattr__(self, attr):
        return getattr(self._qrels_handler, attr)

    def qrels_iter(self):
        catb_segs = {'en0000','en0001','en0002','en0003','en0004','en0005','en0006','en0007','en0008','en0009','en0010','en0011','enwp00','enwp01','enwp02','enwp03'}
        for qrel in self._qrels_handler.qrels_iter():
            _, seg_id, _, _ = qrel.doc_id.split('-')
            if seg_id in catb_segs:
                yield qrel

    def qrels_handler(self):
        return self


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    docs_dlc = dlc['docs']
    chk_dlc = TarExtractAll(dlc['docs.chk'], base_path/'corpus.chk')
    collection = ClueWeb09Docs(docs_dlc, chk_dlc, lang=None) # multiple langs
    collection_ar = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_Arabic_1'], lang='ar')
    collection_zh = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_Chinese_1', 'ClueWeb09_Chinese_2', 'ClueWeb09_Chinese_3', 'ClueWeb09_Chinese_4'], lang='zh')
    collection_en = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_English_1', 'ClueWeb09_English_2', 'ClueWeb09_English_3', 'ClueWeb09_English_4', 'ClueWeb09_English_5', 'ClueWeb09_English_6', 'ClueWeb09_English_7', 'ClueWeb09_English_8', 'ClueWeb09_English_9', 'ClueWeb09_English_10'], lang='en')
    collection_fr = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_French_1'], lang='fr')
    collection_de = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_German_1'], lang='de')
    collection_it = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_Italian_1'], lang='it')
    collection_ja = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_Japanese_1', 'ClueWeb09_Japanese_2'], lang='ja')
    collection_ko = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_Korean_1'], lang='ko')
    collection_pt = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_Portuguese_1'], lang='pt')
    collection_es = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_Spanish_1', 'ClueWeb09_Spanish_2'], lang='es')
    collection_catb = ClueWeb09Docs(docs_dlc, chk_dlc, dirs=['ClueWeb09_English_1'], lang='en')
    base = Dataset(collection, documentation('_'))

    subsets['ar'] = Dataset(collection_ar, documentation('ar'))
    subsets['zh'] = Dataset(collection_zh, documentation('zh'))
    subsets['en'] = Dataset(collection_en, documentation('en'))
    subsets['fr'] = Dataset(collection_fr, documentation('fr'))
    subsets['de'] = Dataset(collection_de, documentation('de'))
    subsets['it'] = Dataset(collection_it, documentation('it'))
    subsets['ja'] = Dataset(collection_ja, documentation('ja'))
    subsets['ko'] = Dataset(collection_ko, documentation('ko'))
    subsets['pt'] = Dataset(collection_pt, documentation('pt'))
    subsets['es'] = Dataset(collection_es, documentation('es'))
    subsets['catb'] = Dataset(collection_catb, documentation('catb'))

    subsets['en/trec-web-2009'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2009/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        TrecPrels(GzipExtract(dlc['trec-web-2009/qrels.adhoc']), QREL_DEFS_09),
        documentation('trec-web-2009'))

    subsets['en/trec-web-2010'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2010/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-web-2010/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2010'))

    subsets['en/trec-web-2011'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2011/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-web-2011/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2011'))

    subsets['en/trec-web-2012'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2012/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        TrecQrels(dlc['trec-web-2012/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2012'))

    subsets['catb/trec-web-2009'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2009/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        CatBQrelFilter(TrecPrels(GzipExtract(dlc['trec-web-2009/qrels.adhoc']), QREL_DEFS_09)),
        documentation('trec-web-2009'))

    subsets['catb/trec-web-2010'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2010/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        CatBQrelFilter(TrecQrels(dlc['trec-web-2010/qrels.adhoc'], QREL_DEFS)),
        documentation('trec-web-2010'))

    subsets['catb/trec-web-2011'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2011/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        CatBQrelFilter(TrecQrels(dlc['trec-web-2011/qrels.adhoc'], QREL_DEFS)),
        documentation('trec-web-2011'))

    subsets['catb/trec-web-2012'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2012/queries'], qtype=TrecWebTrackQuery, namespace=NAME, lang='en'),
        CatBQrelFilter(TrecQrels(dlc['trec-web-2012/qrels.adhoc'], QREL_DEFS)),
        documentation('trec-web-2012'))

    subsets['trec-mq-2009'] = Dataset(
        collection,
        TrecColonQueries(GzipExtract(dlc['trec-mq-2009/queries']), encoding='latin1', lang='en'),
        TrecPrels(GzipExtract(dlc['trec-mq-2009/qrels']), QREL_DEFS_09),
        documentation('trec-mq-2009'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
