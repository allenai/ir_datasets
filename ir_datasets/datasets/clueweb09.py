import os
from collections import namedtuple
from glob import glob
import ir_datasets
from ir_datasets.util import GzipExtract, Lazy, DownloadConfig, TarExtract, Cache, Bz2Extract, ZipExtract
from ir_datasets.formats import TrecQrels, TrecDocs, TrecXmlQueries, WarcDocs, GenericDoc, GenericQuery, TrecQrel
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

TrecWebTrackQuery = namedtuple('TrecWebTrackQuery', ['query_id', 'query', 'description', 'type', 'subtopics'])
TrecPrel = namedtuple('TrecPrel', ['query_id', 'doc_id', 'relevance', 'method', 'iprob'])


class ClueWeb09Docs(WarcDocs):
    def __init__(self, docs_dlc, langs=None):
        super().__init__(warc_cw09=True)
        self.docs_dlc = docs_dlc
        # All available languages
        self.langs = langs or ['Arabic', 'Chinese', 'English', 'French', 'German', 'Italian', 'Japanese', 'Korean', 'Portuguese', 'Spanish']

    def docs_path(self):
        return self.docs_dlc.path()

    def _docs_iter_source_files(self):
        files = []
        for lang in self.langs:
            files += sorted(glob(os.path.join(self.docs_dlc.path(), f'ClueWeb09_{lang}_*', '*')))
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
    collection_en = ClueWeb09Docs(docs_dlc, langs=['English'])
    base = Dataset(collection, documentation('_'))

    subsets['en'] = Dataset(collection_en, documentation('en'))

    subsets['en/trec-web-2009'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2009/queries'], qtype=TrecWebTrackQuery),
        TrecPrels(GzipExtract(dlc['trec-web-2009/qrels.adhoc']), QREL_DEFS_09),
        documentation('trec-web-2009'))

    subsets['en/trec-web-2010'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2010/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2010/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2010'))

    subsets['en/trec-web-2011'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2011/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2011/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2011'))

    subsets['en/trec-web-2012'] = Dataset(
        collection_en,
        TrecXmlQueries(dlc['trec-web-2012/queries'], qtype=TrecWebTrackQuery),
        TrecQrels(dlc['trec-web-2012/qrels.adhoc'], QREL_DEFS),
        documentation('trec-web-2012'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
