import io
import codecs
import re
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import Cache, TarExtract, IterStream, GzipExtract, Lazy, DownloadConfig
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredScoredDocs, FilteredQrels, FilteredDocPairs, YamlDocumentation
from ir_datasets.formats import TsvQueries, TsvDocs, TrecQrels, TrecScoredDocs, TsvDocPairs, BaseQueries

NAME = 'nfcorpus'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    2: "A direct link from the query to the document the cited sources section of a page.",
    1: "A link exists from the query to another query that directly links to the document.",
    0: "Marginally relevant, based on topic containment.",
}

class NfCorpusDoc(NamedTuple):
    doc_id: str
    url: str
    title: str
    abstract: str

class NfCorpusQuery(NamedTuple):
    query_id: str
    title: str
    all: str

class NfCorpusVideoQuery(NamedTuple):
    query_id: str
    title: str
    desc: str

class ZipQueries(BaseQueries):
    def __init__(self, queries, idxs, qtype):
        self._queries = queries
        self._idxs = idxs
        self._qtype = qtype

    def queries_iter(self):
        for qs in zip(*(q.queries_iter() for q in self._queries)):
            assert len({q.query_id for q in qs}) == 1 # all query IDs should be the same
            yield self._qtype(*(qs[i][j] for i, j in self._idxs))

    def queries_cls(self):
        return self._qtype

    def queries_path(self):
        return self._queries[0].queries_path()

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return self._queries[0].queries_lang()


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    main_dlc = dlc['main']

    collection = TsvDocs(Cache(TarExtract(main_dlc, 'nfcorpus/raw/doc_dump.txt'), base_path/'collection.tsv'), doc_cls=NfCorpusDoc, namespace=NAME, lang='en')
    subsets = {}

    def read_lines(file):
        file = Cache(TarExtract(main_dlc, f'nfcorpus/raw/{file}'), base_path/file)
        with file.stream() as stream:
            stream = codecs.getreader('utf8')(stream)
            return {l.rstrip() for l in stream}
    nontopic_qid_filter = Lazy(lambda: read_lines('nontopics.ids'))
    video_qid_filter = Lazy(lambda: read_lines('all_videos.ids'))

    subsets['train'] = Dataset(
        collection,
        ZipQueries([
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/train.titles.queries'), base_path/'train/queries.titles.tsv'), namespace=NAME, lang='en'),
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/train.all.queries'), base_path/'train/queries.all.tsv'), namespace=NAME, lang='en'),
        ], [(0, 0), (0, 1), (1, 1)], NfCorpusQuery),
        TrecQrels(Cache(TarExtract(main_dlc, 'nfcorpus/train.3-2-1.qrel'), base_path/'train/qrels'), QRELS_DEFS),
        documentation('train'),
    )

    subsets['train/nontopic'] = Dataset(
        collection,
        TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/train.nontopic-titles.queries'), base_path/'train/nontopic/queries.tsv'), namespace=NAME, lang='en'),
        FilteredQrels(subsets['train'].qrels_handler(), nontopic_qid_filter, mode='include'),
        documentation('train/nontopic'),
    )

    subsets['train/video'] = Dataset(
        collection,
        ZipQueries([
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/train.vid-titles.queries'), base_path/'train/video/queries.titles.tsv'), namespace=NAME, lang='en'),
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/train.vid-desc.queries'), base_path/'train/video/queries.desc.tsv'), namespace=NAME, lang='en'),
        ], [(0, 0), (0, 1), (1, 1)], NfCorpusVideoQuery),
        FilteredQrels(subsets['train'].qrels_handler(), video_qid_filter, mode='include'),
        documentation('train/video'),
    )

    subsets['dev'] = Dataset(
        collection,
        ZipQueries([
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/dev.titles.queries'), base_path/'dev/queries.titles.tsv'), namespace=NAME, lang='en'),
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/dev.all.queries'), base_path/'dev/queries.all.tsv'), namespace=NAME, lang='en'),
        ], [(0, 0), (0, 1), (1, 1)], NfCorpusQuery),
        TrecQrels(Cache(TarExtract(main_dlc, 'nfcorpus/dev.3-2-1.qrel'), base_path/'dev/qrels'), QRELS_DEFS),
        documentation('dev'),
    )

    subsets['dev/nontopic'] = Dataset(
        collection,
        TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/dev.nontopic-titles.queries'), base_path/'dev/nontopic/queries.tsv'), namespace=NAME, lang='en'),
        FilteredQrels(subsets['dev'].qrels_handler(), nontopic_qid_filter, mode='include'),
        documentation('dev/nontopic'),
    )

    subsets['dev/video'] = Dataset(
        collection,
        ZipQueries([
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/dev.vid-titles.queries'), base_path/'dev/video/queries.titles.tsv'), namespace=NAME, lang='en'),
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/dev.vid-desc.queries'), base_path/'dev/video/queries.desc.tsv'), namespace=NAME, lang='en'),
        ], [(0, 0), (0, 1), (1, 1)], NfCorpusVideoQuery),
        FilteredQrels(subsets['dev'].qrels_handler(), video_qid_filter, mode='include'),
        documentation('dev/video'),
    )

    subsets['test'] = Dataset(
        collection,
        ZipQueries([
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/test.titles.queries'), base_path/'test/queries.titles.tsv'), namespace=NAME, lang='en'),
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/test.all.queries'), base_path/'test/queries.all.tsv'), namespace=NAME, lang='en'),
        ], [(0, 0), (0, 1), (1, 1)], NfCorpusQuery),
        TrecQrels(Cache(TarExtract(main_dlc, 'nfcorpus/test.3-2-1.qrel'), base_path/'test/qrels'), QRELS_DEFS),
        documentation('test'),
    )

    subsets['test/nontopic'] = Dataset(
        collection,
        TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/test.nontopic-titles.queries'), base_path/'test/nontopic/queries.tsv'), namespace=NAME, lang='en'),
        FilteredQrels(subsets['test'].qrels_handler(), nontopic_qid_filter, mode='include'),
        documentation('test/nontopic'),
    )

    subsets['test/video'] = Dataset(
        collection,
        ZipQueries([
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/test.vid-titles.queries'), base_path/'test/video/queries.titles.tsv'), namespace=NAME, lang='en'),
            TsvQueries(Cache(TarExtract(main_dlc, 'nfcorpus/test.vid-desc.queries'), base_path/'test/video/queries.desc.tsv'), namespace=NAME, lang='en'),
        ], [(0, 0), (0, 1), (1, 1)], NfCorpusVideoQuery),
        FilteredQrels(subsets['test'].qrels_handler(), video_qid_filter, mode='include'),
        documentation('test/video'),
    )

    ir_datasets.registry.register(NAME, Dataset(collection, documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return collection, subsets


collection, subsets = _init()
