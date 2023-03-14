from pathlib import Path
import json
import re
import os
import io
import hashlib
from datetime import datetime
from typing import NamedTuple, Tuple
import contextlib
import ir_datasets
from ir_datasets.util import TarExtract, TarExtractAll, RelativePath, DownloadConfig, Cache, IterStream
from ir_datasets.formats import TrecQrels, TrecDocs, TrecQueries, GenericQuery, TrecScoredDocs, BaseQueries, TsvDocPairs, BaseQrels, BaseScoredDocs, TsvDocs, BaseQlogs
from ir_datasets.datasets.base import Dataset, YamlDocumentation


_logger = ir_datasets.log.easy()


NAME = 'tripclick'

QREL_DEFS = {
    1: 'clicked',
    0: 'not clicked and appeared higher in search results',
}

QREL_DCTR_DEFS = {
    3: 'highly relevant; clicked more than 30% of the times it was shown',
    2: 'relevant; clicked more than 4% but less than 30% of times it was shown',
    1: 'partially relevant; clicked less than 4% of times it was shown (but at least once)',
    0: 'not relevant; never clicked',
}

QTYPE_MAP = {
    '<num> *(Number:)? *': 'query_id',
    '<title> *': 'text',
}

Q_HASH_LEN = 11


class ConcatQueries(BaseQueries):
    def __init__(self, queries):
        self._queries = queries

    def queries_iter(self):
        for q in self._queries:
            yield from q.queries_iter()

    def queries_path(self):
        return None

    def queries_cls(self):
        return self._queries[0].queries_cls()

    def queries_namespace(self):
        return self._queries[0].queries_namespace()

    def queries_lang(self):
        return self._queries[0].queries_lang()


class ConcatQrels(BaseQrels):
    def __init__(self, qrels):
        self._qrels = qrels

    def qrels_iter(self):
        for q in self._qrels:
            yield from q.qrels_iter()

    def qrels_path(self):
        return None

    def qrels_cls(self):
        return self._qrels[0].qrels_cls()

    def qrels_defs(self):
        return self._qrels[0].qrels_defs()


class ConcatScoreddocs(BaseScoredDocs):
    def __init__(self, scoreddocs):
        self._scoreddocs = scoreddocs

    def scoreddocs_iter(self):
        for q in self._scoreddocs:
            yield from q.scoreddocs_iter()

    def scoreddocs_path(self, force=True):
        return None

    def scoreddocs_cls(self):
        return self._scoreddocs[0].scoreddocs_cls()


class LogItem(NamedTuple):
    doc_id: str
    clicked: bool


class TripClickQlog(NamedTuple):
    session_id: str
    query_id: str
    query: str
    query_orig: str
    time: datetime
    items: Tuple[LogItem, ...]

class TripClickPartialDoc(NamedTuple):
    doc_id: str
    title: str
    url: str
    def default_text(self):
        """
        title
        """
        return self.title


class TripClickQlogs(BaseQlogs):
    def __init__(self, dlc):
        self.dlc = dlc

    def qlogs_iter(self):
        for file in sorted(Path(self.dlc.path()).glob('logs/*.json')):
            with file.open('rt') as fin:
                for line in fin:
                    record = json.loads(line)
                    time = re.match(r'^/Date\(([0-9]+)\)/$', record['DateCreated']).group(1)
                    query_norm = re.sub(r'\b(AND|OR)\b', ' ', record['Keywords']).replace('title:', ' ')
                    query_norm = ' '.join(ir_datasets.util.ws_tok(query_norm))
                    items = [LogItem(str(did), did == record['DocumentId']) for did in record['Documents']]
                    if record['DocumentId'] and not any(i.clicked for i in items):
                        items += [LogItem(str(record['DocumentId']), True)]
                    yield TripClickQlog(
                        record['SessionId'],
                        hashlib.md5(query_norm.encode()).hexdigest()[:Q_HASH_LEN],
                        query_norm,
                        record['Keywords'],
                        datetime.fromtimestamp(int(time)/1000),
                        tuple(items)
                    )

    def qlogs_cls(self):
        return TripClickQlog

    def qlogs_count(self):
        return 5_317_350


class DocPairGenerator:
    def __init__(self, docpair_dlc, collection, queries, cache_path):
        self._docpair_dlc = docpair_dlc
        self._collection = collection
        self._queries = queries
        self._cache_path = cache_path

    def path(self, force=True):
        if force and not os.path.exists(self._cache_path):
            _logger.info('tripclick includes docpairs in an expanded format (with raw text). Linking these records back to the query and doc IDs.')
            SPACES = re.compile(r'\s+')
            doc_map = {}
            for doc in _logger.pbar(self._collection.docs_iter(), desc='build doc lookup', unit='doc'):
                # doctext = f'{doc.title} <eot> {doc.text}'.replace('\t', ' ').replace('\n', ' ').replace('\u2029', ' ').replace('\u2028', ' ').replace('  ', ' ').strip()
                doctext = SPACES.sub(' ', f'{doc.title} <eot> {doc.text}').strip()
                dochash = hashlib.md5(doctext.encode()).digest()[:6]
                doc_map[dochash] = doc.doc_id
            query_map = {}
            for query in _logger.pbar(self._queries.queries_iter(), desc='build query lookup', unit='query'):
                queryhash = hashlib.md5(SPACES.sub(' ', query.text).strip().encode()).digest()[:6]
                query_map[queryhash] = query.query_id
            with ir_datasets.util.finialized_file(self._cache_path, 'wt') as fout, \
                 self._docpair_dlc.stream() as stream, \
                 _logger.pbar_raw(desc='building docpairs', total=23_222_038, unit='docpair') as pbar:
                skipped = 0
                for line in stream:
                    pbar.update()
                    query, doc1, doc2 = line.strip().split(b'\t')
                    queryhash = hashlib.md5(SPACES.sub(' ', query.decode()).strip().encode()).digest()[:6]
                    doc1hash = hashlib.md5(SPACES.sub(' ', doc1.decode()).strip().encode()).digest()[:6]
                    doc2hash = hashlib.md5(SPACES.sub(' ', doc2.decode()).strip().encode()).digest()[:6]
                    qid, did1, did2 = query_map.get(queryhash), doc_map.get(doc1hash), doc_map.get(doc2hash)
                    if qid is None or did1 is None or did2 is None:
                        skipped += 1
                        pbar.set_postfix({'sk': skipped})
                        continue
                    fout.write(f'{qid}\t{did1}\t{did2}\n')
                _logger.info(f'{skipped} lines skipped because queries/documents could not be matched')
        return self._cache_path

    @contextlib.contextmanager
    def stream(self):
        with open(self.path(), 'rb') as f:
            yield f


# The allarticles.txt file (tsv) has a couple of problems, stemming from the fact that titles
# can include \t and \n characters. This class corrects these problems. It also removed the
# first (header) line and the final line ("(5196956 rows affected)"), and corrects a few strange
# things with some URLs.
class FixAllarticles:
    def __init__(self, streamer):
        self._streamer = streamer

    def stream(self):
        return io.BufferedReader(IterStream(iter(self)), buffer_size=io.DEFAULT_BUFFER_SIZE)

    def __iter__(self):
        with self._streamer.stream() as stream, \
             _logger.pbar_raw(desc='fixing allarticles.txt', unit='B', unit_scale=True) as pbar:
            # NOTE: codecs.getreader is subtly broken here; it sometimes splits lines between special characters (and it's unclear why)
            next(stream) # remove header
            did, title, url = None, [], None
            for line in stream:
                pbar.update(len(line))
                line = line.decode().strip()
                if line == '' or line == '(5196956 rows affected)':
                    continue
                cols = line.split('\t')
                if did is None:
                    did = cols[0]
                    assert did.isnumeric(), line
                    cols = cols[1:]
                if did in ('9283014', '11088688', '11114797'): # a few special cases where the URL is actually missing. If we don't fix this here, we'll end up messing up subsequent records
                    yield ('\t'.join([did,  ' '.join(cols), '']) + '\n').encode()
                    did, title, url = None, [], None
                if len(cols) > 0:
                    if cols[-1].startswith('_http'): # some URLs have this strange prefix, remove
                        cols[-1] = cols[-1][1:]
                    if cols[-1].startswith('ttp://'):
                        cols[-1] = 'h' + cols[-1]
                    if cols[-1].startswith('http') or cols[-1].startswith('file:///C:'):
                        title += cols[:-1]
                        url = cols[-1]
                        yield ('\t'.join([did, ' '.join(title).strip(), url]) + '\n').encode()
                        did, title, url = None, [], None
                    else:
                        title += cols


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection = TrecDocs(dlc['benchmark'], parser='tut', path_globs=['**/docs_grp_*.txt'], namespace=NAME, lang='en', count_hint=ir_datasets.util.count_hint(NAME))
    topics_and_qrels = TarExtractAll(dlc['benchmark'], base_path/"topics_and_qrels", path_globs=['**/topics.*.txt', '**/qrels.*.txt'])
    val_runs = TarExtractAll(dlc['dlfiles'], base_path/"val_runs", path_globs=['**/run.trip.BM25.*.val.txt'])
    test_runs = TarExtractAll(dlc['dlfiles_runs_test'], base_path/"test_runs", path_globs=['**/run.trip.BM25.*.test.txt'])

    base = Dataset(
        collection,
        documentation('_'))

    subsets['logs'] = Dataset(
        TsvDocs(Cache(FixAllarticles(TarExtract(dlc['logs'], 'logs/allarticles.txt')), base_path/'allarticles-fixed.tsv'), doc_cls=TripClickPartialDoc, lang='en', count_hint=ir_datasets.util.count_hint(f'{NAME}/logs')),
        TripClickQlogs(TarExtractAll(dlc['logs'], base_path/'logs', path_globs=['**/*.json'])),
        documentation('logs'))

    ### Train

    subsets['train/head'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.head.train.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.raw.head.train.txt'), QREL_DEFS),
        documentation('train/head'))

    subsets['train/head/dctr'] = Dataset(
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.dctr.head.train.txt'), QREL_DCTR_DEFS),
        subsets['train/head'],
        documentation('train/head/dctr'))

    subsets['train/torso'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.torso.train.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.raw.torso.train.txt'), QREL_DEFS),
        documentation('train/torso'))

    subsets['train/tail'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.tail.train.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.raw.tail.train.txt'), QREL_DEFS),
        documentation('train/tail'))

    train_queries = ConcatQueries([
        subsets['train/head'].queries_handler(),
        subsets['train/torso'].queries_handler(),
        subsets['train/tail'].queries_handler(),
    ])
    train_docpairs = DocPairGenerator(TarExtract(dlc['dlfiles'], 'dlfiles/triples.train.tsv'), collection, train_queries, base_path/'train.docpairs')
    subsets['train'] = Dataset(
        collection,
        train_queries,
        ConcatQrels([
            subsets['train/head'].qrels_handler(),
            subsets['train/torso'].qrels_handler(),
            subsets['train/tail'].qrels_handler(),
        ]),
        TsvDocPairs(train_docpairs),
        documentation('train'))
    subsets['train/hofstaetter-triples'] = Dataset(
        collection,
        train_queries,
        subsets['train'].qrels_handler(),
        TsvDocPairs(dlc['hofstaetter-triples']),
        documentation('train/hofstaetter-triples'))

    ### Val

    subsets['val/head'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.head.val.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.raw.head.val.txt'), QREL_DEFS),
        TrecScoredDocs(RelativePath(val_runs, 'dlfiles/run.trip.BM25.head.val.txt')),
        documentation('val/head'))

    subsets['val/head/dctr'] = Dataset(
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.dctr.head.val.txt'), QREL_DCTR_DEFS),
        subsets['val/head'],
        documentation('val/head/dctr'))

    subsets['val/torso'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.torso.val.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.raw.torso.val.txt'), QREL_DEFS),
        TrecScoredDocs(RelativePath(val_runs, 'dlfiles/run.trip.BM25.torso.val.txt')),
        documentation('val/torso'))

    subsets['val/tail'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.tail.val.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecQrels(RelativePath(topics_and_qrels, 'benchmark/qrels/qrels.raw.tail.val.txt'), QREL_DEFS),
        TrecScoredDocs(RelativePath(val_runs, 'dlfiles/run.trip.BM25.tail.val.txt')),
        documentation('val/tail'))

    subsets['val'] = Dataset(
        collection,
        ConcatQueries([
            subsets['val/head'].queries_handler(),
            subsets['val/torso'].queries_handler(),
            subsets['val/tail'].queries_handler(),
        ]),
        ConcatQrels([
            subsets['val/head'].qrels_handler(),
            subsets['val/torso'].qrels_handler(),
            subsets['val/tail'].qrels_handler(),
        ]),
        ConcatScoreddocs([
            subsets['val/head'].scoreddocs_handler(),
            subsets['val/torso'].scoreddocs_handler(),
            subsets['val/tail'].scoreddocs_handler(),
        ]),
        documentation('val'))

    ### Test

    subsets['test/head'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.head.test.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecScoredDocs(RelativePath(test_runs, 'runs_test/run.trip.BM25.head.test.txt')),
        documentation('val/head'))

    subsets['test/torso'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.torso.test.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecScoredDocs(RelativePath(test_runs, 'runs_test/run.trip.BM25.torso.test.txt')),
        documentation('test/torso'))

    subsets['test/tail'] = Dataset(
        collection,
        TrecQueries(RelativePath(topics_and_qrels, 'benchmark/topics/topics.tail.test.txt'), qtype=GenericQuery, qtype_map=QTYPE_MAP, namespace=NAME, lang='en'),
        TrecScoredDocs(RelativePath(test_runs, 'runs_test/run.trip.BM25.tail.test.txt')),
        documentation('test/tail'))

    subsets['test'] = Dataset(
        collection,
        ConcatQueries([
            subsets['test/head'].queries_handler(),
            subsets['test/torso'].queries_handler(),
            subsets['test/tail'].queries_handler(),
        ]),
        ConcatScoreddocs([
            subsets['test/head'].scoreddocs_handler(),
            subsets['test/torso'].scoreddocs_handler(),
            subsets['test/tail'].scoreddocs_handler(),
        ]),
        documentation('test'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
