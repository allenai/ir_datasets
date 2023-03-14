from datetime import datetime
import json
import pickle
import re
import contextlib
from collections import Counter
from hashlib import md5
import ir_datasets
from typing import NamedTuple, Tuple
from ir_datasets.util import DownloadConfig, GzipExtract, TarExtract, finialized_file
from ir_datasets.formats import TrecQrels, TsvQueries, DocstoreBackedDocs, BaseQlogs
from ir_datasets.datasets.base import Dataset, YamlDocumentation

_logger = ir_datasets.log.easy()

NAME = 'aol-ia'

QREL_DEFS = {
    1: 'clicked',
}

QID_LEN = 14
DID_LEN = 12


class LogItem(NamedTuple):
    doc_id: str
    rank: int
    clicked: bool


class AolQlog(NamedTuple):
    user_id: str
    query_id: str
    query: str
    query_orig: str
    time: datetime
    items: Tuple[LogItem, ...]

class AolIaDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    url: str
    ia_url: str
    def default_text(self):
        """
        title and text
        """
        return f'{self.title} {self.text}'


class AolQlogs(BaseQlogs):
    def __init__(self, dlc):
        self.dlc = dlc

    def qlogs_iter(self):
        LZ4FrameFile = ir_datasets.lazy_libs.lz4_frame().frame.LZ4FrameFile
        with self.dlc.stream() as fin, \
             LZ4FrameFile(fin) as fin:
            try:
                while True:
                    yield pickle.load(fin)
            except EOFError:
                pass

    def qlogs_cls(self):
        return AolQlog

    def qlogs_count(self):
        return 36_389_567


class _ManagedDlc:
    def __init__(self, manager, path):
        self._manager = manager
        self._path = path

    @contextlib.contextmanager
    def stream(self):
        self._manager.build()
        with open(self._path, 'rb') as f:
            yield f

    def path(self, force=True):
        if force:
            self._manager.build()
        return self._path


class AolManager:
    def __init__(self, log_dlcs, id2wb_dlc, base_path):
        self._log_dlcs = log_dlcs
        self.id2wb_dlc = id2wb_dlc # exposed for aolia-tools
        self._docs_store = None
        self._base_path = base_path
        self._logs_built = None
        if not self._base_path.exists():
            self._base_path.mkdir(exist_ok=True, parents=True)

    def docs_store(self):
        self._build_docs()
        return self._internal_docs_store()

    def _internal_docs_store(self):
        if self._docs_store is None:
            self._docs_store = ir_datasets.indices.PickleLz4FullStore(self._base_path/'docs.pklz4', None, AolIaDoc, 'doc_id', ['doc_id'], count_hint=ir_datasets.util.count_hint(NAME))
        return self._docs_store

    def _build_docs(self):
        if self._internal_docs_store().built():
            return
        if not (self._base_path/'downloaded_docs'/'_done').exists():
            raise RuntimeError('''To use the documents of AOLIA, you will need to run the download script in https://github.com/terrierteam/aolia-tools. To run the script, use the following commands:

git clone https://github.com/terrierteam/aolia-tools
cd aolia-tools
pip install -r requirements.txt
python downloader.py
''')
        LZ4FrameFile = ir_datasets.lazy_libs.lz4_frame().frame.LZ4FrameFile
        with _logger.pbar_raw(desc='', total=1525535) as pbar, self._internal_docs_store().lookup.transaction() as transaction:
            for file in sorted((self._base_path/'downloaded_docs').glob('*.jsonl.lz4')):
                pbar.set_postfix({'file': file.name})
                docs = []
                with LZ4FrameFile(file, 'rb') as fin:
                    for line in fin:
                        doc = json.loads(line)
                        docs.append(AolIaDoc(doc['doc_id'], doc['title'], doc['text'], doc['url'], doc['wb_url']))
                        pbar.update()
                for doc in sorted(docs, key=lambda x: x.doc_id): # sort the documents in each file before adding them to the docstore. This ensures a consistent ordering.
                    transaction.add(doc)

    def build(self):
        if self._logs_built is None:
            self._logs_built = (self._base_path/'_built_logs').exists()
        if self._logs_built:
            return # already built

        # sessionizer = Sessionizer()
        lz4_frame = ir_datasets.lazy_libs.lz4_frame().frame

        encountered_qids = set()
        with finialized_file(self._base_path/'queries.tsv', 'wt') as f_queries, \
             finialized_file(self._base_path/'qrels', 'wt') as f_qrels, \
             finialized_file(self._base_path/'log.pkl.lz4', 'wb') as f_log, \
             lz4_frame.LZ4FrameFile(f_log, 'wb') as f_log, \
             _logger.pbar_raw(desc=f'preparing {NAME} log lines', total=36389567) as pbar:
            for dlc in self._log_dlcs:
                with dlc.stream() as fin:
                    assert next(fin) == b'AnonID\tQuery\tQueryTime\tItemRank\tClickURL\n' # skip header
                    for line in fin:
                        pbar.update()
                        cols = line.decode().rstrip('\n').split('\t')
                        if tuple(cols[3:]) == ('', ''):
                            user_id, query, query_time, _, _ = cols
                            rank, url = None, None
                        else:
                            user_id, query, query_time, rank, url = cols
                        norm_query = ' '.join(ir_datasets.util.ws_tok(query))
                        query_id = md5(norm_query.encode()).hexdigest()[:QID_LEN]
                        if query_id not in encountered_qids:
                            f_queries.write(f'{query_id}\t{norm_query}\n')
                            encountered_qids.add(query_id)
                        log_items = []
                        if url is not None:
                            doc_id = md5(url.encode()).hexdigest()[:DID_LEN]
                            f_qrels.write(f'{query_id}\t{user_id}\t{doc_id}\t1\n')
                            log_items.append(LogItem(doc_id, rank, True))
                        log_record = AolQlog(user_id, query_id, norm_query, query, datetime.fromisoformat(query_time), tuple(log_items))
                        pickle.dump(log_record, f_log)

        (self._base_path/'_built_logs').touch()
        self._logs_built = True

    def file_ref(self, path):
        return _ManagedDlc(self, self._base_path/path)



def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    manager = AolManager([
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-01.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-02.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-03.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-04.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-05.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-06.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-07.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-08.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-09.txt.gz')),
        GzipExtract(TarExtract(dlc['logs'], 'AOL-user-ct-collection/user-ct-test-collection-10.txt.gz')),
    ], GzipExtract(dlc['id2wb']), base_path)

    base = Dataset(
        DocstoreBackedDocs(manager.docs_store, docs_cls=AolIaDoc, namespace=NAME, lang=None),
        TsvQueries(manager.file_ref('queries.tsv'), lang=None),
        TrecQrels(manager.file_ref('qrels'), QREL_DEFS),
        AolQlogs(manager.file_ref('log.pkl.lz4')),
        documentation('_'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets, manager, base_path


# Be sure to keep MANAGER and PATH here; they are used by aolia-tools
base, subsets, MANAGER, PATH = _init()
