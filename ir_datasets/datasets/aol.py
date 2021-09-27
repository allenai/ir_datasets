import pickle
import re
import contextlib
from collections import Counter
from hashlib import md5
import ir_datasets
from typing import NamedTuple, Tuple
from ir_datasets.util import DownloadConfig, GzipExtract, TarExtract, finialized_file
from ir_datasets.formats import TrecQrels, TsvQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation

_logger = ir_datasets.log.easy()

NAME = 'aol'

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
    session_id: str
    query_id: str
    query: str
    query_orig: str
    time: str
    items: Tuple[LogItem, ...]


class _ManagedDlc:
    def __init__(self, manager, path):
        self._manager = manager
        self._path = path

    @contextlib.contextmanager
    def stream(self):
        self._manager.build()
        with open(self._path, 'rb') as f:
            yield f

    def path(self):
        self._manager.build()
        return self._path


class Sessionizer:
    def __init__(self, glove_model='glove-wiki-gigaword-300', threshold=0.5):
        import gensim.downloader
        self.prev_user_id = None
        self.prev_repr = None
        self.prev_session_id = None
        self.prev_qid = None
        self.user_session_counter = Counter()
        with _logger.duration(f'loading {glove_model}'):
            self.glove = gensim.downloader.load(glove_model)
        self.threshold = threshold

    def next_session_id(self, qid, query, user_id):
        sim, query_repr = self.get_sim_repr(qid, query, user_id)
        if sim > self.threshold:
            session_id = self.prev_session_id
        else:
            session_id = f'{user_id}_{self.user_session_counter[user_id]}'
            self.user_session_counter[user_id] += 1
        self.prev_user_id = user_id
        self.prev_repr = query_repr
        self.prev_session_id = session_id
        self.prev_qid = qid
        return session_id

    def get_sim_repr(self, qid, query, user_id):
        np = ir_datasets.lazy_libs.numpy()
        if self.prev_user_id != user_id:
            return 0., None
        if self.prev_qid == qid:
            return 1., self.prev_repr
        vecs = [self.glove.get_vector(t.lower()) for t in query.split() if t.lower() in self.glove]
        if vecs:
            query_repr = np.stack(vecs).mean(axis=0)
            if self.prev_repr is not None:
                return np.dot(query_repr, self.prev_repr)/(np.linalg.norm(query_repr)*np.linalg.norm(self.prev_repr)), query_repr
            return 0., query_repr
        return 0., None



class AolManager:
    def __init__(self, log_dlcs, id2wb_dlc, base_path):
        self._log_dlcs = log_dlcs
        self.id2wb_dlc = id2wb_dlc
        self._docs_store = None
        self._base_path = base_path
        self._logs_built = None
        if not self._base_path.exists():
            self._base_path.mkdir(exist_ok=True, parents=True)

    def docs_store(self):
        self.build()
        return self._internal_docs_store()

    def _internal_docs_store(self):
        if self._docs_store is None:
            self._docs_store = ir_datasets.indices.PickleLz4FullStore(self._base_path/'docs.pklz4', None, NqPassageDoc, 'doc_id', ['doc_id'], count_hint=28390850)
        return self._docs_store

    def build(self):
        self._build_logs()

    def _build_logs(self):
        from nltk import word_tokenize
        if self._logs_built is None:
            self._logs_built = (self._base_path/'_built_logs').exists()
        if self._logs_built:
            return # already built

        sessionizer = Sessionizer()
        lz4_frame = ir_datasets.lazy_libs.lz4_frame().frame

        encountered_qids = set()
        with finialized_file(self._base_path/'queries.tsv', 'wt') as f_queries, \
             finialized_file(self._base_path/'qrels', 'wt') as f_qrels, \
             finialized_file(self._base_path/'log.pkl.lz4', 'wb') as f_log, \
             lz4_frame.LZ4FrameFile(f_log, 'wb') as f_log, \
             finialized_file(self._base_path/'log.background.pkl.lz4', 'wb') as f_log_background, \
             lz4_frame.LZ4FrameFile(f_log_background, 'wb') as f_log_background, \
             finialized_file(self._base_path/'log.train.pkl.lz4', 'wb') as f_log_train, \
             lz4_frame.LZ4FrameFile(f_log_train, 'wb') as f_log_train, \
             finialized_file(self._base_path/'log.dev.pkl.lz4', 'wb') as f_log_dev, \
             lz4_frame.LZ4FrameFile(f_log_dev, 'wb') as f_log_dev, \
             finialized_file(self._base_path/'log.test.pkl.lz4', 'wb') as f_log_test, \
             lz4_frame.LZ4FrameFile(f_log_test, 'wb') as f_log_test, \
             _logger.pbar_raw(desc=f'preparing {NAME} log lines', total=36389567) as pbar:
            for dlc in self._log_dlcs:
                with dlc.stream() as fin:
                    assert next(fin) == b'AnonID\tQuery\tQueryTime\tItemRank\tClickURL\n' # skip header
                    for line in fin:
                        pbar.update()
                        cols = line.decode().rstrip('\n').split('\t')
                        if len(cols) == 3:
                            user_id, query, query_time = cols
                            rank, url = None, None
                        else:
                            user_id, query, query_time, rank, url = cols
                        norm_query = ' '.join(word_tokenize(re.sub('[^A-Za-z0-9 ]', ' ', query.rstrip())))
                        query_id = md5(norm_query.encode()).hexdigest()[:QID_LEN]
                        if query_id not in encountered_qids:
                            f_queries.write(f'{query_id}\t{norm_query}\n')
                            encountered_qids.add(query_id)
                        log_items = []
                        session_id = sessionizer.next_session_id(query_id, norm_query, user_id)
                        if url is not None:
                            doc_id = md5(url.encode()).hexdigest()[:DID_LEN]
                            f_qrels.write(f'{query_id}\t{session_id}\t{doc_id}\t1\n')
                            log_items.append(LogItem(doc_id, rank, True))
                        log_record = AolQlog(user_id, session_id, query_id, norm_query, query, query_time, tuple(log_items))
                        pickle.dump(log_record, f_log)
                        y, m, d = map(int, query_time.split(' ')[0].split('-'))
                        if m == 3 or (m == 4 and d < 8):
                            f_split = f_log_background
                        elif m == 4 or (m == 5 and d < 17):
                            f_split = f_log_train
                        elif m == 5 and d < 24:
                            f_split = f_log_dev
                        elif m == 5:
                            f_split = f_log_test
                        else:
                            f_split = None
                        pickle.dump(log_record, f_split)
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
        TsvQueries(manager.file_ref('queries.tsv'), lang='en'),
        TrecQrels(manager.file_ref('qrels'), QREL_DEFS),
        documentation('_'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets, manager


base, subsets, manager = _init()
