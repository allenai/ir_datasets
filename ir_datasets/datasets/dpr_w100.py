from typing import NamedTuple, Tuple
import contextlib
import itertools
import ir_datasets
from ir_datasets.util import GzipExtract
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import TsvDocs, BaseQueries, TrecQrels

_logger = ir_datasets.log.easy()


NAME = 'dpr-w100'


QREL_DEFS = {
    2: 'marked by human annotator as containing the answer',
    1: 'contains the answer text and retrieved in the top BM25 results',
    0: '"hard" negative samples',
    -1: 'negative samples'
}


class DprW100Doc(NamedTuple):
    doc_id: str
    text: str
    title: str
    def default_text(self):
        """
        title + text
        """
        return f'{self.title} {self.text}'


class DprW100Query(NamedTuple):
    query_id: str
    text: str
    answers: Tuple[str, ]
    def default_text(self):
        """
        text
        """
        return self.text


class DprW100Manager:
    def __init__(self, dlc, base_path, passage_id_key='passage_id'):
        self._dlc = dlc
        self._base_path = base_path
        self._base_path.mkdir(parents=True, exist_ok=True)
        self._passage_id_key = passage_id_key

    def build(self):
        ijson = ir_datasets.lazy_libs.ijson()
        if (self._base_path/'queries.tsv').exists():
            return # already built

        with contextlib.ExitStack() as stack:
            f_queries = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'queries.tsv', 'wt'))
            f_qrels = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'qrels', 'wt'))
            stream = stack.enter_context(self._dlc.stream())
            qid_counter = itertools.count()
            for record in _logger.pbar(ijson.items(stream, 'item'), 'building dpr-w100', unit='record'):
                qid = str(next(qid_counter))
                f_queries.write('\t'.join([
                    qid,
                    record['question'].replace('\t', ' ')
                    ] + [
                        a.replace('\t', ' ') for a in record['answers']
                    ]) + '\n')
                seen = set()
                for ctxt in record['positive_ctxs']:
                    if ctxt[self._passage_id_key] not in seen:
                        seen.add(ctxt[self._passage_id_key])
                        rel = 2 if ctxt['score'] == 1000 else 1
                        f_qrels.write(f'{qid} 0 {ctxt[self._passage_id_key]} {rel}\n')
                for ctxt in record['hard_negative_ctxs']:
                    if ctxt[self._passage_id_key] not in seen:
                        seen.add(ctxt[self._passage_id_key])
                        f_qrels.write(f'{qid} 0 {ctxt[self._passage_id_key]} 0\n')
                for ctxt in record['negative_ctxs']:
                    if ctxt[self._passage_id_key] not in seen:
                        seen.add(ctxt[self._passage_id_key])
                        f_qrels.write(f'{qid} 0 {ctxt[self._passage_id_key]} -1\n')

    def file_ref(self, path):
        return _ManagedDlc(self, self._base_path/path)


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


class DprW100Queries(BaseQueries):
    def __init__(self, dlc):
        self._dlc = dlc

    def queries_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                cols = line.decode().strip().split('\t')
                yield DprW100Query(cols[0], cols[1], tuple(cols[2:]))

    def queries_cls(self):
        return DprW100Query

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en' 


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection = TsvDocs(GzipExtract(dlc['docs']), doc_cls=DprW100Doc, namespace=NAME, lang='en', skip_first_line=True, docstore_size_hint=12827215492, count_hint=ir_datasets.util.count_hint(NAME))
    base = Dataset(
        collection,
        documentation('_'))

    subsets = {}

    nq_dev_manager = DprW100Manager(GzipExtract(dlc['nq-dev']), base_path/'nq-dev')
    subsets['natural-questions/dev'] = Dataset(
        collection,
        DprW100Queries(nq_dev_manager.file_ref('queries.tsv')),
        TrecQrels(nq_dev_manager.file_ref('qrels'), QREL_DEFS),
        documentation('natural-questions/dev'))

    nq_train_manager = DprW100Manager(GzipExtract(dlc['nq-train']), base_path/'nq-train')
    subsets['natural-questions/train'] = Dataset(
        collection,
        DprW100Queries(nq_train_manager.file_ref('queries.tsv')),
        TrecQrels(nq_train_manager.file_ref('qrels'), QREL_DEFS),
        documentation('natural-questions/train'))

    tqa_dev_manager = DprW100Manager(GzipExtract(dlc['tqa-dev']), base_path/'tqa-dev', passage_id_key='psg_id')
    subsets['trivia-qa/dev'] = Dataset(
        collection,
        DprW100Queries(tqa_dev_manager.file_ref('queries.tsv')),
        TrecQrels(tqa_dev_manager.file_ref('qrels'), QREL_DEFS),
        documentation('trivia-qa/dev'))

    tqa_train_manager = DprW100Manager(GzipExtract(dlc['tqa-train']), base_path/'tqa-train', passage_id_key='psg_id')
    subsets['trivia-qa/train'] = Dataset(
        collection,
        DprW100Queries(tqa_train_manager.file_ref('queries.tsv')),
        TrecQrels(tqa_train_manager.file_ref('qrels'), QREL_DEFS),
        documentation('trivia-qa/train'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
