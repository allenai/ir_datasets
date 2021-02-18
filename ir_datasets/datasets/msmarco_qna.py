import hashlib
import re
import itertools
import contextlib
import io
import codecs
from typing import NamedTuple, Tuple
import re
import ijson
import ir_datasets
from ir_datasets.util import Cache, TarExtract, IterStream, GzipExtract, Lazy, DownloadConfig
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredScoredDocs, FilteredQrels, FilteredDocPairs, YamlDocumentation
from ir_datasets.formats import TsvQueries, TsvDocs, TrecQrels, TrecScoredDocs, TsvDocPairs, DocstoreBackedDocs

_logger = ir_datasets.log.easy()

NAME = 'msmarco-qna'

DUA = ("Please confirm you agree to the MSMARCO data usage agreement found at "
       "<http://www.msmarco.org/dataset.aspx>")

QRELS_DEFS = {
    1: 'Marked by annotator as a contribution to their answer',
    0: 'Not marked by annotator as a contribution to their answer',
}

NO_ANSWER_PLACEHOLDER = 'No Answer Present.'

class MsMarcoQnAQuery(NamedTuple):
    query_id: str
    text: str
    type: str
    answers: Tuple[str, ...]


class MsMarcoQnAEvalQuery(NamedTuple):
    query_id: str
    text: str
    type: str


class MsMarcoQnADoc(NamedTuple):
    doc_id: str
    text: str
    url: str


# The MS MARCO QnA data files are in a super inconvenient format. They have a script to convert it
# to JSONL format, but it involves loading the entire collection into memory and doing merging via
# pandas, which is a non-starter. So we'll incrementally process the dataset using ijson.
# Format:
#     {
#         "answers": {
#             "XXX": ["", ""],
#             ...
#         },
#         "passages": {
#             "XXX": {
#                 "is_selected": 0,
#                 "passage_text": "",
#                 "url": ""
#             },
#             ...
#         },
#         "query": {"XXX": "", ...},
#         "query_type": {"XXX": "", ...},
#         "query_id": {"XXX": 0, ...}
#     }
# Where XXX is an ID used only for linking the records here in this file. Luckly, they are sorted
# so we don't actually need to deal with them.
# What's worse is that "passages" can be repeated and they don't have an ID. So we'll assign one
# in the order that they appear in the file, skipping duplicates.
# To find duplicates, we'll hash the text and url and keep that in a lookup. It's not ideal, but
# better than keeping a copy of all the passage texts in memory. I found that I can use a shorter
# version of the hashes that do not end up colliding. This reduces the memory overhead.
# The process ends up building out a collection-wide docstore and id/query/type/answers/qrels files
# for each split, that then get merged into query and qrel TSV files.

class MsMarcoQnAManager:
    def __init__(self, train_dlc, dev_dlc, eval_dlc, base_path):
        self._train_dlc = train_dlc
        self._dev_dlc = dev_dlc
        self._eval_dlc = eval_dlc
        self._docs_store = None
        self._base_path = base_path

    def docs_store(self):
        self.build()
        return self._internal_docs_store()

    def _internal_docs_store(self):
        if self._docs_store is None:
            self._docs_store = ir_datasets.indices.PickleLz4FullStore(self._base_path/'docs.pklz4', None, MsMarcoQnADoc, 'doc_id', ['doc_id'])
        return self._docs_store

    def build(self):
        docs_store = self._internal_docs_store()
        if docs_store.built():
            return # already built
        doc_counter = itertools.count(0)
        did_lookup = {}
        nil_doc = MsMarcoQnADoc(None, None, None)
        current_doc = nil_doc

        prefix_passages = re.compile(r'^passages\.\d+\.item$')
        prefix_answers = re.compile(r'^answers\.\d+\.item$')
        prefix_type = re.compile(r'^query_type\.\d+$')
        prefix_text = re.compile(r'^query\.\d+$')
        prefix_id = re.compile(r'^query_id\.\d+$')

        pbar_postfix = {'file': None, 'key': None}
        with contextlib.ExitStack() as outer_stack:
            docs_trans = outer_stack.enter_context(docs_store.lookup.transaction())
            pbar = outer_stack.enter_context(_logger.pbar_raw(desc='processing qna', postfix=pbar_postfix))
            for dlc, file_str in [(self._train_dlc, 'train'), (self._dev_dlc, 'dev'), (self._eval_dlc, 'eval')]:
                pbar_postfix['file'] = file_str
                last_ans_prefix = None
                last_psg_prefix = None
                is_selected = None
                with contextlib.ExitStack() as inner_stack:
                    stream = inner_stack.enter_context(dlc.stream())
                    parser = ijson.parse(stream)
                    out_text = inner_stack.enter_context(open(self._base_path/f'{file_str}.query_text', 'wt'))
                    out_type = inner_stack.enter_context(open(self._base_path/f'{file_str}.query_type', 'wt'))
                    out_id = inner_stack.enter_context(open(self._base_path/f'{file_str}.query_id', 'wt'))
                    if file_str != 'eval':
                        out_qrels = inner_stack.enter_context(open(self._base_path/f'{file_str}.selections', 'wt'))
                        out_answer = inner_stack.enter_context(open(self._base_path/f'{file_str}.query_answer', 'wt+'))
                        out_seq = None
                    else:
                        out_qrels, out_answer = None, None
                        out_seq = inner_stack.enter_context(open(self._base_path/f'{file_str}.seq', 'wt'))
                    for prefix, event, data in parser:
                        pbar_postfix['key'] = prefix
                        pbar.set_postfix(pbar_postfix, refresh=False)
                        pbar.update()

                        if prefix_passages.match(prefix):
                            if event == 'end_map':
                                assert current_doc.text is not None and current_doc.url is not None
                                doc_hash = bytes(hashlib.md5(repr(current_doc).encode()).digest()[:48])
                                if doc_hash in did_lookup:
                                    did = did_lookup[doc_hash]
                                    current_doc = current_doc._replace(doc_id=did)
                                else:
                                    did = str(next(doc_counter))
                                    did_lookup[doc_hash] = did
                                    current_doc = current_doc._replace(doc_id=did)
                                    docs_trans.add(current_doc)
                                if out_qrels is not None:
                                    if last_psg_prefix == prefix:
                                        out_qrels.write(f'\t{did} {is_selected}')
                                    elif last_psg_prefix is None:
                                        out_qrels.write(f'{did} {is_selected}')
                                    else:
                                        out_qrels.write(f'\n{did} {is_selected}')
                                    last_psg_prefix = prefix
                                if out_seq is not None:
                                    if last_psg_prefix == prefix:
                                        out_seq.write(f'\t{did}')
                                    elif last_psg_prefix is None:
                                        out_seq.write(f'{did}')
                                    else:
                                        out_seq.write(f'\n{did}')
                                    last_psg_prefix = prefix
                                is_selected = None
                                current_doc = nil_doc
                            elif event == 'map_key':
                                key = data
                                value = next(parser)[2]
                                if key == 'is_selected':
                                    is_selected = str(value)
                                elif key == 'passage_text':
                                    current_doc = current_doc._replace(text=value)
                                elif key == 'url':
                                    current_doc = current_doc._replace(url=value)
                        elif prefix_answers.match(prefix):
                            # a little more annoying because there can be multiple answers (but there's always at least 1)
                            text = str(data).replace("\n", " ").replace("\t", " ")
                            if last_ans_prefix == prefix:
                                out_answer.write(f'\t{text}')
                            elif last_ans_prefix is None:
                                out_answer.write(text)
                            else:
                                out_answer.write(f'\n{text}')
                            last_ans_prefix = prefix
                        elif prefix_text.match(prefix):
                            text = str(data).replace("\n", " ")
                            out_text.write(f'{text}\n')
                        elif prefix_id.match(prefix):
                            text = str(data).replace("\n", " ")
                            out_id.write(f'{text}\n')
                        elif prefix_type.match(prefix):
                            text = str(data).replace("\n", " ")
                            out_type.write(f'{text}\n')
                    if file_str != 'eval':
                        out_answer.write('\n')
                        out_qrels.write('\n')
                    else:
                        out_seq.write('\n')

            # Merge files
            for file_str in ['train', 'dev', 'eval']:
                with contextlib.ExitStack() as stack:
                    f_qid = stack.enter_context(open(self._base_path/f'{file_str}.query_id', 'rt'))
                    f_type = stack.enter_context(open(self._base_path/f'{file_str}.query_type', 'rt'))
                    f_text = stack.enter_context(open(self._base_path/f'{file_str}.query_text', 'rt'))
                    f_queries = stack.enter_context(open(self._base_path/f'{file_str}.queries.tsv', 'wt'))
                    f_run = stack.enter_context(open(self._base_path/f'{file_str}.run', 'wt'))
                    in_files = [f_qid, f_type, f_text]
                    if file_str != 'eval':
                        f_selections = stack.enter_context(open(self._base_path/f'{file_str}.selections', 'rt'))
                        f_answers = stack.enter_context(open(self._base_path/f'{file_str}.query_answer', 'rt'))
                        f_qrels = stack.enter_context(open(self._base_path/f'{file_str}.qrels', 'wt'))
                        in_files += [f_selections, f_answers]
                    else:
                        f_seq = stack.enter_context(open(self._base_path/f'{file_str}.seq', 'rt'))
                        in_files += [f_seq]
                    for columns in _logger.pbar(zip(*in_files), desc=f'merging {file_str} files'):
                        columns = [x.strip() for x in columns]
                        qid, typ, text = columns[:3]
                        if file_str != 'eval':
                            selections, answers = columns[3:]
                            # Remove the "no answer" placeholder
                            answers = answers.replace(NO_ANSWER_PLACEHOLDER, '')
                            if answers:
                                answers = f'\t{answers}'
                            f_queries.write(f'{qid}\t{text}\t{typ}{answers}\n')
                            for i, qrel in enumerate(selections.split('\t')):
                                did, label = qrel.split()
                                f_qrels.write(f'{qid} 0 {did} {label}\n')
                                f_run.write(f'{qid} Q0 {did} {i} {-i} qna\n')
                        else:
                            seq, = columns[3:]
                            f_queries.write(f'{qid}\t{text}\t{typ}\n')
                            for i, did in enumerate(seq.split('\t')):
                                f_run.write(f'{qid} Q0 {did} {i} {-i} qna\n')
                # clean up temp files
                (self._base_path/f'{file_str}.query_id').unlink()
                (self._base_path/f'{file_str}.query_type').unlink()
                (self._base_path/f'{file_str}.query_text').unlink()
                if file_str != 'eval':
                    (self._base_path/f'{file_str}.selections').unlink()
                    (self._base_path/f'{file_str}.query_answer').unlink()

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

    def path(self):
        self._manager.build()
        return self._path


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    manager = MsMarcoQnAManager(GzipExtract(dlc['train']), GzipExtract(dlc['dev']), GzipExtract(dlc['eval']), base_path)

    collection = DocstoreBackedDocs(manager.docs_store, docs_cls=MsMarcoQnADoc, namespace=NAME, lang='en')

    subsets = {}

    subsets['train'] = Dataset(
        collection,
        TsvQueries(manager.file_ref('train.queries.tsv'), query_cls=MsMarcoQnAQuery, namespace='msmarco', lang='en'),
        TrecQrels(manager.file_ref('train.qrels'), QRELS_DEFS),
        TrecScoredDocs(manager.file_ref('train.run')),
    )

    subsets['dev'] = Dataset(
        collection,
        TsvQueries(manager.file_ref('dev.queries.tsv'), query_cls=MsMarcoQnAQuery, namespace='msmarco', lang='en'),
        TrecQrels(manager.file_ref('dev.qrels'), QRELS_DEFS),
        TrecScoredDocs(manager.file_ref('dev.run')),
    )

    subsets['eval'] = Dataset(
        collection,
        TsvQueries(manager.file_ref('eval.queries.tsv'), query_cls=MsMarcoQnAEvalQuery, namespace='msmarco', lang='en'),
        TrecScoredDocs(manager.file_ref('eval.run')),
    )

    ir_datasets.registry.register(NAME, Dataset(collection, documentation('_')))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return collection, subsets


collection, subsets = _init()
