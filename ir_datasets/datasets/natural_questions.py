from typing import NamedTuple, List
import json
import contextlib
import ir_datasets
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import DocstoreBackedDocs, TsvQueries, BaseQrels, BaseScoredDocs, GenericScoredDoc

_logger = ir_datasets.log.easy()


NAME = 'natural-questions'

class NqPassageDoc(NamedTuple):
    doc_id: str # a sequentially-assigned document ID (unique based on URL) + the index of the passage
    text: str # tokenized text of the passage, with all HTML tokens removed
    html: str # raw HTML of the passage
    start_byte: int # the following are from the `long_answer_candidates` objects and may be useful for something
    end_byte: int
    start_token: int
    end_token: int
    document_title: str # from document itself
    document_url: str # from document itself
    parent_doc_id: str # doc_id of the largest passage it's under (e.g., a sentence under a paragraph), or None if it's a top-level passage
    def default_text(self):
        """
        document_title and text
        """
        return f'{self.document_title} {self.text}'



class NqQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int # always 1
    short_answers: List[str] # the **string** representations of the answers (this is similar to how DPH evaluates)
    yes_no_answer: str


class NqManager:
    def __init__(self, dlcs, base_path):
        self._dlcs = dlcs
        self._docs_store = None
        self._base_path = base_path

    def docs_store(self):
        self.build()
        return self._internal_docs_store()

    def _internal_docs_store(self):
        if self._docs_store is None:
            self._docs_store = ir_datasets.indices.PickleLz4FullStore(self._base_path/'docs.pklz4', None, NqPassageDoc, 'doc_id', ['doc_id'], count_hint=ir_datasets.util.count_hint(NAME))
        return self._docs_store

    def build(self):
        docs_store = self._internal_docs_store()
        if docs_store.built():
            return # already built

        pbar_postfix = {'file': None}
        doc_url_to_id = {}
        with contextlib.ExitStack() as stack:
            docs_trans = stack.enter_context(docs_store.lookup.transaction())
            pbar = stack.enter_context(_logger.pbar_raw(desc='processing nq', postfix=pbar_postfix, unit='question'))
            train_queries = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'train.queries.tsv', 'wt'))
            train_qrels = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'train.qrels.jsonl', 'wt'))
            train_scoreddocs = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'train.scoreddocs.tsv', 'wt'))
            dev_queries = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'dev.queries.tsv', 'wt'))
            dev_qrels = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'dev.qrels.jsonl', 'wt'))
            dev_scoreddocs = stack.enter_context(ir_datasets.util.finialized_file(self._base_path/'dev.scoreddocs.tsv', 'wt'))
            for file_name in sorted(self._dlcs.contents().keys()):
                pbar_postfix['file'] = file_name
                pbar.set_postfix(pbar_postfix)
                if 'train' in file_name:
                    f_queries, f_qrels, f_scoreddocs = train_queries, train_qrels, train_scoreddocs
                elif 'dev' in file_name:
                    f_queries, f_qrels, f_scoreddocs = dev_queries, dev_qrels, dev_scoreddocs
                with ir_datasets.util.GzipExtract(self._dlcs[file_name]).stream() as stream:
                    for line in stream:
                        data = json.loads(line)
                        qid = str(data['example_id'])
                        # docs
                        if data['document_url'] not in doc_url_to_id:
                            did = str(len(doc_url_to_id))
                            doc_url_to_id[data['document_url']] = did
                            last_end_idx, last_did = -1, None
                            for idx, cand in enumerate(data['long_answer_candidates']):
                                text = ' '.join(t['token'] for t in data['document_tokens'][cand['start_token']:cand['end_token']] if not t['html_token'])
                                html = ' '.join(t['token'] for t in data['document_tokens'][cand['start_token']:cand['end_token']])
                                parent_doc_id = last_did if cand['start_token'] < last_end_idx else None
                                doc = NqPassageDoc(
                                    f'{did}-{idx}',
                                    text,
                                    html,
                                    cand['start_byte'],
                                    cand['end_byte'],
                                    cand['start_token'],
                                    cand['end_token'],
                                    data['document_title'],
                                    data['document_url'],
                                    parent_doc_id,
                                )
                                docs_trans.add(doc)
                                if parent_doc_id is None:
                                    last_end_idx, last_did = cand['end_token'], doc.doc_id
                        else:
                            did = doc_url_to_id[data['document_url']]
                        # queries
                        f_queries.write('{}\t{}\n'.format(qid, data['question_text'].replace('\t', ' ')))
                        # qrels
                        qrels = {}
                        for ann in data['annotations']:
                            if ann['long_answer']['candidate_index'] == -1:
                                continue
                            passage_id = '{}-{}'.format(did, ann['long_answer']['candidate_index'])
                            short_answers = [' '.join(t['token'] for t in data['document_tokens'][s['start_token']:s['end_token']] if not t['html_token']) for s in ann['short_answers']]
                            if passage_id in qrels:
                                qrel = qrels[passage_id]
                                short_answers = [s for s in short_answers if s not in short_answers]
                                qrel.short_answers.extend(short_answers)
                            else:
                                qrel = NqQrel(
                                    qid,
                                    passage_id,
                                    1,
                                    short_answers,
                                    ann['yes_no_answer'],
                                )
                                qrels[passage_id] = qrel
                        for qrel in qrels.values():
                            json.dump(qrel._asdict(), f_qrels)
                            f_qrels.write('\n')
                        # scoreddocs
                        count = len(data['long_answer_candidates'])
                        f_scoreddocs.write(f'{qid}\t{did}\t{count}\n')
                        pbar.update(1)

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


class NqQrels(BaseQrels):
    def __init__(self, dlc):
        super().__init__()
        self.dlc = dlc

    def qrels_iter(self):
        with self.dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield NqQrel(**data)

    def qrels_cls(self):
        return NqQrel

    def qrels_defs(self):
        return {1: 'passage marked by annotator as a "long" answer to the question'}


class NqScoredDocs(BaseScoredDocs):
    def __init__(self, dlc):
        super().__init__()
        self.dlc = dlc

    def scoreddocs_iter(self):
        with self.dlc.stream() as stream:
            for line in stream:
                qid, did, count = line.decode().strip().split('\t')
                for i in range(int(count)):
                    yield GenericScoredDoc(qid, f'{did}-{i}', 0.)

    def scoreddocs_cls(self):
        return GenericScoredDoc



def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    manager = NqManager(dlc, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    collection = DocstoreBackedDocs(manager.docs_store, docs_cls=NqPassageDoc, namespace=NAME, lang='en')
    base = Dataset(
        collection,
        documentation('_'))

    subsets = {}
    subsets['train'] = Dataset(
        collection,
        TsvQueries(manager.file_ref('train.queries.tsv'), namespace=NAME, lang='en'),
        NqQrels(manager.file_ref('train.qrels.jsonl')),
        NqScoredDocs(manager.file_ref('train.scoreddocs.tsv')),
        documentation('train'),
        )
    subsets['dev'] = Dataset(
        collection,
        TsvQueries(manager.file_ref('dev.queries.tsv'), namespace=NAME, lang='en'),
        NqQrels(manager.file_ref('dev.qrels.jsonl')),
        NqScoredDocs(manager.file_ref('dev.scoreddocs.tsv')),
        documentation('dev'),
        )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
