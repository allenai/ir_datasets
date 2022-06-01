import sys
import os
from functools import lru_cache
from collections import defaultdict
import re
import json
import itertools
from typing import NamedTuple, Tuple
import ir_datasets
from ir_datasets.util import DownloadConfig, Lazy
from ir_datasets.formats import TrecQrels, TrecScoredDocs, BaseDocs, BaseQueries, GenericDoc
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs
from ir_datasets.indices import PickleLz4FullStore
import numpy as np


_logger = ir_datasets.log.easy()


NAME = 'trec-cast'

QRELS_DEFS = {
    4: "Fully meets. The passage is a perfect answer for the turn. It includes all of the information needed to fully answer the turn in the conversation context. It focuses only on the subject and contains little extra information.",
    3: "Highly meets. The passage answers the question and is focused on the turn. It would be a satisfactory answer if Google Assistant or Alexa returned this passage in response to the query. It may contain limited extraneous information.",
    2: "Moderately meets. The passage answers the turn, but is focused on other information that is unrelated to the question. The passage may contain the answer, but users will need extra effort to pick the correct portion. The passage may be relevant, but it may only partially answer the turn, missing a small aspect of the context.",
    1: "Slightly meets. The passage includes some information about the turn, but does not directly answer it. Users will find some useful information in the passage that may lead to the correct answer, perhaps after additional rounds of conversation (better than nothing).",
    0: "Fails to meet. The passage is not relevant to the question. The passage is unrelated to the target query.",
}

QRELS_DEFS_TRAIN = {
    2: "very relevant",
    1: "relevant",
    0: "not relevant",
}


class CastPassage(NamedTuple):
    passage_id: str
    text: str
    marked_up_text: str


class CastDoc(NamedTuple):
    doc_id: str
    title: str
    url: str
    passages: Tuple[CastPassage, ...]
    def default_text(self):
        """
        Combines the title and text of constituent passages.
        """
        return "\n".join([self.title] + [p.text for p in self.passages])


class CastPassageDoc(NamedTuple):
    doc_id: str
    title: str
    url: str
    text: str
    def default_text(self):
        """
        Combines the title from the source document with the text of this passage.
        """
        return f"{self.title}\n{self.text}"

class Cast2019Query(NamedTuple):
    query_id: str
    raw_utterance: str
    topic_number: int
    turn_number: int
    topic_title: str
    topic_description: str


class Cast2020Query(NamedTuple):
    query_id: str
    raw_utterance: str
    automatic_rewritten_utterance: str
    manual_rewritten_utterance: str
    manual_canonical_result_id: str
    topic_number: int
    turn_number: int


class CastDocs(BaseDocs):
    def __init__(self, corpus_name, docs_mapping, global_dupes=None, docs_cls=GenericDoc, count_hint=None):
        super().__init__()
        self._corpus_name = corpus_name
        self._docs_mapping = docs_mapping
        self._global_dupes = global_dupes
        self._docs_cls = docs_cls
        self._count_hint = count_hint

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        global_dupes = []
        if self._global_dupes is not None:
            with _logger.duration(f'loading dupes'):
                with self._global_dupes.stream() as fin:
                    global_dupes = {dupe_id.decode().strip() for dupe_id in fin}
        for docs_name, docs_iter_fn, dupef in self._docs_mapping:
            if dupef is not None:
                with _logger.duration(f'loading {docs_name} dupes'):
                    with dupef.stream() as fin:
                        dupes = set()
                        for line in fin:
                            _, line_dupes = line.decode().split(':')
                            if line_dupes:
                                dupes.update(line_dupes.split(','))
            else:
                dupes = []
            with _logger.duration(f'processing {docs_name}'):
                yield from (d for d in docs_iter_fn() if d.doc_id not in dupes and d.doc_id not in global_dupes)

    def docs_cls(self):
        return self._docs_cls

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/{self._corpus_name}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'



class CastPassageIter:
    def __init__(self, docstore, doc_psg_offsets, slice):
        self.next_psg_index = 0
        self.docstore = docstore
        self.doc_iter = iter(docstore)
        self.doc = None
        self.slice = slice
        if self.slice.start != 0:
            start_doc_idx = int(np.searchsorted(doc_psg_offsets(), self.slice.start, side='right')) - 1
            self.doc_iter = self.doc_iter[start_doc_idx:]
            self.next_psg_index = self.slice.start - doc_psg_offsets()[start_doc_idx]
        self.doc_psg_offsets = doc_psg_offsets

    def __next__(self):
        if self.slice.start >= self.slice.stop or self.doc is StopIteration:
            raise StopIteration
        if self.doc is None:
            self.doc = next(self.doc_iter, StopIteration)
        while self.next_psg_index >= len(self.doc.passages):
            self.next_psg_index -= len(self.doc.passages)
            self.doc = next(self.doc_iter, StopIteration)
            if self.doc is StopIteration:
                raise StopIteration
        result = self.doc.passages[self.next_psg_index]
        result = CastPassageDoc(result.passage_id, self.doc.title, self.doc.url, result.text)
        self.next_psg_index += (self.slice.step or 1)
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def __iter__(self):
        return self

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return CastPassageIter(self.docstore, self.doc_psg_offsets, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = CastPassageIter(self.docstore, self.doc_psg_offsets, new_slice)
            try:
                return next(new_it)
            except StopIteration as e:
                raise IndexError(e)
        raise TypeError('key must be int or slice')


class CastPassageDocstore(ir_datasets.indices.Docstore):
    def __init__(self, docs_docstore):
        super().__init__(GenericDoc, 'doc_id')
        self._docs_docstore = docs_docstore

    def get_many_iter(self, doc_ids):
        passage_ids = list(doc_ids)
        did2pids = defaultdict(set)
        for pid in passage_ids:
            if pid.count('-') >= 1:
                did, idx = pid.rsplit('-', 1)
                if idx.isnumeric():
                    did2pids[did].add(int(idx))
        for doc in self._docs_docstore.get_many_iter(did2pids.keys()):
            for idx in did2pids[doc.doc_id]:
                if len(doc.passages) > idx:
                    passage = doc.passages[idx]
                    yield CastPassageDoc(passage.passage_id, doc.title, doc.url, passage.text)


class CastPasageDocs(BaseDocs):
    def __init__(self, docs, count):
        super().__init__()
        self._docs = docs
        self._count = count

    def docs_iter(self):
        docstore = self._docs.docs_store()
        @lru_cache()
        def offsets_fn():
            offsets_path = f'{str(docstore.path)}.psg_offsets.np'
            if not os.path.exists(offsets_path):
                offsets = np.empty(docstore.count()+1, dtype=np.uint32)
                count = 0
                for i, doc in enumerate(_logger.pbar(iter(docstore), total=docstore.count(), desc='building passage offset file')):
                    offsets[i] = count
                    count += len(doc.passages)
                offsets[-1] = count
                with ir_datasets.util.finialized_file(offsets_path, 'wb') as fout:
                    fout.write(offsets.tobytes())
                return offsets
            else:
                return np.memmap(offsets_path, dtype=np.uint32, mode='r')
        return CastPassageIter(docstore, offsets_fn, slice(0, self._count, 1))

    def docs_cls(self):
        return CastPassageDoc

    def docs_store(self, field='doc_id'):
        return CastPassageDocstore(self._docs.docs_store(field))

    def docs_count(self):
        return self._count

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


class CastQueries(BaseQueries):
    def __init__(self, dlc, query_type):
        super().__init__()
        self._dlc = dlc
        self._query_type = query_type

    def queries_iter(self):
        with self._dlc.stream() as stream:
            topics = json.load(stream)
            for topic in topics:
                topic_number = topic['number']
                for turn in topic['turn']:
                    turn_number = turn['number']
                    if self._query_type is Cast2019Query:
                        yield Cast2019Query(f'{topic_number}_{turn_number}', turn['raw_utterance'], topic_number, turn_number, topic['title'], topic.get('description', ''))
                    elif self._query_type is Cast2020Query:
                        yield Cast2020Query(f'{topic_number}_{turn_number}', turn['raw_utterance'], turn['automatic_rewritten_utterance'], turn['manual_rewritten_utterance'], turn['manual_canonical_result_id'], topic_number, turn_number)

    def queries_cls(self):
        return self._query_type

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


def _spacy_make_passages(doc):
    import spacy
    passage_size = 250
    passages = []

    current_passage_word_count = 0
    current_passage = ''

    for sent in doc.sents:
        if current_passage_word_count >= (passage_size * 0.67):
            passages.append(current_passage)
            current_passage = ''
            current_passage_word_count = 0
        current_passage += sent.text + ' '
        current_passage_word_count += len([tok for tok in sent])

    passages.append(current_passage[:-1])
    doc = spacy.tokens.Doc(doc.vocab) # Drop all data from the doc; we only need the passages. This saves a lot of time serialising
    doc._.passages = passages
    return doc


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    def spacy_passager_batch(text_extractor=None, passage_size=250):
        try:
            import spacy
        except ImportError as err:
            raise AssertionError('you must install spacy==3.3.0 to prepare trec-cast', err)
        assert spacy.__version__ == '3.3.0', 'you must install spacy==3.3.0 to prepare trec-cast'
        try:
            nlp = spacy.load("en_core_web_sm", exclude=["parser", "tagger", "ner", "attribute_ruler", "lemmatizer", "tok2vec"])
        except OSError as err:
            raise AssertionError('you must install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.3.0/en_core_web_sm-3.3.0-py3-none-any.whl to prepare trec-cast')
        assert nlp.meta['name'] == 'core_web_sm' and nlp.meta['version'] == '3.3.0', 'you must install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.3.0/en_core_web_sm-3.3.0-py3-none-any.whl to prepare trec-cast'
        nlp.enable_pipe("senter")
        nlp.max_length = 10000000
        spacy.Language.component('make_passages')(_spacy_make_passages)
        spacy.tokens.Doc.set_extension("passages", default=None)
        nlp.add_pipe("make_passages", last=True)
        if text_extractor is None:
            text_extractor = lambda x: x
        sanitized = re.compile('<.*?>')
        def wrapped(docs):
            docs1, docs2 = itertools.tee(docs)
            for nlp_doc, doc in zip(nlp.pipe((re.sub(sanitized, '', d.passages) for d in docs1), n_process=15), docs2):
                yield doc._replace(passages=tuple(CastPassage(f'{doc.doc_id}-{i}', p, text_extractor(p)) for i, p in enumerate(nlp_doc._.passages)))
        return wrapped

    def wapo_converter(dsid):
        def wrapped():
            BeautifulSoup = ir_datasets.lazy_libs.bs4().BeautifulSoup
            # NOTE: These rules are very specific in order to replicate the behaviour present in the official script
            # here: <https://github.com/grill-lab/trec-cast-tools/blob/8fa243a7e058ce4b1b378c99768c53546460c0fe/src/main/python/wapo_trecweb.py>
            # Specifically, things like skipping empty documents, filtering by "paragraph" subtype, and starting the
            # paragraph index at 1 are all needed to perfectly match the above script.
            # Note that the script does NOT strip HTML markup, which is meant to be removed out in a later stage (e.g., indexing).
            # We do that here for user simplicity, as it will allow the text to be consumed directly by various models
            # without the need for further pre-processing. (Though a bit of information is lost.)
            for wapo_doc in ir_datasets.load(dsid).docs_handler().docs_wapo_raw_iter():
                doc_id = wapo_doc['id']
                pid = itertools.count(1) # paragrah index starts at 1
                for paragraph in wapo_doc['contents']:
                    if paragraph is not None and paragraph.get('subtype') == 'paragraph' and paragraph['content'] != '':
                        text = paragraph['content']
                        if paragraph.get('mime') == 'text/html':
                            text = BeautifulSoup(f'<OUTER>{text}</OUTER>', 'lxml-xml').get_text()
                        yield GenericDoc(f'WAPO_{doc_id}-{next(pid)}', text)
        return wrapped

    def wapo_v4_converter(dsid):
        def core_iter():
            dup_dids = set()
            for data in ir_datasets.load(dsid).docs_handler().docs_wapo_raw_iter():
                if data["id"] in dup_dids:
                    continue
                dup_dids.add(data["id"])

                doc_id = 'WAPO_' + str(data['id'])

                title = 'No Title'
                if data['title'] != None:
                    title = data['title'].replace("\n", " ")

                url = '/#'
                if data["article_url"]:
                    if "www.washingtonpost.com" not in data["article_url"]:
                        url = "https://www.washingtonpost.com" + data['article_url']
                    else:
                        url = data['article_url']

                body = ''
                try:
                    for item in data['contents']:
                        if 'subtype' in item and item['subtype'] == 'paragraph':
                            body += ' ' + item['content']
                except:
                    body += 'No body'
                yield CastDoc(doc_id, title, url, body)
        def wrapped():
            return spacy_passager_batch()(core_iter())
        return wrapped

    def kilt_converter(dsid):
        def core_iter():
            for doc in ir_datasets.load(dsid).docs_handler().docs_kilt_raw_iter():
                doc_id = 'KILT_' + doc['wikipedia_id']
                title = doc['wikipedia_title']
                body = ' '.join(doc['text'])
                url = doc['history']['url']
                yield CastDoc(doc_id, title, url, body)
        def wrapped():
            return spacy_passager_batch()(core_iter())
        return wrapped

    def marco_v2_converter(dsid):
        def core_iter():
            for doc in ir_datasets.load(dsid).docs:
                doc_id = 'MARCO_' + doc.doc_id[len('msmarco_doc_'):]
                yield CastDoc(doc_id, doc.title, doc.url, doc.body)
        def wrapped():
            return spacy_passager_batch()(core_iter())
        return wrapped

    def prefixer(dsid, prefix):
        def wrapped():
            for doc in ir_datasets.load(dsid).docs_iter():
                yield GenericDoc(f'{prefix}_{doc.doc_id}', doc.text)
        return wrapped

    WAPO_v2 = wapo_converter('wapo/v2')
    WAPO_v4 = wapo_v4_converter('wapo/v4')
    KILT = kilt_converter('kilt')
    MARCO = prefixer('msmarco-passage', 'MARCO')
    MARCO_V2 = marco_v2_converter('msmarco-document-v2')
    CAR = prefixer('car/v2.0', 'CAR')

    docs_v0 = CastDocs('docs_v0', [
        ('WAPO', WAPO_v2, dlc['wapo_dupes']),
        ('MARCO', MARCO, dlc['marco_dupes']),
        ('CAR', CAR, None),
    ])

    docs_v1 = CastDocs('docs_v1', [
        ('MARCO', MARCO, dlc['marco_dupes']),
        ('CAR', CAR, None),
    ])

    base = Dataset(documentation('_'))

    subsets['v0'] = Dataset(docs_v0)

    subsets['v0/train'] = Dataset(
        docs_v0,
        CastQueries(dlc['2019/train/queries'], Cast2019Query),
        TrecQrels(dlc['2019/train/qrels'], QRELS_DEFS_TRAIN),
        TrecScoredDocs(dlc['2019/train/scoreddocs'])
    )
    qids_train_v0 = Lazy(lambda: {q.query_id for q in subsets['v0/train'].qrels_iter()})
    subsets['v0/train/judged'] = Dataset(
        docs_v0,
        FilteredQueries(subsets['v0/train'].queries_handler(), qids_train_v0),
        subsets['v0/train'].qrels_handler(),
        FilteredScoredDocs(subsets['v0/train'].scoreddocs_handler(), qids_train_v0),
    )

    subsets['v1'] = Dataset(docs_v1)

    subsets['v1/2019'] = Dataset(
        docs_v1,
        CastQueries(dlc['2019/eval/queries'], Cast2019Query),
        TrecQrels(dlc['2019/eval/qrels'], QRELS_DEFS),
        TrecScoredDocs(dlc['2019/eval/scoreddocs'])
    )
    qids_2019 = Lazy(lambda: {q.query_id for q in subsets['v1/2019'].qrels_iter()})
    subsets['v1/2019/judged'] = Dataset(
        docs_v1,
        FilteredQueries(subsets['v1/2019'].queries_handler(), qids_2019),
        subsets['v1/2019'].qrels_handler(),
        FilteredScoredDocs(subsets['v1/2019'].scoreddocs_handler(), qids_2019),
    )

    subsets['v1/2020'] = Dataset(
        docs_v1,
        CastQueries(dlc['2020/queries'], Cast2020Query),
        TrecQrels(dlc['2020/qrels'], QRELS_DEFS),
    )
    qids_2020 = Lazy(lambda: {q.query_id for q in subsets['v1/2020'].qrels_iter()})
    subsets['v1/2020/judged'] = Dataset(
        docs_v1,
        FilteredQueries(subsets['v1/2020'].queries_handler(), qids_2020),
        subsets['v1/2020'].qrels_handler(),
    )

    docs_v3_wapo = CastDocs('deleteme_docs_v3_wapo', [
        ('WAPO', WAPO_v4, None),
    ], global_dupes=dlc['v3/dupes'], docs_cls=CastDoc, count_hint=713638)

    subsets['v3/wapo'] = Dataset(docs_v3_wapo)
    subsets['v3/wapo/psgs'] = Dataset(CastPasageDocs(docs_v3_wapo, 3728553))

    docs_v3_kilt = CastDocs('deleteme_docs_v3_kilt', [
        ('KILT', KILT, None),
    ], global_dupes=dlc['v3/dupes'], docs_cls=CastDoc, count_hint=5903530)

    subsets['v3/kilt'] = Dataset(docs_v3_kilt)

    docs_v3_kilt = CastDocs('deleteme_docs_v3_marco', [
        ('MARCO', MARCO_V2, None),
    ], global_dupes=dlc['v3/dupes'], docs_cls=CastDoc)

    subsets['v3/marco'] = Dataset(docs_v3_kilt)

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return base, subsets


base, subsets = _init()
