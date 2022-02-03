import json
import itertools
from typing import NamedTuple, Tuple
import ir_datasets
from ir_datasets.util import DownloadConfig, Lazy
from ir_datasets.formats import TrecQrels, TrecScoredDocs, BaseDocs, BaseQueries, GenericDoc
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs
from ir_datasets.indices import PickleLz4FullStore


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
    def __init__(self, corpus_name, docs_mapping, count_hint=None):
        super().__init__()
        self._corpus_name = corpus_name
        self._docs_mapping = docs_mapping
        self._count_hint = count_hint

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
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
                yield from (d for d in docs_iter_fn() if d.doc_id not in dupes)

    def docs_cls(self):
        return GenericDoc

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



def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

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

    def prefixer(dsid, prefix):
        def wrapped():
            for doc in ir_datasets.load(dsid).docs_iter():
                yield GenericDoc(f'{prefix}_{doc.doc_id}', doc.text)
        return wrapped

    WAPO_v2 = wapo_converter('wapo/v2')
    MARCO = prefixer('msmarco-passage', 'MARCO')
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

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return base, subsets


base, subsets = _init()
