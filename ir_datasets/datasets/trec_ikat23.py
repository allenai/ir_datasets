import json
import itertools
import os
from typing import NamedTuple, Tuple, Dict
import ir_datasets
from ir_datasets.util import DownloadConfig, Bz2Extract
from ir_datasets.formats import TrecQrels, TrecScoredDocs, BaseDocs, BaseQueries, GenericDoc, TsvDocs, TsvQueries
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs
from ir_datasets.indices import PickleLz4FullStore
import bz2

NAME = 'trec-ikat'

QRELS_DEFS = {
    4: "Fully meets. The passage is a perfect answer for the turn. It includes all of the information needed to fully answer the turn in the conversation context. It focuses only on the subject and contains little extra information.",
    3: "Highly meets. The passage answers the question and is focused on the turn. It would be a satisfactory answer if Google Assistant or Alexa returned this passage in response to the query. It may contain limited extraneous information.",
    2: "Moderately meets. The passage answers the turn, but is focused on other information that is unrelated to the question. The passage may contain the answer, but users will need extra effort to pick the correct portion. The passage may be relevant, but it may only partially answer the turn, missing a small aspect of the context.",
    1: "Slightly meets. The passage includes some information about the turn, but does not directly answer it. Users will find some useful information in the passage that may lead to the correct answer, perhaps after additional rounds of conversation (better than nothing).",
    0: "Fails to meet. The passage is not relevant to the question. The passage is unrelated to the target query.",
}

QRELS_PTKB_DEFS = {
    1: "Relevant",
    0: "Irrelevant",
}

class iKATDocs(BaseDocs):
    def __init__(self, dlc):
        super().__init__()
        self._dlc = dlc

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        for chunck_file in os.listdir(self._dlc.path()):
            if ".bz2" not in chunck_file:
                continue
            chunck = os.path.join(self._dlc.path(), chunck_file)

            with bz2.open(chunck, "rt") as bzinput:
                for line in bzinput:
                    data = json.loads(line)
                    yield GenericDoc(data['id'], data['contents'])

    def docs_cls(self):
        return GenericDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/doc.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id']
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()


class iKATQuery(NamedTuple):
    query_id: str
    topic_title: str
    topic_ptkb: Dict[str, str]
    topic_number: int
    turn_id: int
    utterance: str
    resolved_utterance: str
    response: str
    def default_text(self):
        """
        raw_utterance
        """
        return self.utterance


class iKATQueries(BaseQueries):
    def __init__(self, dlc_list):
        super().__init__()
        self._dlc_list = dlc_list

    def queries_iter(self):
        for _dlc in self._dlc_list:
            with _dlc.stream() as stream:
                topics = json.load(stream)
                for topic in topics:
                    topic_number = topic['number']
                    topic_title = topic['title']
                    topic_ptkb = topic['ptkb']
                    for turn in topic['turns']:
                        turn_id = turn['turn_id']
                        yield iKATQuery(f'{topic_number}_{turn_id}', topic_title, topic_ptkb, topic_number, turn_id, turn['utterance'], turn['resolved_utterance'], turn['response'])

    def queries_namespace(self):
        return NAME


# An initialization function is used to keep the namespace clean
def _init():
    base_path = ir_datasets.util.home_path() / NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path)

    base = Dataset(documentation('_'))
    subsets = {}

    docs = iKATDocs(dlc['docs'])
    queries = iKATQueries([dlc['train_queries'], dlc['test_queries']])
    qrels = TrecQrels(dlc['qrels'], QRELS_DEFS)
    subsets['2023'] = Dataset(docs, queries, qrels, documentation('2023'))

    judged_queries = iKATQueries([dlc['test_queries']])
    subsets['2023/judged'] = Dataset(docs, judged_queries, qrels, documentation('2023/judged'))

    ptkb = TrecQrels(dlc['ptkb'], QRELS_PTKB_DEFS)
    subsets['2023/judged/ptkb'] = Dataset(judged_queries, ptkb, documentation('2023/judged/ptkb'))

    for s in subsets:
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])
    
    return base, subsets

base, subsets = _init()