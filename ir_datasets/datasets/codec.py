import json
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import Lazy
from ir_datasets.formats import BaseQueries, TrecQrels, JsonlDocs
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQrels

_logger = ir_datasets.log.easy()


NAME = 'codec'


QREL_DEFS = {
    3: 'Very Valuable. Includes central topic-specific arguments, evidence, or knowledge. This does not include general definitions or background.',
    2: 'Somewhat Valuable. Includes valuable topic-specific arguments, evidence, or knowledge.',
    1: 'Not Valuable. Consists of definitions or background.',
    0: 'Not Relevant. Not useful or on topic.',
}


DOMAINS = ['economics', 'history', 'politics']


class CodecDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    url: str
    def default_text(self):
        return f'{self.title} {self.text}'

class CodecQuery(NamedTuple):
    query_id: str
    query: str
    domain: str
    guidelines: str
    def default_text(self):
        """
        query
        """
        return self.query


class CodecQueries(BaseQueries):
    def __init__(self, streamer, qid_filter=None):
        super().__init__()
        self._streamer = streamer
        self._qid_filter = qid_filter

    def queries_iter(self):
        with self._streamer.stream() as stream:
            data = json.load(stream)
            for qid, query in data.items():
                if self._qid_filter is None or qid.startswith(self._qid_filter):
                    yield CodecQuery(qid, query['Query'], query['Domain'], query['Guidelines'])

    def queries_cls(self):
        return CodecQuery

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


def filter_qids(domain, queries_handler):
    return Lazy(lambda: {q.query_id for q in queries_handler.queries_iter()})


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    corpus = JsonlDocs(dlc['documents'], doc_cls=CodecDoc, mapping={'doc_id': "id", "title": "title", "text": "contents", "url": "url"}, lang='en', count_hint=729824)

    base = Dataset(
        corpus,
        CodecQueries(dlc['topics']),
        TrecQrels(dlc['qrels'], QREL_DEFS),
        documentation('_'))

    subsets = {}

    for domain in DOMAINS:
        queries_handler = CodecQueries(dlc['topics'], qid_filter=domain)
        subsets[domain] = Dataset(
            corpus,
            queries_handler,
            FilteredQrels(base.qrels_handler(), filter_qids(domain, queries_handler), mode='include'),
            documentation(domain))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
