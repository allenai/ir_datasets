from typing import NamedTuple

import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract, ZipExtract
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats import JsonlDocs, JsonlQueries, TrecQrels, BaseScoredDocs, GenericScoredDoc

NAME = 'sr-qa'

SPLITS = ['train-2025', 'train-2026', 'dev-2026', 'test-2026', 'test-2025']

QREL_DEFS = {
    2: 'Phrase match: the passage contains a fragment of the gold answer as a whole phrase.',
    1: 'Number match: the passage contains every distinctive stand-alone number of a gold fragment.',
}


class SrQaPassage(NamedTuple):
    doc_id: str  # "<PMCID>:<position of the passage in the paper>"
    text: str
    type: str  # sentence, row (table data row), cell (table cell with a number), table_meta (caption or footnote)
    section: str
    def default_text(self):
        return self.text


class SrQaQuery(NamedTuple):
    query_id: str
    text: str
    pmcid: str  # the study the question is about; retrieval is within this paper only
    content: str  # "characteristic" (describes the study) or "result" (what it found)
    def default_text(self):
        return self.text


class SrQaPaperPassages(BaseScoredDocs):
    """The candidate set of within-paper retrieval: every passage of the query's own paper,
    in document order. Scores are all 0: this is a candidate set, not a ranking."""
    def __init__(self, docs, queries):
        self._docs = docs
        self._queries = queries

    def scoreddocs_iter(self):
        by_paper = {}
        for doc in self._docs.docs_iter():
            by_paper.setdefault(doc.doc_id.rsplit(':', 1)[0], []).append(doc.doc_id)
        for query in self._queries.queries_iter():
            for doc_id in by_paper.get(query.pmcid, ()):
                yield GenericScoredDoc(query.query_id, doc_id, 0.)

    def scoreddocs_cls(self):
        return GenericScoredDoc


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(documentation('_'))

    subsets = {}
    for split in SPLITS:
        prefix = f'data/v2/retrieval_labels/{split}/'
        docs = JsonlDocs(
            GzipExtract(ZipExtract(dlc['retrieval'], prefix + 'passages.jsonl.gz')),
            doc_cls=SrQaPassage,
            mapping={'doc_id': 'pid', 'text': 'text', 'type': 'type', 'section': 'section'},
            namespace=f'{NAME}/{split}',
            lang='en',
            count_hint=ir_datasets.util.count_hint(f'{NAME}/{split}'),
            docstore_path=str(base_path/split/'docs.pklz4'),
        )
        queries = JsonlQueries(
            ZipExtract(dlc['retrieval'], prefix + 'queries.jsonl'),
            query_cls=SrQaQuery,
            mapping={'query_id': 'item_id', 'text': 'query', 'pmcid': 'source_pmcid', 'content': 'content'},
            lang='en',
            namespaec=f'{NAME}/{split}',
        )
        qrels = TrecQrels(ZipExtract(dlc['retrieval'], prefix + 'qrels.trec'), QREL_DEFS)
        subsets[split] = Dataset(docs, queries, qrels, SrQaPaperPassages(docs, queries), documentation(split))

    ir_datasets.registry.register(NAME, base)
    for split in SPLITS:
        ir_datasets.registry.register(f'{NAME}/{split}', subsets[split])

    return base, subsets


base, subsets = _init()
