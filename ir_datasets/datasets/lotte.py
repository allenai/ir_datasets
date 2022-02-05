import json
import codecs
from typing import NamedTuple, Dict, List
import ir_datasets
from ir_datasets.util import TarExtractAll, Cache, RelativePath, Lazy, Migrator
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import TsvDocs, TsvQueries, BaseQrels, GenericDoc, GenericQuery, TrecQrel
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()


NAME = 'lotte'
QRELS_DEFS = {1: 'Answer upvoted or accepted on stack exchange'}



class LotteQrels(BaseQrels):
    def __init__(self, qrels_dlc):
        self._qrels_dlc = qrels_dlc

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            for line in f:
                data = json.loads(line)
                for did in data['answer_pids']:
                    yield TrecQrel(str(data['qid']), str(did), 1, "0")

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return QRELS_DEFS


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base_dlc = TarExtractAll(dlc['source'], base_path/'lotte_extracted')

    base = Dataset(documentation('_'))

    subsets = {}

    domains = [
        ('lifestyle',),
        ('recreation',),
        ('science',),
        ('technology',),
        ('writing',),
        ('pooled',),
    ]

    for (domain,) in domains:
        for split in ['dev', 'test']:
            corpus = TsvDocs(RelativePath(base_dlc, f'lotte/{domain}/{split}/collection.tsv'), lang='en')
            subsets[f'{domain}/{split}'] = Dataset(
                corpus,
                documentation(f'{domain}/{split}')
            )
            for qtype in ['search', 'forum']:
                subsets[f'{domain}/{split}/{qtype}'] = Dataset(
                    corpus,
                    TsvQueries(RelativePath(base_dlc, f'lotte/{domain}/{split}/questions.{qtype}.tsv'), lang='en'),
                    LotteQrels(RelativePath(base_dlc, f'lotte/{domain}/{split}/qas.{qtype}.jsonl')),
                    documentation(f'{domain}/{split}/{qtype}')
                )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
