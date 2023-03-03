import json
import codecs
from typing import NamedTuple, Dict, List
import ir_datasets
from ir_datasets.util import TarExtract, TarExtractAll, RelativePath, GzipExtract, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredQrels
from ir_datasets.formats import JsonlDocs, JsonlQueries, TrecQrels
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()


class Istella22Doc(NamedTuple):
    doc_id: str
    title: str
    url: str
    text: str
    extra_text: str
    lang: str
    lang_pct: int
    def default_text(self):
        """
        title + text + extra_text
        """
        return f'{self.title} {self.text} {self.extra_text}'


NAME = 'istella22'
QREL_DEFS = {1: 'Least relevant', 2: 'Somewhat relevant', 3: 'Mostly relevant', 4: 'Perfectly relevant'}

DUA = ("To use the Istella22 dataset, you must read and accept the Istella22 Licence Agreement, found here: "
       "<https://istella.ai/data/istella22-dataset/>")

def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path, dua=DUA)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base_dlc = TarExtractAll(dlc['source'], base_path/'istella22_extracted')

    docs = JsonlDocs(GzipExtract(RelativePath(base_dlc, 'istella22/docs.jsonl.gz')), doc_cls=Istella22Doc, lang=None, count_hint=8421456)
    test_queries = JsonlQueries(GzipExtract(RelativePath(base_dlc, 'istella22/queries.test.jsonl.gz')), lang='it')
    test_qrels = TrecQrels(GzipExtract(RelativePath(base_dlc, 'istella22/qrels.test.gz')), QREL_DEFS)

    base = Dataset(
        docs,
        documentation('_'))

    subsets = {}

    subsets['test'] = Dataset(
        docs,
        test_queries,
        test_qrels,
        documentation('test'))

    for fold in ['fold1', 'fold2', 'fold3', 'fold4', 'fold5']:
        fold_qids = Lazy(fold_qids_factory(fold, base_dlc))
        subsets[f'test/{fold}'] = Dataset(
            docs,
            FilteredQueries(test_queries, fold_qids, mode='include'),
            FilteredQrels(test_qrels, fold_qids, mode='include'),
            documentation('test'))

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets

def fold_qids_factory(fold, base_dlc):
    def wrapped():
        with TarExtract(RelativePath(base_dlc, 'istella22/queries.test.folds.tar.gz'), f'./test.queries.{fold}').stream() as f:
            result = [qid.decode().strip().lstrip('0') for qid in f]
        return result
    return wrapped


base, subsets = _init()
