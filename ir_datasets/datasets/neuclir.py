import gzip
import json
from functools import lru_cache

import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats.trec import TrecQrels

from ir_datasets.formats import ExctractedCCDocs, ExctractedCCQueries

from ir_datasets.datasets.hc4 import NAME as HC4_NAME
from ir_datasets.util.fileio import GzipExtract

NAME = 'neuclir'

DOC_COUNTS = {
    'zh': 3179209,
    'fa': 2232016,
    'ru': 4627543
}

@lru_cache(maxsize=3) # three languages
def get_ids(dlcs):
    dlcs = dlcs if isinstance(dlcs, (list, tuple)) else [dlcs]
    ids = []
    for dlc in dlcs:
        with GzipExtract(dlc).stream() as f:
            ids += [ json.loads(line)['id'] for line in f ]
    return set(ids)

class FilteredExctractedCCDocs(ExctractedCCDocs):
    def __init__(self, docs_dlc, subset_lang, include_doc_id_dlc, filter_name=None, namespace=None, count=None):
        super().__init__(docs_dlc, subset_lang, namespace, count)
        self._filter_name = filter_name or "filtered"
        self._include_doc_id_dlc = include_doc_id_dlc
    
    def _doc_store_path(self):
        return self.docs_path(force=False) + f".{self._filter_name}"

    def _internal_docs_iter(self):
        include_doc_id = get_ids(self._include_doc_id_dlc)
        for doc in super()._internal_docs_iter():
            if doc.doc_id in include_doc_id:
                yield doc


class FilteredTrecQrels(TrecQrels):
    def __init__(self, qrels_dlc, qrels_defs, include_doc_id_dlc, format_3col=False):
        super().__init__(qrels_dlc, qrels_defs, format_3col)
        self._include_doc_id_dlc = include_doc_id_dlc
    
    def qrels_iter(self):
        include_doc_id = get_ids(self._include_doc_id_dlc)
        for qrel in super().qrels_iter():
            if qrel.doc_id in include_doc_id:
                yield qrel


QREL_DEFS = {
    3: 'Very-valuable. Information in the document would be found in the lead paragraph of a report that is later written on the topic.',
    1: 'Somewhat-valuable. The most valuable information in the document would be found in the remainder of such a report.',
    0: 'Not-valuable. Information in the document might be included in a report footnote, or omitted entirely.',
}

def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    hc4_dlc = DownloadConfig.context(HC4_NAME, ir_datasets.util.home_path()/HC4_NAME)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(documentation('_')) # dummy top level ds
    subsets["1"] = Dataset(documentation('1')) # dummy year level ds

    # For NeuCLIR Collection 1
    for lang in ['zh', 'fa', 'ru']:
        lang_docs = ExctractedCCDocs(dlc[f'1/{lang}/docs'], subset_lang=lang, namespace=NAME, count=DOC_COUNTS[lang])
        subsets[f"1/{lang}"] = Dataset(
            lang_docs,
            documentation(f"1/{lang}")
        )
        include_doc_id_dlc = hc4_dlc[f'{lang}/docs/ids'] if lang != 'ru' else tuple([ hc4_dlc[f'{lang}/docs/ids/{i}'] for i in range(8) ])
        subsets[f"1/{lang}/hc4-filtered"] = Dataset(
            FilteredExctractedCCDocs(dlc[f'1/{lang}/docs'], subset_lang=lang, namespace=NAME, include_doc_id_dlc=include_doc_id_dlc),
            ExctractedCCQueries([hc4_dlc[f'dev/topics'], hc4_dlc[f'test/topics']], subset_lang=lang, namespace=NAME),
            FilteredTrecQrels([ hc4_dlc[f'{lang}/dev/qrels'], hc4_dlc[f'{lang}/test/qrels'] ], QREL_DEFS, include_doc_id_dlc=include_doc_id_dlc),
            documentation(f"1/{lang}/hc4-filtered")
        )
    
    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets

base, subsets = _init()
