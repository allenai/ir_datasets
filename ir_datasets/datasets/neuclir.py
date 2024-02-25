import gzip
import json
from functools import lru_cache

import ir_datasets
from ir_datasets.util import DownloadConfig, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats.trec import TrecQrels

from ir_datasets.formats import ExctractedCCDocs, ExctractedCCQueries, ExctractedCCNoReportQuery, ExctractedCCNoReportNoHtNarQuery, ExctractedCCMultiMtQuery

from ir_datasets.datasets.hc4 import NAME as HC4_NAME
from ir_datasets.util.fileio import GzipExtract, TarExtract

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


class LangFilteredTrecQrels(TrecQrels):
    def __init__(self, qrels_dlc, qrels_defs, lang, format_3col=False):
        super().__init__(qrels_dlc, qrels_defs, format_3col)
        self._lang = lang
    
    def qrels_iter(self):
        for qrel in super().qrels_iter():
            if qrel.iteration == self._lang:
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

    qrels2022 = dlc['trec-2022/qrels']
    qrels2023 = TarExtract(dlc['trec-2023/qrels'], 'qrels.final')

    # For NeuCLIR Collection 1
    for lang in ['zh', 'fa', 'ru']:
        lang3 = {'fa': 'fas', 'zh': 'zho', 'ru': 'rus'}[lang]
        lang_docs = ExctractedCCDocs(GzipExtract(dlc[f'1/{lang}/docs']), subset_lang=lang, namespace=NAME, count=DOC_COUNTS[lang])
        subsets[f"1/{lang}"] = Dataset(
            lang_docs,
            documentation(f"1/{lang}")
        )
        qrels = LangFilteredTrecQrels(qrels2022, QREL_DEFS, lang3)
        subsets[f"1/{lang}/trec-2022"] = Dataset(
            lang_docs,
            FilteredQueries(ExctractedCCQueries(dlc['trec-2022/queries'], subset_lang=lang, filter_lwq=False, cls=ExctractedCCNoReportQuery, namespace=NAME), _lazy_qids_set(qrels), mode='include'),
            qrels,
            documentation(f"1/{lang}/trec-2022"),
        )
        qrels = LangFilteredTrecQrels(qrels2023, QREL_DEFS, lang3)
        subsets[f"1/{lang}/trec-2023"] = Dataset(
            lang_docs,
            FilteredQueries(ExctractedCCQueries(dlc['trec-2023/queries'], subset_lang=lang, filter_lwq=False, cls=ExctractedCCNoReportNoHtNarQuery, namespace=NAME), _lazy_qids_set(qrels), mode='include'),
            qrels,
            documentation(f"1/{lang}/trec-2023"),
        )
        include_doc_id_dlc = hc4_dlc[f'{lang}/docs/ids'] if lang != 'ru' else tuple([ hc4_dlc[f'{lang}/docs/ids/{i}'] for i in range(8) ])
        subsets[f"1/{lang}/hc4-filtered"] = Dataset(
            FilteredExctractedCCDocs(GzipExtract(dlc[f'1/{lang}/docs']), subset_lang=lang, namespace=NAME, include_doc_id_dlc=include_doc_id_dlc),
            ExctractedCCQueries([hc4_dlc['dev/topics'], hc4_dlc['test/topics']], subset_lang=lang, namespace=NAME),
            FilteredTrecQrels([ hc4_dlc[f'{lang}/dev/qrels'], hc4_dlc[f'{lang}/test/qrels'] ], QREL_DEFS, include_doc_id_dlc=include_doc_id_dlc),
            documentation(f"1/{lang}/hc4-filtered")
        )

    multi_docs = ExctractedCCDocs([GzipExtract(dlc[f'1/{lang}/docs']) for lang in ['zh', 'fa', 'ru']], namespace=NAME, count=sum(DOC_COUNTS.values()), docstore_path=base_path/'1'/'multi')
    subsets['1/multi'] = Dataset(
        multi_docs,
        documentation("1/multi")
    )

    subsets['1/multi/trec-2023'] = Dataset(
        multi_docs,
        ExctractedCCQueries(dlc['trec-2023/queries'], filter_lwq=False, cls=ExctractedCCMultiMtQuery, namespace=NAME),
        TrecQrels(qrels2023, QREL_DEFS),
        documentation("1/multi/trec-2023")
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


def _lazy_qids_set(qrels):
    return Lazy(lambda: {qrel.query_id for qrel in qrels.qrels_iter()})


base, subsets = _init()
