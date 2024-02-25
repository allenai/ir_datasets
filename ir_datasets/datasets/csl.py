from typing import List, NamedTuple
from enum import Enum

import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.formats.trec import TrecQrels

from ir_datasets.formats import JsonlDocs, ExctractedCCQueries, ExctractedCCNoReportQuery

from ir_datasets.util.fileio import TarExtract

NAME = 'csl'

class CslDoc(NamedTuple):
    doc_id: str
    title: str
    abstract: str
    keywords: List[str]
    category: str
    category_eng: str
    discipline: str
    discipline_eng: str
    def default_text(self):
        return f'{self.title}\n{self.abstract}'

QREL_DEFS = {
    3: 'Very-valuable. Information in the document would be found in the lead paragraph of a report that is later written on the topic.',
    1: 'Somewhat-valuable. The most valuable information in the document would be found in the remainder of such a report.',
    0: 'Not-valuable. Information in the document might be included in a report footnote, or omitted entirely.',
}


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    docs = JsonlDocs(GzipExtract(dlc['docs']), doc_cls=CslDoc, namespace=NAME, lang='zh', count_hint=395927)
    base = Dataset(
        docs,
        documentation('_')
    )

    subsets["trec-2023"] = Dataset(
        docs,
        ExctractedCCQueries(dlc['trec-2023/queries'], subset_lang='zh', filter_lwq=False, cls=ExctractedCCNoReportQuery, namespace=NAME),
        TrecQrels(TarExtract(dlc['trec-2023/qrels'], 'tech_final_qrels.txt'), QREL_DEFS),
        documentation('trec-2023'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
