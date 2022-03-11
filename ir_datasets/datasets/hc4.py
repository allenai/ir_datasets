import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.formats import TrecQrels
from ir_datasets.datasets.base import Dataset, YamlDocumentation

from ir_datasets.formats import ExctractedCCDocs, ExctractedCCQueries

NAME = 'hc4'

DOC_COUNTS = {
    'zh': 646305,
    'fa': 486486,
    'ru': 4721064
}

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

    base = Dataset(documentation('_')) # dummy top level ds

    for lang in ['zh', 'fa', 'ru']:
        lang_docs = ExctractedCCDocs(dlc[f'{lang}/docs'], subset_lang=lang, namespace=NAME, count=DOC_COUNTS[lang])
        subsets[lang] = Dataset(
            lang_docs,
            documentation(lang)
        )
        for sep in ['train', 'dev', 'test']:
            subsets[f'{lang}/{sep}'] = Dataset(
                lang_docs,
                ExctractedCCQueries(dlc[f'{sep}/topics'], subset_lang=lang, namespace=NAME),
                TrecQrels(dlc[f'{lang}/{sep}/qrels'], QREL_DEFS),
                documentation(f'{lang}/{sep}'),
            )
    
    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets

base, subsets = _init()
