import ir_datasets
from ir_datasets.util import DownloadConfig
from ir_datasets.datasets.base import Dataset, YamlDocumentation

from ir_datasets.formats import ExctractedCCDocs

NAME = 'neuclir'

DOC_COUNTS = {
    'zh': 3179209,
    'fa': 2232016,
    'ru': 4627543
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

    base = Dataset() # dummy top level ds
    subsets["22"] = Dataset(documentation('22')) # dummy year level ds

    # For NeuCLIR 2022
    for lang in ['zh', 'fa', 'ru']:
        lang_docs = ExctractedCCDocs(dlc[f'22/{lang}/docs'], subset_lang=lang, namespace=NAME, count=DOC_COUNTS[lang])
        subsets[f"22/{lang}"] = Dataset(
            lang_docs,
            documentation(f"22/{lang}")
        )
    
    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets

base, subsets = _init()
