import ir_datasets
from ir_datasets.util import ZipExtract, Cache, Lazy, DownloadConfig
from ir_datasets.formats import TrecQrels, JsonlQueries, JsonlDocs, TrecQrels
from ir_datasets.datasets.base import Dataset, FilteredQueries, FilteredQrels, YamlDocumentation, Deprecated
from typing import NamedTuple, List

NAME = 'trec-tip-of-the-tongue'


class TipOfTheTongueDocument(NamedTuple):
    doc_id: str
    page_title: str
    wikidata_id: str
    wikidata_classes: List[str]
    text: str
    sections: dict
    infoboxes: List[dict]

    def default_text(self):
        """
        We use the title and text of the TipOfTheTongueQuery as default_text because that is everything available for users who want to respond to such an information need.
        """
        return self.page_title + ' ' + self.text


class TipOfTheTongueQuery(NamedTuple):
    id: str
    url: str
    domain: str
    title: list
    text: str
    sentence_annotations: List[dict]

    def default_text(self):
        return self.title + ' ' + self.text
    
    def __getattr__(self, attr):
        if attr == 'query_id':
            return self.id

        return self.__getattribute__(attr)


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {'train': None, 'dev': None}

    main_dlc = dlc['main']
    base = Dataset(
        JsonlDocs(Cache(ZipExtract(main_dlc, 'TREC-TOT/corpus.jsonl'), base_path/'corpus.jsonl'), doc_cls=TipOfTheTongueDocument),
        documentation('_'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        subsets[s] = Dataset(
            JsonlDocs(Cache(ZipExtract(main_dlc, 'TREC-TOT/corpus.jsonl'), base_path/'corpus.jsonl'), doc_cls=TipOfTheTongueDocument),
            JsonlQueries(Cache(ZipExtract(main_dlc, f'TREC-TOT/{s}/queries.jsonl'), base_path/f'{s}/queries.jsonl'), query_cls=TipOfTheTongueQuery),
            TrecQrels(Cache(ZipExtract(main_dlc, f'TREC-TOT/{s}/qrel.txt'), base_path/f'{s}/qrel.txt'), {0: 'Not Relevant', 1: 'Relevant'}),
            documentation('_'),
        )
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()

