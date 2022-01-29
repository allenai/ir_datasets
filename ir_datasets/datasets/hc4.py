from typing import Dict, NamedTuple
import json
import ir_datasets
from ir_datasets.formats.base import BaseDocs, BaseQueries
from ir_datasets.indices.lz4_pickle import PickleLz4FullStore
from ir_datasets.util import DownloadConfig
from ir_datasets.formats import TrecQrels
from ir_datasets.datasets.base import Dataset, YamlDocumentation

NAME = 'hc4'

LANG_CODE_CONVERT = {
    'zh': 'zho',
    'fa': 'fas',
    'ru': 'rus'
}

DOC_COUNTS = {
    'zh': 646305,
    'fa': 486684,
    'ru': 4721064
}

class HC4Doc(NamedTuple):
    doc_id: str
    title: str
    text: str
    url: str
    time: str
    cc_file: str


class HC4Docs(BaseDocs):
    
    def __init__(self, docs_dlc, subset_lang):
        self._docs_dlc = docs_dlc
        self._subset_lang = subset_lang
        self._count = DOC_COUNTS[subset_lang]

    def docs_path(self, force=True):
        return self._docs_dlc.path(force)

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._docs_dlc.stream() as f:
            for line in f:
                line = json.loads(line)
                line['doc_id'] = line['id']
                del line['id']
                yield HC4Doc(**line)

    def docs_store(self):
        return PickleLz4FullStore(
            path=f'{self.docs_path(force=False)}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field='doc_id',
            index_fields=['doc_id'],
            count_hint=self._count,
        )

    def docs_cls(self):
        return HC4Doc

    def docs_namespace(self):
        return NAME

    def docs_count(self):
        return self._count

    def docs_lang(self):
        return self._subset_lang 


class HC4Query(NamedTuple):
    query_id: str
    title: str
    description: str
    ht_title: str
    ht_description: str
    mt_title: str
    mt_description: str
    narrative: Dict[str, str]
    report: str
    report_url: str
    report_date: str
    translation_lang: str

class HC4Queries(BaseQueries):
    def __init__(self, queries_dlc, subset_lang):
        self._queries_dlc = queries_dlc
        self._subset_lang = subset_lang

        self._subset_lang_hc4 = LANG_CODE_CONVERT[self._subset_lang]
    
    def queries_path(self):
        return self._queries_dlc.path()
    
    def queries_cls(self):
        return HC4Query

    def queries_namespace(self):
        return NAME
    
    def queries_iter(self):
        with self._queries_dlc.stream() as f:
            for line in f:
                line = json.loads(line)
                if self._subset_lang_hc4 in line['languages_with_qrels']:
                    yield self._produce_query(line)
    
    def _produce_query(self, line):
        resources = {}
        for tp in line['topics']:
            if tp['lang'] == 'eng':
                resources['org'] = tp
            elif tp['lang'] == self._subset_lang_hc4:
                if tp['source'] == 'human translation':
                    resources['ht'] = tp
                else: # machine translation
                    resources['mt'] = tp
        return HC4Query(
            query_id=line['topic_id'],
            title=resources['org']['topic_title'],
            description=resources['org']['topic_description'],
            ht_title=resources['ht']['topic_title'],
            ht_description=resources['ht']['topic_description'],
            mt_title=resources['mt']['topic_title'],
            mt_description=resources['mt']['topic_description'],
            narrative=line['narratives'][self._subset_lang_hc4],
            report=line['report']['text'],
            report_url=line['report']['url'],
            report_date=line['report']['date'],
            translation_lang=self._subset_lang
        )


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
        subsets[lang] = Dataset(
            HC4Docs(dlc[f'{lang}/docs'], subset_lang=lang)
        )
        for sep in ['train', 'dev', 'test']:
            subsets[f'{lang}/{sep}'] = Dataset(
                HC4Queries(dlc[f'{sep}/topics'], subset_lang=lang),
                TrecQrels(dlc[f'{lang}/{sep}/qrels'], QREL_DEFS)
            )
    
    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets

base, subsets = _init()