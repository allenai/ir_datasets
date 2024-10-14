from typing import Dict, NamedTuple
import json

import ir_datasets
from ir_datasets.formats.base import BaseDocs, BaseQueries
from ir_datasets.indices.lz4_pickle import PickleLz4FullStore

LANG_CODE_CONVERT = {
    'zh': 'zho',
    'fa': 'fas',
    'ru': 'rus'
}

class ExctractedCCDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    url: str
    time: str
    cc_file: str
    def default_text(self):
        """
        title and text
        """
        return f'{self.title} {self.text}'


class ExctractedCCDocs(BaseDocs):
    
    def __init__(self, docs_dlc, subset_lang=None, namespace=None, count=None, docstore_path=None):
        self._docs_dlc = docs_dlc
        self._subset_lang = subset_lang
        self._namespace = namespace
        self._count = count
        self._docstore_path = docstore_path

    def docs_path(self, force=True):
        if self._docstore_path:
            return self._docstore_path
        return self._docs_dlc.path(force)

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        yield from self._internal_docs_iter()

    def _doc_store_path(self):
        return self.docs_path(force=False)

    def docs_store(self):
        return PickleLz4FullStore(
            path=f'{self._doc_store_path()}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field='doc_id',
            index_fields=['doc_id'],
            count_hint=self._count,
        )
    
    def _internal_docs_iter(self):
        if not isinstance(self._docs_dlc, list):
            docs_dlc = [self._docs_dlc]
        else:
            docs_dlc = self._docs_dlc
        for dlc in docs_dlc:
            with dlc.stream() as f:
                for line in f:
                    line = json.loads(line)
                    line['doc_id'] = line['id']
                    del line['id']
                    yield ExctractedCCDoc(**line)

    def docs_cls(self):
        return ExctractedCCDoc

    def docs_namespace(self):
        return self._namespace

    def docs_count(self):
        return self._count

    def docs_lang(self):
        return self._subset_lang 


class ExctractedCCQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    ht_title: str
    ht_description: str
    mt_title: str
    mt_description: str
    narrative_by_relevance: Dict[str, str]
    report: str
    report_url: str
    report_date: str
    translation_lang: str
    def default_text(self):
        """
        title
        """
        return self.title

class ExctractedCCNoReportQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    narrative: str
    ht_title: str
    ht_description: str
    ht_narrative: str
    mt_title: str
    mt_description: str
    mt_narrative: str
    translation_lang: str
    def default_text(self):
        """
        title
        """
        return self.title

class ExctractedCCNoReportNoHtNarQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    narrative: str
    ht_title: str
    ht_description: str
    mt_title: str
    mt_description: str
    mt_narrative: str
    translation_lang: str
    def default_text(self):
        """
        title
        """
        return self.title


class ExctractedCCMultiMtQuery(NamedTuple):
    query_id: str
    title: str
    description: str
    narrative: str
    fa_mt_title: str
    fa_mt_description: str
    fa_mt_narrative: str
    ru_mt_title: str
    ru_mt_description: str
    ru_mt_narrative: str
    zh_mt_title: str
    zh_mt_description: str
    zh_mt_narrative: str
    def default_text(self):
        """
        title
        """
        return self.title


class ExctractedCCQueries(BaseQueries):
    def __init__(self, queries_dlc, subset_lang=None, filter_lwq=True, cls=ExctractedCCQuery, namespace=None):
        self._queries_dlc = queries_dlc if isinstance(queries_dlc, list) else [queries_dlc]
        self._subset_lang = subset_lang
        self._filter_lwq = filter_lwq
        self._namespace = namespace
        self._cls = cls

        self._subset_lang_three = LANG_CODE_CONVERT.get(self._subset_lang)
    
    def queries_path(self):
        return [ dlc.path() for dlc in self._queries_dlc ]
    
    def queries_cls(self):
        return self._cls

    def queries_namespace(self):
        return self._namespace
    
    def queries_iter(self):
        for dlc in self._queries_dlc:
            yield from self._internal_queries_iter(dlc)
    
    def _internal_queries_iter(self, dlc):
        with dlc.stream() as f:
            for line in f:
                line = json.loads(line)
                if not self._filter_lwq or self._subset_lang_three in line['languages_with_qrels']:
                    yield self._produce_query(line)
    
    def _produce_query(self, line):
        resources = {}
        for tp in line['topics']:
            if tp['lang'] == 'eng':
                resources['org'] = tp
            else:
                if tp['source'] == 'human translation':
                    resources['ht_{lang}'.format(**tp)] = tp
                else: # machine translation
                    resources['mt_{lang}'.format(**tp)] = tp
                if tp['lang'] == self._subset_lang_three:
                    if tp['source'] == 'human translation':
                        resources['ht'] = tp
                    else: # machine translation
                        resources['mt'] = tp
        if self._cls is ExctractedCCQuery:
            return ExctractedCCQuery(
                query_id=line['topic_id'],
                title=resources['org']['topic_title'],
                description=resources['org']['topic_description'],
                ht_title=resources['ht']['topic_title'],
                ht_description=resources['ht']['topic_description'],
                mt_title=resources['mt']['topic_title'],
                mt_description=resources['mt']['topic_description'],
                narrative_by_relevance=line['narratives'][self._subset_lang_three],
                report=line['report']['text'],
                report_url=line['report']['url'],
                report_date=line['report']['date'],
                translation_lang=self._subset_lang
            )
        elif self._cls is ExctractedCCNoReportQuery:
            return ExctractedCCNoReportQuery(
                query_id=line['topic_id'],
                title=resources['org']['topic_title'],
                description=resources['org']['topic_description'],
                narrative=resources['org']['topic_narrative'],
                ht_title=resources['ht']['topic_title'],
                ht_description=resources['ht']['topic_description'],
                ht_narrative=resources['ht']['topic_narrative'],
                mt_title=resources['mt']['topic_title'],
                mt_description=resources['mt']['topic_description'],
                mt_narrative=resources['mt']['topic_narrative'],
                translation_lang=self._subset_lang
            )
        elif self._cls is ExctractedCCNoReportNoHtNarQuery:
            return ExctractedCCNoReportNoHtNarQuery(
                query_id=line['topic_id'],
                title=resources['org']['topic_title'],
                description=resources['org']['topic_description'],
                narrative=resources['org']['topic_narrative'],
                ht_title=resources['ht']['topic_title'],
                ht_description=resources['ht']['topic_description'],
                mt_title=resources['mt']['topic_title'],
                mt_description=resources['mt']['topic_description'],
                mt_narrative=resources['mt']['topic_narrative'],
                translation_lang=self._subset_lang
            )
        elif self._cls is ExctractedCCMultiMtQuery:
            return ExctractedCCMultiMtQuery(
                query_id=line['topic_id'],
                title=resources['org']['topic_title'],
                description=resources['org']['topic_description'],
                narrative=resources['org']['topic_narrative'],
                fa_mt_title=resources['mt_fas']['topic_title'],
                fa_mt_description=resources['mt_fas']['topic_description'],
                fa_mt_narrative=resources['mt_fas']['topic_narrative'],
                ru_mt_title=resources['mt_rus']['topic_title'],
                ru_mt_description=resources['mt_rus']['topic_description'],
                ru_mt_narrative=resources['mt_rus']['topic_narrative'],
                zh_mt_title=resources['mt_zho']['topic_title'],
                zh_mt_description=resources['mt_zho']['topic_description'],
                zh_mt_narrative=resources['mt_zho']['topic_narrative'],
            )
