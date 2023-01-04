import contextlib
from pathlib import Path
from typing import NamedTuple
import ir_datasets
from ir_datasets.util import TarExtractAll, DownloadConfig, RelativePath, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import TsvDocs, TsvQueries, TrecQrels

NAME = 'wikiclir'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    2: "Document assigned to the (English) cross-lingual mate",
    1: "All other articles that link to the mate, and are linked by the mate",
}


class WikiClirQuery(NamedTuple):
    query_id: str
    title: str
    first_sent: str
    def default_text(self):
        """
        title
        """
        return f"{self.title}"


class WikiClirDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    def default_text(self):
        """
        title and text
        """
        return f"{self.title} {self.text}"


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    langs = [
        ('arabic', 'ar', 'ar'),
        ('catalan', 'ca', 'ca'),
        ('chinese', 'zh', 'zh'),
        ('czech', 'cs', 'cs'),
        ('dutch', 'nl', 'nl'),
        ('finnish', 'fi', 'fi'),
        ('french', 'fr', 'fr'),
        ('german', 'de', 'de'),
        ('italian', 'it', 'it'),
        ('japanese', 'ja', 'ja'),
        ('korean', 'ko', 'ko'),
        ('norwegian_(bokmal)', 'no', 'no'),
        ('norwegian_(nynorsk)', 'nn', 'nn'),
        ('polish', 'pl', 'pl'),
        ('portuguese', 'pt', 'pt'),
        ('romanian', 'ro', 'ro'),
        ('russian', 'ru', 'ru'),
        ('simple_english', 'en', 'en-simple'),
        ('spanish', 'es', 'es'),
        ('swahili', 'sw', 'sw'),
        ('swedish', 'sv', 'sv'),
        ('tagalog', 'tl', 'tl'),
        ('turkish', 'tr', 'tr'),
        ('ukrainian', 'uk', 'uk'),
        ('vietnamese', 'vi', 'vi'),
    ]

    dlc = TarExtractAll(dlc['source'], base_path/'source')

    queries = TsvQueries(RelativePath(dlc, 'wiki-clir/english/wiki_en.queries'), namespace=NAME, query_cls=WikiClirQuery, lang='en')

    base = Dataset(documentation('_'))

    subsets = {}

    for source_path, lang, dsid in langs:
        file_suffix = lang if dsid != 'en-simple' else 'simple'
        qrels = TrecQrels(RelativePath(dlc, f'wiki-clir/{source_path}/en2{file_suffix}.rel'), QRELS_DEFS, format_3col=True)
        qids = _qid_filter(qrels)
        subsets[dsid] = Dataset(
            TsvDocs(RelativePath(dlc, f'wiki-clir/{source_path}/wiki_{file_suffix}.documents'), doc_cls=WikiClirDoc, namespace=NAME, lang=lang),
            FilteredQueries(queries, qids, mode='include'),
            qrels,
            documentation(dsid),
        )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets

def _qid_filter(qrels):
    return Lazy(lambda: {q.query_id for q in qrels.qrels_iter()})


collection, subsets = _init()
