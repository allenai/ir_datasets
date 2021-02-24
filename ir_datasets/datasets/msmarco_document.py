from typing import NamedTuple
import ir_datasets
from ir_datasets.util import DownloadConfig, GzipExtract, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs
from ir_datasets.formats import TrecDocs, TsvQueries, TrecQrels, TrecScoredDocs
from ir_datasets.datasets.msmarco_passage import DUA, QRELS_DEFS

_logger = ir_datasets.log.easy()

TREC_DL_QRELS_DEFS = {
    3: "Perfectly relevant: Document is dedicated to the query, it is worthy of being a top result "
       "in a search engine.",
    2: "Highly relevant: The content of this document provides substantial information on the query.",
    1: "Relevant: Document provides some information relevant to the query, which may be minimal.",
    0: "Irrelevant: Document does not provide any useful information about the query",
}

ORCAS_QLRES_DEFS = {
    1: "User click",
}

class MsMarcoDocument(NamedTuple):
    doc_id: str
    url: str
    title: str
    body: str


# Use the TREC-formatted docs so we get all the available formatting (namely, line breaks)
class MsMarcoTrecDocs(TrecDocs):
    def __init__(self, docs_dlc):
        super().__init__(docs_dlc, parser='text', lang='en')

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        for doc in super().docs_iter():
            if isinstance(doc, MsMarcoDocument):
                # It's coming from the docstore
                yield doc
            else:
                # It's coming from the TredDocs parser... Do a little more reformatting:
                # The first two lines are the URL and page title
                url, title, *body = doc.text.lstrip('\n').split('\n', 2)
                body = body[0] if body else ''
                yield MsMarcoDocument(doc.doc_id, url, title, body)

    def docs_cls(self):
        return MsMarcoDocument

    def docs_namespace(self):
        return NAME


def _init():
    base_path = ir_datasets.util.home_path()/'msmarco-document'
    documentation = YamlDocumentation('docs/msmarco-document.yaml')
    dlc = DownloadConfig.context('msmarco-document', base_path, dua=DUA)
    subsets = {}
    collection = MsMarcoTrecDocs(GzipExtract(dlc['docs']))

    subsets['train'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['train/queries']), namespace='msmarco', lang='en'),
        TrecQrels(GzipExtract(dlc['train/qrels']), QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['train/scoreddocs'])),
    )

    subsets['dev'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['dev/queries']), namespace='msmarco', lang='en'),
        TrecQrels(GzipExtract(dlc['dev/qrels']), QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['dev/scoreddocs'])),
    )

    subsets['eval'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['eval/queries']), namespace='msmarco', lang='en'),
        TrecScoredDocs(GzipExtract(dlc['eval/scoreddocs'])),
    )

    subsets['trec-dl-2019'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['trec-dl-2019/queries']), namespace='msmarco', lang='en'),
        TrecQrels(dlc['trec-dl-2019/qrels'], TREC_DL_QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2019/scoreddocs'])),
    )

    subsets['trec-dl-2020'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['trec-dl-2020/queries']), namespace='msmarco', lang='en'),
        TrecQrels(dlc['trec-dl-2020/qrels'], TREC_DL_QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2020/scoreddocs'])),
    )

    subsets['orcas'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['orcas/queries']), namespace='orcas', lang='en'),
        TrecQrels(GzipExtract(dlc['orcas/qrels']), ORCAS_QLRES_DEFS),
        TrecScoredDocs(GzipExtract(dlc['orcas/scoreddocs'])),
    )

    dl19_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2019'].qrels_iter()})
    subsets['trec-dl-2019/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2019'].queries_handler(), dl19_judged),
        FilteredScoredDocs(subsets['trec-dl-2019'].scoreddocs_handler(), dl19_judged),
        subsets['trec-dl-2019'],
    )

    dl20_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2020'].qrels_iter()})
    subsets['trec-dl-2020/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2020'].queries_handler(), dl20_judged),
        FilteredScoredDocs(subsets['trec-dl-2020'].scoreddocs_handler(), dl20_judged),
        subsets['trec-dl-2020'],
    )

    ir_datasets.registry.register('msmarco-document', Dataset(collection, documentation("_")))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'msmarco-document/{s}', Dataset(subsets[s], documentation(s)))

    return collection, subsets


collection, subsets = _init()
