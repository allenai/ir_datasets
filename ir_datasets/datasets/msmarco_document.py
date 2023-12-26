from typing import NamedTuple, List
import json
import ir_datasets
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache, DownloadConfig, GzipExtract, Lazy, Migrator
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs, FilteredQrels
from ir_datasets.formats import TrecDocs, TsvQueries, TrecQrels, TrecScoredDocs, BaseDocs
from ir_datasets.datasets.msmarco_passage import DUA, DL_HARD_QIDS_BYFOLD, DL_HARD_QIDS

NAME = 'msmarco-document'

_logger = ir_datasets.log.easy()

QRELS_DEFS = {
    1: 'Document contains a passage labeled as relevant in msmarco-passage'
}

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
    def default_text(self):
        """
        title + body
        """
        return f'{self.title} {self.body}'


# Use the TREC-formatted docs so we get all the available formatting (namely, line breaks)
class MsMarcoTrecDocs(TrecDocs):
    def __init__(self, docs_dlc):
        super().__init__(docs_dlc, parser='text', lang='en', docstore_size_hint=14373971970, count_hint=ir_datasets.util.count_hint(NAME))

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


class MsMarcoAnchorTextDocument(NamedTuple):
    doc_id: str
    text: str
    anchors: List[str]
    def default_text(self):
        """
        text + anchors
        """
        return f'{self.text} ' + ' '.join(self.anchors)


class MsMarcoAnchorTextDocs(BaseDocs):
    def __init__(self, dlc, count_hint):
        super().__init__()
        self._dlc = dlc
        self._count_hint = count_hint

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield MsMarcoAnchorTextDocument(data['id'], ' '.join(data['anchors']), data['anchors'])

    def docs_cls(self):
        return MsMarcoAnchorTextDocument

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()}/{NAME}/anchor-text.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return f'{NAME}/anchor-text'

    def docs_lang(self):
        return 'en'


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
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

    # DL-Hard
    dl_hard_qrels_migrator = Migrator(base_path/'trec-dl-hard'/'irds_version.txt', 'v2',
        affected_files=[base_path/'trec-dl-hard'/'qrels'],
        message='Updating trec-dl-hard qrels')
    hard_qids = Lazy(lambda: DL_HARD_QIDS)
    dl_hard_base_queries = TsvQueries([
            Cache(GzipExtract(dlc['trec-dl-2019/queries']), base_path/'trec-dl-2019/queries.tsv'),
            Cache(GzipExtract(dlc['trec-dl-2020/queries']), base_path/'trec-dl-2020/queries.tsv')], namespace='msmarco', lang='en')
    subsets['trec-dl-hard'] = Dataset(
        collection,
        FilteredQueries(dl_hard_base_queries, hard_qids),
        dl_hard_qrels_migrator(TrecQrels(dlc['trec-dl-hard/qrels'], TREC_DL_QRELS_DEFS)),
        documentation('trec-dl-hard')
    )
    hard_qids = Lazy(lambda: DL_HARD_QIDS_BYFOLD['1'])
    subsets['trec-dl-hard/fold1'] = Dataset(
        collection,
        FilteredQueries(dl_hard_base_queries, hard_qids),
        FilteredQrels(subsets['trec-dl-hard'], hard_qids),
        documentation('trec-dl-hard/fold1')
    )
    hard_qids = Lazy(lambda: DL_HARD_QIDS_BYFOLD['2'])
    subsets['trec-dl-hard/fold2'] = Dataset(
        collection,
        FilteredQueries(dl_hard_base_queries, hard_qids),
        FilteredQrels(subsets['trec-dl-hard'], hard_qids),
        documentation('trec-dl-hard/fold2')
    )
    hard_qids = Lazy(lambda: DL_HARD_QIDS_BYFOLD['3'])
    subsets['trec-dl-hard/fold3'] = Dataset(
        collection,
        FilteredQueries(dl_hard_base_queries, hard_qids),
        FilteredQrels(subsets['trec-dl-hard'], hard_qids),
        documentation('trec-dl-hard/fold3')
    )
    hard_qids = Lazy(lambda: DL_HARD_QIDS_BYFOLD['4'])
    subsets['trec-dl-hard/fold4'] = Dataset(
        collection,
        FilteredQueries(dl_hard_base_queries, hard_qids),
        FilteredQrels(subsets['trec-dl-hard'], hard_qids),
        documentation('trec-dl-hard/fold4')
    )
    hard_qids = Lazy(lambda: DL_HARD_QIDS_BYFOLD['5'])
    subsets['trec-dl-hard/fold5'] = Dataset(
        collection,
        FilteredQueries(dl_hard_base_queries, hard_qids),
        FilteredQrels(subsets['trec-dl-hard'], hard_qids),
        documentation('trec-dl-hard/fold5')
    )
    
    subsets['anchor-text'] = Dataset(
        MsMarcoAnchorTextDocs(
            Cache(GzipExtract(dlc['anchor-text']), base_path / "anchor-text.json"),
            count_hint=1703834
        ),
        documentation('anchor-text')
    )

    ir_datasets.registry.register(NAME, Dataset(collection, documentation("_")))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return collection, subsets


collection, subsets = _init()
