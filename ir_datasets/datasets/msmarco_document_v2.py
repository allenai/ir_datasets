import contextlib
import gzip
import io
from pathlib import Path
import json
from typing import NamedTuple, Tuple, List
import tarfile
import ir_datasets
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache, DownloadConfig, GzipExtract, Lazy, Migrator, TarExtractAll
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs, FilteredQrels
from ir_datasets.formats import TsvQueries, TrecQrels, TrecScoredDocs, BaseDocs
from ir_datasets.datasets.msmarco_passage import DUA, DL_HARD_QIDS_BYFOLD, DL_HARD_QIDS
from ir_datasets.datasets.msmarco_document import TREC_DL_QRELS_DEFS

_logger = ir_datasets.log.easy()

NAME = 'msmarco-document-v2'

QRELS_DEFS = {
    1: 'Document contains a passage labeled as relevant in msmarco-passage'
}


class MsMarcoV2Document(NamedTuple):
    doc_id: str
    url: str
    title: str
    headings: str
    body: str
    def default_text(self):
        """
        title + headings + body
        """
        return f'{self.title} {self.headings} {self.body}'


class MsMarcoV2Docs(BaseDocs):
    def __init__(self, dlc):
        super().__init__()
        self._dlc = dlc

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._dlc.stream() as stream, \
             tarfile.open(fileobj=stream, mode='r|') as tarf:
            for record in tarf:
                if not record.name.endswith('.gz'):
                    continue
                file = tarf.extractfile(record)
                with gzip.open(file) as file:
                    for line in file:
                        data = json.loads(line)
                        yield MsMarcoV2Document(
                            data['docid'],
                            data['url'],
                            data['title'],
                            data['headings'],
                            data['body'])

    def docs_cls(self):
        return MsMarcoV2Document

    def docs_store(self, field='doc_id'):
        # NOTE: the MS MARCO v2 documents have this really neat quality that they contain the offset
        # position in the source file: <https://microsoft.github.io/msmarco/TREC-Deep-Learning.html>.
        # Unfortunately, it points to the position in the *uncompressed* file, so for this to work, we'd
        # need to decompress the source files, inflating the size ~3.3x. The options would be to:
        #  1) Always de-compress the source files, costing everybody ~3.3x the storage. Ouch.
        #  2) De-compress the source files the first time that the docstore is requested. This would
        #     only cost the users who use the docstore 3.3x, but increases the complexity of the
        #     iteration code to handle both compressed and non-compressed versions. Would also need code
        #     to handle stuff like fancy slicing, which wouldn't be trivial. Would we also keep
        #     the original source file around? If so, it actually ends up being 4.3x.
        #  3) Build a PickleLz4FullStore on demand, as normal. This would only cost the users who use
        #     the docstore ~2.7x (accounting for worse lz4 compression rate and keeping around original
        #     copy of the data), but is also slightly slower because of the O(log n) position lookups and
        #     decompression. (This may be offset because pickle parsing is faster than json though.)
        #     It also reduces the complexity of the code, as it does not require a new docstore
        #     implementation for this dataset, and is just doing the normal procedure.
        return PickleLz4FullStore(
            path=f'{self._dlc.path(force=False)}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            key_field_prefix='msmarco_doc_', # cut down on storage by removing prefix in lookup structure
            size_hint=66500029281,
            count_hint=ir_datasets.util.count_hint(NAME),
        )
        # return MsMArcoV2DocStore(self)

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'



class MsMarcoV2AnchorTextDocument(NamedTuple):
    doc_id: str
    text: str
    anchors: List[str]
    def default_text(self):
        """
        text + anchors
        """
        return f'{self.text} ' + ' '.join(self.anchors)


class MsMarcoV2AnchorTextDocs(BaseDocs):
    def __init__(self, dlc, count_hint):
        super().__init__()
        self._dlc = dlc
        self._count_hint = count_hint

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield MsMarcoV2AnchorTextDocument(data['id'], ' '.join(data['anchors']), data['anchors'])

    def docs_cls(self):
        return MsMarcoV2AnchorTextDocument

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
    collection = MsMarcoV2Docs(dlc['docs'])

    subsets['train'] = Dataset(
        collection,
        TsvQueries(dlc['train_queries'], namespace='msmarco', lang='en'),
        TrecQrels(dlc['train_qrels'], QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['train_scoreddocs'])),
    )
    subsets['dev1'] = Dataset(
        collection,
        TsvQueries(dlc['dev1_queries'], namespace='msmarco', lang='en'),
        TrecQrels(dlc['dev1_qrels'], QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['dev1_scoreddocs'])),
    )
    subsets['dev2'] = Dataset(
        collection,
        TsvQueries(dlc['dev2_queries'], namespace='msmarco', lang='en'),
        TrecQrels(dlc['dev2_qrels'], QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['dev2_scoreddocs'])),
    )
    subsets['trec-dl-2019'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['trec-dl-2019/queries']), namespace='msmarco', lang='en'),
        TrecQrels(GzipExtract(dlc['trec_dl_2019_qrels']), TREC_DL_QRELS_DEFS),
    )
    subsets['trec-dl-2020'] = Dataset(
        collection,
        TsvQueries(GzipExtract(dlc['trec-dl-2020/queries']), namespace='msmarco', lang='en'),
        TrecQrels(GzipExtract(dlc['trec_dl_2020_qrels']), TREC_DL_QRELS_DEFS),
    )
    dl19_v2_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2019'].qrels_iter()})
    subsets['trec-dl-2019/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2019'].queries_handler(), dl19_v2_judged),
        subsets['trec-dl-2019'],
    )
    dl20_v2_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2020'].qrels_iter()})
    subsets['trec-dl-2020/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2020'].queries_handler(), dl20_v2_judged),
        subsets['trec-dl-2020'],
    )
    subsets['trec-dl-2021'] = Dataset(
        collection,
        TsvQueries(dlc['trec-dl-2021/queries'], namespace='msmarco', lang='en'),
        TrecQrels(dlc['trec-dl-2021/qrels'], TREC_DL_QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2021/scoreddocs'])),
    )
    dl21_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2021'].qrels_iter()})
    subsets['trec-dl-2021/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2021'].queries_handler(), dl21_judged),
        FilteredScoredDocs(subsets['trec-dl-2021'].scoreddocs_handler(), dl21_judged),
        subsets['trec-dl-2021'],
    )
    subsets['trec-dl-2022'] = Dataset(
        collection,
        TsvQueries(dlc['trec-dl-2022/queries'], namespace='msmarco', lang='en'),
        TrecQrels(dlc['trec-dl-2022/qrels'], TREC_DL_QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2022/scoreddocs'])),
    )
    dl22_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2022'].qrels_iter()})
    subsets['trec-dl-2022/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2022'].queries_handler(), dl22_judged),
        FilteredScoredDocs(subsets['trec-dl-2022'].scoreddocs_handler(), dl22_judged),
        subsets['trec-dl-2022'],
    )
    subsets['trec-dl-2023'] = Dataset(
        collection,
        TsvQueries(dlc['trec-dl-2023/queries'], namespace='msmarco', lang='en'),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2023/scoreddocs'])),
    )

    subsets['anchor-text'] = Dataset(
        MsMarcoV2AnchorTextDocs(
            Cache(GzipExtract(dlc['anchor-text']), base_path / "anchor-text.json"),
            count_hint=4821244
        ),
        documentation('anchor-text')
    )

    ir_datasets.registry.register(NAME, Dataset(collection, documentation("_")))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return collection, subsets


collection, subsets = _init()
