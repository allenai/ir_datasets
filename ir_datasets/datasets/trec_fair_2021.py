import json
import codecs
from typing import NamedTuple, Dict, List, Optional
import ir_datasets
from ir_datasets.util import GzipExtract, Cache, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import BaseQueries, BaseDocs, BaseQrels, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
from itertools import chain


_logger = ir_datasets.log.easy()


NAME = 'trec-fair-2021'

QREL_DEFS = {
    1: "relevant"
}


class FairTrecDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    marked_up_text: str
    url: str
    quality_score: Optional[float]
    geographic_locations: Optional[List[str]]
    quality_score_disk: Optional[str]


class FairTrecQuery(NamedTuple):
    query_id: str
    text: str
    keywords: List[str]
    scope: str
    homepage: str


class FairTrecDocs(BaseDocs):
    def __init__(self, dlc, mlc):
        super().__init__()
        self._dlc = dlc
        self._mlc = mlc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        def _metadata_iter():
            with self._mlc.stream() as stream2:
                for metadata_line in stream2:
                    yield json.loads(metadata_line)
        textifier =  ir_datasets.lazy_libs.pyautocorpus().Textifier()
        metadata_iter = _metadata_iter()
        next_metadata = None
        with self._dlc.stream() as stream1:
            for line in stream1:
                data1 = json.loads(line)
                if next_metadata is None:
                    next_metadata = next(metadata_iter, None)
                if next_metadata is not None:
                    if data1['id'] == next_metadata['page_id']:
                        match = next_metadata
                        next_metadata = None
                try:
                    plaintext = textifier.textify(data1['text'])
                except ValueError as err:
                    message, position = err.args
                    if message == "Expected markup type 'comment'":
                        # unmatched <!-- comment tag
                        # The way Wikipedia renders this is it cuts the article off at this point.
                        # We'll follow that here, given it's only 22 articles of the 6M.
                        # (Note: the position is a byte offset, so that's why it encodes/decodes.)
                        plaintext = textifier.textify(data1['text'].encode()[:position].decode())
                    else:
                        raise
                if match: # has metadata
                    yield FairTrecDoc(str(data1['id']), data1['title'], plaintext, data1['text'], data1['url'], match['quality_score'], match['geographic_locations'], str(match['quality_score_disc']))
                else: # no metadata
                    yield FairTrecDoc(str(data1['id']), data1['title'], plaintext, data1['text'], data1['url'], None, None, None)

    def docs_cls(self):
        return FairTrecDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/docs.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            size_hint=30735927055,
            index_fields=['doc_id'],
            count_hint=6280328,
        )

    def docs_count(self):
        return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


class FairTrecQueries(BaseQueries):
    def __init__(self, dlc):
        super().__init__()
        self._dlc = dlc

    def queries_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield FairTrecQuery(str(data['id']), data['title'], data["keywords"], data["scope"], data["homepage"])

    def queries_cls(self):
        return FairTrecQuery

    def queries_lang(self):
        return 'en'

class FairTrecQrels(BaseQrels):
    def __init__(self, qrels_dlc):
        self._qrels_dlc = qrels_dlc

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        with self._qrels_dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                for rlDoc in data["rel_docs"]:
                    yield TrecQrel(str(data["id"]), str(rlDoc), 1, "0")

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return QREL_DEFS


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    collection = FairTrecDocs(GzipExtract(dlc["docs"]), GzipExtract(dlc["metadata"]))

    base = Dataset(
        collection,
        documentation('_'))

    subsets = {}

    dev_topics = GzipExtract(dlc["dev/topics"])
    subsets['dev'] = Dataset(
        collection,
        FairTrecQueries(dev_topics),
        FairTrecQrels(dev_topics),
        documentation('dev'))

    ir_datasets.registry.register(NAME, base)
    for s in subsets:
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
