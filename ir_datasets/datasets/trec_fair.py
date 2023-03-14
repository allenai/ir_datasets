import json
import codecs
from typing import NamedTuple, Dict, List, Optional
import ir_datasets
from ir_datasets.util import GzipExtract, Cache, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, Deprecated
from ir_datasets.formats import BaseQueries, BaseDocs, BaseQrels, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
from itertools import chain


_logger = ir_datasets.log.easy()


NAME = 'trec-fair'

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
    def default_text(self):
        """
        title and text
        """
        return f"{self.title} {self.text}"


class FairTrec2022Doc(NamedTuple):
    doc_id: str
    title: str
    text: str
    url: str
    pred_qual: Optional[float]
    qual_cat: Optional[str]
    page_countries: Optional[List[str]]
    page_subcont_regions: Optional[List[str]]
    source_countries: Optional[Dict[str, int]]
    source_subcont_regions: Optional[Dict[str, int]]
    gender: Optional[List[str]]
    occupations: Optional[List[str]]
    years: Optional[List[int]]
    num_sitelinks: Optional[int]
    relative_pageviews: Optional[float]
    first_letter: Optional[str]
    creation_date: Optional[str]
    first_letter_category: Optional[str]
    gender_category: Optional[str]
    creation_date_category: Optional[str]
    years_category: Optional[str]
    relative_pageviews_category: Optional[str]
    num_sitelinks_category: Optional[str]
    def default_text(self):
        """
        title and text
        """
        return f'{self.title} {self.text}'


class FairTrecQuery(NamedTuple):
    query_id: str
    text: str
    keywords: List[str]
    scope: str
    homepage: str
    def default_text(self):
        """
        text
        """
        return self.text

class FairTrec2022TrainQuery(NamedTuple):
    query_id: str
    text: str
    url: str
    def default_text(self):
        """
        text
        """
        return self.text


class FairTrecEvalQuery(NamedTuple):
    query_id: str
    text: str
    keywords: List[str]
    scope: str
    def default_text(self):
        """
        text
        """
        return self.text


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
            path=f'{ir_datasets.util.home_path()/NAME}/2021/docs.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            size_hint=30735927055,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(NAME),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


class FairTrecQueries(BaseQueries):
    def __init__(self, dlc, qtype):
        super().__init__()
        self._dlc = dlc
        self._qtype = qtype

    def queries_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                if self._qtype is FairTrecEvalQuery:
                    yield FairTrecEvalQuery(str(data['id']), data['title'], data["keywords"], data["scope"])
                elif self._qtype is FairTrecQuery:
                    yield FairTrecQuery(str(data['id']), data['title'], data["keywords"], data["scope"], data["homepage"])
                elif self._qtype is FairTrec2022TrainQuery:
                    yield FairTrec2022TrainQuery(str(data['id']), data['title'], data["url"])

    def queries_cls(self):
        return self._qtype

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


class JsonlDocs(BaseDocs):
    def __init__(self, dlc, metadata_dlc, doc_type, field_map, count_hint):
        super().__init__()
        self._metadata_dlc = metadata_dlc
        self._dlc = dlc
        self._doc_type = doc_type
        self._field_map = field_map
        self._count_hint = count_hint

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter_first(self):
        metadata = {}
        with self._metadata_dlc.stream() as stream:
            for line in _logger.pbar(stream, desc='pre-loading metadata', total=6460238):
                doc = json.loads(line)
                metadata[doc['page_id']] = doc
        with self._dlc.stream() as stream:
            for line in stream:
                doc = json.loads(line)
                if doc['id'] in metadata:
                    doc.update(metadata[doc['id']])
                yield self._doc_type(**{dest: self._doc_type.__annotations__[dest](doc.get(src)) if 'typing' not in str(self._doc_type.__annotations__[dest]) else doc.get(src) for dest, src in self._field_map.items()})

    def docs_cls(self):
        return self._doc_type

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{self._dlc.path(force=False)}.pklz4',
            init_iter_fn=self._docs_iter_first,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=self._count_hint,
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'

def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    collection2021 = FairTrecDocs(GzipExtract(dlc["2021/docs"]), GzipExtract(dlc["2021/metadata"]))
    mapping2022 = {'doc_id': 'id', 'title': 'title', 'url': 'url', 'text': 'plain', 'pred_qual': 'pred_qual','qual_cat': 'qual_cat','page_countries': 'page_countries','page_subcont_regions': 'page_subcont_regions','source_countries': 'source_countries','source_subcont_regions': 'source_subcont_regions','gender': 'gender','occupations': 'occupations','years': 'years','num_sitelinks': 'num_sitelinks','relative_pageviews': 'relative_pageviews','first_letter': 'first_letter','creation_date': 'creation_date','first_letter_category': 'first_letter_category','gender_category': 'gender_category','creation_date_category': 'creation_date_category','years_category': 'years_category','relative_pageviews_category': 'relative_pageviews_category','num_sitelinks_category': 'num_sitelinks_category'}
    collection2022 = JsonlDocs(GzipExtract(dlc["2022/docs"]), GzipExtract(dlc["2022/metadata"]), FairTrec2022Doc, mapping2022, ir_datasets.util.count_hint(f'{NAME}/2022'))

    base = Dataset(documentation('_'))

    subsets = {}

    subsets['2021'] = Dataset(
        collection2021,
        documentation('_'))

    train2021_topics = GzipExtract(dlc["2021/train/topics"])
    subsets['2021/train'] = Dataset(
        collection2021,
        FairTrecQueries(train2021_topics, FairTrecQuery),
        FairTrecQrels(train2021_topics),
        documentation('2021/train'))

    subsets['2021/eval'] = Dataset(
        collection2021,
        FairTrecQueries(GzipExtract(dlc["2021/eval/topics"]), FairTrecEvalQuery),
        FairTrecQrels(GzipExtract(dlc["2021/eval/qrels"])),
        documentation('2021/eval'))

    subsets['2022'] = Dataset(
        collection2022,
        documentation('2022'))

    train2022_topics = dlc["2022/train/topics"]
    subsets['2022/train'] = Dataset(
        collection2022,
        FairTrecQueries(train2022_topics, FairTrec2022TrainQuery),
        FairTrecQrels(train2022_topics),
        documentation('2022/train'))

    ir_datasets.registry.register(NAME, base)
    for s in subsets:
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    # old versions that include the year in the top level ID
    ir_datasets.registry.register('trec-fair-2021', Dataset(subsets['2021'], Deprecated('acessing TREC Fair Ranking 2021 through trec-fair-2021 is deprecated; use trec-fair/2021 instead.')))
    ir_datasets.registry.register('trec-fair-2021/train', Dataset(subsets['2021/train'], Deprecated('acessing TREC Fair Ranking 2021 through trec-fair-2021/train is deprecated; use trec-fair/2021/train instead.')))
    ir_datasets.registry.register('trec-fair-2021/eval', Dataset(subsets['2021/eval'], Deprecated('acessing TREC Fair Ranking 2021 through trec-fair-2021/train is deprecated; use trec-fair/2021/train instead.')))

    # move old version if it's found
    base_2021 = ir_datasets.util.home_path()/'trec-fair-2021'
    if base_2021.exists():
        if not base_path.exists():
            base_path.mkdir(parents=True, exist_ok=True)
        target = base_path/'2021'
        if not target.exists():
            base_2021.rename(target)

    return base, subsets


base, subsets = _init()
