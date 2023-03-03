import json
import codecs
from typing import NamedTuple, Dict, List
import ir_datasets
from ir_datasets.util import ZipExtract, Cache, Lazy, Migrator
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import BaseQueries, BaseDocs, BaseQrels, GenericDoc, GenericQuery, TrecQrel
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()


NAME = 'beir'


class BeirDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    metadata: Dict[str, str]
    def default_text(self):
        """
        title text
        """
        return f'{self.title} {self.text}'


class BeirTitleDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    def default_text(self):
        """
        title text
        """
        return f'{self.title} {self.text}'

class BeirTitleUrlDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    url: str
    def default_text(self):
        """
        title text
        """
        return f'{self.title} {self.text}'

class BeirSciDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    authors: List[str]
    year: int
    cited_by: List[str]
    references: List[str]

class BeirCordDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    url: str
    pubmed_id: str
    def default_text(self):
        """
        title text
        """
        return f'{self.title} {self.text}'

class BeirToucheDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    stance: str
    url: str

class BeirCqaDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    tags: List[str]
    def default_text(self):
        """
        title text
        """
        return f'{self.title} {self.text}'

class BeirUrlQuery(NamedTuple):
    query_id: str
    text: str
    url: str

class BeirSciQuery(NamedTuple):
    query_id: str
    text: str
    authors: List[str]
    year: int
    cited_by: List[str]
    references: List[str]

class BeirToucheQuery(NamedTuple):
    query_id: str
    text: str
    description: str
    narrative: str

class BeirCovidQuery(NamedTuple):
    query_id: str
    text: str
    query: str
    narrative: str
    def default_text(self):
        """
        text
        """
        return self.text

class BeirCqaQuery(NamedTuple):
    query_id: str
    text: str
    tags: List[str]
    def default_text(self):
        """
        text
        """
        return self.text

def _map_field(field, data):
    if field in ('doc_id', 'query_id'):
        return data['_id']
    if field == 'text':
        return data['text']
    if field == 'title':
        return data['title']
    else:
        return data['metadata'][field]

class BeirDocs(BaseDocs):
    def __init__(self, name, dlc, doc_type):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._doc_type = doc_type

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield self._doc_type(*(_map_field(f, data) for f in self._doc_type._fields))

    def docs_cls(self):
        return self._doc_type

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME/self._name}/docs.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(f'{NAME}/{self._name}'),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return f'{NAME}/{self._name}'

    def docs_lang(self):
        return 'en'


class BeirQueries(BaseQueries):
    def __init__(self, name, dlc, query_type):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._query_type = query_type

    def queries_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield self._query_type(*(_map_field(f, data) for f in self._query_type._fields))

    def queries_cls(self):
        return self._query_type

    def queries_namespace(self):
        return f'{NAME}/{self._name}'

    def queries_lang(self):
        return 'en'


class BeirQrels(BaseQrels):
    def __init__(self, qrels_dlc, qrels_defs):
        self._qrels_dlc = qrels_dlc
        self._qrels_defs = qrels_defs

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            it = iter(f)
            assert next(it).strip() == 'query-id\tcorpus-id\tscore' # header row
            for line in it:
                if line == '\n':
                    continue # ignore blank lines
                cols = line.rstrip().split()
                if len(cols) != 3:
                    raise RuntimeError(f'expected 3 columns, got {len(cols)}')
                qid, did, score = cols
                yield TrecQrel(qid, did, int(score), '0')

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return self._qrels_defs


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    dlc = ir_datasets.util.DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    base = Dataset(documentation('_'))

    subsets = {}

    benchmarks = {
        'msmarco': (['train', 'dev', 'test'], GenericDoc, GenericQuery),
        'trec-covid': (['test'], BeirCordDoc, BeirCovidQuery),
        'nfcorpus': (['train', 'dev', 'test'], BeirTitleUrlDoc, BeirUrlQuery),
        'nq': (['test'], BeirTitleDoc, GenericQuery),
        'hotpotqa': (['train', 'dev', 'test'], BeirTitleUrlDoc, GenericQuery),
        'fiqa': (['train', 'dev', 'test'], GenericDoc, GenericQuery),
        'arguana': (['test'], BeirTitleDoc, GenericQuery),
        'webis-touche2020': (['test'], BeirToucheDoc, BeirToucheQuery),
        'webis-touche2020/v2': (['test'], BeirToucheDoc, BeirToucheQuery),
        'quora': (['dev', 'test'], GenericDoc, GenericQuery),
        'dbpedia-entity': (['dev', 'test'], BeirTitleUrlDoc, GenericQuery),
        'scidocs': (['test'], BeirSciDoc, BeirSciQuery),
        'fever': (['train', 'dev', 'test'], BeirTitleDoc, GenericQuery),
        'climate-fever': (['test'], BeirTitleDoc, GenericQuery),
        'scifact': (['train', 'test'], BeirTitleDoc, GenericQuery),
    }

    for ds, (qrels, doc_type, query_type) in benchmarks.items():
        dlc_ds = dlc[ds]
        ds_zip = ds.split('/')[0]
        docs_migrator = Migrator(base_path/ds/'irds_version.txt', 'v2',
            affected_files=[f'{base_path/ds}/docs.pklz4'],
            message=f'Migrating {NAME}/{ds} (structuring fields)')
        docs = docs_migrator(BeirDocs(ds, ZipExtract(dlc_ds, f'{ds_zip}/corpus.jsonl'), doc_type))
        queries = BeirQueries(ds, Cache(ZipExtract(dlc_ds, f'{ds_zip}/queries.jsonl'), base_path/ds/'queries.json'), query_type)
        if len(qrels) == 1:
            subsets[ds] = Dataset(
                docs,
                queries,
                BeirQrels(Cache(ZipExtract(dlc_ds, f'{ds_zip}/qrels/{qrels[0]}.tsv'), base_path/ds/f'{qrels[0]}.qrels'), qrels_defs={}),
                documentation(ds)
            )
        else:
            subsets[ds] = Dataset(
                docs,
                queries,
                documentation(ds)
            )
            for qrel in qrels:
                subset_qrels = BeirQrels(Cache(ZipExtract(dlc_ds, f'{ds_zip}/qrels/{qrel}.tsv'), base_path/ds/f'{qrel}.qrels'), qrels_defs={})
                subset_qids = qid_filter(subset_qrels)
                subsets[f'{ds}/{qrel}'] = Dataset(
                    docs,
                    FilteredQueries(queries, subset_qids, mode='include'),
                    subset_qrels,
                    documentation(f'{ds}/{qrel}')
                )

    cqa = ['android', 'english', 'gaming', 'gis', 'mathematica', 'physics', 'programmers', 'stats', 'tex', 'unix', 'webmasters', 'wordpress']
    cqa_dlc = dlc['cqadupstack']
    for ds in cqa:
        docs_migrator = Migrator(base_path/'cqadupstack'/ds/'irds_version.txt', 'v2',
            affected_files=[f'{base_path/"cqadupstack"/ds}/docs.pklz4'],
            message=f'Migrating {NAME}/cqadupstack/{ds} (structuring fields)')
        subsets[f'cqadupstack/{ds}'] = Dataset(
            docs_migrator(BeirDocs(f'cqadupstack/{ds}', ZipExtract(cqa_dlc, f'cqadupstack/{ds}/corpus.jsonl'), BeirCqaDoc)),
            BeirQueries(f'cqadupstack/{ds}', Cache(ZipExtract(cqa_dlc, f'cqadupstack/{ds}/queries.jsonl'), base_path/'cqadupstack'/ds/'queries.json'), BeirCqaQuery),
            BeirQrels(Cache(ZipExtract(cqa_dlc, f'cqadupstack/{ds}/qrels/test.tsv'), base_path/'cqadupstack'/ds/f'test.qrels'), qrels_defs={}),
            documentation(f'cqadupstack/{ds}')
        )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets

def qid_filter(subset_qrels):
    # NOTE: this must be in a separate function otherwise there can be weird lambda binding problems
    return Lazy(lambda: {q.query_id for q in subset_qrels.qrels_iter()})


base, subsets = _init()
