import json
import codecs
from typing import NamedTuple, Dict
import ir_datasets
from ir_datasets.util import ZipExtract, Cache, Lazy
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries
from ir_datasets.formats import BaseQueries, BaseDocs, BaseQrels, TrecQrel
from ir_datasets.indices import PickleLz4FullStore

_logger = ir_datasets.log.easy()


NAME = 'beir'


class BeirDoc(NamedTuple):
    doc_id: str
    text: str
    title: str
    metadata: Dict[str, str]


class BeirQuery(NamedTuple):
    query_id: str
    text: str
    metadata: Dict[str, str]


class BeirDocs(BaseDocs):
    def __init__(self, name, dlc):
        super().__init__()
        self._name = name
        self._dlc = dlc

    def docs_iter(self):
        return iter(self.docs_store())

    def _docs_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield BeirDoc(data['_id'], data['text'], data.get('title', ''), data.get('metadata', {}))

    def docs_cls(self):
        return BeirDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME/self._name}/docs.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_count(self):
        return self.docs_store().count()

    def docs_namespace(self):
        return f'{NAME}/{self._name}'

    def docs_lang(self):
        return 'en'


class BeirQueries(BaseQueries):
    def __init__(self, name, dlc, keep_metadata=None):
        super().__init__()
        self._name = name
        self._dlc = dlc
        self._keep_metadata = keep_metadata

    def queries_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                metadata = data.get('metadata', {})
                if self._keep_metadata is not None:
                    metadata = {k: v for k, v in metadata.items() if k in self._keep_metadata}
                yield BeirQuery(data['_id'], data['text'], metadata)

    def queries_cls(self):
        return BeirQuery

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
        'msmarco': ['train', 'dev', 'test'],
        'trec-covid': ['test'],
        'nfcorpus': ['train', 'dev', 'test'],
        'nq': ['test'],
        'hotpotqa': ['train', 'dev', 'test'],
        'fiqa': ['train', 'dev', 'test'],
        'arguana': ['test'],
        'webis-touche2020': ['test'],
        'quora': ['dev', 'test'],
        'dbpedia-entity': ['dev', 'test'],
        'scidocs': ['test'],
        'fever': ['train', 'dev', 'test'],
        'climate-fever': ['test'],
        'scifact': ['train', 'test'],
    }

    for ds, qrels in benchmarks.items():
        dlc_ds = dlc[ds]
        docs = BeirDocs(ds, ZipExtract(dlc_ds, f'{ds}/corpus.jsonl'))
        queries = BeirQueries(ds, Cache(ZipExtract(dlc_ds, f'{ds}/queries.jsonl'), base_path/ds/'queries.json'))
        if len(qrels) == 1:
            subsets[ds] = Dataset(
                docs,
                queries,
                BeirQrels(Cache(ZipExtract(dlc_ds, f'{ds}/qrels/{qrels[0]}.tsv'), base_path/ds/f'{qrels[0]}.qrels'), qrels_defs={}),
                documentation(ds)
            )
        else:
            subsets[ds] = Dataset(
                docs,
                queries,
                documentation(ds)
            )
            for qrel in qrels:
                subset_qrels = BeirQrels(Cache(ZipExtract(dlc_ds, f'{ds}/qrels/{qrel}.tsv'), base_path/ds/f'{qrel}.qrels'), qrels_defs={})
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
        subsets[f'cqadupstack/{ds}'] = Dataset(
            BeirDocs(f'cqadupstack/{ds}', ZipExtract(cqa_dlc, f'cqadupstack/{ds}/corpus.jsonl')),
            BeirQueries(f'cqadupstack/{ds}', Cache(ZipExtract(cqa_dlc, f'cqadupstack/{ds}/queries.jsonl'), base_path/'cqadupstack'/ds/'queries.json'), keep_metadata=['tags']),
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
