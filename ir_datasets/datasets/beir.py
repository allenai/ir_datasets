import json
import codecs
from typing import NamedTuple, Dict
import ir_datasets
from ir_datasets.util import ZipExtract, Cache
from ir_datasets.datasets.base import Dataset, YamlDocumentation
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
    def __init__(self, name, dlc):
        super().__init__()
        self._name = name
        self._dlc = dlc

    def queries_iter(self):
        with self._dlc.stream() as stream:
            for line in stream:
                data = json.loads(line)
                yield BeirQuery(data['_id'], data['text'], data.get('metadata', {}))

    def queries_cls(self):
        return BeirQuery

    def queries_namespace(self):
        return f'{NAME}/{self._name}'

    def queries_lang(self):
        return 'en'


class TrecQrels(BaseQrels):
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
                    raise RuntimeError(f'expected 4 columns, got {len(cols)}')
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

    for ds in ['msmarco', 'trec-covid', 'nfcorpus', 'nq', 'hotpotqa', 'fiqa', 'arguana', 'webis-touche2020', 'quora', 'dbpedia-entity', 'scidocs', 'fever', 'climate-fever', 'scifact']:
        dlc_ds = dlc[ds]
        subsets[ds] = Dataset(
            BeirDocs(ds, ZipExtract(dlc_ds, f'{ds}/corpus.jsonl')),
            BeirQueries(ds, Cache(ZipExtract(dlc_ds, f'{ds}/queries.jsonl'), base_path/ds/'queries.json')),
            TrecQrels(Cache(ZipExtract(dlc_ds, f'{ds}/qrels/test.tsv'), base_path/ds/'qrels'), qrels_defs={}),
            documentation(ds)
        )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
