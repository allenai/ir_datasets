import json
import csv
import gzip
from typing import NamedTuple
import io
import itertools
from pathlib import Path
import ir_datasets
from ir_datasets.util import DownloadConfig, TarExtract, ZipExtractCache
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'codesearchnet'


QREL_DEFS = {
    1: 'Matches docstring',
}


QREL_DEFS_CHALLENGE = {
    0: 'Irrelevant',
    1: 'Weak Match',
    2: 'String Match',
    3: 'Exact Match',
}


class CodeSearchNetDoc(NamedTuple):
    doc_id: str
    repo: str
    path: str
    func_name: str
    code: str
    language: str


class CodeSearchNetChallengeQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: str
    note: str


class CodeSearchNetDocs(BaseDocs):
    def __init__(self, docs_dlcs):
        super().__init__()
        self.docs_dlcs = docs_dlcs

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        for dlc in self.docs_dlcs:
            base_path = Path(dlc.path())
            for file in sorted(base_path.glob('**/*.gz')):
                with gzip.open(file, 'rt') as f:
                    for line in f:
                        data = json.loads(line)
                        yield CodeSearchNetDoc(
                            data['url'], # doc_id = url
                            data['repo'],
                            data['path'],
                            data['func_name'],
                            data['code'],
                            data['language'],
                        )

    def docs_cls(self):
        return CodeSearchNetDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/docs.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
        )

    def docs_count(self):
        return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return None # not natural languages


class CodeSearchNetQueries(BaseQueries):
    def __init__(self, queries_dlcs, split):
        super().__init__()
        self.queries_dlcs = queries_dlcs
        self.split = split

    def queries_iter(self):
        for dlc in self.queries_dlcs:
            base_path = Path(dlc.path())
            for file in sorted(base_path.glob(f'**/{self.split}/*.gz')):
                with gzip.open(file, 'rt') as f:
                    for line in f:
                        data = json.loads(line)
                        yield GenericQuery(
                            data['url'], # query_id = url
                            data['docstring'], # text = docstring
                        )

    def queries_cls(self):
        return GenericQuery

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


class CodeSearchNetQrels(BaseQrels):
    def __init__(self, qrels_dlcs, split):
        super().__init__()
        self.qrels_dlcs = qrels_dlcs
        self.split = split

    def qrels_iter(self):
        for dlc in self.qrels_dlcs:
            base_path = Path(dlc.path())
            for file in sorted(base_path.glob(f'**/{self.split}/*.gz')):
                with gzip.open(file, 'rt') as f:
                    for line in f:
                        data = json.loads(line)
                        yield TrecQrel(
                            query_id=data['url'],
                            doc_id=data['url'],
                            relevance=1,
                            iteration='0',
                        )

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return QREL_DEFS

    def queries_lang(self):
        return 'en'


class CodeSearchNetChallengeQueries(BaseQueries):
    def __init__(self, queries_dlc):
        super().__init__()
        self.queries_dlc = queries_dlc

    def queries_path(self):
        return self.queries_dlc.path()

    def queries_iter(self):
        with self.queries_dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for i, line in enumerate(stream):
                if i == 0:
                    continue # skip first (header) line
                yield GenericQuery(str(i), line.rstrip())

    def queries_cls(self):
        return GenericQuery

    def queries_namespace(self):
        return NAME


class CodeSearchNetChallengeQrels(BaseQrels):
    def __init__(self, qrels_dlc, queries_handler):
        super().__init__()
        self.qrels_dlc = qrels_dlc
        self._queries_handler = queries_handler

    def qrels_path(self):
        return self.qrels_dlc.path()

    def qrels_iter(self):
        query_map = {q.text: q.query_id for q in self._queries_handler.queries_iter()}
        with self.qrels_dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for data in csv.DictReader(stream):
                yield CodeSearchNetChallengeQrel(
                    query_id=query_map[data['Query']],
                    doc_id=data['GitHubUrl'],
                    relevance=data['Relevance'],
                    note=data['Notes'])

    def qrels_cls(self):
        return CodeSearchNetChallengeQrel

    def qrels_defs(self):
        return QREL_DEFS_CHALLENGE


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    langs = ['python', 'java', 'go', 'php', 'ruby', 'javascript']

    dlcs = {lang: ZipExtractCache(dlc[lang], base_path/lang) for lang in langs}
    all_dlcs = [dlcs[lang] for lang in langs]
    base = Dataset(
        CodeSearchNetDocs(all_dlcs),
        documentation('_'),
    )
    subsets['train'] = Dataset(
        CodeSearchNetDocs(all_dlcs),
        CodeSearchNetQueries(all_dlcs, 'train'),
        CodeSearchNetQrels(all_dlcs, 'train'),
        documentation('train'),
    )
    subsets['valid'] = Dataset(
        CodeSearchNetDocs(all_dlcs),
        CodeSearchNetQueries(all_dlcs, 'valid'),
        CodeSearchNetQrels(all_dlcs, 'valid'),
        documentation('valid'),
    )
    subsets['test'] = Dataset(
        CodeSearchNetDocs(all_dlcs),
        CodeSearchNetQueries(all_dlcs, 'test'),
        CodeSearchNetQrels(all_dlcs, 'test'),
        documentation('test'),
    )
    challenge_queries = CodeSearchNetChallengeQueries(dlc['challenge/queries'])
    subsets['challenge'] = Dataset(
        CodeSearchNetDocs(all_dlcs),
        challenge_queries,
        CodeSearchNetChallengeQrels(dlc['challenge/qrels'], challenge_queries),
        documentation('challenge'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
