import io
import codecs
import itertools
import ir_datasets
from typing import NamedTuple
from ir_datasets.util import DownloadConfig, TarExtract, Cache
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'cranfield'


QREL_DEFS = {
    -1: 'References of no interest.',
    1: 'References of minimum interest, for example, those that have been included from an historical viewpoint.',
    2: 'References which were useful, either as general background to the work or as suggesting methods of tackling certain aspects of the work.',
    3: 'References of a high degree of relevance, the lack of which either would have made the research impracticable or would have resulted in a considerable amount of extra work.',
    4: 'References which are a complete answer to the question.',
}


class CranfieldDoc(NamedTuple):
    doc_id: str
    title: str
    text: str
    author: str
    bib: str
    def default_text(self):
        """
        title + text
        """
        return f'{self.title} {self.text}'


def prefix_sentinel_splitter(it, sentinel):
    lines = None
    for is_sentinel, group in itertools.groupby(it, lambda l: l.startswith(sentinel)):
        if is_sentinel:
            lines = [list(group)[0].replace(sentinel, '')]
        else:
            lines += list(group)
            yield lines


class CranfieldDocs(BaseDocs):
    def __init__(self, docs_dlc):
        super().__init__()
        self.docs_dlc = docs_dlc

    def docs_path(self, force=True):
        return self.docs_dlc.path(force)

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self.docs_dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for lines in prefix_sentinel_splitter(stream, sentinel='.I '):
                record = {'doc_id': '', 'title': '', 'author': '', 'bib': '', 'text': ''}
                field = 'doc_id'
                for line in lines:
                    if line.startswith('.T'):
                        field = 'title'
                    elif line.startswith('.A'):
                        field = 'author'
                    elif line.startswith('.B'):
                        field = 'bib'
                    elif line.startswith('.W'):
                        field = 'text'
                    else:
                        record[field] += line
                record = {k: v.strip() for k, v in record.items()}
                yield CranfieldDoc(**record)

    def docs_cls(self):
        return CranfieldDoc

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/docs.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=['doc_id'],
            count_hint=ir_datasets.util.count_hint(NAME),
        )

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace():
        return NAME

    def docs_lang(self):
        return 'en'


class CranfieldQueries(BaseQueries):
    def __init__(self, queries_dlc):
        super().__init__()
        self.queries_dlc = queries_dlc

    def queries_path(self):
        return self.queries_dlc.path()

    def queries_iter(self):
        with self.queries_dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for lines in prefix_sentinel_splitter(stream, sentinel='.I '):
                record = {'query_id': '', 'text': ''}
                field = 'query_id'
                for line in lines:
                    if line.startswith('.W'):
                        field = 'text'
                    else:
                        record[field] += line
                record = {k: v.strip() for k, v in record.items()}
                record['query_id'] = record['query_id'].lstrip('0') # remove leading 0s to match qrels
                yield GenericQuery(**record)

    def queries_cls(self):
        return GenericQuery

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


class CranfieldQrels(BaseQrels):
    def __init__(self, qrels_dlc):
        self._qrels_dlc = qrels_dlc

    def qrels_path(self):
        return self._qrels_dlc.path()

    def qrels_iter(self):
        with self._qrels_dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            for line in f:
                cols = line.rstrip().split()
                if len(cols) != 3:
                    raise RuntimeError(f'expected 3 columns, got {len(cols)}')
                qid, did, score = cols
                yield TrecQrel(qid, did, int(score), '0')

    def qrels_cls(self):
        return TrecQrel

    def qrels_defs(self):
        return QREL_DEFS


def _init():
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    subsets = {}

    main_dlc = dlc['main']
    base = Dataset(
        CranfieldDocs(Cache(TarExtract(main_dlc, 'cran.all.1400'), base_path/'docs.txt')),
        CranfieldQueries(Cache(TarExtract(main_dlc, 'cran.qry'), base_path/'queries.txt')),
        CranfieldQrels(Cache(TarExtract(main_dlc, 'cranqrel'), base_path/'qrels.txt')),
        documentation('_'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
