import io
import itertools
import ir_datasets
from ir_datasets.util import DownloadConfig, TarExtract, Cache
from ir_datasets.formats import BaseDocs, BaseQueries, BaseQrels
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.datasets.base import Dataset, YamlDocumentation


NAME = 'vaswani'


QREL_DEFS = {
    1: 'Relevant',
}


def sentinel_splitter(it, sentinel):
    for is_sentinel, group in itertools.groupby(it, lambda l: l == sentinel):
        if not is_sentinel:
            yield list(group)


class VaswaniDocs(BaseDocs):
    def __init__(self, docs_dlc):
        super().__init__()
        self.docs_dlc = docs_dlc

    def docs_path(self):
        return self.docs_dlc.path()

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        with self.docs_dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for lines in sentinel_splitter(stream, sentinel='   /\n'):
                doc_id = lines[0].rstrip('\n')
                doc_text = ''.join(lines[1:])
                yield GenericDoc(doc_id, doc_text)

    def docs_cls(self):
        return GenericDoc

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

    def docs_namespace():
        return NAME

    def docs_lang(self):
        return 'en'


class VaswaniQueries(BaseQueries):
    def __init__(self, queries_dlc):
        super().__init__()
        self.queries_dlc = queries_dlc

    def queries_path(self):
        return self.queries_dlc.path()

    def queries_iter(self):
        with self.queries_dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for lines in sentinel_splitter(stream, sentinel='/\n'):
                query_id = lines[0].rstrip('\n')
                query_text = ''.join(lines[1:])
                yield GenericQuery(query_id, query_text)

    def queries_cls(self):
        return GenericQuery

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


class VaswaniQrels(BaseQrels):
    def __init__(self, qrels_dlc):
        self.qrels_dlc = qrels_dlc

    def qrels_path(self):
        return self.qlres_dlc.path()

    def qrels_iter(self):
        with self.qrels_dlc.stream() as stream:
            stream = io.TextIOWrapper(stream)
            for lines in sentinel_splitter(stream, sentinel='   /\n'):
                query_id = lines[0].rstrip('\n')
                for line in lines[1:]:
                    for doc_id in line.split():
                        yield TrecQrel(query_id, doc_id, 1, '0')

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
        VaswaniDocs(Cache(TarExtract(main_dlc, 'doc-text'), base_path/'docs.txt')),
        VaswaniQueries(Cache(TarExtract(main_dlc, 'query-text'), base_path/'queries.txt')),
        VaswaniQrels(Cache(TarExtract(main_dlc, 'rlv-ass'), base_path/'qrels.txt')),
        documentation('_'),
    )

    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', subsets[s])

    return base, subsets


base, subsets = _init()
