import pkgutil
import contextlib
import itertools
from pathlib import Path
import ir_datasets


_logger = ir_datasets.log.easy()


class Dataset:
    def __init__(self, *constituents):
        self._constituents = constituents

    def __getattr__(self, attr):
        for cons in self._constituents:
            if hasattr(cons, attr):
                return getattr(cons, attr)
        raise AttributeError(attr)

    def __repr__(self):
        supplies = []
        if self.has_docs():
            supplies.append('docs')
        if self.has_queries():
            supplies.append('queries')
        if self.has_qrels():
            supplies.append('qrels')
        if self.has_scoreddocs():
            supplies.append('scoreddocs')
        if self.has_docpairs():
            supplies.append('docpairs')
        supplies = ', '.join(supplies)
        return f'Dataset({supplies})'

    def __dir__(self):
        result = set(dir(super()))
        for cons in self._constituents:
            result |= set(dir(cons))
        return list(result)

    def has_docs(self):
        return hasattr(self, 'docs_handler')

    def has_queries(self):
        return hasattr(self, 'queries_handler')

    def has_qrels(self):
        return hasattr(self, 'qrels_handler')

    def has_scoreddocs(self):
        return hasattr(self, 'scoreddocs_handler')

    def has_docpairs(self):
        return hasattr(self, 'docpairs_handler')


class FilteredQueries:
    def __init__(self, queries_handler, lazy_qids, mode='include'):
        self._queries_handler = queries_handler
        self._lazy_qids = lazy_qids
        self._mode = mode

    def __getattr__(self, attr):
        return getattr(self._queries_handler, attr)

    def queries_iter(self):
        qids = self._lazy_qids()
        operator = {
            'include': (lambda x: x.query_id in qids),
            'exclude': (lambda x: x.query_id not in qids),
        }[self._mode]
        for query in self._queries_handler.queries_iter():
            if operator(query):
                yield query

    def queries_handler(self):
        return self


class FilteredQrels:
    def __init__(self, qrels_handler, lazy_qids, mode='include'):
        self._qrels_handler = qrels_handler
        self._lazy_qids = lazy_qids
        self._mode = mode

    def __getattr__(self, attr):
        return getattr(self._qrels_handler, attr)

    def qrels_iter(self):
        qids = self._lazy_qids()
        operator = {
            'include': (lambda x: x.query_id in qids),
            'exclude': (lambda x: x.query_id not in qids),
        }[self._mode]
        for query in self._qrels_handler.qrels_iter():
            if operator(query):
                yield query

    def qrels_handler(self):
        return self


class FilteredScoredDocs:
    def __init__(self, scoreddocs_handler, lazy_qids, mode='include'):
        self._scoreddocs_handler = scoreddocs_handler
        self._lazy_qids = lazy_qids
        self._mode = mode

    def __getattr__(self, attr):
        return getattr(self._scoreddocs_handler, attr)

    def scoreddocs_iter(self):
        qids = self._lazy_qids()
        operator = {
            'include': (lambda x: x.query_id in qids),
            'exclude': (lambda x: x.query_id not in qids),
        }[self._mode]
        for query in self._scoreddocs_handler.scoreddocs_iter():
            if operator(query):
                yield query

    def scoreddocs_handler(self):
        return self


class FilteredDocPairs:
    def __init__(self, docpairs_handler, lazy_qids, mode='include'):
        self._docpairs_handler = docpairs_handler
        self._lazy_qids = lazy_qids
        self._mode = mode

    def __getattr__(self, attr):
        return getattr(self._docpairs_handler, attr)

    def docpairs_iter(self):
        qids = self._lazy_qids()
        operator = {
            'include': (lambda x: x.query_id in qids),
            'exclude': (lambda x: x.query_id not in qids),
        }[self._mode]
        for query in self._docpairs_handler.docpairs_iter():
            if operator(query):
                yield query

    def docpairs_handler(self):
        return self


class YamlDocumentation:
    def __init__(self, file):
        self._file = file
        self._contents = None

    def __call__(self, key):
        return YamlDocumentationProvider(self, key)

    def get_key(self, key):
        if not self._contents:
            yaml = ir_datasets.lazy_libs.yaml()
            data = pkgutil.get_data('ir_datasets', self._file)
            self._contents = yaml.load(data, Loader=yaml.BaseLoader) # only strings
        return self._contents.get(key)


def _wrapped(value):
    def _wrapper():
        return value
    return _wrapper


class YamlDocumentationProvider:
    def __init__(self, documentation, key):
        self._documentation = documentation
        self._key = key

    def __getattr__(self, attr):
        if attr not in ('desc', 'bibtex'):
            raise AttributeError(attr)
        documentation = self._documentation.get_key(self._key)
        if documentation and attr in documentation:
            return _wrapped(documentation[attr])
        raise AttributeError(attr)


class ExpectedFile:
    def __init__(self, path, expected_md5=None, instructions=None):
        self._path = Path(path)
        self._expected_md5 = expected_md5
        self._instructions = instructions

    def path(self):
        if not self._path.exists():
            self._path.parent.mkdir(parents=True, exist_ok=True)
            inst = '\n\n' + self._instructions.format(path=self._path) if self._instructions else ''
            raise IOError(f"{self._path} does not exist.{inst}")
        return self._path

    @contextlib.contextmanager
    def stream(self):
        with self.path().open('rb') as result:
            if self._expected_md5:
                result = ir_datasets.util.HashStream(result, expected=self._expected_md5, algo='md5')
            yield result


class Concat(Dataset):
    def __getattr__(self, attr):
        if attr.endswith('_iter'):
            iters = []
            for ds in self._constituents:
                if hasattr(ds, attr):
                    iters.append(getattr(ds, attr)())
            if iters:
                return lambda: itertools.chain(*iters)
        return super().__getattr__(attr)
