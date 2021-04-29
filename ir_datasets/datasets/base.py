import pkgutil
import contextlib
import itertools
from pathlib import Path
import ir_datasets


_logger = ir_datasets.log.easy()


class Dataset:
    def __init__(self, *constituents):
        self._constituents = constituents
        self._beta_apis = {}

    def __getstate__(self):
        return self._constituents

    def __setstate__(self, state):
        self._constituents = state

    def __getattr__(self, attr):
        if attr == 'docs' and self.has_docs():
            if 'docs' not in self._beta_apis:
                self._beta_apis['docs'] = _BetaPythonApiDocs(self.docs_handler())
            return self._beta_apis['docs']
        if attr == 'queries' and self.has_queries():
            if 'queries' not in self._beta_apis:
                self._beta_apis['queries'] = _BetaPythonApiQueries(self.queries_handler())
            return self._beta_apis['queries']
        if attr == 'qrels' and self.has_qrels():
            if 'qrels' not in self._beta_apis:
                self._beta_apis['qrels'] = _BetaPythonApiQrels(self.qrels_handler())
            return self._beta_apis['qrels']
        if attr == 'scoreddocs' and self.has_scoreddocs():
            if 'scoreddocs' not in self._beta_apis:
                self._beta_apis['scoreddocs'] = _BetaPythonApiScoreddocs(self.scoreddocs_handler())
            return self._beta_apis['scoreddocs']
        if attr == 'docpairs' and self.has_docpairs():
            if 'docpairs' not in self._beta_apis:
                self._beta_apis['docpairs'] = _BetaPythonApiDocpairs(self.docpairs_handler())
            return self._beta_apis['docpairs']
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


class _BetaPythonApiDocs:
    def __init__(self, handler):
        self._handler = handler
        self._docstore = None
        self.type = handler.docs_cls()
        self.lang = handler.docs_lang()

    def __iter__(self):
        return self._handler.docs_iter()

    def __len__(self):
        return self._handler.docs_count()

    def __getitem__(self, key):
        return self._handler.docs_iter()[key]

    def __repr__(self):
        return f'BetaPythonApiDocs({repr(self._handler)})'

    def lookup(self, doc_ids):
        if self._docstore is None:
            self._docstore = self._handler.docs_store()
        if isinstance(doc_ids, str):
            return self._docstore.get(doc_ids)
        return self._docstore.get_many(doc_ids)

    def lookup_iter(self, doc_ids):
        if self._docstore is None:
            self._docstore = self._handler.docs_store()
        if isinstance(doc_ids, str):
            yield self._docstore.get(doc_ids)
        else:
            yield from self._docstore.get_many_iter(doc_ids)


class _BetaPythonApiQueries:
    def __init__(self, handler):
        self._handler = handler
        self._query_lookup = None
        self.type = handler.queries_cls()
        self.lang = handler.queries_lang()

    def __iter__(self):
        return self._handler.queries_iter()

    def __repr__(self):
        return f'BetaPythonApiQueries({repr(self._handler)})'

    def __len__(self):
        if self._query_lookup is None:
            self._query_lookup = {q.query_id: q for q in self._handler.queries_iter()}
        return len(self._query_lookup)

    def lookup(self, query_ids):
        if self._query_lookup is None:
            self._query_lookup = {q.query_id: q for q in self._handler.queries_iter()}
        if isinstance(query_ids, str):
            return self._query_lookup[query_ids]
        return {qid: self._query_lookup[qid] for qid in query_ids if qid in self._query_lookup}

    def lookup_iter(self, query_ids):
        if self._query_lookup is None:
            self._query_lookup = {q.query_id: q for q in self._handler.queries_iter()}
        if isinstance(query_ids, str):
            yield self._query_lookup[query_ids]
        else:
            for qid in query_ids:
                if qid in self._query_lookup:
                    yield self._query_lookup[qid]


class _BetaPythonApiQrels:
    def __init__(self, handler):
        self._handler = handler
        self.type = handler.qrels_cls()
        self.defs = handler.qrels_defs()
        self._qrels_dict = None

    def __iter__(self):
        return self._handler.qrels_iter()

    def __repr__(self):
        return f'BetaPythonApiQrels({repr(self._handler)})'

    def asdict(self):
        if self._qrels_dict is None:
            self._qrels_dict = self._handler.qrels_dict()
        return self._qrels_dict

    def __len__(self):
        if self._qrels_dict is None:
            self._qrels_dict = self._handler.qrels_dict()
        return sum(len(x) for x in self._qrels_dict.values())


class _BetaPythonApiScoreddocs:
    def __init__(self, handler):
        self._handler = handler
        self.type = handler.scoreddocs_cls()

    def __iter__(self):
        return self._handler.scoreddocs_iter()

    def __repr__(self):
        return f'BetaPythonApiScoreddocs({repr(self._handler)})'


class _BetaPythonApiDocpairs:
    def __init__(self, handler):
        self._handler = handler
        self.type = handler.docpairs_cls()

    def __iter__(self):
        return self._handler.docpairs_iter()

    def __repr__(self):
        return f'BetaPythonApiDocpairs({repr(self._handler)})'


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


class YamlDocumentationProvider:
    def __init__(self, documentation, key):
        self._documentation = documentation
        self._key = key

    def documentation(self):
        docs = self._documentation.get_key(self._key)
        if self._documentation.get_key(self._key):
            return dict(docs.items())
        return {}


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
