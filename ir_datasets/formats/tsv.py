import io
from .base import GenericDoc, GenericQuery, GenericDocPair, BaseDocs, BaseQueries, BaseDocPairs


class _TsvBase:
    def __init__(self, dlc, cls):
        super().__init__()
        self._dlc = dlc
        self._cls = cls

    def _path(self):
        return self._dlc.path()

    def _iter(self):
        with self._dlc.stream() as f:
            f = io.TextIOWrapper(f)
            for line in f:
                if line == '\n':
                    continue # ignore blanks
                cols = line.rstrip('\n').split('\t')
                if len(cols) != len(self._cls._fields):
                    raise RuntimeError(f'expected {len(self._cls._fields)} fields, got {len(cols)}')
                yield self._cls(*cols)


class TsvDocs(_TsvBase, BaseDocs):
    def __init__(self, docs_dlc, doc_cls=GenericDoc):
        super().__init__(docs_dlc, doc_cls)

    def docs_path(self):
        return self._path()

    def docs_iter(self):
        return self._iter()

    def docs_cls(self):
        return self._cls


class TsvQueries(_TsvBase, BaseQueries):
    def __init__(self, queries_dlc, query_cls=GenericQuery):
        super().__init__(queries_dlc, query_cls)

    def queries_path(self):
        return self._path()

    def queries_iter(self):
        return self._iter()

    def queries_cls(self):
        return self._cls


class TsvDocPairs(_TsvBase, BaseDocPairs):
    def __init__(self, docpairs_dlc, docpair_cls=GenericDocPair):
        super().__init__(docpairs_dlc, docpair_cls)

    def docpairs_path(self):
        return self._path()

    def docpairs_iter(self):
        return self._iter()

    def docpairs_cls(self):
        return self._cls
