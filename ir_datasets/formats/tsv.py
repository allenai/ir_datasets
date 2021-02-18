import contextlib
from typing import Tuple
import io
import ir_datasets
from .base import GenericDoc, GenericQuery, GenericDocPair, BaseDocs, BaseQueries, BaseDocPairs
from ir_datasets.indices import PickleLz4FullStore


class FileLineIter:
    def __init__(self, dlc, start=None, stop=None, step=1):
        self.dlc = dlc
        self.stream = None
        self.pos = -1
        self.start = start
        self.stop = stop
        self.step = step
        self.ctxt = contextlib.ExitStack()

    def __next__(self):
        if self.stop is not None and self.start >= self.stop:
            self.ctxt.close()
            raise StopIteration
        if self.stream is None:
            self.stream = io.TextIOWrapper(self.ctxt.enter_context(self.dlc.stream()))
        while self.pos < self.start:
            line = self.stream.readline()
            if line != '\n':
                self.pos += 1
        if line == '':
            raise StopIteration
        self.start += self.step
        return line

    def __iter__(self):
        return self

    def __del__(self):
        self.ctxt.close()

    def __getitem__(self, key):
        if not isinstance(key, slice):
            raise TypeError('key must be slice')
        start, stop, step = self.start, self.stop, self.step
        if key.start is not None:
            if not isinstance(key.start, int):
                raise TypeError('start must be int')
            if key.start < 0:
                if stop is None:
                    raise ValueError('start cannot be negative with unknown size')
                start = stop + key.start
            else:
                start = start + key.start
        if key.stop is not None:
            if not isinstance(key.stop, int):
                raise TypeError('stop must be int')
            if key.stop < 0:
                if stop is None:
                    raise ValueError('stop cannot be negative with unknown size')
                stop = stop + (key.stop + 1)
            else:
                stop = key.stop
        if key.step is not None:
            if not isinstance(key.step, int):
                raise TypeError('step must be int')
            if key.step <= 0:
                raise ValueError('step must be a positive')
            step = self.step * key.step
        return FileLineIter(self.dlc, start, stop, step)


class TsvIter:
    def __init__(self, cls, line_iter):
        self.cls = cls
        self.line_iter = line_iter

    def __iter__(self):
        return self

    def __next__(self):
        line = next(self.line_iter)
        cols = line.rstrip('\n').split('\t')
        num_cols = len(self.cls._fields)
        last_field = self.cls.__annotations__[self.cls._fields[-1]] if hasattr(self.cls, '__annotations__') else None
        if last_field == Tuple[str, ...]:
            if len(cols) < len(self.cls._fields) - 1:
                raise RuntimeError(f'expected at least {len(self.cls._fields)-1} fields, got {len(cols)}')
            if len(cols) == len(self.cls._fields) - 1:
                cols += ((),)
            else:
                cols[len(self.cls._fields)-1] = tuple(cols[len(self.cls._fields)-1:])
                cols = cols[:len(self.cls._fields)]
        else:
            if len(cols) != len(self.cls._fields):
                raise RuntimeError(f'expected {len(self.cls._fields)} fields, got {len(cols)}')
        return self.cls(*cols)

    def __getitem__(self, key):
        return TsvIter(self.cls, self.line_iter[key])


class _TsvBase:
    def __init__(self, dlc, cls, datatype):
        super().__init__()
        self._dlc = dlc
        self._cls = cls
        self._datatype = datatype

    def _path(self):
        return self._dlc.path()

    def _iter(self):
        stop = None
        if hasattr(self, f'{self._datatype}_count'):
            stop = getattr(self, f'{self._datatype}_count')()
        return TsvIter(self._cls, FileLineIter(self._dlc, start=0, stop=stop, step=1))


class TsvDocs(_TsvBase, BaseDocs):
    def __init__(self, docs_dlc, doc_cls=GenericDoc, doc_store_index_fields=None, namespace=None, lang=None):
        super().__init__(docs_dlc, doc_cls, "docs")
        self._doc_store_index_fields = doc_store_index_fields
        self._docs_namespace = namespace
        self._docs_lang = lang

    def docs_path(self):
        return self._path()

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        return self._iter()

    def docs_cls(self):
        return self._cls

    def docs_store(self, field='doc_id'):
        fields = (self._doc_store_index_fields or ['doc_id'])
        return PickleLz4FullStore(
            path=f'{self.docs_path()}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=fields,
        )

    def docs_namespace(self):
        return self._docs_namespace

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()
        return None

    def docs_lang(self):
        return self._docs_lang


class TsvQueries(_TsvBase, BaseQueries):
    def __init__(self, queries_dlc, query_cls=GenericQuery, namespace=None, lang=None):
        super().__init__(queries_dlc, query_cls, "queries")
        self._queries_namespace = namespace
        self._queries_lang = lang

    def queries_path(self):
        return self._path()

    def queries_iter(self):
        return self._iter()

    def queries_cls(self):
        return self._cls

    def queries_namespace(self):
        return self._queries_namespace

    def queries_lang(self):
        return self._queries_lang


class TsvDocPairs(_TsvBase, BaseDocPairs):
    def __init__(self, docpairs_dlc, docpair_cls=GenericDocPair):
        super().__init__(docpairs_dlc, docpair_cls, "docpairs")

    def docpairs_path(self):
        return self._path()

    def docpairs_iter(self):
        return self._iter()

    def docpairs_cls(self):
        return self._cls
