import sys
import codecs
import contextlib
import csv
from typing import Tuple
import io
import ir_datasets
from .base import GenericDoc, GenericQuery, GenericDocPair, BaseDocs, BaseQueries, BaseDocPairs
from ir_datasets.indices import PickleLz4FullStore


class _CsvBase:
    def __init__(self, dlc, cls, datatype):
        super().__init__()
        self._dlc = dlc
        self._cls = cls
        self._datatype = datatype

    def _path(self, force=True):
        return self._dlc.path(force)

    def _iter(self):
        csv.field_size_limit(sys.maxsize // 1000)
        field_count = len(self._cls._fields)
        with self._dlc.stream() as f:
            f = codecs.getreader('utf8')(f)
            f = iter(f)
            next(f) # skip header row
            for cols in csv.reader(f):
                assert len(cols) == field_count
                yield self._cls(*cols)


class CsvDocs(_CsvBase, BaseDocs):
    def __init__(self, docs_dlc, doc_cls=GenericDoc, doc_store_index_fields=None, namespace=None, lang=None, count_hint=None, docstore_path=None):
        super().__init__(docs_dlc, doc_cls, "docs")
        self._doc_store_index_fields = doc_store_index_fields
        self._docs_namespace = namespace
        self._docs_lang = lang
        self._count_hint = count_hint
        self._docstore_path = docstore_path if docstore_path is not None else f'{self.docs_path(force=False)}.pklz4'

    def docs_path(self, force=True):
        return self._path(force)

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        return self._iter()

    def docs_cls(self):
        return self._cls

    def docs_store(self, field='doc_id'):
        fields = (self._doc_store_index_fields or ['doc_id'])
        return PickleLz4FullStore(
            path=self._docstore_path,
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=fields,
            count_hint=self._count_hint,
        )

    def docs_namespace(self):
        return self._docs_namespace

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()
        return None

    def docs_lang(self):
        return self._docs_lang


class CsvQueries(_CsvBase, BaseQueries):
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


class CsvDocPairs(_CsvBase, BaseDocPairs):
    def __init__(self, docpairs_dlc, docpair_cls=GenericDocPair):
        super().__init__(docpairs_dlc, docpair_cls, "docpairs")

    def docpairs_path(self):
        return self._path()

    def docpairs_iter(self):
        return self._iter()

    def docpairs_cls(self):
        return self._cls
