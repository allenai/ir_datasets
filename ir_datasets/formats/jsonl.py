import sys
import codecs
import contextlib
import json
from typing import Tuple
import io
import ir_datasets
from .base import GenericDoc, GenericQuery, GenericDocPair, BaseDocs, BaseQueries, BaseDocPairs
from ir_datasets.indices import PickleLz4FullStore


class _JsonlBase:
    def __init__(self, dlcs, cls, datatype, mapping=None):
        super().__init__()
        self._dlcs = dlcs if isinstance(dlcs, (tuple, list)) else [dlcs]
        self._cls = cls
        self._datatype = datatype
        if mapping is None:
            self._mapping = {f: f for f in cls._fields}
        else:
            self._mapping = mapping

    def _path(self, force=True):
        return self._dlcs[0].path(force)

    def _iter(self):
        for dlc in self._dlcs:
            with dlc.stream() as f:
                for line in f:
                    data = json.loads(line)
                    yield self._cls(**{dockey: data[datakey] for dockey, datakey in self._mapping.items()})


class JsonlDocs(_JsonlBase, BaseDocs):
    def __init__(self, docs_dlcs, doc_cls=GenericDoc, mapping=None, doc_store_index_fields=None, namespace=None, lang=None, count_hint=None, docstore_path=None):
        super().__init__(docs_dlcs, doc_cls, "docs", mapping)
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


class JsonlQueries(_JsonlBase, BaseQueries):
    def __init__(self, query_dlcs, query_cls=GenericQuery, mapping=None, lang=None, namespaec=None):
        super().__init__(query_dlcs, query_cls, "queries", mapping)
        self._queries_lang = lang
        self._queries_namespace = namespaec

    def queries_path(self, force=True):
        return self._path(force)

    def queries_iter(self):
        return self._iter()

    def queries_cls(self):
        return self._cls

    def queries_namespace(self):
        return self._queries_namespace

    def queries_lang(self):
        return self._queries_lang
