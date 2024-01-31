from abc import ABC, abstractmethod
from functools import cached_property, lru_cache
from typing import Iterator, Union

import ir_datasets
from ir_datasets.formats import BaseDocs, GenericDoc
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.indices.base import Docstore

_logger = ir_datasets.log.easy()


class IRDSDocuments(BaseDocs):
    """Document collection based on another ir_datasets one"""

    def __init__(self, ds_id: str):
        """Construct a new lazy docs

        :param ds_id: The ID of the ir_datasets collection
        """
        self._ds_id = ds_id

    @cached_property
    def docs(self):
        return ir_datasets.load(self._ds_id)

    def docs_cls(self):
        return self.docs.docs_cls()

    def docs_lang(self):
        return self.docs.docs_lang()

    def docs_count(self):
        return self.docs.docs_count()

    def docs_iter(self):
        return self.docs.docs_iter()


class LazyDocs(IRDSDocuments):
    """Proxy for a IR dataset collection"""
    def docs_store(self, field="doc_id"):
        return self.docs.docs_store(field=field)


class DocsListView:
    """View over document lists"""

    def __init__(self, docs: "DocsList", slice: slice):
        self._docs = docs
        self._indices = [c for c in slice]

    def __getitem__(self, slice: Union[int, slice]):
        if isinstance(slice, int):
            return self._docs.get(slice)

        return DocsListView(self, self._docs, slice[slice])


class DocsList(ABC):
    """A document list"""

    @abstractmethod
    def get(self, ix: int):
        ...

    @abstractmethod
    def __len__(self):
        ...

    def __getitem__(self, slice: Union[int, slice]):
        if isinstance(slice, int):
            return self.get(slice)

        return DocsListView(self, slice)


class DirectAccessDocs:
    def docs_list(self) -> DocsList:
        return DocsList


class LazyDocsIter:
    """Iterate over documents unless a specific range is queried"""

    def __init__(self, docs: DirectAccessDocs, iter):
        self.docs = docs
        self._iter = iter

    def __getitem__(self, slice: Union[int, slice]):
        return self.docs_list()[slice]

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iter)


class BaseTransformedDocs(BaseDocs):
    def __init__(self, docs: BaseDocs, cls, store_name, count=None):
        """Document collection tranformed using a transform function

        :param docs: The base documents
        :param store_name: The name of the LZ4 document store
        """
        self._docs = docs
        self._cls = cls
        self._store_name = store_name
        self._count_hint = count

    def docs_cls(self):
        return self._cls

    def docs_lang(self):
        return self._docs.docs_lang()

    def docs_count(self):
        return self._count_hint or self._docs.docs_count()

    @lru_cache
    def docs_store(self, field="doc_id"):
        return PickleLz4FullStore(
            path=f"{ir_datasets.util.home_path()}/{self._store_name}.pklz4",
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=[field],
            count_hint=self._count_hint,
        )


class TransformedDocs(BaseTransformedDocs):
    def __init__(
        self, docs: BaseDocs, cls, transform=None, store_name=None, count=None
    ):
        """Document collection tranformed using a transform function

        :param docs: The base documents
        :param transform: The transformation function
        :param store_name: if set, creates a LZ4 document store, otherwise
            transform on the fly, defaults to None
        """
        super().__init__(docs, cls, store_name, count=count)
        self._transform = transform or self

    @lru_cache
    def docs_store(self, field="doc_id"):
        if self._store_name is None:
            return TransformedDocstore(self._docs.docs_store(field), self._transform)
        return super().docs_store()

    def docs_iter(self):
        for doc in map(self._transform, self._docs.docs_iter()):
            if doc is not None:
                yield doc


class TransformedDocstore(Docstore):
    """On the fly transform of documents"""

    def __init__(self, store, transform):
        self._store = store
        self._transform = transform

    def get_many(self, doc_ids, field=None):
        return {
            key: self._transform(doc)
            for key, doc in self._store.get_many(doc_ids, field)
        }


class IterDocs(BaseDocs):
    """Documents based on an iterator"""

    def __init__(
        self,
        corpus_name,
        docs_iter_fn: Iterator,
        docs_lang="en",
        docs_namespace=None,
        docs_cls=GenericDoc,
        count_hint=None,
    ):
        super().__init__()
        self._corpus_name = corpus_name
        self._docs_iter_fn = docs_iter_fn
        self._docs_cls = docs_cls
        self._count_hint = count_hint
        self._docs_namespace = docs_namespace
        self._docs_lang = docs_lang

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()
        return self._count_hint

    def docs_iter(self):
        def iter():
            with _logger.duration(f"processing {self._corpus_name}"):
                yield from (d for d in self._docs_iter_fn())

        return LazyDocsIter(self, iter)

    def docs_list(self):
        return self.docs_store()

    def docs_cls(self):
        return self._docs_cls

    def docs_store(self, field="doc_id"):
        return PickleLz4FullStore(
            path=f"{ir_datasets.util.home_path()}/{self._corpus_name}.pklz4",
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=[field],
            count_hint=self._count_hint,
        )

    def docs_namespace(self):
        return self._docs_namespace

    def docs_lang(self):
        return self._docs_lang
