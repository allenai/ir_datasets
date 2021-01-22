import os
from contextlib import contextmanager
import ir_datasets
from . import Docstore, Lz4PickleLookup


class CacheDocstore(Docstore):
    def __init__(self, full_store, path, cache_cls=Lz4PickleLookup):
        super().__init__(full_store._doc_cls, full_store._id_field)
        self.full_store = full_store
        self._path = path
        self.cache = cache_cls(path, self._doc_cls, self._id_field, [self._id_field])

    def get_many_iter(self, doc_ids):
        doc_ids_remaining = set(doc_ids)
        for doc in self.cache[doc_ids]:
            yield doc
            doc_ids_remaining.discard(doc[self._id_field_idx])
        if doc_ids_remaining:
            # fall back on full_store & cache the results
            with self.cache.transaction() as trans:
                for doc in self.full_store.get_many_iter(doc_ids_remaining):
                    yield doc
                    trans.add(doc)

    def clear_cache(self):
        self.cache.clear()
        self.full_store.clear_cache()
