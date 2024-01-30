from functools import cached_property, lru_cache
from typing import List, Sequence, Tuple

from ir_datasets.formats import BaseDocs
from ir_datasets.indices.base import Docstore


class PrefixedDocstore(Docstore):
    def __init__(self, docs_mapping: List[Tuple[str, BaseDocs]], id_field="doc_id"):
        self._id_field = id_field
        self._stores = [
            (mapping[0], len(mapping[0]), mapping[1].docs_store(id_field=id_field))
            for mapping in docs_mapping
        ]

    def get_many(self, doc_ids: Sequence[str], field=None):
        assert field is None

        result = {}
        if field is None or field == self._id_field:
            # If field is ID field, remove the prefix
            for prefix, ix, store in self._stores:
                doc_ids = [
                    doc_id[ix:] for doc_id in doc_ids if doc_id.startswith(prefix)
                ]
                if doc_ids:
                    for key, doc in store.get_many(doc_ids):
                        key = f"{prefix}{key}"
                        result[key] = doc._replace(doc_id=key)
        else:
            # Just use the field
            for prefix, store in self._stores:
                for key, doc in store.get_many(doc_ids):
                    key = f"{prefix}{key}"
                    result[key] = doc._replace(doc_id=key)

        return result


class PrefixedDocs(BaseDocs):
    """Mixes documents and use a prefix to distinguish them"""

    def __init__(self, *docs_mapping: Tuple[str, BaseDocs]):
        assert all(len(mapping) == 2 for mapping in docs_mapping)
        self._docs_mapping = docs_mapping

    @cached_property
    def _docs_cls(self):
        _docs_cls = self._docs_mapping[0][1].docs_cls()
        assert all(
            mapping[1].docs_cls() == _docs_cls for mapping in self._docs_mapping[1:]
        )
        return _docs_cls

    @cached_property
    def _docs_lang(self):
        _docs_lang = self._docs_mapping[0][1].docs_lang()
        if any(
            mapping[1].docs_lang() == self._docs_lang
            for mapping in self._docs_mapping[1:]
        ):
            return None
        return _docs_lang

    @cached_property
    def _docs_namespace(self):
        _docs_namespace = self._docs_mapping[0][1].docs_namespace()
        if any(
            mapping[1].docs_namespace() == self._docs_namespace
            for mapping in self._docs_mapping[1:]
        ):
            return None
        return _docs_namespace

    @lru_cache()
    def docs_count(self):
        return sum(mapping[1].docs_count() for mapping in self._docs_mapping)

    def __iter__(self):
        return self.docs_iter()

    def docs_iter(self):
        for prefix, mapping in self._docs_mapping:
            for doc in mapping.docs_iter():
                doc = doc._replace(doc_id=f"{prefix}{doc.doc_id}")
                yield doc

    def docs_cls(self):
        return self._docs_cls

    def docs_lang(self):
        return self._docs_lang

    def docs_namespace(self):
        return self._docs_namespace

    @lru_cache
    def docs_store(self, field="doc_id"):
        return PrefixedDocstore(self._docs_mapping, field=field)
