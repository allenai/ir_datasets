from functools import cached_property, lru_cache
from dataclasses import dataclass
from typing import List, NamedTuple, Sequence, Tuple

import ir_datasets
from ir_datasets.formats import BaseDocs
from ir_datasets.indices.base import Docstore

_logger = ir_datasets.log.easy()


@dataclass(frozen=True)
class PrefixedDocsSpec:
    prefix: str
    """The prefix for this document collection"""

    docs: BaseDocs
    """The base documents"""

    has_prefix: bool = False
    """Whether documents have already the prefix"""
    
    @cached_property
    def length(self):
        return len(self.prefix)


class PrefixedDocstore(Docstore):
    def __init__(self, docs_mapping: List[PrefixedDocsSpec], id_field="doc_id"):
        self._id_field = id_field
        self._stores = [
            (mapping, mapping.docs.docs_store(field=id_field))
            for mapping in docs_mapping
        ]

    def get_many(self, doc_ids: Sequence[str], field=None):
        assert field is None

        result = {}
        if field is None or field == self._id_field:
            # If field is ID field, remove the prefix
            for mapping, store in self._stores:
                if _doc_ids := [
                    doc_id if mapping.has_prefix else doc_id[mapping.length:]  
                    for doc_id in doc_ids if doc_id.startswith(mapping.prefix)
                ]:
                    if mapping.has_prefix:
                        result.update(store.get_many(_doc_ids))
                    else:
                        for key, doc in store.get_many(_doc_ids).items():
                            key = f"{mapping.prefix}{key}"
                            result[key] = doc._replace(doc_id=key)
        else:
            # Just use the field
            for mapping, store in self._stores:
                if mapping.has_prefix:
                    result.update(store.get_many(_doc_ids))
                else:
                    for key, doc in store.get_many(doc_ids):
                        key = f"{mapping.prefix}{key}"
                        result[key] = doc._replace(doc_id=key)

        return result


class PrefixedDocs(BaseDocs):
    """Mixes documents and use a prefix to distinguish them"""

    def __init__(self, *docs_mapping: PrefixedDocsSpec):
        """Each mapping = (prefix, documents, boolean indicating whether
        documents have already a prefix)"""
        self._docs_mapping = docs_mapping

    @cached_property
    def lazy_self(self):
        try:
            self._docs_cls = self._docs_mapping[0].docs.docs_cls()
            if not all(
                mapping.docs.docs_cls() == self._docs_cls
                for mapping in self._docs_mapping[1:]
            ):
                _logger.error( f"Differing classes for documents, got {[mapping.docs.docs_cls() for mapping in self._docs_mapping[1:]]}")
                raise AssertionError()

            self._docs_lang = self._docs_mapping[0].docs.docs_lang()
            if any(
                mapping.docs.docs_lang() != self._docs_lang
                for mapping in self._docs_mapping[1:]
            ):
                self._docs_lang = None

            self._docs_namespace = self._docs_mapping[0].docs.docs_namespace()
            if any(
                mapping.docs.docs_namespace() != self._docs_namespace
                for mapping in self._docs_mapping[1:]
            ):
                self._docs_namespace = None
        except Exception:
            _logger.logger().exception("Error while computing lazy attributes")
            return None
        return self

    def docs_cls(self):
        return self.lazy_self._docs_cls

    def docs_namespace(self):
        return self.lazy_self._docs_namespace

    def docs_lang(self):
        return self.lazy_self._docs_lang

    def __iter__(self):
        return self.docs_iter()

    def docs_iter(self):
        for mapping in self._docs_mapping:
            for doc in mapping.docs.docs_iter():
                if not mapping.has_prefix:
                    doc = doc._replace(doc_id=f"{mapping.prefix}{doc.doc_id}")
                yield doc

    @lru_cache()
    def docs_count(self):
        return sum(mapping[1].docs_count() for mapping in self._docs_mapping)

    @lru_cache
    def docs_store(self, field="doc_id"):
        return PrefixedDocstore(self._docs_mapping, id_field=field)
