import array
import os
from functools import cached_property, lru_cache
from typing import Optional

import ir_datasets
from ir_datasets.formats import BaseDocs
from ir_datasets.util import BaseDownload
from ir_datasets.util.docs.lazy import DocsList, LazyDocsIter

_logger = ir_datasets.log.easy()


class DocsSubsetList(DocsList):
    """List view of a document subset"""

    def __init__(self, main: "DocsSubset", indices: array.array):
        self._main = main
        self._indices = indices

    def get(self, ix: int):
        count = 0
        for removed_ix in self._indices:
            if ix <= removed_ix:
                count += 1
            else:
                break
        return self._main[ix]

    def __len__(self):
        return super().__len__()


class Dupes:
    def __init__(self, base: BaseDownload, prefix: Optional[str] = None):
        self._base = base
        self._prefix = prefix
        self._prefix_len = len(prefix) if prefix else 0
        self._remove_prefix = self.remove_prefix if prefix else lambda x: x

    def remove_prefix(self, doc_id: str):
        if doc_id.startswith(self._prefix):
            return doc_id[self._prefix_len :]

    @cached_property
    def doc_ids(self):
        doc_ids = set()
        with self._base.stream() as fp:
            for line in fp:
                if doc_id := self._remove_prefix(line.strip().decode("utf-8")):
                    doc_ids.add(doc_id)
        
        return doc_ids

    def has(self, doc_id: str):
        return doc_id in self.doc_ids   

    def __len__(self):
        return len(self.doc_ids)


class ColonCommaDupes(Dupes):
    """Dupes with the format

    doc_id:dupe_1_id,dupe_2_id,...
    """

    @cached_property
    def doc_ids(self):
        doc_ids = set()
        with self._base.stream() as fp:
            for line in fp:
                _, dupes = line.strip().decode("utf-8").split(":")
                for doc_id in dupes.split(","):
                    if doc_id := self._remove_prefix(doc_id):
                        doc_ids.add(doc_id)

        return doc_ids
    


class DocsSubset(BaseDocs):
    """Document collection minus a set of duplicated"""

    def __init__(self, store_name: str, docs: BaseDocs, removed_ids: "Dupes"):
        self._docs = docs
        self._store_name = store_name
        self._removed_ids = removed_ids
        self._store = None

    def docs_list(self):
        @lru_cache()
        def indices():
            """Stores the indices of removed documents"""
            indices_path = f"{ir_datasets.util.home_path()}/{self._store_name}.intarray"
            indices = array.array("L")
            if not os.path.exists(indices_path):
                for ix, doc in enumerate(
                    _logger.pbar(
                        iter(self.docs_iter()),
                        total=self.docs_count(),
                        desc="identifying removed documents",
                    )
                ):
                    if self._removed_ids.has(doc.doc_id):
                        indices.append(indices)
                with ir_datasets.util.finialized_file(indices_path, "wb") as fout:
                    fout.write(indices.tobytes())
                return indices
            else:
                with indices_path.open("rb") as fin:
                    indices.frombytes(fin)
                return indices

        return DocsSubsetList(self._docs.docs_iter(), indices)

    def docs_cls(self):
        return self._docs.docs_cls()

    def docs_lang(self):
        return self._docs.docs_lang()

    def docs_count(self):
        if count := self._docs.docs_count():
            return count - len(self._removed_ids)
        return None

    def docs_iter(self):
        return LazyDocsIter(
            self.docs_list,
            (
                doc
                for doc in self._docs.docs_iter()
                if not self._removed_ids.has(doc.doc_id)
            ),
        )

    def docs_namespace(self):
        return self._docs.docs_namespace()

    def docs_store(self, field="doc_id"):
        return self._docs.docs_store(field=field)
