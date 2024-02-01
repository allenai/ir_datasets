from itertools import chain
import random
import string
from typing import NamedTuple

import pytest
from ir_datasets.formats.base import BaseDocs, GenericDoc
from ir_datasets.indices.base import Docstore
from ir_datasets.util.docs.multiple import PrefixedDocs, PrefixedDocsSpec


class OtherDoc(NamedTuple):
    doc_id: str
    text: str


class FakeDocs(BaseDocs):
    def __init__(self, n_docs: int, namespace = 'test', lang='en', docs_cls=GenericDoc):
        self._docs = [
            docs_cls(
                f'{ix:05d}',
                ''.join(random.sample(string.ascii_lowercase, 16))
            )
            for ix in range(n_docs)
        ]
        self._docs_cls = docs_cls
        self._namespace = namespace
        self._lang = lang

    def docs_count(self):
        return len(self._docs)
    
    def docs_iter(self):
        return self._docs
    
    def docs_cls(self):
        return self._docs_cls
    
    def docs_lang(self):
        return self._lang
    
    def docs_namepace(self):
        return self._namespace
    
    def docs_store(self, field="doc_id") -> Docstore:
        return FakeDocstore(self)


class FakeDocstore(Docstore):
    def __init__(self, docs: FakeDocs):
        self._docs = docs
        
    def get_many(self, doc_ids, field=None):
        doc_ids = set(doc_ids)
        return {doc.doc_id: doc for doc in self._docs.docs_iter() if doc.doc_id in doc_ids}


def test_multiple_prefixes():
    docs_1 = FakeDocs(5)
    docs_2 = FakeDocs(3)

    spec = [
        PrefixedDocsSpec("D1-", docs_1),
        PrefixedDocsSpec("D2-", docs_2)
    ]

    all_docs = PrefixedDocs(
        *spec
    )
    
    assert all_docs.docs_cls() == GenericDoc
    assert all_docs.docs_lang() == 'en'
    assert all_docs.docs_count() == 8

    all_store = all_docs.docs_store()
    assert set(all_store.get_many(["D1-00001", "D1-00004", "D2-00002"]).values()) == set(
        GenericDoc(f"D1-{doc.doc_id}", doc.text) for doc in docs_1.docs_store().get_many(["00001", "00004"]).values()
    ) | set(
        GenericDoc(f"D2-{doc.doc_id}", doc.text) for doc in docs_2.docs_store().get_many(["00002"]).values()        
    )
    
    assert [doc.doc_id for doc in all_docs.docs_iter()] == [
        "D1-00000", "D1-00001", "D1-00002", "D1-00003", "D1-00004",
        "D2-00000", "D2-00001", "D2-00002"
    ]
    
    # Check that the doc IDs are the same    
    set_1 = set()
    for spec in spec:
        set_1.update(f"{spec.prefix}{doc.doc_id}" for doc in spec.docs.docs_iter())

    assert set_1 == set(doc.doc_id for doc in all_docs.docs_iter())
    
    with pytest.raises(AttributeError):
        PrefixedDocs(
            PrefixedDocsSpec("D1-", FakeDocs(5)),
            PrefixedDocsSpec("D2-", FakeDocs(3, docs_cls=OtherDoc)) 
        ).docs_cls()
        
    blank = PrefixedDocs(
        PrefixedDocsSpec("D1-", FakeDocs(5)),
        PrefixedDocsSpec("D2-", FakeDocs(3, lang='fr', namespace='other')) 
    )
    
    assert blank.docs_lang() is None
    assert blank.docs_namespace() is None
    
    
def test_multiple_prefixes_inlined():
    """Test support for already prefixed collections"""
    
    docs_1 = FakeDocs(5)
    docs_2 = FakeDocs(3)

    spec = [
        PrefixedDocsSpec("D1-", PrefixedDocs(PrefixedDocsSpec("D1-", docs_1)), True),
        PrefixedDocsSpec("D2-", PrefixedDocs(PrefixedDocsSpec("D2-", docs_2)), True)
    ]

    all_docs = PrefixedDocs(
        *spec
    )
    
    assert all_docs.docs_cls() == GenericDoc
    assert all_docs.docs_lang() == 'en'
    assert all_docs.docs_count() == 8

    assert [doc.doc_id for doc in all_docs.docs_iter()] == [
        "D1-00000", "D1-00001", "D1-00002", "D1-00003", "D1-00004",
        "D2-00000", "D2-00001", "D2-00002"
    ]

    all_store = all_docs.docs_store()
    assert set(all_store.get_many(["D1-00001", "D1-00004", "D2-00002"]).values()) == set(
        GenericDoc(f"D1-{doc.doc_id}", doc.text) for doc in docs_1.docs_store().get_many(["00001", "00004"]).values()
    ) | set(
        GenericDoc(f"D2-{doc.doc_id}", doc.text) for doc in docs_2.docs_store().get_many(["00002"]).values()        
    )