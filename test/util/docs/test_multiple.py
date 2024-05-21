import pytest
from ir_datasets.formats.base import GenericDoc
from ir_datasets.util.docs.multiple import PrefixedDocs, PrefixedDocsSpec
from .data import FakeDocs, OtherDoc



def test_multiple_prefixes():
    docs_1 = FakeDocs(5)
    docs_2 = FakeDocs(3)

    spec = [
        PrefixedDocsSpec("D1-", docs_1),
        PrefixedDocsSpec("D2-", docs_2)
    ]

    all_docs = PrefixedDocs(
        None,
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
            None,
            PrefixedDocsSpec("D1-", FakeDocs(5)),
            PrefixedDocsSpec("D2-", FakeDocs(3, docs_cls=OtherDoc)) 
        ).docs_cls()
        
    blank = PrefixedDocs(
        None,
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
        PrefixedDocsSpec("D1-", PrefixedDocs(None, PrefixedDocsSpec("D1-", docs_1)), True),
        PrefixedDocsSpec("D2-", PrefixedDocs(None, PrefixedDocsSpec("D2-", docs_2)), True)
    ]

    all_docs = PrefixedDocs(
        None,
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