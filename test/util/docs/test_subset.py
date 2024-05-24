from ir_datasets.util.docs.subset import DocsSubset, Dupes
from .data import FakeDocs

class SimpleDupes(Dupes):
    def __init__(self, doc_ids):
        self.doc_ids = doc_ids        


def test_subset_simple():
    docs = FakeDocs(5)
    
    dupe_ids = set(doc.doc_id for ix, doc in zip(range(docs.docs_count()), docs.docs_iter()) if ix in [1, 3])
    dupes = SimpleDupes(dupe_ids)
    docs_subset = DocsSubset("__fake_name__", docs, dupes)
    
    assert [
        doc.doc_id for doc in docs_subset.docs_iter()
    ] == [
        doc.doc_id for doc in docs.docs_iter() if doc.doc_id not in dupe_ids
    ]