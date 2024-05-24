import random
import string

from ir_datasets.formats.base import BaseDocs, GenericDoc
from ir_datasets.indices.base import Docstore


class OtherDoc:
    def __init__(self, id: str, text: str):
        self.id = id
        self.text = text


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
