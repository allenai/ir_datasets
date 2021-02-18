import hashlib
import json
import types
from typing import NamedTuple

class GenericDoc(NamedTuple):
    doc_id: str
    text: str

class GenericQuery(NamedTuple):
    query_id: str
    text: str

class GenericQrel(NamedTuple):
    query_id: str
    doc_id: str
    relevance: int

class GenericScoredDoc(NamedTuple):
    query_id: str
    doc_id: str
    score: float

class GenericDocPair(NamedTuple):
    query_id: str
    doc_id_a: str
    doc_id_b: str


class BaseDocs:
    PREFIX = 'docs_'
    EXTENSIONS = {}

    def __getattr__(self, attr):
        if attr.startswith(self.PREFIX) and attr in self.EXTENSIONS:
            # Return method bound to this instance
            return types.MethodType(self.EXTENSIONS[attr], self, type(self))
        raise AttributeError(attr)

    def docs_iter(self):
        raise NotImplementedError()

    def docs_count(self):
        raise NotImplementedError()

    def docs_handler(self):
        return self

    def docs_cls(self):
        return GenericDoc

    def docs_namespace(self):
        return None # No namespace defined

    def docs_lang(self):
        return None # ISO 639-1 language code, or None for multiple/other/unknown


class BaseQueries:
    PREFIX = 'queries_'
    EXTENSIONS = {}

    def __getattr__(self, attr):
        if attr.startswith(self.PREFIX) and attr in self.EXTENSIONS:
            # Return method bound to this instance
            return types.MethodType(self.EXTENSIONS[attr], self, type(self))
        raise AttributeError(attr)

    def queries_iter(self):
        raise NotImplementedError()

    def queries_handler(self):
        return self

    def queries_cls(self):
        return GenericQuery

    def queries_namespace(self):
        return None # No namespace defined

    def queries_lang(self):
        return None # ISO 639-1 language code, or None for multiple/other/unknown


class BaseQrels:
    PREFIX = 'qrels_'
    EXTENSIONS = {}

    def __getattr__(self, attr):
        if attr.startswith(self.PREFIX) and attr in self.EXTENSIONS:
            # Return method bound to this instance
            return types.MethodType(self.EXTENSIONS[attr], self)
        raise AttributeError(attr)

    def qrels_iter(self):
        raise NotImplementedError()

    def qrels_defs(self):
        raise NotImplementedError()

    def qrels_path(self):
        raise NotImplementedError()

    def qrels_cls(self):
        return GenericQrel

    def qrels_handler(self):
        return self


class BaseScoredDocs:
    PREFIX = 'scoreddocs_'
    EXTENSIONS = {}

    def __getattr__(self, attr):
        if attr.startswith(self.PREFIX) and attr in self.EXTENSIONS:
            # Return method bound to this instance
            return types.MethodType(self.EXTENSIONS[attr], self)
        raise AttributeError(attr)

    def scoreddocs_path(self):
        raise NotImplementedError()

    def scoreddocs_iter(self):
        raise NotImplementedError()

    def scoreddocs_cls(self):
        return GenericScoredDoc

    def scoreddocs_handler(self):
        return self


class BaseDocPairs:
    PREFIX = 'docpairs_'
    EXTENSIONS = {}

    def __getattr__(self, attr):
        if attr.startswith(self.PREFIX) and attr in self.EXTENSIONS:
            # Return method bound to this instance
            return types.MethodType(self.EXTENSIONS[attr], self)
        raise AttributeError(attr)

    def docpairs_path(self):
        raise NotImplementedError()

    def docpairs_iter(self):
        raise NotImplementedError()

    def docpairs_cls(self):
        return GenericDocPair

    def docpairs_handler(self):
        return self


BaseQueries.EXTENSIONS['queries_dict'] = lambda x: {q.query_id: q for q in x.iter_queries()}


def qrels_dict(qrels_handler):
    result = {}
    for qrel in qrels_handler.qrels_iter():
        if qrel.query_id not in result:
            result[qrel.query_id] = {}
        result[qrel.query_id][qrel.doc_id] = qrel.relevance
    return result
BaseQrels.EXTENSIONS['qrels_dict'] = qrels_dict


def hasher(iter_fn, hashfn=hashlib.md5):
    def wrapped(self):
        h = hashfn()
        for record in getattr(self, iter_fn):
            js = [[field, value] for field, value in zip(record._fields, record)]
            h.update(json.dumps(js).encode())
        return h.hexdigest()
    return wrapped


BaseDocs.EXTENSIONS['docs_hash'] = hasher('docs_iter')
BaseQueries.EXTENSIONS['queries_hash'] = hasher('queries_iter')
BaseQrels.EXTENSIONS['qrels_hash'] = hasher('qrels_iter')
BaseScoredDocs.EXTENSIONS['scoreddocs_hash'] = hasher('scoreddocs_iter')
BaseDocPairs.EXTENSIONS['docpairs_hash'] = hasher('docpairs_iter')


class DocstoreBackedDocs(BaseDocs):
    """
    A Docs implementation that defers all operations to a pre-built docstore instance.
    """
    def __init__(self, docstore_lazy, docs_cls=GenericDoc, namespace=None, lang=None):
        self._docstore_lazy = docstore_lazy
        self._docs_cls = docs_cls
        self._docs_namespace = namespace
        self._docs_lang = lang

    def docs_iter(self):
        return iter(self._docstore_lazy())

    def docs_count(self):
        raise self._docstore_lazy().count()

    def docs_cls(self):
        return self._docs_cls

    def docs_namespace(self):
        return self._docs_namespace

    def docs_lang(self):
        return self._docs_lang

    def docs_store(self):
        return self._docstore_lazy()
