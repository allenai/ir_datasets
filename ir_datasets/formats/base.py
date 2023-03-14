import hashlib
import json
import types
import itertools
from typing import NamedTuple
import ir_datasets

_logger = ir_datasets.log.easy()

class GenericDoc(NamedTuple):
    doc_id: str
    text: str
    def default_text(self):
        return self.text

class GenericQuery(NamedTuple):
    query_id: str
    text: str
    def default_text(self):
        return self.text

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
            return types.MethodType(self.EXTENSIONS[attr], self)
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
            return types.MethodType(self.EXTENSIONS[attr], self)
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


class BaseQlogs:
    PREFIX = 'qlogs_'
    EXTENSIONS = {}

    def __getattr__(self, attr):
        if attr.startswith(self.PREFIX) and attr in self.EXTENSIONS:
            # Return method bound to this instance
            return types.MethodType(self.EXTENSIONS[attr], self)
        raise AttributeError(attr)

    def qlogs_iter(self):
        raise NotImplementedError()

    def qlogs_cls(self):
        raise NotImplementedError()

    def qlogs_count(self):
        raise NotImplementedError()

    def qlogs_handler(self):
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
        for record in getattr(self, iter_fn)():
            js = [[field, value] for field, value in zip(record._fields, record)]
            h.update(json.dumps(js).encode())
        return h.hexdigest()
    return wrapped


BaseDocs.EXTENSIONS['docs_hash'] = hasher('docs_iter')
BaseQueries.EXTENSIONS['queries_hash'] = hasher('queries_iter')
BaseQrels.EXTENSIONS['qrels_hash'] = hasher('qrels_iter')
BaseScoredDocs.EXTENSIONS['scoreddocs_hash'] = hasher('scoreddocs_iter')
BaseDocPairs.EXTENSIONS['docpairs_hash'] = hasher('docpairs_iter')


def _calc_metadata(iter_fn, metadata_fields=(), count_by_value_field=None):
    def wrapped(self, verbose=True, hashfn=hashlib.sha256):
        count = 0
        it = getattr(self, iter_fn)()
        if verbose:
            it = _logger.pbar(it)
        field_lens = {f: 0 for f in metadata_fields}
        field_prefixes = {}
        count_by_field_values = {}
        for record in it:
            count += 1
            for f in metadata_fields:
                field = getattr(record, f)
                field_lens[f] = max(field_lens[f], len(field.encode()))
                if f not in field_prefixes:
                    field_prefixes[f] = field
                elif len(field_prefixes[f]) > 0:
                    field_prefixes[f] = ''.join(x[0] for x in itertools.takewhile(lambda x: x[0] == x[1], zip(field_prefixes[f], field)))
            if count_by_value_field is not None:
                count_by_value_field_value = getattr(record, count_by_value_field)
                if count_by_value_field_value not in count_by_field_values:
                    count_by_field_values[count_by_value_field_value] = 0
                count_by_field_values[count_by_value_field_value] += 1
        result = {'count': count}
        if metadata_fields:
            result['fields'] = {}
        for f in metadata_fields:
            result['fields'][f] = {
                'max_len': field_lens[f],
                'common_prefix': field_prefixes[f],
            }
        if count_by_value_field is not None:
            result.setdefault('fields', {}).setdefault(count_by_value_field, {})['counts_by_value'] = count_by_field_values
        return result
    return wrapped


BaseDocs.EXTENSIONS['docs_calc_metadata'] = _calc_metadata('docs_iter', ('doc_id',))
BaseQueries.EXTENSIONS['queries_calc_metadata'] = _calc_metadata('queries_iter')
BaseQrels.EXTENSIONS['qrels_calc_metadata'] = _calc_metadata('qrels_iter', count_by_value_field='relevance')
BaseScoredDocs.EXTENSIONS['scoreddocs_calc_metadata'] = _calc_metadata('scoreddocs_iter')
BaseDocPairs.EXTENSIONS['docpairs_calc_metadata'] = _calc_metadata('docpairs_iter')
BaseQlogs.EXTENSIONS['qlogs_calc_metadata'] = _calc_metadata('qlogs_iter')


class DocstoreBackedDocs(BaseDocs):
    """
    A Docs implementation that defers all operations to a pre-built docstore instance.
    """
    def __init__(self, docstore_lazy, docs_cls=GenericDoc, namespace=None, lang=None):
        self._docstore_lazy = docstore_lazy
        self._loaded_docstore = False
        self._docs_cls = docs_cls
        self._docs_namespace = namespace
        self._docs_lang = lang

    def docs_iter(self):
        return iter(self._docstore_lazy())

    def docs_count(self):
        if self._loaded_docstore and self.docs_store().built():
            return self.docs_store().count()

    def docs_cls(self):
        return self._docs_cls

    def docs_namespace(self):
        return self._docs_namespace

    def docs_lang(self):
        return self._docs_lang

    def docs_store(self):
        result = self._docstore_lazy()
        self._loaded_docstore = True
        return result


class DocSourceSeekableIter:
    def __next__(self) -> NamedTuple:
        """
        Returns the next document encountered
        """
        raise NotImplementedError()

    def seek(self, pos):
        """
        Seeks to the document as `index` pos within the source.
        """
        raise NotImplementedError()

    def close(self):
        """
        Performs any cleanup work when done with this iterator (e.g., close open files)
        """
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return self


class DocSource:
    def __len__(self) -> int:
        """
        Returns the number of documents in this source
        """
        raise NotImplementedError()

    def __iter__(self) -> DocSourceSeekableIter:
        """
        Returns a seekable iterator over this source
        """
        raise NotImplementedError()


class SourceDocIter:
    def __init__(self, docs, slice):
        self.docs = docs
        self.next_index = 0
        self.slice = slice
        self.current_iter = None
        self.current_start_idx = 0
        self.current_end_idx = 0
        self.sources = docs.docs_source_iter()

    def __next__(self):
        if self.slice.start >= self.slice.stop:
            raise StopIteration
        if self.current_iter is None or self.current_end_idx <= self.slice.start:
            # First iteration or no docs remaining in this file
            if self.current_iter is not None:
                self.current_iter.close()
                self.current_iter = None
            # jump ahead to the file that contains the desired index
            first = True
            while first or self.current_end_idx < self.slice.start:
                source = next(self.sources)
                self.next_index = self.current_end_idx
                self.current_start_idx = self.current_end_idx
                self.current_end_idx = self.current_start_idx + len(source)
                first = False
            self.current_iter = iter(source)
        if self.next_index != self.slice.start:
            self.current_iter.seek(self.slice.start - self.current_start_idx)
        result = next(self.current_iter)
        self.next_index += 1
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def close(self):
        if self.current_iter is not None:
            self.current_iter.close()
        self.current_iter = None

    def __iter__(self):
        return self

    def __del__(self):
        self.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return SourceDocIter(self.docs, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = SourceDocIter(self.docs, new_slice)
            result = next(new_it, StopIteration)
            if result is StopIteration:
                raise IndexError((self.slice, slice(key, key+1), new_slice))
            return result
        raise TypeError('key must be int or slice')
