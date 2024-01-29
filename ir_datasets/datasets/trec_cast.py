from abc import ABC, abstractmethod
import array
import contextlib
import gzip
from hashlib import md5
import os
from functools import cached_property, lru_cache
from collections import defaultdict
import re
import json
import itertools
from typing import Iterator, List, NamedTuple, Optional, Sequence, Set, Tuple, Union
import ir_datasets
from ir_datasets.indices.base import Docstore
from ir_datasets.util import BaseDownload, DownloadConfig, Lazy
from ir_datasets.formats import TrecQrels, TrecScoredDocs, BaseDocs, BaseQueries, GenericDoc
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs
from ir_datasets.indices import PickleLz4FullStore
import numpy as np

# --- Generic classes TODO: move elsewhere



class PrefixedDocstore(Docstore):
    def __init__(self, docs_mapping: List[Tuple[str, BaseDocs]], id_field='doc_id'):
        self._id_field = id_field
        self._stores = [
            (mapping[0], len(mapping[0]), mapping[1].docs_store(id_field=id_field)) for mapping in docs_mapping
        ]
    
    def get_many(self, doc_ids: Sequence[str], field=None):
        assert field is None

        result = {}
        if field is None or field == self._id_field:
            # If field is ID field, remove the prefix
            for prefix, ix, store in self._stores:
                doc_ids = [doc_id[ix:] for doc_id in doc_ids if doc_id.startswith(prefix)]
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
        assert all(mapping[1].docs_cls() == _docs_cls for mapping in self._docs_mapping[1:])
        return _docs_cls

    @cached_property
    def _docs_lang(self):
        _docs_lang = self._docs_mapping[0][1].docs_lang()
        if any(mapping[1].docs_lang() == self._docs_lang for mapping in self._docs_mapping[1:]):
            return None
        return _docs_lang

    @cached_property
    def _docs_namespace(self):
        _docs_namespace = self._docs_mapping[0][1].docs_namespace()
        if any(mapping[1].docs_namespace() == self._docs_namespace for mapping in self._docs_mapping[1:]):
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
    def docs_store(self, field='doc_id'):
        return PrefixedDocstore(self._docs_mapping, field=field)


class DocsListView:
    def __init__(self, docs: "DocsList", slice: slice):
        self._docs = docs
        self._indices = [c for c in slice]

    def __getitem__(self, slice: Union[int, slice]):
        if isinstance(slice, int):
            return self._docs.get(slice)

        return DocsListView(self, self._docs, slice[slice])


class DocsList(ABC):
    """A document list"""
    @abstractmethod
    def get(self, ix: int):
        ...

    @abstractmethod
    def __len__(self):
        ...

    def __getitem__(self, slice: Union[int, slice]):
        if isinstance(slice, int):
            return self.get(slice)

        return DocsListView(self, slice)


class DirectAccessDocs:
    def docs_list(self) -> DocsList:
        return DocsList


class LazyDocsIter:
    """Iterate over documents unless a specific range is queried"""
    def __init__(self, docs: DirectAccessDocs, iter):
        self.docs = docs
        self._iter = iter

    def __getitem__(self, slice: Union[int, slice]):
        return self.docs_list()[slice]

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iter)


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


class DocsSubset(BaseDocs, DirectAccessDocs):
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
            indices_path = f'{ir_datasets.util.home_path()}/{self._store_name}.intarray'
            indices = array.array('L')
            if not os.path.exists(indices_path):
                for ix, doc in enumerate(_logger.pbar(iter(self.docs_iter()), total=self.docs_count(), desc='identifying removed documents')):
                    if self._removed_ids.has(doc.doc_id):
                        indices.append(indices)
                with ir_datasets.util.finialized_file(indices_path, 'wb') as fout:
                    fout.write(indices.tobytes())
                return indices
            else:
                with indices_path.open('rb') as fin:
                    indices.frombytes(fin)
                return indices

        return DocsSubsetList(self._docs.docs_iter(), indices)

    def docs_cls(self):
        return self._docs.docs_cls()

    def docs_lang(self):
        return self._docs.docs_lang()

    def docs_count(self):
        return self._docs.docs_count() - len(self._removed_ids)

    def docs_iter(self):
        return LazyDocsIter(self, (doc for doc in self._docs.docs_iter() if not self._removed_ids.has(doc.doc_id)))

    def docs_namespace(self):
        return self._docs.docs_namespace()

    def docs_store(self, field='doc_id'):
        return self._docs.docs_store(field=field)


class BaseLazyDocs(BaseDocs):
    def __init__(self, ds_id: str):
        self._ds_id = ds_id

    @cached_property
    def docs(self):
        return ir_datasets.load(self._ds_id)

    def docs_cls(self):
        return self.docs.docs_cls()

    def docs_lang(self):
        return self.docs.docs_lang()

    def docs_count(self):
        return self.docs.docs_count()

    def docs_iter(self):
        return self.docs.docs_iter()


class LazyDocs(BaseLazyDocs):
    def docs_store(self, id_field='doc_id'):
        return self.docs.docs_store(id_field=id_field)
                

class BaseTransformedDocs(BaseDocs):
    def __init__(self, docs: BaseDocs, cls, store_name, count=None):
        """Document collection tranformed using a transform function

        :param docs: The base documents
        :param store_name: The name of the LZ4 document store
        """
        self._docs = docs
        self._cls = cls
        self._store_name = store_name
        self._count_hint = count
   
    def docs_cls(self):
        return self._cls
    
    def docs_lang(self):
        return self._docs.docs_lang()

    def docs_count(self):
        return self._count_hint or self._docs.docs_count()
    
    @lru_cache
    def docs_store(self, id_field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()/NAME}/{self._store_name}.pklz4',
            init_iter_fn=self.docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=id_field,
            index_fields=[id_field],
            count_hint=self._count_hint,
        )


class TransformedDocs(BaseTransformedDocs):
    def __init__(self, docs: BaseDocs, cls, transform=None, store_name=None, count=None):
        """Document collection tranformed using a transform function

        :param docs: The base documents
        :param transform: The transformation function
        :param store_name: if set, creates a LZ4 document store, otherwise
            transform on the fly, defaults to None
        """
        super().__init__(docs, cls, store_name, count=count)
        self._transform = transform or self

    @lru_cache
    def docs_store(self, id_field='doc_id'):
        if self._store_name is None:
            return TransformedDocstore(self._docs.docs_store(id_field), self._transform)
        return super().docs_store()
    
    def docs_iter(self):
        for doc in map(self._transform, self._docs.docs_iter()):
            if doc is not None:
                yield doc


class TransformedDocstore(Docstore):
    """On the fly transform of documents"""
    def __init__(self, store, transform):
        self._store = store
        self._transform = transform
        
    def get_many(self, doc_ids, field=None):
        return {key: self._transform(doc) for key, doc in self._store.get_many(doc_ids, field)}


class IterDocs(BaseDocs):
    """Documents based on an iterator"""
    def __init__(self,
        corpus_name, 
        docs_iter_fn: Iterator,
        docs_lang='en',
        docs_cls=GenericDoc, 
        count_hint=None,
    ):
        super().__init__()
        self._corpus_name = corpus_name
        self._docs_iter_fn = docs_iter_fn
        self._docs_cls = docs_cls
        self._count_hint = count_hint
        self._docs_lang = docs_lang

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()
        return self._count_hint

    def docs_iter(self):
        def iter():
            with _logger.duration(f'processing {self._corpus_name}'):
                yield from (d for d in self._docs_iter_fn())

        return LazyDocsIter(self, iter)
    
    def docs_list(self):
        return self.docs_store()

    def docs_cls(self):
        return self._docs_cls

    def docs_store(self, field='doc_id'):
        return PickleLz4FullStore(
            path=f'{ir_datasets.util.home_path()}/{self._corpus_name}.pklz4',
            init_iter_fn=self._docs_iter,
            data_cls=self.docs_cls(),
            lookup_field=field,
            index_fields=[field],
            count_hint=self._count_hint,
        )

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return self._docs_lang



# --- (end of generic classes)

_logger = ir_datasets.log.easy()


NAME = 'trec-cast'

QRELS_DEFS = {
    4: "Fully meets. The passage is a perfect answer for the turn. It includes all of the information needed to fully answer the turn in the conversation context. It focuses only on the subject and contains little extra information.",
    3: "Highly meets. The passage answers the question and is focused on the turn. It would be a satisfactory answer if Google Assistant or Alexa returned this passage in response to the query. It may contain limited extraneous information.",
    2: "Moderately meets. The passage answers the turn, but is focused on other information that is unrelated to the question. The passage may contain the answer, but users will need extra effort to pick the correct portion. The passage may be relevant, but it may only partially answer the turn, missing a small aspect of the context.",
    1: "Slightly meets. The passage includes some information about the turn, but does not directly answer it. Users will find some useful information in the passage that may lead to the correct answer, perhaps after additional rounds of conversation (better than nothing).",
    0: "Fails to meet. The passage is not relevant to the question. The passage is unrelated to the target query.",
}

QRELS_DEFS_TRAIN = {
    2: "very relevant",
    1: "relevant",
    0: "not relevant",
}


class CastPassage(NamedTuple):
    passage_id: str
    text: str
    marked_up_text: str


class CastDoc(NamedTuple):
    doc_id: str
    title: str
    url: str
    passages: Tuple[CastPassage, ...]
    def default_text(self):
        """
        Combines the title and text of constituent passages.
        """
        return "\n".join([self.title] + [p.text for p in self.passages])


class CastPassageDoc(NamedTuple):
    doc_id: str
    title: str
    url: str
    text: str
    def default_text(self):
        """
        Combines the title from the source document with the text of this passage.
        """
        return f"{self.title}\n{self.text}"

class Cast2019Query(NamedTuple):
    query_id: str
    raw_utterance: str
    topic_number: int
    turn_number: int
    topic_title: str
    topic_description: str
    def default_text(self):
        """
        raw_utterance
        """
        return self.raw_utterance


class Cast2020Query(NamedTuple):
    query_id: str
    raw_utterance: str
    automatic_rewritten_utterance: str
    manual_rewritten_utterance: str
    manual_canonical_result_id: str
    topic_number: int
    turn_number: int
    def default_text(self):
        """
        raw_utterance
        """
        return self.raw_utterance


class Cast2021Query(NamedTuple):
    query_id: str
    raw_utterance: str
    automatic_rewritten_utterance: str
    manual_rewritten_utterance: str
    canonical_result_id: str
    topic_number: int
    turn_number: int
    def default_text(self):
        """
        raw_utterance
        """
        return self.raw_utterance


class Cast2022Query(NamedTuple):
    query_id: str
    parent_id: Optional[str]
    participant: str
    raw_utterance: str
    manual_rewritten_utterance: str
    response: str
    provenance: List[str]
    topic_number: int
    turn_number: int
    
    def default_text(self):
        """
        raw_utterance
        """
        return self.raw_utterance

class CastPassageIter:
    def __init__(self, docstore, doc_psg_offsets, slice):
        self.next_psg_index = 0
        self.docstore = docstore
        self.doc_iter = iter(docstore)
        self.doc = None
        self.slice = slice
        if self.slice.start != 0:
            start_doc_idx = int(np.searchsorted(doc_psg_offsets(), self.slice.start, side='right')) - 1
            self.doc_iter = self.doc_iter[start_doc_idx:]
            self.next_psg_index = self.slice.start - doc_psg_offsets()[start_doc_idx]
        self.doc_psg_offsets = doc_psg_offsets

    def __next__(self):
        if self.slice.start >= self.slice.stop or self.doc is StopIteration:
            raise StopIteration
        if self.doc is None:
            self.doc = next(self.doc_iter, StopIteration)
        while self.next_psg_index >= len(self.doc.passages):
            self.next_psg_index -= len(self.doc.passages)
            self.doc = next(self.doc_iter, StopIteration)
            if self.doc is StopIteration:
                raise StopIteration
        result = self.doc.passages[self.next_psg_index]
        result = CastPassageDoc(result.passage_id, self.doc.title, self.doc.url, result.text)
        self.next_psg_index += (self.slice.step or 1)
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def __iter__(self):
        return self

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return CastPassageIter(self.docstore, self.doc_psg_offsets, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = CastPassageIter(self.docstore, self.doc_psg_offsets, new_slice)
            try:
                return next(new_it)
            except StopIteration as e:
                raise IndexError(e)
        raise TypeError('key must be int or slice')


class CastPassageDocstore(ir_datasets.indices.Docstore):
    def __init__(self, docs_docstore):
        super().__init__(GenericDoc, 'doc_id')
        self._docs_docstore = docs_docstore

    def get_many_iter(self, doc_ids):
        passage_ids = list(doc_ids)
        did2pids = defaultdict(set)
        for pid in passage_ids:
            if pid.count('-') >= 1:
                did, idx = pid.rsplit('-', 1)
                if idx.isnumeric():
                    did2pids[did].add(int(idx)-1)
        for doc in self._docs_docstore.get_many_iter(did2pids.keys()):
            for idx in did2pids[doc.doc_id]:
                if len(doc.passages) > idx:
                    passage = doc.passages[idx]
                    yield CastPassageDoc(passage.passage_id, doc.title, doc.url, passage.text)


class LazyCastPassageIter:
    def __init__(self, docs: "CastPassageDocs"):
        self._docs = docs
        self._doc_iter = docs._docs.docs_iter()
        self._doc = None
        self._passage_ix = None
    
    def __iter__(self):
        return self
    
    def __next__(self):
        while (self._doc is None) or (len(self._doc.passages) <= self._passage_ix):
            self._doc = next(self._doc_iter)
            self._passage_ix = 0

        self._passage_ix += 1
        
        return CastPassageDoc(f"{self._doc.doc_id}-{self._passage_ix+1}", self._doc.title, self._doc.url,
            self._doc.passages[self._passage_ix-1]    
        )

    def __getitem__(self, key):
        docstore = self._docs._docs.docs_store()
        
        @lru_cache()
        def offsets_fn():
            """Stores the number of passages for each document of the initial
            collection"""
            offsets_path = f'{str(docstore.path)}.psg_offsets.np'
            if not os.path.exists(offsets_path):
                offsets = np.empty(docstore.count()+1, dtype=np.uint32)
                count = 0
                for i, doc in enumerate(_logger.pbar(iter(docstore), total=docstore.count(), desc='building passage offset file')):
                    offsets[i] = count
                    count += len(doc.passages)
                offsets[-1] = count
                with ir_datasets.util.finialized_file(offsets_path, 'wb') as fout:
                    fout.write(offsets.tobytes())
                return offsets
            else:
                return np.memmap(offsets_path, dtype=np.uint32, mode='r')
        return CastPassageIter(docstore, offsets_fn, slice(0, self._docs._count, 1))


class CastPassageDocs(BaseDocs):
    def __init__(self, docs, count):
        super().__init__()
        self._docs = docs
        self._count = count

    def docs_iter(self):
        return LazyCastPassageIter(self)

    def docs_cls(self):
        return CastPassageDoc

    def docs_store(self, field='doc_id'):
        return CastPassageDocstore(self._docs.docs_store(field))

    def docs_count(self):
        return self._count

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'


class SegmentedDocs(BaseTransformedDocs):
    """Segmented document collection based on pre-computed offsets

    segments_dl points to a compressed JSONL file where the ranges refer to the
    original document text, e.g.:
    
    {"id":"MARCO_00_1454834","ranges":[[[0,917]],[[918,2082]],[[2083,3220]],[[3221,3763]]],"md5":"f0577db28de265852932224525710486"}
    """

    def __init__(self, docs, segments_dl: BaseDownload, store_name: str):
        super().__init__(docs, CastDoc, store_name)
        self._segments_dl = segments_dl
        
    def docs_iter(self):
        # Process files
        with self._segments_dl.stream() as fin, gzip.open(fin) as offsets_stream:
            for doc, data_json in zip(self._docs, offsets_stream):
                data = json.loads(data_json)
                assert doc.doc_id == data["id"], f"Error in processing offsets, docids differ: expected {data['id']}, got {doc.doc_id}"
                body: str = doc.passages[0]

                computer = md5()
                passages = []
                for ranges in data["ranges"]:
                    texts = []
                    computer.update(b"\x00")
                    for start, end in ranges:
                        computer.update(b"\x01")
                        text = body[start:end]
                        texts.append(text)
                        computer.update(text.encode("utf-8"))
                    passages.append(" ".join(texts))
                
                assert computer.digest().hex() == data["md5"]

                yield doc._replace(passages=passages)


class CastQueries(BaseQueries):
    def __init__(self, dlc, query_type):
        super().__init__()
        self._dlc = dlc
        self._query_type = query_type

    def queries_iter(self):
        with self._dlc.stream() as stream:
            topics = json.load(stream)
            
            for topic in topics:
                topic_number = topic['number']
                for turn in topic['turn']:
                    turn_number = turn['number']
                    if self._query_type is Cast2019Query:
                        yield Cast2019Query(f'{topic_number}_{turn_number}', turn['raw_utterance'], topic_number, turn_number, topic['title'], topic.get('description', ''))
                    elif self._query_type is Cast2020Query:
                        yield Cast2020Query(f'{topic_number}_{turn_number}', turn['raw_utterance'], turn['automatic_rewritten_utterance'], turn['manual_rewritten_utterance'], turn['manual_canonical_result_id'], topic_number, turn_number)
                    elif self._query_type is Cast2021Query:
                        yield Cast2021Query(f'{topic_number}_{turn_number}', turn['raw_utterance'], turn['automatic_rewritten_utterance'], turn['manual_rewritten_utterance'], turn['canonical_result_id'], topic_number, turn_number)
                    elif self._query_type is Cast2022Query:
                        if parent_id := turn.get("parent"):
                            parent_id = f"{parent_id}_{turn_number}"
                        yield Cast2022Query(f'{topic_number}_{turn_number}', parent_id, turn["participant"], turn.get('utterance', None), turn.get('manual_rewritten_utterance', None), turn.get('response', None), turn.get('provenance', []), topic_number, turn_number)

    def queries_cls(self):
        return self._query_type

    def queries_namespace(self):
        return NAME

    def queries_lang(self):
        return 'en'


class WapoV4Docs(BaseLazyDocs):
    def __init__(self, dsid: str):
        super().__init__(dsid)

    def docs_cls(self):
        return CastDoc

    def docs_iter(self):
        CLEANR = re.compile('<.*?>')
        dup_dids = set()
        for data in self.docs.docs_handler().docs_wapo_raw_iter():
            if data["id"] in dup_dids:
                continue
            dup_dids.add(data["id"])

            doc_id = str(data['id'])

            title = data.get('title', 'No Title')

            if data["article_url"]:
                if "www.washingtonpost.com" not in data["article_url"]:
                    url = "https://www.washingtonpost.com" + data['article_url']
                else:
                    url = data['article_url']
            else:
                url = ''

            body = ''
            if data.get('contents') and len(data['contents']) > 0:
                for item in data['contents']:
                    # if item is not None and item.get('subtype') == 'paragraph':
                    if item is not None and item.get('subtype') == 'paragraph':
                        body += ' ' + item['content']
            body = re.sub(CLEANR, '', body)
            body = body.replace('\n', ' ').strip()
            if body:
                yield CastDoc(doc_id, title, url, [body])


class KiltCastDocs(TransformedDocs):
    def __init__(self, dsid: str):
        super().__init__(LazyDocs(dsid), CastDoc)
        
    def docs_iter(self):
        for doc in map(self.transform, self._docs.docs.docs_handler().docs_kilt_raw_iter()):
            if doc is not None:
                yield doc

    def transform(self, doc):
        title = doc['wikipedia_title']
        body = ' '.join(doc['text']).replace('\n', ' ').strip()
        url = doc['history']['url']
        return CastDoc(doc["wikipedia_id"], title, url, [body])


class Dupes:
    def __init__(self, base: BaseDownload, prefix: Optional[str]=None):
        self._base = base
        self._prefix = prefix
        self._prefix_len = len(prefix) if prefix else 0
        self._remove_prefix = self.remove_prefix if prefix else lambda x: x

    def remove_prefix(self, doc_id: str):
        if doc_id.startswith(self._prefix):
            return doc_id[self._prefix_len:]

    @cached_property
    def doc_ids(self):
        doc_ids = set()
        with self._base.stream() as fp:
            for line in fp:
                if doc_id := self._remove_prefix(line.strip().decode("utf-8")):
                    doc_ids.append(doc_id)
        return doc_ids

    def has(self, doc_id: str):
        return doc_id in self.doc_ids


class WapoDupes(Dupes):
    @cached_property
    def doc_ids(self):
        doc_ids = set()
        with self._base.stream() as fp:
            for line in fp:
                base_id, wapo_id, *__  = line.strip().split(b" ", 3)
                if base_id != wapo_id:
                    if doc_id := self._remove_prefix(wapo_id.decode("utf-8")):
                        doc_ids.add(doc_id)
        return doc_ids


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


def transform_msmarco_v1(doc):
    return CastDoc(doc.doc_id, doc.title, doc.url, [doc.body.replace('\n', ' ').strip()])


def transform_msmarco_v2(doc):
    doc_id = doc.doc_id[len('msmarco_doc_'):]
    return CastDoc(doc_id, doc.title, doc.url, [doc.body.replace('\n', ' ').strip()])


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    def wapo_converter(dsid, dupes: Dupes):
        def wrapped():
            BeautifulSoup = ir_datasets.lazy_libs.bs4().BeautifulSoup
            # NOTE: These rules are very specific in order to replicate the behaviour present in the official script
            # here: <https://github.com/grill-lab/trec-cast-tools/blob/8fa243a7e058ce4b1b378c99768c53546460c0fe/src/main/python/wapo_trecweb.py>
            # Specifically, things like skipping empty documents, filtering by "paragraph" subtype, and starting the
            # paragraph index at 1 are all needed to perfectly match the above script.
            # Note that the script does NOT strip HTML markup, which is meant to be removed out in a later stage (e.g., indexing).
            # We do that here for user simplicity, as it will allow the text to be consumed directly by various models
            # without the need for further pre-processing. (Though a bit of information is lost.)
            for wapo_doc in ir_datasets.load(dsid).docs_handler().docs_wapo_raw_iter():
                doc_id = wapo_doc['id']
                
                # Ignore this one
                if dupes.has(doc_id):
                    continue
                
                pid = itertools.count(1) # paragrah index starts at 1
                for paragraph in wapo_doc['contents']:
                    if paragraph is not None and paragraph.get('subtype') == 'paragraph' and paragraph['content'] != '':
                        text = paragraph['content']
                        if paragraph.get('mime') == 'text/html':
                            text = BeautifulSoup(f'<OUTER>{text}</OUTER>', 'lxml-xml').get_text()
                        yield GenericDoc(f'WAPO_{doc_id}-{next(pid)}', text)
        return wrapped


    # --- Version 0 and 1 (2019 and 2020)
    # https://github.com/daltonj/treccastweb#year-2-trec-2020
    # documents = MARCO Ranking passages (v1) and Wikipedia (TREC CAR)
    # Version 0 contains WAPO (but this is not used)
    
    docs_v0 = PrefixedDocs(
        ('WAPO_', IterDocs(f"{NAME}/v1/wapo-v2", wapo_converter('wapo/v2', ColonCommaDupes(dlc['wapo_dupes'], prefix='WAPO_')))),
        ('MARCO_', DocsSubset(f"{NAME}/v1/msmarco-passages", LazyDocs("msmarco-passage"), ColonCommaDupes(dlc['marco_dupes'], prefix='MARCO_'))),
        ('CAR_', LazyDocs("car/v2.0")),
    )

    docs_v1 = PrefixedDocs(
        ('MARCO_', DocsSubset(f"{NAME}/v1/msmarco-passages", LazyDocs("msmarco-passage"), ColonCommaDupes(dlc['marco_dupes'], prefix='MARCO_'))),
        ('CAR_', LazyDocs("car/v2.0")),
    )

    base = Dataset(documentation('_'))

    subsets['v0'] = Dataset(docs_v0)

    subsets['v0/train'] = Dataset(
        docs_v0,
        CastQueries(dlc['2019/train/queries'], Cast2019Query),
        TrecQrels(dlc['2019/train/qrels'], QRELS_DEFS_TRAIN),
        TrecScoredDocs(dlc['2019/train/scoreddocs'])
    )
    qids_train_v0 = Lazy(lambda: {q.query_id for q in subsets['v0/train'].qrels_iter()})
    subsets['v0/train/judged'] = Dataset(
        docs_v0,
        FilteredQueries(subsets['v0/train'].queries_handler(), qids_train_v0),
        subsets['v0/train'].qrels_handler(),
        FilteredScoredDocs(subsets['v0/train'].scoreddocs_handler(), qids_train_v0),
    )

    subsets['v1'] = Dataset(docs_v1)

    subsets['v1/2019'] = Dataset(
        docs_v1,
        CastQueries(dlc['2019/eval/queries'], Cast2019Query),
        TrecQrels(dlc['2019/eval/qrels'], QRELS_DEFS),
        TrecScoredDocs(dlc['2019/eval/scoreddocs'])
    )
    qids_2019 = Lazy(lambda: {q.query_id for q in subsets['v1/2019'].qrels_iter()})
    subsets['v1/2019/judged'] = Dataset(
        docs_v1,
        FilteredQueries(subsets['v1/2019'].queries_handler(), qids_2019),
        subsets['v1/2019'].qrels_handler(),
        FilteredScoredDocs(subsets['v1/2019'].scoreddocs_handler(), qids_2019),
    )

    subsets['v1/2020'] = Dataset(
        docs_v1,
        CastQueries(dlc['2020/queries'], Cast2020Query),
        TrecQrels(dlc['2020/qrels'], QRELS_DEFS),
    )
    qids_2020 = Lazy(lambda: {q.query_id for q in subsets['v1/2020'].qrels_iter()})
    subsets['v1/2020/judged'] = Dataset(
        docs_v1,
        FilteredQueries(subsets['v1/2020'].queries_handler(), qids_2020),
        subsets['v1/2020'].qrels_handler(),
    )

    # --- Version 2 (2021)
    # https://github.com/daltonj/treccastweb#year-3-trec-2021
    # Documents = WAPO 2020, KILT and MS Marco v1 (documents)
    # We provide passage offsets for the three document collections

    # Duplicates are in two files:
    # wapo-near-duplicates for WAPO
    # marco_duplicates.txt for MS-MARCO

    def register_docs(namespace: str, *tuples):
        all_docs_spec = []
        for dsid, prefix, raw, count in tuples:
            all_docs_spec.append((prefix, raw))
            prefixed = PrefixedDocs((prefix, raw))
            subsets[f"{namespace}/{dsid}"] = Dataset(prefixed)
            segmented = SegmentedDocs(prefixed, dlc[f"{namespace}/offsets/{dsid}"], f"docs_{namespace}_{dsid}")
            subsets[f"{namespace}/{dsid}/segmented"] = Dataset(segmented)
            subsets[f'{namespace}/{dsid}/passages'] = Dataset(CastPassageDocs(segmented, count))
            
        all_docs = PrefixedDocs(*all_docs_spec)
        subsets[f"{namespace}"] = all_docs
        return all_docs

    docs_v2 = register_docs(
        "v2",
        (
            "msmarco",
            "MARCO_",
            TransformedDocs(
                DocsSubset(f"{NAME}/v2/msmarco-documents", LazyDocs("msmarco-document"), ColonCommaDupes(dlc['v2/dupes/marco_v1'])),
                CastDoc,
                transform_msmarco_v1
            ),
            999999,
        ),
        (
            "wapo",
            "WAPO_",
            DocsSubset(
                f"{NAME}/v2/wapo-v4",
                WapoV4Docs("wapo/v4"),
                WapoDupes(dlc['v2/dupes/wapo'])
            ),
            3728553
        ),
        (
            "kilt",
            "KILT_",
            KiltCastDocs("kilt"),
            99999999
        )
    )

    subsets['v2/2021'] = Dataset(
        docs_v2,
        CastQueries(dlc['2021/queries'], Cast2021Query),
        TrecQrels(dlc['2021/qrels'], QRELS_DEFS),
    )

    # --- Version 3 (2022)
    # https://github.com/daltonj/treccastweb#year-4-trec-2022
    # Official documents = processed (split) WAPO 2020, KILT, MS Marco V2

    v3_dupes = dlc['v3/dupes']
    docs_v3 = register_docs(
        "v3",
        (
            "msmarco",
            "MARCO_",
            DocsSubset(
                f"{NAME}/v3/msmarco-documents-v2",
                TransformedDocs(LazyDocs("msmarco-document-v2"), CastDoc, transform_msmarco_v2),
                Dupes(v3_dupes, prefix="MARCO_")
            ),
            999999,
        ),
        (
            "wapo",
            "WAPO_",
            DocsSubset(
                f"{NAME}/v3/wapo-v4",
                WapoV4Docs("wapo/v4"),
                Dupes(v3_dupes, prefix="WAPO_")
            ),
            3728553
        ),
        (
            "kilt",
            "KILT_",
            DocsSubset(
                f"{NAME}/v3/kilt-v4",
                KiltCastDocs("kilt"),
                Dupes(v3_dupes, prefix="KILT_")
            ),
            99999999
        )
    )
    
    subsets['v3/2022'] = Dataset(
        docs_v3,
        CastQueries(dlc['2022/queries'], Cast2022Query),
        TrecQrels(dlc['2022/qrels'], QRELS_DEFS),
    )
    
    # --- Register all datasets
    ir_datasets.registry.register(NAME, base)
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return base, subsets


base, subsets = _init()
