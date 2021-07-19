import re
import os
import json
import pickle
from pathlib import Path
from typing import NamedTuple, Tuple
import ir_datasets
from ir_datasets.util import DownloadConfig, Download, RequestsDownload
from ir_datasets.formats import BaseDocs
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import Docstore


NAME = 'c4'


class C4Doc(NamedTuple):
    doc_id: str
    text: str
    url: str
    timestamp: str



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


class C4Source(DocSource):
    def __init__(self, name, dlc, checkpoint_dlc, doc_count, checkpoint_freq, size_hint, cache_path):
        self.name = name # e.g., en.noclean.c4-train.01234-of-07168
        self.dlc = dlc
        self.checkpoint_dlc = checkpoint_dlc
        self.doc_count = doc_count
        self.checkpoint_freq = checkpoint_freq
        self._checkpoints = None
        self.size_hint = size_hint
        self.cache_path = cache_path

    def __len__(self):
        return self.doc_count

    def __iter__(self):
        return C4SourceIter(self)

    def checkpoints(self):
        if self._checkpoints is None:
            with ir_datasets.lazy_libs.lz4_frame().frame.open(f'{self.checkpoint_dlc.path()}/{self.name}.chk.pkl.lz4') as f:
                self._checkpoints = pickle.load(f)
        return self._checkpoints


class C4SourceIter(DocSourceSeekableIter):
    def __init__(self, source):
        self.source = source
        self.idx = 0
        self.source_f = ir_datasets.lazy_libs.zlib_state().GzipStateFile(self.source.dlc.path())

    def close(self):
        if self.source_f is not None:
            self.source_f.close()
            self.source_f = None

    def __next__(self):
        line = self.source_f.readline()
        if not line:
            raise StopIteration()
        data = json.loads(line)
        doc_id = f'{self.source.name}.{self.idx}'
        self.idx += 1
        return C4Doc(doc_id, data['text'], data['url'], data['timestamp'])

    def seek(self, idx):
        if (idx < self.idx) or \
           (idx // self.source.checkpoint_freq != self.idx // self.source.checkpoint_freq) and \
           (idx - self.idx > 100):
            # either we're going backward in the file or the index is in a different
            # checkpoint than we're at now, so we can jump ahead.
            # (or we're not jumping very far ahead (<100 documents), so don't bother
            # loading checkpoints, e.g., this is a case where step is used when iterating
            # over the documents.)
            target_checkpoint = idx // self.source.checkpoint_freq
            checkpoints = self.source.checkpoints()
            effective_checkpoint = min(target_checkpoint, len(checkpoints) - 1)
            pos, state, offset = checkpoints[effective_checkpoint]
            self.source_f.zseek(pos, state)
            self.source_f.read(offset)
            self.idx = effective_checkpoint * self.source.checkpoint_freq
        while idx > self.idx:
            # read the file in sequence 'till we get to the desired index
            self.source_f.readline()
            self.idx += 1


class C4Docstore(Docstore):
    def __init__(self, docs):
        super().__init__(docs.docs_cls(), 'doc_id')
        self.docs = docs

    def get_many_iter(self, doc_ids):
        files_to_search = {}
        for doc_id in doc_ids:
            match = re.match(r'^en.noclean.c4-train.(\d+)-of-07168.(\d+)$', doc_id)
            if not match:
                continue
            file_idx, doc_idx = match.groups()
            file_idx, doc_idx = int(file_idx), int(doc_idx)
            if file_idx not in files_to_search:
                files_to_search[file_idx] = []
            files_to_search[file_idx].append(doc_idx)
        sources = self.docs._docs_sources()
        for file_idx, doc_idxs in files_to_search.items():
            if file_idx >= len(sources):
                continue
            source = sources[file_idx]
            doc_idxs = sorted(doc_idxs)
            with iter(source) as it:
                for doc_idx in doc_idxs:
                    it.seek(doc_idx)
                    res = next(it, StopIteration)
                    if res is not StopIteration:
                        yield res


class C4Docs(BaseDocs):
    def __init__(self, sources_dlc, checkpoint_dlc, base_path):
        super().__init__()
        self._sources_dlc = sources_dlc
        self._checkpoint_dlc = checkpoint_dlc
        self._sources = None
        self._base_path = Path(base_path)

    def docs_iter(self):
        return SourceDocIter(self, slice(0, self.docs_count()))

    def docs_cls(self):
        return C4Doc

    def docs_store(self, field='doc_id'):
        assert field == 'doc_id'
        return C4Docstore(self)

    def docs_count(self):
        return sum(s.doc_count for s in self._docs_sources())

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'

    def docs_source_iter(self):
        return iter(self._docs_sources())

    def _docs_sources(self):
        if self._sources is None:
            sources = []
            with self._sources_dlc.stream() as stream:
                for source in json.load(stream):
                    cache_path = os.path.join(self._base_path, 'en.noclean', source['url'].split('/')[-1])
                    dlc = Download([RequestsDownload(source['url'])], expected_md5=source['expected_md5'], cache_path=cache_path)
                    sources.append(C4Source(source['name'], dlc, self._checkpoint_dlc, source['doc_count'], source['checkpoint_freq'], source['size_hint'], cache_path))
            self._sources = sources
            if not (self._base_path / 'en.noclean' / '_built').exists():
                remaining_size = sum(s.size_hint for s in sources if not os.path.exists(s.cache_path))
                if remaining_size > 0:
                    _logger.info(f'Will start downloading c4/en-noclean files ({ir_datasets.util.format_file_size(remaining_size)}). '
                                 f'If you already have a copy, you may link them to {self._base_path / 'en.noclean'} (should contain '
                                 f'files like c4-train.00000-of-07168.json.gz)'):
                    ir_datasets.util.check_disk_free(self._base_path / 'en.noclean', remaining_size)
                for source in sources:
                    path = source.path() # downloads if it doesn't already exist
                    # A quick check that should help make sure it's probably correct if the user downloaded
                    # it themselves. (Not much overhead if downloaded ourselves.)
                    true_size = os.path.getsize(path)
                    if true_size != source.size_hint:
                        raise RuntimeError(f'Expected {path} to be {source.size_hint} B but it was actually {true_size} B.')
                (self._base_path / 'en.noclean' / '_built').touch()
        return self._sources


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


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    en_noclean_collection = C4Docs(
        dlc['en-noclean/sources'],
        TarExtractAll(dlc['en-noclean/checkpoints'], base_path / 'en.noclean.checkpoints'),
        base_path)
    base = Dataset(documentation('_'))

    subsets['en-noclean'] = Dataset(
        en_noclean_collection,
        documentation('en-noclean'))

    ir_datasets.registry.register(NAME, base)
    for subset in subsets:
        ir_datasets.registry.register(f'{NAME}/{subset}', subsets[subset])

    return base, subsets


base, subsets = _init()
