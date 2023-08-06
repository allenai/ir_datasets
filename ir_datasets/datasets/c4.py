import re
import os
import json
import pickle
from pathlib import Path
from typing import NamedTuple, Tuple
import ir_datasets
from ir_datasets.util import DownloadConfig, Download, RequestsDownload, TarExtractAll, GzipExtract
from ir_datasets.formats import BaseDocs, TrecXmlQueries, DocSourceSeekableIter, DocSource, SourceDocIter
from ir_datasets.datasets.base import Dataset, YamlDocumentation
from ir_datasets.indices import Docstore

_logger = ir_datasets.log.easy()

NAME = 'c4'

misinfo_map = {'number': 'query_id', 'query': 'text', 'description': 'description', 'narrative': 'narrative', 'disclaimer': 'disclaimer', 'stance': 'stance', 'evidence': 'evidence'}


class C4Doc(NamedTuple):
    doc_id: str
    text: str
    url: str
    timestamp: str
    def default_text(self):
        """
        text
        """
        return self.text


class MisinfoQuery(NamedTuple):
    query_id: str
    text: str
    description: str
    narrative: str
    disclaimer: str
    stance: str
    evidence: str
    def default_text(self):
        """
        text
        """
        return self.text


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
            chk_file_name = self.dlc.path().split('/')[-1] + '.chk.pkl.lz4'
            with ir_datasets.lazy_libs.lz4_frame().frame.open(os.path.join(self.checkpoint_dlc.path(), chk_file_name)) as f:
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
    def __init__(self, sources_dlc, checkpoint_dlc, base_path, source_name_filter=None, filter_name=''):
        super().__init__()
        self._sources_dlc = sources_dlc
        self._checkpoint_dlc = checkpoint_dlc
        self._sources = None
        self._base_path = Path(base_path)
        self._source_name_filter = source_name_filter
        self._filter_name = filter_name

    def docs_iter(self):
        return SourceDocIter(self, slice(0, self.docs_count(force=True)))

    def docs_cls(self):
        return C4Doc

    def docs_store(self, field='doc_id'):
        assert field == 'doc_id'
        return C4Docstore(self)

    def docs_count(self, force=False):
        if force or self._sources is not None:
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
                    if self._source_name_filter:
                        if not re.match(self._source_name_filter, source['name']):
                            continue
                    cache_path = os.path.join(self._base_path, 'en.noclean', source['url'].split('/')[-1])
                    dlc = Download([RequestsDownload(source['url'])], expected_md5=source['expected_md5'], cache_path=cache_path)
                    sources.append(C4Source(source['name'].replace('.json.gz', ''), dlc, self._checkpoint_dlc, source['doc_count'], source['checkpoint_freq'], source['size_hint'], cache_path))
            self._sources = sources
            build_flag = self._base_path / 'en.noclean' / f'_built{self._filter_name}'
            if not build_flag.exists():
                remaining_size = sum(s.size_hint for s in sources if not os.path.exists(s.cache_path))
                if remaining_size > 0:
                    _logger.info(f'Will start downloading c4/en-noclean files ({ir_datasets.util.format_file_size(remaining_size)}). '
                                 f'If you already have a copy, you may link them to {self._base_path / "en.noclean"} (should contain '
                                 f'files like c4-train.00000-of-07168.json.gz)')
                    ir_datasets.util.check_disk_free(self._base_path / 'en.noclean', remaining_size)
                for source in sources:
                    path = source.dlc.path() # downloads if it doesn't already exist
                    # A quick check that should help make sure it's probably correct if the user downloaded
                    # it themselves. (Not much overhead if downloaded ourselves.)
                    true_size = os.path.getsize(path)
                    if true_size != source.size_hint:
                        raise RuntimeError(f'Expected {path} to be {source.size_hint} bytes but it was actually {true_size} bytes.')
                build_flag.touch()
        return self._sources


def _init():
    subsets = {}
    base_path = ir_datasets.util.home_path()/NAME
    dlc = DownloadConfig.context(NAME, base_path)
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')

    en_noclean_tr_collection = C4Docs(
        GzipExtract(dlc['en-noclean/sources']),
        TarExtractAll(dlc['en-noclean/checkpoints'], base_path / 'en.noclean.checkpoints'),
        base_path, source_name_filter=r'en\.noclean\.c4-train', filter_name='train') # exclude validation files (only include train)
    base = Dataset(documentation('_'))

    subsets['en-noclean-tr'] = Dataset(
        en_noclean_tr_collection,
        documentation('en-noclean-tr'))

    subsets['en-noclean-tr/trec-misinfo-2021'] = Dataset(
        en_noclean_tr_collection,
        TrecXmlQueries(dlc['trec-misinfo-2021/queries'], qtype=MisinfoQuery, qtype_map=misinfo_map, namespace='trec-misinfo', lang='en'),
        documentation('en-noclean-tr/trec-misinfo-2021'))

    ir_datasets.registry.register(NAME, base)
    for subset in subsets:
        ir_datasets.registry.register(f'{NAME}/{subset}', subsets[subset])

    return base, subsets


base, subsets = _init()
