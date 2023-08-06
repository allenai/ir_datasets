import re
import os
import contextlib
import gzip
import io
from pathlib import Path
import json
from typing import NamedTuple, Tuple
import tarfile
import ir_datasets
from ir_datasets.indices import PickleLz4FullStore
from ir_datasets.util import Cache, DownloadConfig, GzipExtract, Lazy, Migrator, TarExtractAll
from ir_datasets.datasets.base import Dataset, YamlDocumentation, FilteredQueries, FilteredScoredDocs, FilteredQrels
from ir_datasets.formats import TsvQueries, TrecQrels, TrecScoredDocs, BaseDocs
from ir_datasets.datasets.msmarco_passage import DUA, DL_HARD_QIDS_BYFOLD, DL_HARD_QIDS, TREC_DL_QRELS_DEFS

QRELS_DEFS = {
    1: 'Based on mapping from v1 of MS MARCO'
}

_logger = ir_datasets.log.easy()

NAME = 'msmarco-passage-v2'


class MsMarcoV2Passage(NamedTuple):
    doc_id: str
    text: str
    spans: Tuple[Tuple[int, int], ...]
    msmarco_document_id: str
    def default_text(self):
        """
        text
        """
        return self.text


def parse_msmarco_passage(line):
    data = json.loads(line)
    # extract spans in the format of "(123,456),(789,101123)"
    spans = tuple((int(a), int(b)) for a, b in re.findall(r'\((\d+),(\d+)\)', data['spans']))
    return MsMarcoV2Passage(
        data['pid'],
        data['passage'],
        spans,
        data['docid'])


class MsMarcoV2Passages(BaseDocs):
    def __init__(self, dlc, pos_dlc=None):
        super().__init__()
        self._dlc = dlc
        self._pos_dlc = pos_dlc

    @ir_datasets.util.use_docstore
    def docs_iter(self):
        if self._pos_dlc is not None:
            # the shortcut only applies if the default pos
            # files are used (i.e., no filtering is applied)
            yield from self.docs_store()
        else:
            with self._dlc.stream() as stream, \
                 tarfile.open(fileobj=stream, mode='r|') as tarf:
                for record in tarf:
                    if not record.name.endswith('.gz'):
                        continue
                    file = tarf.extractfile(record)
                    with gzip.open(file) as file:
                        for line in file:
                            yield parse_msmarco_passage(line)

    def docs_cls(self):
        return MsMarcoV2Passage

    def docs_store(self, field='doc_id'):
        assert field == 'doc_id'
        # Unlike for msmarco-document-v2, using the docstore actually hurts performance.
        return MsMarcoV2DocStore(self)

    def docs_count(self):
        if self.docs_store().built():
            return self.docs_store().count()

    def docs_namespace(self):
        return NAME

    def docs_lang(self):
        return 'en'

    def docs_path(self, force=True):
        return self._dlc.path(force)


class MsMarcoV2DocStore(ir_datasets.indices.Docstore):
    def __init__(self, docs_handler):
        super().__init__(docs_handler.docs_cls(), 'doc_id')
        self.np = ir_datasets.lazy_libs.numpy()
        self.docs_handler = docs_handler
        self.dlc = docs_handler._dlc
        self.pos_dlc = docs_handler._pos_dlc
        self.base_path = docs_handler.docs_path(force=False) + '.extracted'
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)
        self.size_hint = 60880127751

    def get_many_iter(self, keys):
        self.build()
        # adapted from <https://microsoft.github.io/msmarco/TREC-Deep-Learning.html>
        bundles = {}
        for key in keys:
            if not key.count('_') == 3:
                continue
            (string1, string2, bundlenum, position) = key.split('_')
            assert string1 == 'msmarco' and string2 == 'passage'
            if bundlenum not in bundles:
                bundles[bundlenum] = []
            bundles[bundlenum].append(int(position))
        for bundlenum, positions in bundles.items():
            positions = sorted(positions)
            file = f'{self.base_path}/msmarco_passage_{bundlenum}'
            if not os.path.exists(file):
                # invalid doc_id -- doesn't point to a real bundle
                continue
            if self.docs_handler._pos_dlc is not None:
                # check the positions are valid for these doc_ids -- only return valid ones
                mmp = self.np.memmap(os.path.join(self.pos_dlc.path(), f'msmarco_passage_{bundlenum}.pos'), dtype='<u4')
                positions = self.np.array(positions, dtype='<u4')
                positions = positions[self.np.isin(positions, mmp)].tolist()
                del mmp
            with open(file, 'rt', encoding='utf8') as in_fh:
                for position in positions:
                    in_fh.seek(position)
                    try:
                        yield parse_msmarco_passage(in_fh.readline())
                    except json.JSONDecodeError:
                        # invalid doc_id -- pointed to a wrong position
                        pass

    def build(self):
        if self.built():
            return
        np = ir_datasets.lazy_libs.numpy()
        ir_datasets.util.check_disk_free(self.base_path, self.size_hint)
        with _logger.pbar_raw('extracting source documents', total=70, unit='file') as pbar, \
             self.dlc.stream() as stream, \
             tarfile.open(fileobj=stream, mode='r|') as tarf:
            for record in tarf:
                if not record.name.endswith('.gz'):
                    continue
                file = tarf.extractfile(record)
                fname = record.name.split('/')[-1][:-len('.gz')]
                positions = []
                with gzip.open(file) as fin, \
                     open(os.path.join(self.base_path, fname), 'wb') as fout:
                    for line in fin:
                        positions.append(fout.tell())
                        fout.write(line)
                # keep track of the positions for efficient slicing
                with open(os.path.join(self.base_path, f'{fname}.pos'), 'wb') as posout:
                    posout.write(np.array(positions, dtype='<u4').tobytes())
                pbar.update(1)
        (Path(self.base_path) / '_built').touch()

    def built(self):
        return (Path(self.base_path) / '_built').exists()

    def __iter__(self):
        self.build()
        return MsMarcoV2PassageIter(self, slice(0, self.count()))

    def _iter_source_files(self):
        for i in range(70):
            yield os.path.join(self.base_path, f'msmarco_passage_{i:02d}')

    def count(self):
        if self.docs_handler._pos_dlc is not None:
            base_path = self.pos_dlc.path()
            return sum(os.path.getsize(os.path.join(base_path, f)) for f in os.listdir(base_path)) // 4
        return 138_364_198


class MsMarcoV2PassageIter:
    def __init__(self, docstore, slice):
        self.np = ir_datasets.lazy_libs.numpy()
        self.docstore = docstore
        self.slice = slice
        self.next_index = 0
        self.file_iter = docstore._iter_source_files()
        self.current_file = None
        self.current_pos_mmap = None
        self.current_file_start_idx = 0
        self.current_file_end_idx = 0

    def __next__(self):
        if self.slice.start >= self.slice.stop:
            raise StopIteration
        while self.next_index != self.slice.start or self.current_file is None or self.current_file_end_idx <= self.slice.start or self.current_pos_mmap[self.slice.start - self.current_file_start_idx] != self.current_file.tell():
            if self.current_file is None or self.current_file_end_idx <= self.slice.start:
                # First iteration or no docs remaining in this file
                if self.current_file is not None:
                    self.current_file.close()
                    self.current_file = None
                # jump ahead to the file that contains the desired index
                first = True
                while first or self.current_file_end_idx < self.slice.start:
                    source_file = next(self.file_iter)
                    self.next_index = self.current_file_end_idx
                    self.current_file_start_idx = self.current_file_end_idx
                    pos_file = source_file + '.pos'
                    if self.docstore.pos_dlc is not None:
                        pos_file = os.path.join(self.docstore.pos_dlc.path(), source_file.split('/')[-1] + '.pos')
                    self.current_file_end_idx = self.current_file_start_idx + (os.path.getsize(pos_file) // 4)
                    first = False
                self.current_file = open(source_file, 'rb')
                self.current_pos_mmap = self.np.memmap(pos_file, dtype='<u4')
            else:
                # jump to the position of the next document
                pos = self.current_pos_mmap[self.slice.start - self.current_file_start_idx]
                self.current_file.seek(pos)
                self.next_index = self.slice.start
        result = parse_msmarco_passage(self.current_file.readline())
        self.next_index += 1
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def close(self):
        self.file_iter = None
        if self.current_file is not None:
            self.current_file.close()
            self.current_file = None
        self.current_pos_mmap = None

    def __iter__(self):
        return self

    def __del__(self):
        self.close()

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return MsMarcoV2PassageIter(self.docstore, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = MsMarcoV2PassageIter(self.docstore, new_slice)
            try:
                return next(new_it)
            except StopIteration as e:
                raise IndexError((self.slice, slice(key, key+1), new_slice))
        raise TypeError('key must be int or slice')


def _init():
    base_path = ir_datasets.util.home_path()/NAME
    documentation = YamlDocumentation(f'docs/{NAME}.yaml')
    dlc = DownloadConfig.context(NAME, base_path, dua=DUA)
    subsets = {}
    migrator = Migrator(base_path/'irds_version.txt', 'v2',
        affected_files=[base_path/'msmarco_v2_passage.tar.pklz4'],
        message='Cleaning up pklz4 lookup structure in favor of ID-based lookups')
    collection = MsMarcoV2Passages(dlc['passages'])
    collection = migrator(collection)

    qrels_migrator = Migrator(base_path/'qrels_version.txt', 'v2',
        affected_files=[base_path/'train'/'qrels.tsv', base_path/'dev1'/'qrels.tsv', base_path/'dev2'/'qrels.tsv'],
        message='Updating qrels (task organizers removed duplicates)')

    subsets['dedup'] = Dataset(
        MsMarcoV2Passages(dlc['passages'], TarExtractAll(dlc['dedup_positions'], base_path/'dedup_positions'))
    )

    subsets['train'] = Dataset(
        collection,
        TsvQueries(dlc['train/queries'], namespace='msmarco', lang='en'),
        qrels_migrator(TrecQrels(dlc['train/qrels'], QRELS_DEFS)),
        TrecScoredDocs(GzipExtract(dlc['train/scoreddocs'])),
    )
    subsets['dev1'] = Dataset(
        collection,
        TsvQueries(dlc['dev1/queries'], namespace='msmarco', lang='en'),
        qrels_migrator(TrecQrels(dlc['dev1/qrels'], QRELS_DEFS)),
        TrecScoredDocs(GzipExtract(dlc['dev1/scoreddocs'])),
    )
    subsets['dev2'] = Dataset(
        collection,
        TsvQueries(dlc['dev2/queries'], namespace='msmarco', lang='en'),
        qrels_migrator(TrecQrels(dlc['dev2/qrels'], QRELS_DEFS)),
        TrecScoredDocs(GzipExtract(dlc['dev2/scoreddocs'])),
    )
    subsets['trec-dl-2021'] = Dataset(
        collection,
        TsvQueries(dlc['trec-dl-2021/queries'], namespace='msmarco', lang='en'),
        TrecQrels(dlc['trec-dl-2021/qrels'], TREC_DL_QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2021/scoreddocs'])),
    )
    dl21_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2021'].qrels_iter()})
    subsets['trec-dl-2021/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2021'].queries_handler(), dl21_judged),
        FilteredScoredDocs(subsets['trec-dl-2021'].scoreddocs_handler(), dl21_judged),
        subsets['trec-dl-2021'],
    )
    subsets['trec-dl-2022'] = Dataset(
        collection,
        TsvQueries(dlc['trec-dl-2022/queries'], namespace='msmarco', lang='en'),
        TrecQrels(dlc['trec-dl-2022/qrels'], TREC_DL_QRELS_DEFS),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2022/scoreddocs'])),
    )
    dl22_judged = Lazy(lambda: {q.query_id for q in subsets['trec-dl-2022'].qrels_iter()})
    subsets['trec-dl-2022/judged'] = Dataset(
        FilteredQueries(subsets['trec-dl-2022'].queries_handler(), dl22_judged),
        FilteredScoredDocs(subsets['trec-dl-2022'].scoreddocs_handler(), dl22_judged),
        subsets['trec-dl-2022'],
    )
    subsets['trec-dl-2023'] = Dataset(
        collection,
        TsvQueries(dlc['trec-dl-2023/queries'], namespace='msmarco', lang='en'),
        TrecScoredDocs(GzipExtract(dlc['trec-dl-2023/scoreddocs'])),
    )

    ir_datasets.registry.register(NAME, Dataset(collection, documentation("_")))
    for s in sorted(subsets):
        ir_datasets.registry.register(f'{NAME}/{s}', Dataset(subsets[s], documentation(s)))

    return collection, subsets


collection, subsets = _init()
