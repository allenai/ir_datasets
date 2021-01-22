import io
import os
import pickle
try:
    import fcntl
except:
    fcntl = None # not available on Windows :shrug:
from contextlib import contextmanager
import ir_datasets
from . import Docstore, NumpySortedIndex, NumpyPosIndex


_logger = ir_datasets.log.easy()


def _read_next(f, data_cls):
    lz4 = ir_datasets.lazy_libs.lz4_block()
    content_length = int.from_bytes(f.read(4), 'little')
    content = f.read(content_length)
    content = lz4.block.decompress(content)
    content = pickle.loads(content)
    return data_cls(*content)


def _skip_next(f):
    content_length = int.from_bytes(f.read(4), 'little')
    f.seek(content_length, io.SEEK_CUR)


def _write_next(f, record):
    lz4 = ir_datasets.lazy_libs.lz4_block()
    content = tuple(record)
    content = pickle.dumps(content)
    content = lz4.block.compress(content, store_size=True)
    content_length = len(content)
    f.write(content_length.to_bytes(4, 'little'))
    f.write(content)


def safe_str(s):
    return "".join(c for c in s if c.isalnum() or c == '_')


class Lz4PickleIter:
    def __init__(self, lookup, slice):
        self.next_index = 0
        self.lookup = lookup
        self.slice = slice
        self.bin = None
        self.pos_idx = None

    def __next__(self):
        if self.slice.start >= self.slice.stop:
            raise StopIteration
        if self.bin is None:
            self.bin = open(self.lookup._bin_path, 'rb')
        if self.next_index != self.slice.start:
            # Fast -- lookup keeps track of position of each index
            if self.pos_idx is None:
                self.pos_idx = NumpyPosIndex(self.lookup._pos_path)
            new_pos = self.pos_idx[self.slice.start][0]
            self.bin.seek(new_pos) # this seek is smart -- if alrady in buffer, skips to that point
            self.next_index = self.slice.start
        result = _read_next(self.bin, self.lookup._doc_cls)
        self.next_index += 1
        self.slice = slice(self.slice.start + (self.slice.step or 1), self.slice.stop, self.slice.step)
        return result

    def __iter__(self):
        return self

    def __del__(self):
        if self.bin is not None:
            self.bin.close()
            self.bin = None
        if self.pos_idx:
            self.pos_idx.close()
            self.pos_idx = None

    def __getitem__(self, key):
        if isinstance(key, slice):
            # it[start:stop:step]
            new_slice = ir_datasets.util.apply_sub_slice(self.slice, key)
            return Lz4PickleIter(self.lookup, new_slice)
        elif isinstance(key, int):
            # it[index]
            new_slice = ir_datasets.util.slice_idx(self.slice, key)
            new_it = Lz4PickleIter(self.lookup, new_slice)
            try:
                return next(new_it)
            except StopIteration as e:
                raise IndexError(e)
        raise TypeError('key must be int or slice')


class Lz4PickleLookup:
    def __init__(self, path, doc_cls, key_field, index_fields):
        self._path = path
        self._key_field = key_field
        self._key_idx = doc_cls._fields.index(key_field)
        self._index_fields = list(index_fields)
        self._doc_cls = doc_cls
        os.makedirs(self._path, exist_ok=True)
        self._bin = None
        self._bin_path = os.path.join(self._path, 'bin')
        self._pos = None
        self._pos_path = os.path.join(self._path, 'bin.pos')
        self._idx = None
        self._idx_path = os.path.join(self._path, f'idx.{safe_str(self._key_field)}')

        # check that the fields match
        meta_path = os.path.join(self._path, 'bin.meta')
        meta_info = ' '.join(doc_cls._fields)
        if not os.path.exists(meta_path):
            with open(meta_path, 'wt') as f:
                f.write(meta_info)
        else:
            with open(meta_path, 'rt') as f:
                existing_meta = f.read()
            assert existing_meta == meta_info, f"fields do not match; you may need to re-build this store {path}"

    def bin(self):
        if self._bin is None:
            self._bin = open(self._bin_path, 'rb')
        return self._bin

    def pos(self):
        if self._pos is None:
            self._pos = NumpyPosIndex(self._pos_path)
        return self._pos

    def idx(self):
        if self._idx is None:
            self._idx = NumpySortedIndex(self._idx_path)
        return self._idx

    def close(self):
        if self._idx:
            self._idx.close()
            self._idx = None
        if self._pos:
            self._pos.close()
            self._pos = None
        if self._bin:
            self._bin.close()
            self._bin = None

    def clear(self):
        self.close()
        if os.path.exists(self._bin_path):
            os.remove(self._bin_path)
        if os.path.exists(self._pos_path):
            os.remove(self._pos_path)
        NumpySortedIndex(self._idx_path).clear()

    def __del__(self):
        self.close()

    @contextmanager
    def transaction(self):
        with Lz4PickleTransaction(self) as trans:
            yield trans

    def __getitem__(self, values):
        if isinstance(values, str):
            values = (values,)
        poss = self.idx()[values]
        poss = sorted(poss) # go though the file in increasing order-- better for HDDs
        binf = None
        for pos in poss:
            if pos == -1:
                continue # not found
            if binf is None:
                binf = self.bin()
            binf.seek(pos)
            yield _read_next(binf, self._doc_cls)

    def path(self):
        return self._path

    def __iter__(self):
        return Lz4PickleIter(self, slice(0, len(self), 1))

    def __len__(self):
        # number of keys
        return len(self.pos())


class Lz4PickleTransaction:
    def __init__(self, lookup):
        self.lookup = lookup
        self.path = self.lookup.path()
        self.bin = None
        self.pos = None
        self.idxs = None
        self.start_pos = None

    def __enter__(self):
        self.bin = open(self.lookup._bin_path, 'ab')
        if fcntl:
            fcntl.lockf(self.bin, fcntl.LOCK_EX)
        self.start_pos = self.bin.tell() # for rolling back
        self.pos = NumpyPosIndex(self.lookup._pos_path)
        self.idxs = []
        for index_field in self.lookup._index_fields:
            idx_path = os.path.join(self.lookup._path, f'idx.{safe_str(index_field)}')
            self.idxs.append(NumpySortedIndex(idx_path))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.idxs is not None:
            if not exc_val:
                self.commit()
            else:
                self.rollback()

    def commit(self):
        self.pos.commit()
        self.pos = None
        for idx in self.idxs:
            idx.commit()
        self.idxs = None
        self.bin.flush()
        if fcntl:
            fcntl.lockf(self.bin, fcntl.LOCK_UN)
        self.bin.close()
        self.bin = None

    def rollback(self):
        self.bin.truncate(self.start_pos) # remove appended content
        if fcntl:
            fcntl.lockf(self.bin, fcntl.LOCK_UN)
        self.bin.close()
        self.bin = None
        self.pos.close()
        self.pos = None
        for idx in self.idxs:
            idx.close()
        self.idxs = None

    def add(self, record):
        bin_pos = self.bin.tell()
        self.pos.add(bin_pos)
        for idx, field in zip(self.idxs, self.lookup._index_fields):
            value = getattr(record, field)
            idx.add(value, bin_pos)
        _write_next(self.bin, record)


class PickleLz4FullStore(Docstore):
    def __init__(self, path, init_iter_fn, data_cls, lookup_field, index_fields):
        super().__init__(data_cls, lookup_field)
        self.path = path
        self.init_iter_fn = init_iter_fn
        self.lookup = Lz4PickleLookup(path, data_cls, lookup_field, index_fields)

    def get_many_iter(self, keys):
        self.build()
        yield from self.lookup[keys]

    def build(self):
        if not self.built():
            with self.lookup.transaction() as trans, _logger.duration('building docstore'):
                for doc in _logger.pbar(self.init_iter_fn(), 'docs_iter'):
                    trans.add(doc)

    def built(self):
        return len(self.lookup) > 0

    def clear_cache(self):
        self.lookup.clear()

    def __iter__(self):
        self.build()
        return iter(self.lookup)

    def count(self):
        self.build()
        return len(self.lookup)
