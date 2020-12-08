import os
import pickle
import fcntl
from contextlib import contextmanager
import ir_datasets
from . import NumpySortedIndex


class Lz4PickleLookup:
    def __init__(self, path, doc_cls, key_field='doc_id'):
        self._path = path
        self._key_field = key_field
        self._key_idx = doc_cls._fields.index(key_field)
        self._doc_cls = doc_cls
        os.makedirs(self._path, exist_ok=True)
        self._idx = None
        self._idx_path = os.path.join(self._path, 'idx.' + "".join(x for x in self._key_field if x.isalnum() or x == '_'))
        self._bin = None
        self._bin_path = os.path.join(self._path, 'bin')

    def idx(self):
        if self._idx is None:
            self._idx = NumpySortedIndex(self._idx_path)
        return self._idx

    def bin(self):
        if self._bin is None:
            self._bin = open(self._bin_path, 'rb')
        return self._bin

    def close(self):
        if self._idx:
            self._idx.close()
            self._idx = None
        if self._bin:
            self._bin.close()
            self._bin = None

    def clear(self):
        self.close()
        if os.path.exists(self._bin_path):
            os.remove(self._bin_path)
        NumpySortedIndex(self._idx_path).clear()

    def __del__(self):
        self.close()

    @contextmanager
    def transaction(self):
        with Lz4PickleTransaction(self) as trans:
            yield trans

    def __getitem__(self, values):
        lz4 = ir_datasets.lazy_libs.lz4_block()
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
            content_length = int.from_bytes(binf.read(4), 'little')
            content = binf.read(content_length)
            content = lz4.block.decompress(content)
            content = pickle.loads(content)
            content = dict(content)
            yield self._doc_cls(*(content.get(f) for f in self._doc_cls._fields))

    def path(self):
        return self._path

    def __iter__(self):
        # iterates documents
        lz4 = ir_datasets.lazy_libs.lz4_block()
        try:
            binf = self.bin()
        except FileNotFoundError:
            pass
        else:
            binf.seek(0)
            while binf.read(1): # peek
                binf.seek(-1, 1) # un-peek
                content_length = int.from_bytes(binf.read(4), 'little')
                content = binf.read(content_length)
                content = lz4.block.decompress(content)
                content = pickle.loads(content)
                content = dict(content)
                yield self._doc_cls(*(content.get(f) for f in self._doc_cls._fields))

    def __len__(self):
        # number of keys
        return len(self.idx())


class Lz4PickleTransaction:
    def __init__(self, lookup):
        self.lz4 = ir_datasets.lazy_libs.lz4_block()
        self.lookup = lookup
        self.path = self.lookup.path()
        self.idx = None
        self.bin = None
        self.start_pos = None

    def __enter__(self):
        self.idx = self.lookup.idx()
        self.bin = open(os.path.join(self.path, 'bin'), 'ab')
        fcntl.lockf(self.bin, fcntl.LOCK_EX)
        self.start_pos = self.bin.tell() # for rolling back
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.idx is not None:
            if not exc_val:
                self.commit()
            else:
                self.rollback()

    def commit(self):
        self.idx.commit()
        self.idx = None
        self.bin.flush()
        fcntl.lockf(self.bin, fcntl.LOCK_UN)
        self.bin.close()
        self.bin = None

    def rollback(self):
        self.bin.truncate(self.start_pos) # remove appended content
        fcntl.lockf(self.bin, fcntl.LOCK_UN)
        self.bin.close()
        self.bin = None
        self.idx.close()
        self.idx = None

    def add(self, record):
        key = record[self.lookup._key_idx]
        self.idx.add(key, self.bin.tell())
        content = tuple(zip(type(record)._fields, record))
        content = pickle.dumps(content)
        content = self.lz4.block.compress(content, store_size=True)
        content_length = len(content)
        self.bin.write(content_length.to_bytes(4, 'little'))
        self.bin.write(content)
