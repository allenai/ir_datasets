import os
import shutil
import json
import zlib
import pickle
from contextlib import contextmanager
from .indexed_tsv_docstore import NumpyPosIndex
import ir_datasets


_logger = ir_datasets.log.easy()



class ZPickleKeyValueStore:
    def __init__(self, path, id_idx, doc_cls):
        self._path = path
        self._id_idx = id_idx
        self._doc_cls = doc_cls
        self._idx = None
        self._bin = None

    def built(self):
        return len(self) > 0

    def idx(self):
        if self._idx is None:
            self._idx = NumpyPosIndex(os.path.join(self._path, 'idx'))
        return self._idx

    def bin(self):
        if self._bin is None:
            self._bin = open(os.path.join(self._path, 'bin'), 'rb')
        return self._bin

    def purge(self):
        if self._idx:
            self._idx.close()
            self._idx = None
        if self._bin:
            self._bin.close()
            self._bin = None

    @contextmanager
    def transaction(self):
        os.makedirs(self._path, exist_ok=True)
        with ZPickleDocStoreTransaction(self) as trans:
            yield trans

    def __getitem__(self, value):
        if isinstance(value, tuple) and len(value) == 2:
            key, field = value
        else:
            # assume key and all fields
            key, field = value, Ellipsis
        binf = self.bin()
        binf.seek(self.idx().get(key))
        content_length = int.from_bytes(binf.read(4), 'little')
        content = binf.read(content_length)
        content = zlib.decompress(content)
        content = pickle.loads(content)
        if content[self._id_idx][1] != key:
            raise KeyError(f'key={key} not found')
        if field is Ellipsis:
            content = dict(content)
            return self._doc_cls(*(content.get(f) for f in self._doc_cls._fields))
        for f, val in content:
            if field == f:
                return val
        raise KeyError(f'field={field} not found for key={key}')

    def path(self):
        return self._path

    def __iter__(self):
        # iterates documents
        binf = self.bin()
        binf.seek(0)
        while binf.read(1): # peek
            binf.seek(-1, 1) # un-peek
            content_length = int.from_bytes(binf.read(4), 'little')
            content = binf.read(content_length)
            content = zlib.decompress(content)
            content = pickle.loads(content)
            content = dict(content)
            yield self._doc_cls(*(content.get(f) for f in self._doc_cls._fields))

    def __len__(self):
        # number of keys
        return len(self.idx())


class ZPickleDocStoreTransaction:
    def __init__(self, docstore):
        self.docstore = docstore
        self.path = self.docstore.path()
        self.idx = NumpyPosIndex(os.path.join(self.path, 'idx'))
        self.bin = open(os.path.join(self.path, 'bin'), 'wb')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            self.commit()
        else:
            self.discard()

    def commit(self):
        self.idx.commit()
        self.bin.flush()
        self.bin.close()

    def discard(self):
        shutil.rmtree(self.path)

    def add(self, key, fields):
        self.idx.add(key, self.bin.tell())
        content = tuple(zip(type(fields)._fields, fields))
        content = pickle.dumps(content)
        content = zlib.compress(content)
        content_length = len(content)
        self.bin.write(content_length.to_bytes(4, 'little'))
        self.bin.write(content)


class ZPickleDocStore:
    file_ext = 'zpkl'

    def __init__(self, path, doc_cls, id_field='doc_id'):
        self._path = path
        self._doc_cls = doc_cls
        self._id_field = id_field
        self._id_field_idx = doc_cls._fields.index(id_field)
        self._store = ZPickleKeyValueStore(path, self._id_field_idx, self._doc_cls)

    def built(self):
        return os.path.exists(self._path)

    def purge(self):
        self._store.purge()

    def build(self, documents):
        with self._store.transaction() as trans:
            for doc in documents:
                trans.add(doc[self._id_field_idx], doc)

    def get(self, did, field=None):
        if field is not None:
            return self._store[did, field]
        return self._store[did]

    def get_many(self, dids, field=None):
        result = {}
        for did in dids:
            try:
                result[did] = self.get(did, field)
            except ValueError:
                pass
        return result

    def num_docs(self):
        return len(self._store)

    def docids(self):
        return iter(self._store.idx())

    def __iter__(self):
        return iter(self._store)

    def path(self):
        return self._path
