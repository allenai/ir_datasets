import os
import shutil
import json
import zlib
import pickle
from contextlib import contextmanager
import ir_datasets


_logger = ir_datasets.log.easy()



class ZPickleKeyValueStore:
    def __init__(self, path, value_encoder=None):
        self._path = path
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
        if content[0] != key:
            raise KeyError(f'key={key} not found')
        if field is Ellipsis:
            return dict(content[1:])
        for f, val in content[1:]:
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
            binf.seek(-1) # un-peek
            content_length = int.from_bytes(binf.read(4), 'little')
            content = binf.read(content_length)
            content = zlib.decompress(content)
            content = pickle.loads(content)
            yield content[0], dict(content[1:])

    def __len__(self):
        # number of keys
        return len(self.idx())



class IndexedTsvKeyValueStore:
    def __init__(self, path, value_encoder=None):
        self._path = path
        self._value_encoder = value_encoder
        self._idx = None
        self._tsv = None

    def built(self):
        return len(self) > 0

    def idx(self):
        if self._idx is None:
            self._idx = NumpyPosIndex(os.path.join(self._path, 'idx'))
        return self._idx

    def tsv(self):
        if self._tsv is None:
            self._tsv = open(os.path.join(self._path, 'tsv'), 'rt')
        return self._tsv

    def purge(self):
        if self._idx:
            self._idx.close()
            self._idx = None
        if self._tsv:
            self._tsv.close()
            self._tsv = None

    @contextmanager
    def transaction(self):
        os.makedirs(self._path, exist_ok=True)
        with IndexedTsvDocStoreTransaction(self) as trans:
            yield trans

    def __getitem__(self, value):
        if isinstance(value, tuple) and len(value) == 2:
            key, field = value
        else:
            # assume key and all fields
            key = value
            field = ...
            record = {}
        tsv = self.tsv()
        tsv.seek(self.idx().get(key))
        for line in tsv:
            cols = line.rstrip().split('\t')
            if len(cols) == 1:
                if cols[0] != key:
                    break # end of doc
                else:
                    continue # key verified
            l_field, l_text = cols
            if field is Ellipsis:
                if self._value_encoder == 'json':
                    l_text = json.loads(l_text)
                record[l_field] = l_text
            else:
                if l_field == field:
                    if self._value_encoder == 'json':
                        l_text = json.loads(l_text)
                    return l_text
        if field is Ellipsis:
            if not record:
                raise KeyError(f'key={key} not found')
            return record
        raise KeyError(f'key={key} field={field} not found')

    def path(self):
        return self._path

    def __iter__(self):
        # iterates documents
        tsv = self.tsv()
        tsv.seek(0)
        key = None
        doc = None
        for line in tsv:
            cols = line.rstrip().split('\t')
            if len(cols) == 1:
                if doc is not None:
                    yield key, doc
                key = cols[0]
                doc = {}
            else:
                if self._value_encoder == 'json':
                    cols[1] = json.loads(cols[1])
                doc[cols[0]] = cols[1]
        if doc is not None:
            yield key, doc

    def __len__(self):
        # number of keys
        return len(self.idx())


class IndexedTsvDocStoreTransaction:
    def __init__(self, docstore):
        self.docstore = docstore
        self.path = self.docstore.path()
        self.idx = NumpyPosIndex(os.path.join(self.path, 'idx'))
        self.tsv = open(os.path.join(self.path, 'tsv'), 'wt')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_val:
            self.commit()
        else:
            self.discard()

    def commit(self):
        self.idx.commit()
        self.tsv.flush()
        self.tsv.close()
        # self.docstore.merge(IndexedTsvDocstore(self.path))
        # shutil.rmtree(self.path)

    def discard(self):
        shutil.rmtree(self.path)

    def add(self, key, fields):
        self.idx.add(key, self.tsv.tell())
        self.tsv.write(f'{key}\n')
        for field, value in zip(type(fields)._fields, fields):
            if self.docstore._value_encoder == 'json':
                value = json.dumps(value)
            elif self.docstore._value_encoder is None:
                value = value.replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            self.tsv.write(f'{field}\t{value}\n')


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


class NumpyPosIndex:
    def __init__(self, path):
        self.path = path
        self.data = None
        self.mmap1 = None
        self.mmap2 = None
        self.doccount = None
        self.didlen = None
        self.np = ir_datasets.lazy_libs.numpy()

    def add(self, did, idx):
        if self.data is None:
            self.data = {}
        self.data[did] = idx

    def commit(self):
        didlen = max(len(x) for x in self.data)
        sorted_data = sorted(self.data.items())
        # Use zero-terminated bytes here (S) rather than unicode type (U) because U includes a ton
        # of extra padding (for longer unicode formats), which can inflate the size of the index greatly.
        array1 = self.np.array([x[0].encode('utf8') for x in sorted_data], dtype=f'S{didlen}')
        array2 = self.np.array([x[1] for x in sorted_data], dtype=f'int64')
        m1 = self.np.memmap(f'{self.path}.did', dtype=array1.dtype, mode='w+', shape=array1.shape)
        m1[:] = array1[:]
        del m1
        m2 = self.np.memmap(f'{self.path}.pos', dtype=array2.dtype, mode='w+', shape=array2.shape)
        m2[:] = array2[:]
        del m2
        with ir_datasets.util.finialized_file(f'{self.path}.meta', 'wt') as f:
            f.write(f'{didlen} {len(self.data)}')
        self.data = None

    def _lazy_load(self):
        if self.mmap1 is None:
            with open(f'{self.path}.meta', 'rt') as f:
                self.didlen, self.doccount = f.read().split()
                self.didlen, self.doccount = int(self.didlen), int(self.doccount)
            self.mmap1 = self.np.memmap(f'{self.path}.did', dtype=f'S{self.didlen}', mode='r', shape=(self.doccount,))
            self.mmap2 = self.np.memmap(f'{self.path}.pos', dtype='int64', mode='r', shape=(self.doccount,))

    def get(self, did):
        self._lazy_load()
        did = did.encode('utf8')
        loc = self.np.searchsorted(self.mmap1, did)
        if self.mmap1[loc] == did:
            return self.mmap2[loc]
        return 0

    def close(self):
        if self.mmap1 is not None:
            del self.mmap1
            self.mmap1 = None
        if self.mmap2 is not None:
            del self.mmap2
            self.mmap2 = None
        self.data = None

    def __iter__(self):
        # iterates keys
        self._lazy_load()
        for i in range(len(self)):
            yield self.mmap1[i].decode('utf8')

    def __len__(self):
        # number of keys
        self._lazy_load()
        return self.doccount


def dir_size(path):
    # Adapted from <https://stackoverflow.com/questions/1392413/calculating-a-directorys-size-using-python>
    total = 0
    for entry in os.scandir(path):
        if entry.is_file():
            total += entry.stat().st_size
        elif entry.is_dir():
            total += dir_size(entry.path)
    return total



class IndexedTsvDocstore:
    file_ext = 'itsv'

    def __init__(self, path, doc_cls, value_encoder='json', id_field='doc_id', store=IndexedTsvKeyValueStore):
        self._path = path
        self._doc_cls = doc_cls
        self._id_field = id_field
        self._id_field_idx = doc_cls._fields.index(id_field)
        self._store = store(path, value_encoder=value_encoder)

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
        result = self._store[did]
        return self._doc_cls(*(result[f] for f in self._doc_cls._fields))

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

    def iter_docs(self):
        for did, fields in iter(self._store):
            yield self._doc_cls(*(fields[f] for f in self._doc_cls._fields))

    def path(self):
        return self._path

    def file_size(self):
        return dir_size(self._path)
