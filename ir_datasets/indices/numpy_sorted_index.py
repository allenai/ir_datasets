import os
import ir_datasets


class NumpySortedIndex:
    def __init__(self, path):
        self.path = path
        self.transaction = None
        self.mmap_keys = None
        self.mmap_poss = None
        self.doccount = None
        self.keylen = None
        self.np = None

    def add(self, key, idx):
        if self.transaction is None:
            self.transaction = {}
        self.transaction[key] = idx

    def commit(self):
        self._lazy_load()
        if self.transaction is None:
            return
        transaction = sorted(self.transaction.items())
        transaction = [(x[0].encode('utf8'), x[1]) for x in transaction]
        if self._exists():
            transaction += [(k, p) for k, p in zip(self.mmap_keys, self.mmap_poss) if (k.decode() not in self.transaction)]
            transaction = sorted(transaction)
        keys = [x[0] for x in transaction]
        poss = [x[1] for x in transaction]
        self.keylen = max(len(k) for k in keys)
        self.doccount = len(keys)
        # Use zero-terminated bytes here (S) rather than unicode type (U) because U includes a ton
        # of extra padding (for longer unicode formats), which can inflate the size of the index greatly.
        keys = self.np.array(keys, dtype=f'S{self.keylen}')
        poss = self.np.array(poss, dtype='int64')
        self.mmap_keys = self.np.memmap(f'{self.path}.key', dtype=keys.dtype, mode='w+', shape=keys.shape)
        self.mmap_keys[:] = keys[:]
        self.mmap_poss = self.np.memmap(f'{self.path}.pos', dtype=poss.dtype, mode='w+', shape=poss.shape)
        self.mmap_poss[:] = poss[:]
        with ir_datasets.util.finialized_file(f'{self.path}.meta', 'wt') as f:
            f.write(f'{self.keylen} {self.doccount}')
        self.transaction = None

    def _exists(self):
        return os.path.exists(f'{self.path}.key')

    def _lazy_load(self):
        if self.np is None:
            self.np = ir_datasets.lazy_libs.numpy()
        if self.mmap_keys is None and self._exists():
            with open(f'{self.path}.meta', 'rt') as f:
                self.keylen, self.doccount = f.read().split()
                self.keylen, self.doccount = int(self.keylen), int(self.doccount)
            self.mmap_keys = self.np.memmap(f'{self.path}.key', dtype=f'S{self.keylen}', mode='r', shape=(self.doccount,))
            self.mmap_poss = self.np.memmap(f'{self.path}.pos', dtype='int64', mode='r', shape=(self.doccount,))

    def __getitem__(self, keys):
        self._lazy_load()
        if isinstance(keys, str):
            keys = (keys,)
        if not self._exists():
            return [-1 for _ in keys]
        keys = self.np.array([key.encode('utf8') for key in keys], dtype=f'S{self.keylen}')
        locs = self.np.searchsorted(self.mmap_keys, keys)
        locs[locs >= self.mmap_keys.shape[0]] = self.mmap_keys.shape[0] - 1 # could be placed AFTER existing keys
        mask = self.mmap_keys[locs] == keys
        return ((self.mmap_poss[locs] * mask) + (~mask * -1)).tolist()

    def close(self):
        if self.mmap_keys is not None:
            del self.mmap_keys
            self.mmap_keys = None
        if self.mmap_poss is not None:
            del self.mmap_poss
            self.mmap_poss = None
        self.data = None

    def clear(self):
        self.close()
        for file in ['meta', 'key', 'pos']:
            path = f'{self.path}.{file}'
            if os.path.exists(path):
                os.remove(path)

    def __del__(self):
        self.close()

    def __iter__(self):
        # iterates keys
        self._lazy_load()
        if self._exists():
            for i in range(len(self)):
                yield self.mmap_keys[i].decode('utf8')

    def __len__(self):
        # number of keys
        self._lazy_load()
        if self._exists():
            return self.doccount
        return 0


class NumpyPosIndex:
    def __init__(self, path):
        self.path = path
        self.transaction = None
        self.mmap = None
        self.np = None

    def add(self, idx):
        if self.transaction is None:
            self.transaction = []
        self.transaction.append(idx)

    def commit(self):
        self._lazy_load()
        if self.transaction is None:
            return
        if self.mmap is not None:
            del self.mmap
            self.mmap = None
        if os.path.exists(self.path):
            current_count = os.stat(self.path).st_size // 8
            mmap = self.np.memmap(self.path, dtype='int64', mode='r+', shape=(current_count + len(self.transaction),))
        else:
            mmap = self.np.memmap(self.path, dtype='int64', mode='w+', shape=(len(self.transaction),))
        mmap[-len(self.transaction):] = self.np.array(self.transaction, dtype='int64')
        del mmap
        self.transaction = None

    def _exists(self):
        return os.path.exists(self.path)

    def _lazy_load(self):
        if self.np is None:
            self.np = ir_datasets.lazy_libs.numpy()
        if self.mmap is None and self._exists():
            current_count = os.stat(self.path).st_size // 8
            self.mmap = self.np.memmap(self.path, dtype='int64', mode='r', shape=(current_count,))

    def __getitem__(self, idxs):
        self._lazy_load()
        if isinstance(idxs, int):
            idxs = (idxs,)
        if not self._exists():
            return [-1 for _ in idxs]
        idxs = self.np.array(idxs)
        mask = idxs > 0 & idxs < self.mmap.shape[0]
        return ((self.mmap[idxs] * mask) + (~mask * -1)).tolist()

    def close(self):
        if self.mmap is not None:
            del self.mmap
            self.mmap = None

    def clear(self):
        self.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def __del__(self):
        self.close()

    def __iter__(self):
        # iterates keys
        self._lazy_load()
        if self._exists():
            for i in range(len(self)):
                yield self.mmap[i]

    def __len__(self):
        # number of keys
        self._lazy_load()
        if self._exists():
            return self.mmap.shape[0]
        return 0
