import os
import math
import functools
from contextlib import contextmanager
from threading import Lock
from pathlib import Path
from .fileio import IterStream, Cache, TarExtract, TarExtractAll, GzipExtract, ZipExtract, ZipExtractCache, StringFile, ReTar, Bz2Extract
from .download import Download, DownloadConfig, BaseDownload, RequestsDownload, LocalDownload
from .hash import HashVerificationError, HashVerifier, HashStream
from .registry import Registry


def tmp_path():
    p = Path(os.environ.get('IR_DATASETS_TMP', '/tmp/ir_datasets/'))
    p.mkdir(parents=True, exist_ok=True)
    return p


def home_path():
    p = Path(os.environ.get('IR_DATASETS_HOME', Path.home() / '.ir_datasets'))
    p.mkdir(parents=True, exist_ok=True)
    return p


@contextmanager
def finialized_file(path, mode):
    if path == os.devnull:
        with open(path, mode) as f:
            yield f
    else:
        try:
            with open(f'{path}.tmp', mode) as f:
                yield f
            os.replace(f'{path}.tmp', path)
        except:
            try:
                os.remove(f'{path}.tmp')
            except:
                pass # ignore
            raise


class Lazy:
    def __init__(self, fn):
        self._lock = Lock()
        self._fn = fn
        self._loaded = False
        self._result = None

    def __call__(self):
        if not self._loaded:
            with self._lock:
                if not self._loaded: # repeat condition from above in thread-safe way
                    self._result = self._fn()
                    self._loaded = True
        return self._result

    @property
    def is_loaded(self):
        return self._loaded


def apply_sub_slice(orig_slice: slice, new_slice: slice):
    start, stop, step = None, None, None
    if new_slice.start is not None:
        if isinstance(new_slice.start, int):
            if new_slice.start < 0:
                if orig_slice.stop is None:
                    raise ValueError('start cannot be negative with unknown size')
                start = orig_slice.stop + new_slice.start
            else:
                start = orig_slice.start + new_slice.start
        elif isinstance(new_slice.start, float):
            if orig_slice.stop is None:
                raise ValueError('start cannot be float with unknown size')
            if not (0. <= new_slice.stop <= 1.):
                raise ValueError('start must be in interval [0,1] if float')
            size = orig_slice.stop - (orig_slice.start or 0)
            start = (new_slice.start * size) + (orig_slice.start or 0)
            start = math.floor(start)
        else:
            raise TypeError('start must be int or float')
    else:
        start = orig_slice.start

    if new_slice.stop is not None:
        if isinstance(new_slice.stop, int):
            if new_slice.stop < 0:
                if orig_slice.stop is None:
                    raise ValueError('stop cannot be negative with unknown size')
                stop = orig_slice.stop + new_slice.stop
            else:
                stop = min(orig_slice.start + new_slice.stop, orig_slice.stop)
        elif isinstance(new_slice.stop, float):
            if orig_slice.stop is None:
                raise ValueError('stop cannot be float with unknown size')
            if not (0. <= new_slice.stop <= 1.):
                raise ValueError('stop must be in interval [0,1] if float')
            size = orig_slice.stop - (orig_slice.start or 0)
            stop = (new_slice.stop * size) + (orig_slice.start or 0)
            stop = math.floor(stop)
        else:
            raise TypeError('stop must be int or float')
    else:
        stop = orig_slice.stop

    if new_slice.step is not None:
        if isinstance(new_slice.step, int):
            if new_slice.step <= 0:
                raise ValueError('step must be a positive')
            step = (orig_slice.step or 1) * new_slice.step
        else:
            raise TypeError('step must be int')
    else:
        step = orig_slice.step
    return slice(start, stop, step)

def slice_idx(orig_slice: slice, index: int):
    if index >= 0:
        index = orig_slice.start + index
    else:
        index = orig_slice.stop + index
    return slice(index, min(index + 1, orig_slice.stop))


class DocstoreSplitter:
    def __init__(self, it, docs_store):
        self.it = it
        self.docs_store = docs_store

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.it)

    def __getitem__(self, key):
        return iter(self.docs_store)[key]


def use_docstore(fn):
    # For use as an @annotation
    # use docs_store if it's already built, otherwise only use it if the user
    # specifies a split (docs_it[split])
    @functools.wraps(fn)
    def wrapper(self):
        docs_store = self.docs_store()
        if docs_store.built():
            return iter(docs_store) # iterate from the docstore -- really fast
        return DocstoreSplitter(fn(self), docs_store) # avoid building docstore if not needed
    return wrapper
