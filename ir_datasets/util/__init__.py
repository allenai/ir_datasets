import os
from contextlib import contextmanager
from threading import Lock
from pathlib import Path
from .fileio import IterStream, Cache, TarExtract, GzipExtract, ZipExtract, ZipExtractCache, StringFile, ReTar
from .download import Download, DownloadConfig, BaseDownload, RequestsDownload, LocalDownload
from .hash import HashVerificationError, HashVerifier, HashStream
from .registry import Registry


def tmp_path():
    p = Path(os.environ.get('IR_DATASETS_TMP', '/tmp/ir_datasets/'))
    p.mkdir(parents=True, exist_ok=True)
    return p


def cache_path():
    p = Path(os.environ.get('IR_DATASETS_CACHE', Path.home() / '.cache' / 'ir_datasets'))
    p.mkdir(parents=True, exist_ok=True)
    return p


@contextmanager
def finialized_file(path, mode):
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
