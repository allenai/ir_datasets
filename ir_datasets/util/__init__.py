import re
import os
import math
import functools
import shutil
from contextlib import contextmanager
from threading import Lock
from pathlib import Path
import tempfile
import ir_datasets
from .. import log
from .fileio import IterStream, Cache, TarExtract, TarExtractAll, RelativePath, GzipExtract, Lz4Extract, ZipExtract, ZipExtractCache, StringFile, ReTar, Bz2Extract, PackageDataFile
from .download import Download, DownloadConfig, BaseDownload, RequestsDownload, LocalDownload, _DownloadConfig
from .hash import HashVerificationError, HashVerifier, HashStream
from .metadata import MetadataComponent, MetadataProvider, default_metadata_provider, count_hint
from .registry import Registry
from .html_parsing import sax_html_parser


_logger = log.easy()


def tmp_path():
    p = Path(os.environ.get('IR_DATASETS_TMP', os.path.join(tempfile.gettempdir(), 'ir_datasets')))
    if not p.exists(): # per #107, we likely need both the exists check AND the exist_ok for occasional failures when directory is linked to NFS
        p.mkdir(parents=True, exist_ok=True)
    return p


def home_path():
    p = Path(os.environ.get('IR_DATASETS_HOME', Path.home() / '.ir_datasets'))
    if not p.exists(): # per #107, we likely need both the exists check AND the exist_ok for occasional failures when directory is linked to NFS
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


class Migrator:
    def __init__(self, version_file, version, affected_files, message=None, wrapped=None):
        self._wrapped = wrapped
        self._version_file = Path(version_file)
        self._version = version
        self._affected_files = affected_files
        self._message = message
        self._state = 'NOT_CHECKED'

    def __getattr__(self, attr):
        item = getattr(self._wrapped, attr)
        if callable(item):
            item = self._migrate(item)
        return item

    def __call__(self, wrapped):
        return Migrator(self._version_file, self._version, self._affected_files, self._message, wrapped)

    def _migrate(self, fn):
        # optionally wrap the function to perform cleanup of affected files
        if not self._state == 'OK':
            @functools.wraps(fn)
            def wrapped(*args, **kwargs):
                if not self._state == 'OK':
                    self._version_file.parent.mkdir(parents=True, exist_ok=True)
                    if not self._version_file.exists() or self._read_version() != self._version:
                        self._state = 'IN_PROGRESS'
                        paths_to_remove = [f for f in self._affected_files if os.path.exists(f)]
                        if paths_to_remove:
                            if self._message:
                                _logger.info(self._message)
                            for file in paths_to_remove:
                                if Path(file).is_file():
                                    os.unlink(file)
                                else:
                                    shutil.rmtree(file)
                        with self._version_file.open('wt') as f:
                            f.write(self._version)
                    self._state = 'OK'
                return fn(*args, **kwargs)
            return wrapped
        return fn

    def _read_version(self):
        with self._version_file.open('rt') as f:
            return f.read()


def check_disk_free(target_path, required_size, message='Insufficient disk space: {target_path} requires {required_size_fmt} but only {free_size_fmt} is available ({missing_size_fmt} more needed)'):
    """
    Checks if there is required_size bytes available on the device associated with target_path
    (or the closest target_path parent that exists if it doesn't exist). If there isn't enough
    space, throws an error with the specified message (or generic default message).

    The check is skipped if IR_DATASETS_SKIP_DISK_FREE is true.
    """
    skip = os.environ.get('IR_DATASETS_SKIP_DISK_FREE', 'false').lower() == 'true'
    if skip:
        return
    path = Path(target_path)
    while not path.exists():
        path = path.parent
    _, _, free_size = shutil.disk_usage(str(path))
    if free_size < required_size:
        missing_size = required_size - free_size
        missing_size_fmt = format_file_size(missing_size)
        required_size_fmt = format_file_size(required_size)
        free_size_fmt = format_file_size(free_size)
        raise ValueError(message.format(
            target_path=target_path,
            required_size=required_size,
            required_size_fmt=required_size_fmt,
            missing_size=missing_size,
            missing_size_fmt=missing_size_fmt,
            free_size=free_size,
            free_size_fmt=free_size_fmt))


def format_file_size(size):
    unit = '{:.0f}B'
    units = ['{:.1f}KB', '{:.1f}MB', '{:.1f}GB', '{:.1f}TB']
    while units and size > 1000:
        size = size / 1000
        unit = units.pop(0)
    return unit.format(size)


def ws_tok(s):
    s = re.sub('[^A-Za-z0-9 ]', ' ', s)
    left = 0
    for m in re.finditer(r"\s+", s):
        right, next = m.span()
        if right != left:
            yield s[left:right]
        left = next
    if left != len(s):
        yield s[left:len(s)]
