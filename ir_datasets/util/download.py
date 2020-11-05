import pkgutil
import os
from pathlib import Path
import atexit
from collections import deque
import io
import shutil
import tempfile
import contextlib
import ir_datasets
from ir_datasets import util


__all__ = ['Download', 'BaseDownload', 'RequestsDownload']
_logger = ir_datasets.log.easy()


class BaseDownload:
    def stream(self):
        raise NotImplementedError()


class RequestsDownload(BaseDownload):
    def __init__(self, url):
        self.url = url

    @contextlib.contextmanager
    def stream(self):
        with io.BufferedReader(util.IterStream(iter(self)), buffer_size=io.DEFAULT_BUFFER_SIZE) as stream:
            yield stream

    def __iter__(self):
        response = ir_datasets.lazy_libs.requests().get(self.url, stream=True)
        dlen = response.headers.get('content-length')
        if dlen is not None:
            dlen = int(dlen)

        fmt = '{desc}: {percentage:3.1f}%{r_bar}'
        with _logger.pbar_raw(desc=self.url, total=dlen, unit='B', unit_scale=True, bar_format=fmt) as pbar:
            for data in response.iter_content(chunk_size=io.DEFAULT_BUFFER_SIZE):
                pbar.update(len(data))
                yield data
            pbar.bar_format = '{desc} [{elapsed}] [{n_fmt}] [{rate_fmt}]'

    def __repr__(self):
        return f'RequestsDownload({repr(self.url)})'


class LocalDownload(BaseDownload):
    def __init__(self, path, message=None):
        self._path = Path(path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._message = message

    def path(self):
        if not self._path.exists():
            if self._message:
                _logger.info(self._message)
            raise FileNotFoundError(self._path)
        return self._path

    @contextlib.contextmanager
    def stream(self):
        with self.path().open('rb') as f:
            yield f


_ENCOUNTERD_DUAS = set()


def _cleanup_tmp(file):
    try:
        os.remove(file.name)
    except FileNotFoundError:
        pass


class Download:
    _dua_ctxt = deque([None])

    def __init__(self, *mirrors, cache_path=None, expected_md5=None, dua=None):
        self.mirrors = list(*mirrors)
        # self.url = url
        self.expected_md5 = expected_md5
        self.dua = dua or self._dua_ctxt[-1]
        self._cache_path = cache_path
        self._path = None

    def path(self):
        if self._path is not None:
            return self._path

        if self._cache_path is not None:
            download_path = self._cache_path
            if os.path.exists(download_path):
                self._path = download_path
                return self._path
        else:
            tmpfile = tempfile.NamedTemporaryFile(delete=False)
            atexit.register(_cleanup_tmp, tmpfile)
            download_path = tmpfile.name

        if self.dua is not None and self.dua not in _ENCOUNTERD_DUAS:
            _logger.info(self.dua)
            _ENCOUNTERD_DUAS.add(self.dua)

        errors = []

        Path(download_path).parent.mkdir(parents=True, exist_ok=True)

        for mirror in self.mirrors:
            try:
                with util.finialized_file(download_path, 'wb') as f:
                    with mirror.stream() as stream:
                        stream = util.HashStream(stream, self.expected_md5, algo='md5')
                        shutil.copyfileobj(stream, f)
                        break
            except Exception as e:
                errors.append((mirror, e))
                if not isinstance(mirror, LocalDownload):
                    _logger.warn(f'Download failed: {e}')
        else:
            raise RuntimeError('All download sources failed', errors)
        self._path = download_path
        return self._path

    @contextlib.contextmanager
    def stream(self):
        with open(self.path(), 'rb') as f:
            yield f

    @classmethod
    @contextlib.contextmanager
    def dua_ctxt(cls, dua):
        cls._dua_ctxt.append(dua)
        yield
        cls._dua_ctxt.pop()


class _DownloadConfig:
    def __init__(self, file=None, base_path=None, contents=None, dua=None):
        self._file = file
        self._base_path = base_path
        self._contents = contents
        self._dua = dua

    def contents(self):
        if self._contents is None:
            yaml = ir_datasets.lazy_libs.yaml()
            data = pkgutil.get_data('ir_datasets', self._file)
            self._contents = yaml.load(data, Loader=yaml.BaseLoader)
        return self._contents

    def context(self, key, base_path=None, dua=None):
        contents = self.contents()
        return _DownloadConfig(contents=contents[key] if key else contents, base_path=base_path or self._base_path, dua=dua or self._dua)

    def __getitem__(self, key):
        dlc = self.contents()[key]
        sources = []
        cache_path = None
        if 'cache_path' in dlc:
            if self._base_path:
                cache_path = os.path.join(self._base_path, dlc['cache_path'])
            else:
                cache_path = dlc['cache_path']
        if 'url' in dlc:
            if not dlc.get('skip_local') and dlc.get('expected_md5'):
                local_path = Path(util.cache_path()) / 'downloads' / dlc['expected_md5']
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_msg = (f'If you have a local copy of {dlc["url"]}, you can symlink it here '
                             f'to avoid downloading it again: {local_path}')
                sources.append(LocalDownload(local_path, local_msg))
            sources.append(RequestsDownload(dlc['url']))
        elif 'instructions' in dlc:
            if 'cache_path' in dlc:
                local_path = Path(cache_path)
            else:
                local_path = Path(util.cache_path()) / 'downloads' / dlc['expected_md5']
            sources.append(LocalDownload(local_path, dlc['instructions'].format(path=local_path)))
        else:
            raise RuntimeError('Must either provide url or instructions')
        return Download(sources, expected_md5=dlc.get('expected_md5'), cache_path=cache_path, dua=self._dua)


DownloadConfig = _DownloadConfig(file='etc/downloads.yaml')
