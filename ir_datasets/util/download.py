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
    def __init__(self, url, tries=None):
        self.url = url
        self.tries = tries

    @contextlib.contextmanager
    def stream(self):
        with io.BufferedReader(util.IterStream(iter(self)), buffer_size=io.DEFAULT_BUFFER_SIZE) as stream:
            yield stream

    def __iter__(self):
        requests = ir_datasets.lazy_libs.requests()
        http_args = {
            'url': self.url,
            'stream': True, # return the response as a stream, rather than loading it all into memory
            'headers': {'User-Agent': f'ir_datasets/{ir_datasets.__version__}'}, # identify itself
            'timeout': float(os.environ.get('IR_DATASETS_DL_TIMEOUT', '15')), # raise error if 15 seconds pass without any data from the socket
        }
        done = False
        pbar = None
        response = None
        skip = 0
        default_tries = self.tries if self.tries is not None else int(os.environ.get('IR_DATASETS_DL_TRIES', '3'))
        remaining_tries = default_tries
        with contextlib.ExitStack() as stack:
            while not done:
                try:
                    response = stack.enter_context(requests.get(**http_args))
                    if pbar is None:
                        dlen = response.headers.get('content-length')
                        if dlen is not None:
                            dlen = int(dlen)
                        fmt = '{desc}: {percentage:3.1f}%{r_bar}'
                        if os.environ.get('IR_DATASETS_DL_DISABLE_PBAR', '').lower() == 'true':
                            pbar_f = stack.enter_context(open(os.devnull, 'w')) # still maintain the pbar, but write to /dev/null
                        else:
                            pbar_f = None # defaults to stderr
                        pbar = stack.enter_context(_logger.pbar_raw(desc=self.url, total=dlen, unit='B', unit_scale=True, bar_format=fmt, file=pbar_f))
                    for data in self._iter_response_data(response, http_args, skip):
                        pbar.update(len(data))
                        if response.headers.get('accept-ranges') == 'bytes':
                            # since we got more data and the server accepts range requests, reset the "tries" counter
                            remaining_tries = default_tries
                        yield data
                except requests.exceptions.RequestException as ex:
                    remaining_tries -= 1
                    if remaining_tries <= 0:
                        raise # no more tries
                    if response is not None and response.headers.get('accept-ranges') == 'bytes':
                        # woo hoo! We can issue a range request, so we don't need to download all the data again,
                        # just pick up from where we left off.
                        _logger.info(f'download error: {ex}. Retrying range "{pbar.n}-" [{remaining_tries} attempts left]')
                        http_args['headers']['Range'] = f'bytes={pbar.n}-'
                        skip = 0
                    elif pbar is not None:
                        # The server doesn't accept range requests, so we'll need to re-download the file up to
                        # where we got, and then start up again from there
                        _logger.info(f'download error: {ex}. Retrying from start (skipping {pbar.n} bytes) because server doesn\'t accept range requests [{remaining_tries} attempts left]')
                        if 'Range' in http_args['headers']:
                            del http_args['headers']['Range']
                        skip = pbar.n
                    else:
                        # We didn't get any data, start from the start
                        _logger.info(f'download error: {ex}. Retrying from start.')
                        if 'Range' in http_args['headers']:
                            del http_args['headers']['Range']
                        skip = 0
                else:
                    done = True
            pbar.bar_format = '{desc} [{elapsed}] [{n_fmt}] [{rate_fmt}]'

    def _iter_response_data(self, response, http_args, skip):
        with contextlib.ExitStack() as stack:
            skip_pbar = None
            if skip > 0:
                fmt = '{desc}: {percentage:3.1f}%{r_bar}'
                skip_pbar = stack.enter_context(_logger.pbar_raw(desc=f'skipping ahead to {skip}', total=skip, unit='B', unit_scale=True, bar_format=fmt))
            # Some web servers (which?) annoyingly set the content-encoding to gzip when the file itself is
            # gzipped. This will transparently decompress the stream here, and would mean that hash verification
            # would fail. So instead, detect this situation and use the raw stream in that case here. Note that
            # we DO normally want this transparent decompression.
            # An example is NFCorpus: <https://www.cl.uni-heidelberg.de/statnlpgroup/nfcorpus/nfcorpus.tar.gz>
            if http_args['url'].endswith('.gz') and response.headers.get('content-encoding') == 'gzip':
                data_iter = response.raw.stream(io.DEFAULT_BUFFER_SIZE, decode_content=False)
            else:
                data_iter = response.iter_content(chunk_size=io.DEFAULT_BUFFER_SIZE)
            for data in data_iter:
                if skip > 0:
                    data, skipped = data[skip:], len(data[:skip])
                    skip -= skipped
                    skip_pbar.update(skipped)
                if data:
                    yield data

    def __repr__(self):
        return f'RequestsDownload({repr(self.url)}, tries={self.tries})'


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

    def __init__(self, mirrors, cache_path=None, expected_md5=None, dua=None, stream=False):
        self.mirrors = list(mirrors)
        self.expected_md5 = expected_md5
        self.dua = dua or self._dua_ctxt[-1]
        self._cache_path = cache_path
        self._stream = stream
        self._path = None

    def path(self):
        if self._path is not None:
            return self._path

        if self._cache_path is not None:
            download_path = self._cache_path
            if os.path.exists(download_path) and download_path != os.devnull:
                self._path = download_path
                return self._path
        else:
            tmpfile = tempfile.NamedTemporaryFile(delete=False, dir=util.tmp_path())
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
            if len(self.mirrors) == 1:
                raise errors[0][1]
            if len(self.mirrors) == 2 and isinstance(self.mirrors[0], LocalDownload):
                raise errors[1][1]
            raise RuntimeError('All download sources failed', errors)
        self._path = download_path
        return self._path

    @contextlib.contextmanager
    def stream(self):
        if self._stream:
            assert len(self.mirrors) == 1, "cannot stream with multiple mirrors"
            with self.mirrors[0].stream() as stream:
                stream = util.HashStream(stream, self.expected_md5, algo='md5')
                yield stream
        else:
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
                local_path = Path(util.home_path()) / 'downloads' / dlc['expected_md5']
                local_path.parent.mkdir(parents=True, exist_ok=True)
                local_msg = (f'If you have a local copy of {dlc["url"]}, you can symlink it here '
                             f'to avoid downloading it again: {local_path}')
                sources.append(LocalDownload(local_path, local_msg))
            sources.append(RequestsDownload(dlc['url']))
        elif 'instructions' in dlc:
            if 'cache_path' in dlc:
                local_path = Path(cache_path)
            else:
                local_path = Path(util.home_path()) / 'downloads' / dlc['expected_md5']
            sources.append(LocalDownload(local_path, dlc['instructions'].format(path=local_path)))
        else:
            raise RuntimeError('Must either provide url or instructions')
        return Download(sources, expected_md5=dlc.get('expected_md5'), cache_path=cache_path, dua=self._dua, stream=dlc.get('stream', False))


DownloadConfig = _DownloadConfig(file='etc/downloads.yaml')
