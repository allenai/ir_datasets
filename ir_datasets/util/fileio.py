import os
import contextlib
import shutil
from pathlib import Path
from fnmatch import fnmatch
import tarfile
import gzip
import bz2
import io
from zipfile import ZipFile
import ir_datasets
from ir_datasets import util


__all__ = ['IterStream', 'Cache', 'TarExtract', 'TarExtractAll', 'RelativePath', 'GzipExtract', 'Lz4Extract', 'ZipExtract', 'ZipExtractCache', 'StringFile', 'PackageDataFile']


_logger = ir_datasets.log.easy()


class IterStream(io.RawIOBase):
    def __init__(self, it):
        super().__init__()
        self.leftover = None
        self.it = it

    def readable(self):
        return True

    def readinto(self, b):
        pos = 0
        try:
            while pos < len(b):
                l = len(b) - pos  # We're supposed to return at most this much
                chunk = self.leftover or next(self.it)
                output, self.leftover = chunk[:l], chunk[l:]
                b[pos:pos+len(output)] = output
                pos += len(output)
            return pos
        except StopIteration:
            return pos    # indicate EOF



class Cache:
    def __init__(self, streamer, path):
        self._streamer = streamer
        self._path = path

    def verify(self):
        if not self._path.exists():
            # stream not cached
            # write stream to a .tmpXX file and then move it to the
            # correct path when successfully downloaded.
            self._path.parent.mkdir(parents=True, exist_ok=True)
            with contextlib.ExitStack() as ctxt:
                tmp_idx = 0
                while True:
                    try:
                        f = open(f'{self._path}.tmp{tmp_idx}', 'xb') # exclusive open
                        ctxt.push(f)
                        break # success opening file
                    except IOError:
                        tmp_idx += 1
                        if tmp_idx >= 100: # Up to 100 attempts to find a file
                            raise
                try:
                    with self._streamer.stream() as stream:
                        shutil.copyfileobj(stream, f)
                    f.close() # close file before move... Needed because of Windows
                    shutil.move(f.name, self._path)
                finally:
                    if Path(f.name).exists():
                        Path(f.name).unlink()

    @contextlib.contextmanager
    def stream(self):
        self.verify()
        with self._path.open('rb') as f:
            yield f

    def path(self, force=True):
        if force:
            self.verify()
        return self._path


class TarExtract:
    def __init__(self, streamer, tar_path, compression='gz'):
        self._streamer = streamer
        self._tar_path = tar_path
        self._compression = compression

    @contextlib.contextmanager
    def stream(self):
        with contextlib.ExitStack() as ctxt, self._streamer.stream() as stream:
            # IMPORTANT: open this file in streaming mode (| in mode). This means that the
            # content need not be written to disk or be fully read.
            tarf = ctxt.enter_context(tarfile.open(fileobj=stream, mode=f'r|{self._compression or ""}'))
            for block in tarf:
                if block.name == self._tar_path:
                    result = tarf.extractfile(block)
                    break
            else:
                raise RuntimeError(f'{self._tar_path} not found in tar file')
            yield result


class TarExtractAll:
    def __init__(self, streamer, extract_path, compression='gz', path_globs=None):
        self._streamer = streamer
        self._extract_path = extract_path
        self._compression = compression
        self._path_globs = path_globs

    def path(self, force=True):
        if force and not os.path.exists(self._extract_path):
            try:
                with self._streamer.stream() as stream, tarfile.open(fileobj=stream, mode=f'r|{self._compression or ""}') as tarf, \
                     _logger.duration('extracting from tar file'):
                    if self._path_globs is None:
                        # shortcut to extract everything
                        tarf.extractall(self._extract_path)
                    else:
                        for member in tarf:
                            if any(fnmatch(member.name, g) for g in self._path_globs):
                                tarf.extract(member, self._extract_path)
            except:
                if os.path.exists(self._extract_path):
                    shutil.rmtree(self._extract_path)
                raise
        return self._extract_path

    def stream(self):
        raise NotImplementedError()


class RelativePath:
    def __init__(self, streamer, path):
        self._streamer = streamer
        self._path = path

    def path(self, force=True):
        return os.path.join(self._streamer.path(force), self._path)

    @contextlib.contextmanager
    def stream(self):
        with open(self.path(), 'rb') as f:
            yield f


class ReTar:
    def __init__(self, streamer, output_file, keep_globs, compression='gz'):
        self._streamer = streamer
        self._output_file = Path(output_file)
        self._keep_globs = keep_globs
        self._compression = compression

    @contextlib.contextmanager
    def stream(self):
        if not self._output_file.exists():
            with contextlib.ExitStack() as ctxt, self._streamer.stream() as stream:
                ctxt.enter_context(_logger.duration('re-taring file'))
                outf = ctxt.enter_context(util.finialized_file(self._output_file, 'wb'))
                o_tarf = ctxt.enter_context(tarfile.open(fileobj=outf, mode=f'w|{self._compression or ""}'))
                # IMPORTANT: open this file in streaming mode (| in mode). This means that the
                # content need not be written to disk or be fully read.
                i_tarf = ctxt.enter_context(tarfile.open(fileobj=stream, mode=f'r|{self._compression or ""}'))
                for block in i_tarf:
                    if any(fnmatch(block.name, g) for g in self._keep_globs):
                        o_tarf.addfile(block, i_tarf.extractfile(block))
                        _logger.info(f'extracted {block.name}')
        with self._output_file.open('rb') as f:
            yield f


class GzipExtract:
    def __init__(self, streamer):
        self._streamer = streamer

    def __getattr__(self, attr):
        return getattr(self._streamer, attr)

    @contextlib.contextmanager
    def stream(self):
        with self._streamer.stream() as stream:
            yield gzip.GzipFile(fileobj=stream)


class Bz2Extract:
    def __init__(self, streamer):
        self._streamer = streamer

    def __getattr__(self, attr):
        return getattr(self._streamer, attr)

    @contextlib.contextmanager
    def stream(self):
        with self._streamer.stream() as stream:
            yield bz2.BZ2File(stream)


class Lz4Extract:
    def __init__(self, streamer):
        self._streamer = streamer

    def __getattr__(self, attr):
        return getattr(self._streamer, attr)

    @contextlib.contextmanager
    def stream(self):
        lz4 = ir_datasets.lazy_libs.lz4_frame()
        with self._streamer.stream() as stream:
            yield lz4.frame.open(stream, 'rb')


class ZipExtract:
    def __init__(self, dlc, zip_path):
        self.dlc = dlc
        self.zip_path = zip_path

    def path(self, force=True):
        return self.dlc.path(force)

    @contextlib.contextmanager
    def stream(self):
        with contextlib.ExitStack() as ctxt:
            with _logger.duration('opening zip file'):
                zipf = ctxt.enter_context(ZipFile(self.path()))
                result = zipf.open(self.zip_path)
            yield result


class ZipExtractCache:
    def __init__(self, dlc, extract_path):
        self.dlc = dlc
        self.extract_path = extract_path

    def path(self, force=True):
        if force and not os.path.exists(self.extract_path):
            try:
                with ZipFile(self.dlc.path()) as zipf:
                    zipf.extractall(self.extract_path)
            except:
                if os.path.exists(self.extract_path):
                    shutil.rmtree(self.extract_path)
                raise
        return self.extract_path

    def stream(self):
        raise NotImplementedError


class StringFile:
    def __init__(self, contents, path='MOCK'):
        if isinstance(contents, str):
            contents = contents.encode() # to bytes
        self.contents = contents
        self._path = path

    def path(self, force=True):
        return self._path

    @contextlib.contextmanager
    def stream(self):
        yield io.BytesIO(self.contents)


class PackageDataFile:
    def __init__(self, path, package='ir_datasets'):
        self._package = package
        self._path = path

    def path(self, force=True):
        return self._path

    @contextlib.contextmanager
    def stream(self):
        import pkgutil
        data = pkgutil.get_data(self._package, self._path)
        yield io.BytesIO(data)
