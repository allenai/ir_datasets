import os
import tempfile
import contextlib
import functools
import shutil
from pathlib import Path
from fnmatch import fnmatch
import tarfile
import atexit
import gzip
import bz2
import io
from zipfile import ZipFile
from enum import Enum
import ir_datasets
from ir_datasets import util


__all__ = ['Fio', 'FioStream', 'IterStream', 'Cache', 'TarExtract', 'TarExtractAll', 'RelativePath', 'GzipExtract', 'ZipExtract', 'ZipExtractCache', 'StringFile']


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


class FioError(IOError):
    pass


class FioType(Enum):
    REGULAR_FILE = 0
    DIRECTORY = 1
    STREAM = 2


class FioAvailability(Enum):
    UNAVAILABLE = 0
    AVAILABLE = 1
    PROCURABLE = 2


class Fio:
    _REGISTERED_CHAINS = {}
    @classmethod
    def register(cls, name, op):
        if name in cls._REGISTERED_CHAINS:
            raise KeyError(f'{name} already registered')
        cls._REGISTERED_CHAINS[name] = op

    @classmethod
    def register_chainable(cls, chainable):
        assert chainable.CHAIN_NAME is not None
        cls.register(chainable.CHAIN_NAME, chainable.chain)
        return chainable

    def __getattr__(self, attr):
        if attr in self._REGISTERED_CHAINS:
            return self._REGISTERED_CHAINS[attr](self)
        raise AttributeError(attr)

    def io_type(self) -> FioType:
        raise NotImplementedError()

    def availability(self) -> FioAvailability:
        raise NotImplementedError()


class FioChainable:
    CHAIN_NAME = None
    CHAIN_INP_TYPES = None

    def __init__(self, inp):
        self.inp = inp
        if self.CHAIN_INP_TYPES is not None and hasattr(self.inp, 'io_type'):
            assert self.inp.io_type() in self.CHAIN_INP_TYPES, f"{self.inp} not of type {self.CHAIN_INP_TYPES}"

    @classmethod
    def chain(cls, chain):
        return functools.partial(cls, chain)

    def __repr__(self):
        return f'{repr(self.inp)}.{self.CHAIN_NAME}({self._chain_args()})'

    def _chain_args(self) -> str:
        return f''


class FioRegularFile(Fio):
    def path(self, *, hard=True) -> Path:
        raise NotImplementedError()

    @contextlib.contextmanager
    def stream(self):
        with self.path().open('rb') as f:
            yield f

    def io_type(self) -> FioType:
        return FioType.REGULAR_FILE

    def availability(self) -> FioAvailability:
        if self.path(hard=False).is_file():
            return FioAvailability.AVAILABLE
        elif isinstance(self, FioChainable):
            return {
                FioAvailability.UNAVAILABLE: FioAvailability.UNAVAILABLE,
                FioAvailability.AVAILABLE: FioAvailability.PROCURABLE,
                FioAvailability.PROCURABLE: FioAvailability.PROCURABLE,
            }[self.inp.availability()]
        return FioAvailability.UNAVAILABLE


class FioStream(Fio):
    def stream(self):
        raise NotImplementedError()

    def io_type(self) -> FioType:
        return FioType.STREAM

    def availability(self) -> FioAvailability:
        if isinstance(self, FioChainable):
            return {
                FioAvailability.UNAVAILABLE: FioAvailability.UNAVAILABLE,
                FioAvailability.AVAILABLE: FioAvailability.PROCURABLE,
                FioAvailability.PROCURABLE: FioAvailability.PROCURABLE,
            }[self.inp.availability()]
        return FioAvailability.PROCURABLE


class FioDirectory(Fio):
    def path(self, *, hard=True) -> Path:
        raise NotImplementedError()

    def io_type(self) -> FioType:
        return FioType.DIRECTORY

    def availability(self) -> FioAvailability:
        if self.path(hard=False).is_dir():
            return FioAvailability.AVAILABLE
        elif isinstance(self, FioChainable):
            return {
                FioAvailability.UNAVAILABLE: FioAvailability.UNAVAILABLE,
                FioAvailability.AVAILABLE: FioAvailability.PROCURABLE,
                FioAvailability.PROCURABLE: FioAvailability.PROCURABLE,
            }[self.inp.availability()]
        return FioAvailability.UNAVAILABLE


@Fio.register_chainable
class Cache(FioRegularFile, FioChainable):
    CHAIN_NAME = 'cache'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    def __init__(self, inp, path):
        super().__init__(inp)
        self._path = Path(path)

    def path(self, *, hard=True):
        if hard and not self._path.exists():
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
                    with self.inp.stream() as stream:
                        shutil.copyfileobj(stream, f)
                    f.close() # close file before move... Needed because of Windows
                    shutil.move(f.name, self._path)
                finally:
                    if Path(f.name).exists():
                        Path(f.name).unlink()
        return self._path

    def _chain_args(self):
        return str(self._path)


@Fio.register_chainable
class TarExtract(FioStream, FioChainable):
    CHAIN_NAME = 'un_tar'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    def __init__(self, inp, tar_path, compression='gz'):
        super().__init__(inp)
        self._tar_path = tar_path
        self._compression = compression

    @contextlib.contextmanager
    def stream(self):
        with contextlib.ExitStack() as ctxt, self.inp.stream() as stream:
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

    def _chain_args(self):
        return f'{repr(self._tar_path)}, compression={repr(self._compression)}'


@Fio.register_chainable
class TarExtractAll(FioDirectory, FioChainable):
    CHAIN_NAME = 'un_tar_all'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    def __init__(self, inp, extract_path, compression='gz', path_globs=None):
        super().__init__(inp)
        self._extract_path = Path(extract_path)
        self._compression = compression
        self._path_globs = path_globs

    def path(self, *, hard=True):
        if hard and not self._extract_path.exists():
            try:
                with self.inp.stream() as stream, \
                     tarfile.open(fileobj=stream, mode=f'r|{self._compression or ""}') as tarf, \
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

    def _chain_args(self):
        return f'{str(self._extract_path)}, compression={repr(self._compression)}, path_globs={repr(self._path_globs)}'


@Fio.register_chainable
class RelativePath(FioRegularFile, FioChainable):
    CHAIN_NAME = 'join'
    CHAIN_INP_TYPES = (FioType.DIRECTORY,)
    def __init__(self, inp, path):
        super().__init__(inp)
        self._path = Path(path)

    def path(self, *, hard=True):
        result = self.inp.path(hard=hard) / self._path
        if hard and not result.exists():
            raise IOError(f'{result} does not exist')
        return result

    def availability(self):
        if (self.inp.path(hard=False) / self._path).exists():
            return FioAvailability.AVAILABLE
        if self.inp.availability() == FioAvailability.AVAILABLE:
            return FioAvailability.UNAVAILABLE # directory should be there, but the file is not, so this file is unavailable
        if self.inp.availability() == FioAvailability.PROCURABLE:
            return FioAvailability.PROCURABLE
        return FioAvailability.UNAVAILABLE

    def _chain_args(self):
        return str(self._path)


@Fio.register_chainable
class ReTar(FioRegularFile, FioChainable):
    CHAIN_NAME = 'filter_tar'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    def __init__(self, inp, output_file, keep_globs, compression='gz'):
        super().__init__(inp)
        self._output_file = Path(output_file)
        self._keep_globs = keep_globs
        self._compression = compression

    def path(self, *, hard=True):
        # There may be a way to do this that is fully streamable, but I think in most cases,
        # we'd want the filtered tar to be directory saved to disk after anway.
        if hard and not self._output_file.exists():
            with contextlib.ExitStack() as ctxt, self.inp.stream() as stream:
                ctxt.enter_context(_logger.duration('filtering tar file'))
                outf = ctxt.enter_context(util.finialized_file(self._output_file, 'wb'))
                # IMPORTANT: open this file in streaming mode (| in mode). This means that the
                # content need not be written to disk or be fully read.
                o_tarf = ctxt.enter_context(tarfile.open(fileobj=outf, mode=f'w|{self._compression or ""}'))
                i_tarf = ctxt.enter_context(tarfile.open(fileobj=stream, mode=f'r|{self._compression or ""}'))
                for block in i_tarf:
                    if any(fnmatch(block.name, g) for g in self._keep_globs):
                        o_tarf.addfile(block, i_tarf.extractfile(block))
                        _logger.info(f'extracted {block.name}')
        return self._output_file

    def _chain_args(self):
        return f'{repr(str(self._output_file))}, keep_globs={repr(self._keep_globs)}, compression={repr(self._compression)}'


@Fio.register_chainable
class GzipExtract(FioStream, FioChainable):
    CHAIN_NAME = 'un_gzip'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    @contextlib.contextmanager
    def stream(self):
        with self.inp.stream() as stream:
            yield gzip.GzipFile(fileobj=stream)


@Fio.register_chainable
class Bz2Extract(FioStream, FioChainable):
    CHAIN_NAME = 'un_bz2'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    @contextlib.contextmanager
    def stream(self):
        with self.inp.stream() as stream:
            yield bz2.BZ2File(stream)


@Fio.register_chainable
class Lz4FrameExtract(FioStream, FioChainable):
    CHAIN_NAME = 'un_lz4'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    @contextlib.contextmanager
    def stream(self):
        LZ4FrameFile = ir_datasets.lazy_libs.lz4_frame().frame.LZ4FrameFile
        with self.inp.stream() as stream:
            yield LZ4FrameFile(stream, mode='r')


@Fio.register_chainable
class VerifyHash(FioStream, FioChainable):
    CHAIN_NAME = 'verify_hash'
    CHAIN_INP_TYPES = (FioType.STREAM, FioType.REGULAR_FILE)
    def __init__(self, inp, expected, algo='md5'):
        super().__init__(inp)
        self.expected = expected
        self.algo = algo

    @contextlib.contextmanager
    def stream(self):
        with self.inp.stream() as stream:
            yield util.HashStream(stream, self.expected, algo=self.algo)

    def _chain_args(self):
        return f'expected={repr(self.expected)}, algo={repr(self.algo)}'


@Fio.register_chainable
class ZipExtract(FioStream, FioChainable):
    CHAIN_NAME = 'un_zip'
    CHAIN_INP_TYPES = (FioType.REGULAR_FILE, FioType.STREAM)
    def __init__(self, inp, zip_path):
        super().__init__(inp)
        self.zip_path = zip_path

    @contextlib.contextmanager
    def stream(self):
        with contextlib.ExitStack() as ctxt:
            with _logger.duration('opening zip file'):
                if self.inp.io_type() == FioType.STREAM:
                    zipf_inp = ctxt.enter_context(self.inp.stream())
                elif self.inp.io_type() == FioType.REGULAR_FILE:
                    zipf_inp = self.inp.path()
                zipf = ctxt.enter_context(ZipFile(zipf_inp))
                result = zipf.open(self.zip_path)
            yield result

    def _chain_args(self):
        return repr(self.zip_path)


@Fio.register_chainable
class ZipExtractCache(FioDirectory, FioChainable):
    CHAIN_NAME = 'un_zip_all'
    CHAIN_INP_TYPES = (FioType.REGULAR_FILE, FioType.STREAM)
    def __init__(self, inp, extract_path):
        super().__init__(inp)
        self.extract_path = Path(extract_path)

    def path(self, *, hard=True):
        if not hard:
            return self.extract_path
        if not os.path.exists(self.extract_path):
            try:
                if self.inp.io_type() == FioType.STREAM:
                    with self.inp.stream() as stream, \
                         ZipFile(stream) as zipf:
                        zipf.extractall(self.extract_path)
                else:
                    with ZipFile(self.inp.path()) as zipf:
                        zipf.extractall(self.extract_path)
            except:
                if os.path.exists(self.extract_path):
                    shutil.rmtree(self.extract_path)
                raise
        return self.extract_path

    def _chain_args(self):
        return str(self.extract_path)


class String(FioStream):
    def __init__(self, contents):
        if isinstance(contents, str):
            contents = contents.encode() # to bytes
        self.contents = contents

    @contextlib.contextmanager
    def stream(self):
        yield io.BytesIO(self.contents)

    def availability(self) -> FioAvailability:
        return FioAvailability.AVAILABLE

    def __repr__(self):
        if len(self.contents) > 100:
            return (f'String({repr(self.contents[:50])}'
                    f' [...{len(self.contents)-100} bytes...] '
                    f'{repr(self.contents[-50:])})')
        return f'String({repr(self.contents)})'


StringFile = String # backwards compat.


class File(FioRegularFile):
    def __init__(self, path):
        self._path = Path(path)

    def path(self, *, hard=True):
        if hard and not self._path.is_file():
            raise IOError(f'{self._path} not a file')
        return self._path

    def __repr__(self):
        return f'File({repr(str(self._path))})'


class Directory(FioDirectory):
    def __init__(self, path):
        self._path = Path(path)

    def path(self, *, hard=True):
        if hard and not self._path.is_dir():
            raise IOError(f'{self._path} not a directory')
        return self._path

    def __repr__(self):
        return f'Directory({repr(str(self._path))})'


class Concat(FioStream):
    def __init__(self, *inputs):
        self.inputs = inputs

    @contextlib.contextmanager
    def stream(self):
        yield IterStream(self._iter_stream_data())

    def _iter_stream_data(self):
        for inp in self.inputs:
            with inp.stream() as s:
                while True:
                    chunk = s.read(io.DEFAULT_BUFFER_SIZE)
                    if chunk == b'':
                        break
                    yield chunk

    def availability(self):
        max_avail = FioAvailability.AVAILABLE
        for inp in self.inputs:
            av = inp.availability()
            if av == FioAvailability.UNAVAILABLE:
                max_avail = av
            elif av == FioAvailability.PROCURABLE and max_avail == FioAvailability.AVAILABLE:
                max_avail = FioAvailability.PROCURABLE
        return max_avail


def _cleanup_tmp(file):
    try:
        os.remove(file.name)
    except FileNotFoundError:
        pass


class Alternatives(FioRegularFile):
    def __init__(self, *alternatives, path=None):
        self.alternatives = alternatives
        self._path = Path(path) if path is not None else None

    def path(self, *, hard=True) -> Path:
        if self._path is None:
            tmpfile = tempfile.NamedTemporaryFile(delete=False, dir=util.tmp_path())
            atexit.register(_cleanup_tmp, tmpfile)
            self._path = Path(tmpfile.name)
            self._path.unlink()
        if not hard or self._path.exists():
            return self._path
        avs, exs = [], []
        # first pass: anything that claims to be fully available
        for alt in self.alternatives:
            av = alt.availability()
            if av == FioAvailability.AVAILABLE:
                try:
                    self._apply(alt)
                    return self._path
                except Exception as ex:
                    wrapper_ex = FioError(f'{alt} failed')
                    wrapper_ex.__cause__ = ex
                    if exs:
                        wrapper_ex.__cause__.__cause__ = exs[-1]
                    exs.append(wrapper_ex)
            avs.append(av)
        # second pass: anything that can be procured
        for av, alt in zip(avs, self.alternatives):
            if av == FioAvailability.PROCURABLE:
                try:
                    self._apply(alt)
                    return self._path
                except Exception as ex:
                    wrapper_ex = FioError(f'{alt} failed')
                    wrapper_ex.__cause__ = ex
                    if exs:
                        wrapper_ex.__cause__.__cause__ = exs[-1]
                    exs.append(wrapper_ex)
        if len(exs) == 1:
            raise exs[0]
        top_ex = RuntimeError("all sources unavailable or failed")
        ex = top_ex
        if exs:
            ex.__cause__ = exs[-1]
        raise top_ex

    def _apply(self, alt):
        if self._path.is_symlink():
            self._path.unlink()
        if not self._path.parent.exists():
            self._path.parent.mkdir(parents=True)
        if alt.io_type() == FioType.STREAM:
            # special case: just a hash check of an existing file. Link instead of copy & allow user to skip hash check with ctrl+c
            if isinstance(alt, VerifyHash) and alt.inp.io_type() == FioType.REGULAR_FILE:
                try:
                    with alt.stream() as stream, \
                         _logger.pbar_raw(desc='verifying hash (ctrl+c to skip)', total=alt.inp.path().stat().st_size, unit='B', unit_scale=True) as pbar:
                        while True:
                            buf = stream.read(io.DEFAULT_BUFFER_SIZE)
                            if len(buf) == 0:
                                break
                            pbar.update(len(buf))
                except KeyboardInterrupt:
                    _logger.info('user skipped hash verification of file')
                target = alt.inp.path()
                src_parents, tgt_parents = splitpath(self._path), splitpath(target)
                while src_parents[0] == tgt_parents[0]:
                    src_parents.pop(0)
                    tgt_parents.pop(0)
                for _ in src_parents[:-1]:
                    tgt_parents = ['..'] + tgt_parents
                target = Path(*tgt_parents)
                self._path.symlink_to(target) # link the files
            else:
                with alt.stream() as stream, \
                     util.finialized_file(self._path, 'wb') as fout:
                    shutil.copyfileobj(stream, fout)
        elif alt.io_type() == FioType.REGULAR_FILE:
            self._path.symlink_to(alt.path()) # link the file
        elif alt.io_type() == FioType.DIRECTORY:
            self._path.symlink_to(alt.path(), target_is_directory=True) # link the directory
        else:
            raise RuntimeError('unknown io_type')

    def availability(self) -> FioAvailability:
        if self._path is not None and self._path.exists():
            return FioAvailability.AVAILABLE
        elif any(a.availability() in (FioAvailability.AVAILABLE, FioAvailability.PROCURABLE) for a in self.alternatives):
            return FioAvailability.PROCURABLE
        return FioAvailability.UNAVAILABLE

    def __repr__(self):
        return f'Alternatives({", ".join(repr(a) for a in self.alternatives)}, path={repr(self._path)})'


def splitpath(path):
    parts=[]
    (path, tail)=os.path.split( path)
    while path and tail:
         parts.append( tail)
         (path,tail)=os.path.split(path)
    parts.append( os.path.join(path,tail) )
    return list(map(os.path.normpath, parts))[::-1]
