import io
import hashlib
import ir_datasets


__all__ = ['HashVerificationError', 'HashVerifier', 'HashStream']
_logger = ir_datasets.log.easy()


class HashVerificationError(IOError):
    pass


class HashVerifier:
    def __init__(self, expected, algo='md5'):
        self.expected = expected
        self.algo = algo
        self.hasher = None

    def update(self, b):
        self.hasher.update(b)

    def __enter__(self):
        self.hasher = hashlib.new(self.algo)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_val is None:
            h = self.hasher.hexdigest().lower()
            if self.expected is not None:
                if self.expected.lower() != h:
                    raise HashVerificationError(f"Expected {self.algo} hash to be {self.expected} but got {h}")
            else:
                _logger.warn(f'consider adding expected_{self.algo}={repr(h)} to ensure data integrity')


class HashStream(io.RawIOBase):
    def __init__(self, stream, expected, algo='md5'):
        super().__init__()
        self._stream = stream
        self._verifier = HashVerifier(expected, algo)
        self._verifier.__enter__()

    def readable(self):
        return True

    def readinto(self, b):
        count = self._stream.readinto(b)
        self._verifier.update(b[:count])
        if count == 0:
            self._verifier.__exit__(None, None, None)
        return count
