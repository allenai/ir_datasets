import tempfile
import unittest
import ir_datasets.util.fileio as fio
from ir_datasets.util import Download


class TestFileIo(unittest.TestCase):
    def test_string(self):
        f = fio.StringFile(b'some text')
        with f.stream() as s:
            self.assertEqual(s.read(), b'some text')
            self.assertEqual(f.availability(), fio.IoAvailability.AVAILABLE)

        f = fio.StringFile('some text')
        with f.stream() as s:
            self.assertEqual(s.read(), b'some text')
            self.assertEqual(f.availability(), fio.IoAvailability.AVAILABLE)

    def test_file(self):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'some text')
            tmp.flush()
            f = fio.File(tmp.name)
            with f.stream() as s:
                self.assertEqual(s.read(), b'some text')
            self.assertEqual(f.availability(), fio.IoAvailability.AVAILABLE)
        self.assertEqual(f.availability(), fio.IoAvailability.UNAVAILABLE)
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'some text')
            tmp.flush()
            f = fio.File(tmp.name)
            self.assertEqual(str(f.path()), tmp.name)
            self.assertEqual(f.availability(), fio.IoAvailability.AVAILABLE)
        self.assertEqual(f.availability(), fio.IoAvailability.UNAVAILABLE)

    def test_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = fio.Directory(tmp)
            self.assertEqual(str(f.path()), tmp)
            self.assertEqual(f.availability(), fio.IoAvailability.AVAILABLE)
        self.assertEqual(f.availability(), fio.IoAvailability.UNAVAILABLE)

    def test_download(self):
        f = Download("https://macavaney.us/test.txt")
        self.assertEqual(f.availability(), fio.IoAvailability.PROCURABLE)
        # with f.stream() as s:
        #     self.assertEqual(s.read(), b"")

    def test_concat(self):
        f = fio.Concat(fio.StringFile('foo'), fio.StringFile('bar'), fio.StringFile('baz'))
        self.assertEqual(f.availability(), fio.IoAvailability.AVAILABLE)
        with f.stream() as s:
            self.assertEqual(s.read(), b"foobarbaz")


if __name__ == '__main__':
    unittest.main()
