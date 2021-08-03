import os
import tempfile
import unittest
from pathlib import Path
from ir_datasets.util import fio, HashVerificationError, Download


class TestFIO(unittest.TestCase):
    def test_string(self):
        f = fio.StringFile(b'some text')
        with f.stream() as s:
            self.assertEqual(s.read(), b'some text')
            self.assertEqual(f.availability(), fio.FioAvailability.AVAILABLE)

        f = fio.StringFile('some text')
        with f.stream() as s:
            self.assertEqual(s.read(), b'some text')
            self.assertEqual(f.availability(), fio.FioAvailability.AVAILABLE)

    def test_file(self):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'some text')
            tmp.flush()
            f = fio.File(tmp.name)
            with f.stream() as s:
                self.assertEqual(s.read(), b'some text')
            self.assertEqual(f.availability(), fio.FioAvailability.AVAILABLE)
        self.assertEqual(f.availability(), fio.FioAvailability.UNAVAILABLE)
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'some text')
            tmp.flush()
            f = fio.File(tmp.name)
            self.assertEqual(str(f.path()), tmp.name)
            self.assertEqual(f.availability(), fio.FioAvailability.AVAILABLE)
        self.assertEqual(f.availability(), fio.FioAvailability.UNAVAILABLE)

    def test_directory(self):
        with tempfile.TemporaryDirectory() as tmp:
            f = fio.Directory(tmp)
            self.assertEqual(str(f.path()), tmp)
            self.assertEqual(f.availability(), fio.FioAvailability.AVAILABLE)
        self.assertEqual(f.availability(), fio.FioAvailability.UNAVAILABLE)

    def test_download(self):
        f = Download("https://macavaney.us/test.txt")
        self.assertEqual(f.availability(), fio.FioAvailability.PROCURABLE)
        with f.stream() as s:
            self.assertEqual(s.read(), b"test file for unit tests\n")

    def test_concat(self):
        f = fio.Concat(fio.StringFile('foo'), fio.StringFile('bar'), fio.StringFile('baz'))
        self.assertEqual(f.availability(), fio.FioAvailability.AVAILABLE)
        with f.stream() as s:
            self.assertEqual(s.read(), b"foobarbaz")

    def test_alternatives(self):
        with tempfile.NamedTemporaryFile() as tmp:
            tmp.write(b'bar')
            tmp.flush()
            f = fio.File(tmp.name)
            f_stream = f.verify_hash('37b51d194a7513e45b56f6524f2d51f2')
            f_dne = fio.File('does_not_exist.txt')
            s = fio.String('foo')
            s2 = fio.String('baz')
            stream = s.verify_hash('acbd18db4cc2f85cedef654fccc4a4d8')
            stream2 = s2.verify_hash('73feffa4b7f6bb68e44cf984c85f6e88')
            stream_invalid = s.verify_hash('not valid hash')
            stream2_invalid = s2.verify_hash('not valid hash')

            # both available; check priority
            alt = fio.Alternatives(s, f)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'foo')

            alt = fio.Alternatives(f, s)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'bar')

            # first one procurable, second avaiable; should go for second
            alt = fio.Alternatives(stream, f)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'bar')

            # first one unavailable, second avaiable; should go for second
            alt = fio.Alternatives(f_dne, f)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'bar')

            # first one unavailable, second procurable; should go for second
            alt = fio.Alternatives(f_dne, stream)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'foo')

            # both procurable, should go for first
            alt = fio.Alternatives(stream, stream2)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'foo')

            # both procurable, but first results in an error; should go for second
            alt = fio.Alternatives(stream_invalid, stream2)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'baz')

            # both procurable, but both result in an error; should throw error
            alt = fio.Alternatives(stream_invalid, stream2_invalid)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with self.assertRaises(RuntimeError):
                with alt.stream() as fin:
                    pass

            # test special case with hash-verified file; should symlink it to file instad of making a copy
            # (also gives user chance to ctrl+c to skip hash verification)
            alt = fio.Alternatives(f_stream, stream)
            self.assertEqual(alt.availability(), fio.FioAvailability.PROCURABLE)
            with alt.stream() as fin:
                self.assertEqual(fin.read(), b'bar')
            self.assertEqual(alt.path().resolve(), f.path().resolve())

    def test_un_gzip(self):
        f = fio.String(b'\x1f\x8b\x08\x00\xfd\xd5\x06a\x02\xffK\xcb\xcf\x07\x00!es\x8c\x03\x00\x00\x00')
        with f.un_gzip().stream() as stream:
            self.assertEqual(stream.read(), b'foo')

    def test_un_bz2(self):
        f = fio.String(b"BZh91AY&SYI\xfe\xc4\xa5\x00\x00\x00\x01\x00\x01\x00\xa0\x00!\x00\x82,]\xc9\x14\xe1BA'\xfb\x12\x94")
        with f.un_bz2().stream() as stream:
            self.assertEqual(stream.read(), b'foo')

    def test_un_lz4(self):
        f = fio.String(b'\x04"M\x18@@\xc0\x03\x00\x00\x80foo\x00\x00\x00\x00')
        with f.un_lz4().stream() as stream:
            self.assertEqual(stream.read(), b'foo')

    def test_verify_hash(self):
        f = fio.String('foo')
        with f.verify_hash('acbd18db4cc2f85cedef654fccc4a4d8').stream() as s:
            self.assertEqual(s.read(), b'foo')
        with self.assertRaises(HashVerificationError):
            with f.verify_hash('wrong hash').stream() as s:
                s.read()

    def test_un_tar(self):
        f = fio.String(b"\x1f\x8b\x08\x00\x06\xdc\x06a\x02\xff\xed\xd3A\n\x830\x10\x05\xd0\x1c%'(\xa9\x06\xef\xa3\x0b\xb7\x01\x9bB\x8fop%\xae[\xa9\xf8\xde\xe6\x0f\xb3\x1b\x86?\x97\xf2\xa8\x9f\x1a~)5C\xce[6\xc7l\xfa\xdd\xdc\xf6\xcf\x94\x87.\xc4\x14N\xf0~\xd5q\x89\xf1+G\xee\x8f\xbb\x88\xb9\x94\xc0}M\xe3\xf2\x97\xfd\xcf\xbd\xfe\x9f\xf4\x7f%\x00\x00\x00\x00\x00\x00\x00\x00\xb8\xb0\x15]a\xdd\xec\x00(\x00\x00")
        with f.un_tar('foo.txt').stream() as s:
            self.assertEqual(s.read(), b'foo')
        with f.un_tar('bar.txt').stream() as s:
            self.assertEqual(s.read(), b'bar')
        with self.assertRaises(RuntimeError):
            with f.un_tar('baz.txt').stream() as s:
                s.read()

    def test_filter_tar(self):
        f = fio.String(b"\x1f\x8b\x08\x00\x06\xdc\x06a\x02\xff\xed\xd3A\n\x830\x10\x05\xd0\x1c%'(\xa9\x06\xef\xa3\x0b\xb7\x01\x9bB\x8fop%\xae[\xa9\xf8\xde\xe6\x0f\xb3\x1b\x86?\x97\xf2\xa8\x9f\x1a~)5C\xce[6\xc7l\xfa\xdd\xdc\xf6\xcf\x94\x87.\xc4\x14N\xf0~\xd5q\x89\xf1+G\xee\x8f\xbb\x88\xb9\x94\xc0}M\xe3\xf2\x97\xfd\xcf\xbd\xfe\x9f\xf4\x7f%\x00\x00\x00\x00\x00\x00\x00\x00\xb8\xb0\x15]a\xdd\xec\x00(\x00\x00")
        with tempfile.NamedTemporaryFile() as fout:
            os.unlink(fout.name)
            filtered = f.filter_tar(fout.name, ['bar.*'])
            with self.assertRaises(RuntimeError):
                with filtered.un_tar('foo.txt').stream() as s:
                    s.read()
            with filtered.un_tar('bar.txt').stream() as s:
                self.assertEqual(s.read(), b'bar')

    def test_un_tar_all(self):
        f = fio.String(b"\x1f\x8b\x08\x00\x06\xdc\x06a\x02\xff\xed\xd3A\n\x830\x10\x05\xd0\x1c%'(\xa9\x06\xef\xa3\x0b\xb7\x01\x9bB\x8fop%\xae[\xa9\xf8\xde\xe6\x0f\xb3\x1b\x86?\x97\xf2\xa8\x9f\x1a~)5C\xce[6\xc7l\xfa\xdd\xdc\xf6\xcf\x94\x87.\xc4\x14N\xf0~\xd5q\x89\xf1+G\xee\x8f\xbb\x88\xb9\x94\xc0}M\xe3\xf2\x97\xfd\xcf\xbd\xfe\x9f\xf4\x7f%\x00\x00\x00\x00\x00\x00\x00\x00\xb8\xb0\x15]a\xdd\xec\x00(\x00\x00")
        with tempfile.TemporaryDirectory() as d:
            os.rmdir(d)
            fall = f.un_tar_all(d)
            with fall.join('foo.txt').stream() as s:
                self.assertEqual(s.read(), b'foo')
            with fall.join('bar.txt').stream() as s:
                self.assertEqual(s.read(), b'bar')

    def test_un_zip(self):
        f = fio.String(b'PK\x03\x04\x14\x00\x00\x00\x00\x00\x8am\x01S!es\x8c\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00foo.txtfooPK\x03\x04\x14\x00\x00\x00\x00\x00\x8am\x01S\xaa\x8c\xffv\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00bar.txtbarPK\x01\x02\x14\x03\x14\x00\x00\x00\x00\x00\x8am\x01S!es\x8c\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x01\x00\x00\x00\x00foo.txtPK\x01\x02\x14\x03\x14\x00\x00\x00\x00\x00\x8am\x01S\xaa\x8c\xffv\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x01(\x00\x00\x00bar.txtPK\x05\x06\x00\x00\x00\x00\x02\x00\x02\x00j\x00\x00\x00P\x00\x00\x00\x00\x00')
        with f.un_zip('foo.txt').stream() as s:
            self.assertEqual(s.read(), b'foo')
        with f.un_zip('bar.txt').stream() as s:
            self.assertEqual(s.read(), b'bar')
        with self.assertRaises(KeyError):
            with f.un_zip('baz.txt').stream() as s:
                s.read()

    def test_un_zip_all(self):
        f = fio.String(b'PK\x03\x04\x14\x00\x00\x00\x00\x00\x8am\x01S!es\x8c\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00foo.txtfooPK\x03\x04\x14\x00\x00\x00\x00\x00\x8am\x01S\xaa\x8c\xffv\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00bar.txtbarPK\x01\x02\x14\x03\x14\x00\x00\x00\x00\x00\x8am\x01S!es\x8c\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x01\x00\x00\x00\x00foo.txtPK\x01\x02\x14\x03\x14\x00\x00\x00\x00\x00\x8am\x01S\xaa\x8c\xffv\x03\x00\x00\x00\x03\x00\x00\x00\x07\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x80\x01(\x00\x00\x00bar.txtPK\x05\x06\x00\x00\x00\x00\x02\x00\x02\x00j\x00\x00\x00P\x00\x00\x00\x00\x00')
        with tempfile.TemporaryDirectory() as d:
            os.rmdir(d)
            fall = f.un_zip_all(d)
            with fall.join('foo.txt').stream() as s:
                self.assertEqual(s.read(), b'foo')
            with fall.join('bar.txt').stream() as s:
                self.assertEqual(s.read(), b'bar')

    def test_cache(self):
        with tempfile.NamedTemporaryFile() as tmp:
            os.unlink(tmp.name)
            s = fio.String('foo').cache(tmp.name)
            self.assertEqual(s.availability(), fio.FioAvailability.PROCURABLE)
            with s.stream() as stream:
                self.assertEqual(stream.read(), b'foo')
            self.assertEqual(s.availability(), fio.FioAvailability.AVAILABLE)
            with s.stream() as stream:
                self.assertEqual(stream.read(), b'foo')

    def test_join(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            d = fio.Directory(tmp_dir)
            with (Path(tmp_dir) / 'foo.txt').open('wt') as f:
                f.write('foo')
            f = d.join('foo.txt')
            self.assertEqual(f.availability(), fio.FioAvailability.AVAILABLE)
            with f.stream() as fin:
                self.assertEqual(fin.read(), b'foo')
            f = d.join('bar.txt')
            self.assertEqual(f.availability(), fio.FioAvailability.UNAVAILABLE)


if __name__ == '__main__':
    unittest.main()
