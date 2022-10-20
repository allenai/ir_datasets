import os
from typing import NamedTuple, Tuple
import shutil
import unittest
from ir_datasets.formats import TsvDocs, TsvQueries, TsvDocPairs
from ir_datasets.util import StringFile


class TestTsv(unittest.TestCase):

    def test_core(self):
        class data_type(NamedTuple):
            doc_id: str
            field1: str
            field2: str
        mock_file = StringFile('''
123\tsome field\tanother field
123\t  repeated  entry \tshouldn't filter

456\tanother query\tsomething
'''.lstrip())
        expected_results = [
            data_type('123', 'some field', 'another field'),
            data_type('123', '  repeated  entry ', 'shouldn\'t filter'),
            data_type('456', 'another query', 'something'),
        ]

        queries = TsvQueries(mock_file, data_type)
        self.assertEqual(queries.queries_path(), 'MOCK')
        self.assertEqual(list(queries.queries_iter()), expected_results)

        docs = TsvDocs(mock_file, data_type)
        self.assertEqual(docs.docs_path(), 'MOCK')
        self.assertEqual(list(docs.docs_iter()), expected_results)

        docpairs = TsvDocPairs(mock_file, data_type)
        self.assertEqual(docpairs.docpairs_path(), 'MOCK')
        self.assertEqual(list(docpairs.docpairs_iter()), expected_results)


    def test_too_many_columns(self):
        class data_type(NamedTuple):
            doc_id: str
            field1: str
            field2: str
        mock_file = StringFile('''
123\tsome field\tanother field
123\trepeated entry\tshouldn't filter\ttoo many columns

456\tanother query\tsomething
'''.strip())

        queries = TsvQueries(mock_file, data_type)
        with self.assertRaises(RuntimeError):
            list(queries.queries_iter())

        docs = TsvDocs(mock_file, data_type)
        with self.assertRaises(RuntimeError):
            list(docs.docs_iter())

        docpairs = TsvDocPairs(mock_file, data_type)
        with self.assertRaises(RuntimeError):
            list(docpairs.docpairs_iter())


    def test_too_few_columns(self):
        class data_type(NamedTuple):
            doc_id: str
            field1: str
            field2: str
        mock_file = StringFile('''
123\tsome field\tanother field
123\ttoo few fields

456\tanother query\tsomething
'''.strip())

        queries = TsvQueries(mock_file, data_type)
        with self.assertRaises(RuntimeError):
            list(queries.queries_iter())

        docs = TsvDocs(mock_file, data_type)
        with self.assertRaises(RuntimeError):
            list(docs.docs_iter())

        docpairs = TsvDocPairs(mock_file, data_type)
        with self.assertRaises(RuntimeError):
            list(docpairs.docpairs_iter())

    def test_flex_columns(self):
        class data_type(NamedTuple):
            doc_id: str
            field1: str
            field2: Tuple[str, ...]
        mock_file = StringFile('''
123\tsome field\tanother field
123\ttoo few fields

456\tanother query\tsomething
456\tanother query\tsomething\ttoo many fields\teven more
'''.strip())

        expected_results = [
            data_type('123', 'some field', ('another field',)),
            data_type('123', 'too few fields', ()),
            data_type('456', 'another query', ('something',)),
            data_type('456', 'another query', ('something', 'too many fields', 'even more')),
        ]

        queries = TsvQueries(mock_file, data_type)
        self.assertEqual(queries.queries_path(), 'MOCK')
        self.assertEqual(list(queries.queries_iter()), expected_results)

        docs = TsvDocs(mock_file, data_type)
        self.assertEqual(docs.docs_path(), 'MOCK')
        self.assertEqual(list(docs.docs_iter()), expected_results)

        docpairs = TsvDocPairs(mock_file, data_type)
        self.assertEqual(docpairs.docpairs_path(), 'MOCK')
        self.assertEqual(list(docpairs.docpairs_iter()), expected_results)

    def tearDown(self):
        if os.path.exists('MOCK.pklz4'):
            shutil.rmtree('MOCK.pklz4')


if __name__ == '__main__':
    unittest.main()
