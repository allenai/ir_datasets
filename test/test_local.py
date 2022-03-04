import unittest
import ir_datasets


class TestLocal(unittest.TestCase):
    def test_local(self):
        docs = [
            {'doc_id': '1', 'text': 'hello world'},
            {'doc_id': '2', 'text': 'some document'}
        ]
        queries = [
            {'query_id': 'Q1', 'text': 'search'},
            {'query_id': 'Q2', 'text': 'information retrieval'}
        ]
        qrels = [
            {'query_id': 'Q1', 'doc_id': '1', 'relevance': 1},
            {'query_id': 'Q1', 'doc_id': '2', 'relevance': 2},
            {'query_id': 'Q2', 'doc_id': '2', 'relevance': 0},
        ]
        try:
            ir_datasets.create_local_dataset('_testlocal', docs=docs)
            ir_datasets.create_local_dataset('_testlocal/subset', docs='_testlocal', queries=queries, qrels=qrels)

            test_docs = list(ir_datasets.load('_testlocal').docs)
            self.assertEqual(test_docs[0].doc_id, '1')
            self.assertEqual(test_docs[0].text, 'hello world')
            self.assertEqual(test_docs[1].doc_id, '2')
            self.assertEqual(test_docs[1].text, 'some document')

            test_docs = list(ir_datasets.load('_testlocal/subset').docs)
            self.assertEqual(test_docs[0].doc_id, '1')
            self.assertEqual(test_docs[0].text, 'hello world')
            self.assertEqual(test_docs[1].doc_id, '2')
            self.assertEqual(test_docs[1].text, 'some document')

            self.assertEqual(ir_datasets.load('_testlocal').docs.lookup('1').text, 'hello world')
            self.assertEqual(ir_datasets.load('_testlocal').docs.lookup('2').text, 'some document')

            self.assertEqual(ir_datasets.load('_testlocal/subset').docs.lookup('1').text, 'hello world')
            self.assertEqual(ir_datasets.load('_testlocal/subset').docs.lookup('2').text, 'some document')

            test_queries = list(ir_datasets.load('_testlocal/subset').queries)
            self.assertEqual(test_queries[0].query_id, 'Q1')
            self.assertEqual(test_queries[0].text, 'search')
            self.assertEqual(test_queries[1].query_id, 'Q2')
            self.assertEqual(test_queries[1].text, 'information retrieval')

            test_qrels = list(ir_datasets.load('_testlocal/subset').qrels)
            self.assertEqual(test_qrels[0].query_id, 'Q1')
            self.assertEqual(test_qrels[0].doc_id, '1')
            self.assertEqual(test_qrels[0].relevance, 1)
            self.assertEqual(test_qrels[1].query_id, 'Q1')
            self.assertEqual(test_qrels[1].doc_id, '2')
            self.assertEqual(test_qrels[1].relevance, 2)
            self.assertEqual(test_qrels[2].query_id, 'Q2')
            self.assertEqual(test_qrels[2].doc_id, '2')
            self.assertEqual(test_qrels[2].relevance, 0)
        finally:
            import gc
            gc.collect()
            ir_datasets.delete_local_dataset('_testlocal')
            gc.collect()
            ir_datasets.delete_local_dataset('_testlocal/subset')


if __name__ == '__main__':
    unittest.main()
