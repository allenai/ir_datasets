import re
import unittest
from ir_datasets.formats import TrecQrel
from ir_datasets.datasets.trec_tip_of_the_tongue import TipOfTheTongueDocument, TipOfTheTongueQuery
from .base import DatasetIntegrationTest
import ir_datasets


class TestTipOfTheTongue(DatasetIntegrationTest):
    def test_tip_of_the_tongue_docs(self):
        self._test_docs('trec-tip-of-the-tongue', count=231852, items={})
        doc = ir_datasets.load('trec-tip-of-the-tongue').docs_store().get('3837')
        
        self.assertEqual(doc.doc_id, '3837')
        self.assertEqual(doc.page_title,  'Blazing Saddles')

    def test_test_tip_of_the_tongue_queries_train(self):
        self._test_queries('trec-tip-of-the-tongue/train', count=150)
        query = list(ir_datasets.load('trec-tip-of-the-tongue/train').queries_iter())[0]
        self.assertEqual('763', query.id)
        
        self.assertTrue(query.text.startswith("Very rare movie that is scifi/dystopian/experimental/surreal. It’s like Stalker meets el Topo meets"))


    def test_test_tip_of_the_tongue_queries_dev(self):
        self._test_queries('trec-tip-of-the-tongue/dev', count=150)
        query = list(ir_datasets.load('trec-tip-of-the-tongue/dev').queries_iter())[0]
        self.assertEqual('152', query.id)
        self.assertTrue(query.text.startswith("Movie from  the early 2000s I believe about three people living in an apartment"))


    def test_test_tip_of_the_tongue_qrels_train(self):
        self._test_qrels('trec-tip-of-the-tongue/train', count=150, items={
            0: TrecQrel('763', '16742289', 1, '0'),
            9: TrecQrel('293', '142456', 1, '0'),
            149: TrecQrel('828', '30672517', 1, '0'),
        })


    def test_test_tip_of_the_tongue_qrels_dev(self):
        self._test_qrels('trec-tip-of-the-tongue/dev', count=150, items={
            0: TrecQrel('152', '1940119', 1, '0'),
            9: TrecQrel('813', '2310134', 1, '0'),
            149: TrecQrel('521', '2911505', 1, '0'),
        })


if __name__ == '__main__':
    unittest.main()
