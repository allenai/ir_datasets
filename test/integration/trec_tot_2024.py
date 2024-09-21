import re
import unittest
from ir_datasets.formats import TrecQrel
from ir_datasets.datasets.trec_tot import TipOfTheTongueDoc, TipOfTheTongueQuery
from test.integration.base import DatasetIntegrationTest
import ir_datasets

print(len([i for i in ir_datasets.load('trec-tot/2024').docs_iter()]))

class TestTipOfTheTongue(DatasetIntegrationTest):
    def test_tip_of_the_tongue_docs(self):
         self._test_docs('trec-tot/2024', count=231852, items={})

    def test_test_tip_of_the_tongue_qrels_train(self):
        #self._test_qrels('trec-tot/2024/test', count=150, items={
        #    0: TrecQrel('763', '16742289', 1, '0'),
        #    9: TrecQrel('293', '142456', 1, '0'),
        #    149: TrecQrel('828', '30672517', 1, '0'),
        #})
        pass


if __name__ == '__main__':
    unittest.main()

