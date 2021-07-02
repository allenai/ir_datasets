import re
import unittest
from ir_datasets.formats import TrecQrel
from ir_datasets.datasets.trec_fair_2021 import FairTrecDoc, FairTrecQuery
from base import DatasetIntegrationTest


class TestFairTrec(DatasetIntegrationTest):
    def test_docs(self):
        self._build_test_docs("trec_fair_2021",include_count=False)
        

    def test_queries(self):
        self._build_test_queries("trec_fair_2021")
        

    def test_qrels(self):
        self._build_test_qrels("trec_fair_2021")
       


if __name__ == '__main__':
    unittest.main()
