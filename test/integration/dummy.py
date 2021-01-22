import re
import os
import shutil
import unittest
import ir_datasets
from ir_datasets.formats import GenericQuery, GenericDoc, TrecQrel
from .base import DatasetIntegrationTest


class TestDummy(DatasetIntegrationTest):
    def test_dummy_docs(self):
        dataset = ir_datasets.create_dataset(
            docs_tsv='test/dummy/docs.tsv',
            queries_tsv='test/dummy/queries.tsv',
            qrels_trec='test/dummy/qrels'
        )
        self._test_docs(dataset, count=15, items={
            0: GenericDoc('T1', 'CUT, CAP AND BALANCE. TAXED ENOUGH ALREADY!'),
            9: GenericDoc('T10', 'Perhaps this is the kind of thinking we need in Washington ...'),
            14: GenericDoc('T15', "I've been visiting Trump Int'l Golf Links Scotland and the course will be unmatched anywhere in the world. Spectacular!"),
        })

    def test_dummy_queries(self):
        dataset = ir_datasets.create_dataset(
            docs_tsv='test/dummy/docs.tsv',
            queries_tsv='test/dummy/queries.tsv',
            qrels_trec='test/dummy/qrels'
        )
        self._test_qrels(dataset, count=55, items={
            0: TrecQrel('1', 'T1', 0, '0'),
            9: TrecQrel('1', 'T11', 0, '0'),
            54: TrecQrel('4', 'T15', 0, '0'),
        })

    def test_dummy_qrels(self):
        dataset = ir_datasets.create_dataset(
            docs_tsv='test/dummy/docs.tsv',
            queries_tsv='test/dummy/queries.tsv',
            qrels_trec='test/dummy/qrels'
        )
        self._test_queries(dataset, count=4, items={
            0: GenericQuery('1', 'republican party'),
            3: GenericQuery('4', 'media'),
        })

    def tearDown(self):
        if os.path.exists('test/dummy/docs.tsv.pklz4'):
            shutil.rmtree('test/dummy/docs.tsv.pklz4')


if __name__ == '__main__':
    unittest.main()
