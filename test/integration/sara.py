from ir_datasets.formats import GenericQuery, GenericDoc, TrecQrel
from .base import DatasetIntegrationTest

class TestDummy(DatasetIntegrationTest):
    def test_docs(self):
        # Test that the dataset 'dummy' has 15 documents, and test the specific docs at indices 0, 9, and 14
        self._test_docs('dummy', count=15, items={
            0: GenericDoc('T1', 'CUT, CAP AND BALANCE. TAXED ENOUGH ALREADY!'),
            9: GenericDoc('T10', 'Perhaps this is the kind of thinking we need in Washington ...'),
            14: GenericDoc('T15', "I've been visiting Trump Int'l Golf Links Scotland and the course will be unmatched anywhere in the world. Spectacular!"),
        })

    def test_queries(self):
        # Test that the dataset 'dummy' has 4 queries, and test the specific queries at indices 0 and 3
        self._test_queries('dummy', count=4, items={
            0: GenericQuery('1', 'republican party'),
            3: GenericQuery('4', 'media'),
        })

    def test_qrels(self):
        # Test that the dataset 'dummy' has 60 qrels, and test the specific qrels at indices 0, 9, and 59
        self._test_qrels('dummy', count=60, items={
            0: TrecQrel('1', 'T1', 0, '0'),
            9: TrecQrel('1', 'T10', 0, '0'),
            59: TrecQrel('4', 'T15', 0, '0'),
        })