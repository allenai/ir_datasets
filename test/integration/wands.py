import re
import unittest
from ir_datasets.datasets.wapo import WapoDoc, WapoDocMedia, TrecBackgroundLinkingQuery
from ir_datasets.formats import GenericQrel, GenericQuery, TrecQuery, TrecQrel
from .base import DatasetIntegrationTest
from ir_datasets.datasets.wands import WandsDoc


class TestWands(DatasetIntegrationTest):
    def test_docs(self):
        self._build_test_docs('wands')


if __name__ == '__main__':
    unittest.main()
