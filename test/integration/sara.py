import ir_datasets
from ir_datasets.formats import GenericQuery, GenericDoc, TrecQrel
from ir_datasets.datasets.sara import SaraDoc
from .base import DatasetIntegrationTest
import unittest
import re

class TestSara(DatasetIntegrationTest):
    def test_docs(self):
        # Test that the dataset 'dummy' has 15 documents, and test the specific docs at indices 0, 9, and 14
        self._test_docs('sara', count=1702, items={
            0: SaraDoc('114715', re.compile('^Message\\-ID: <26804150\\.1075842955435\\.JavaMail\\.evans@thyme>\\\r\nDate: Tue, 29 Aug 2000 09:11:00 \\-0700 \\(PD.{4497}s to me to be bad feelings and crossed\\-wires that would be no\n> good to anyone\\. Reactions\\?\n>\n> Lee\n\n$', flags=48), 0),
            9: SaraDoc('136551', re.compile('^Message\\-ID: <6318224\\.1075858704020\\.JavaMail\\.evans@thyme>\\\r\nDate: Thu, 9 Aug 2001 15:29:50 \\-0700 \\(PDT\\).{2621} after the NERC deadline\\.  That is not deterring NERC from moving forward with the above time frame\\.$', flags=48), 0),
            1701: SaraDoc('175841', re.compile('^Message\\-ID: <29023172\\.1075847627587\\.JavaMail\\.evans@thyme>\\\r\nDate: Wed, 28 Feb 2001 06:49:00 \\-0800 \\(PS.{24748} into the Power Exchange against the debts they hav=\\\r\ne=20\\\r\naccrued due to the retail price cap\\.\\\r\n\\\r\n$', flags=48), 0),
        })

    def test_queries(self):
        # Test that the dataset 'dummy' has 4 queries, and test the specific queries at indices 0 and 3
        self._test_queries('sara', count=150, items={
            0: GenericQuery('1', 'Politicians that decide the plans from state to state'),
            87: GenericQuery('88', 'Enron procurement'),
            122: GenericQuery('123', 'Senators linked to enron')
        })

    def test_qrels(self):
        # Test that the dataset 'dummy' has 60 qrels, and test the specific qrels at indices 0, 9, and 59
        self._test_qrels('sara', count=34413, items={
            0: TrecQrel('1', '201645', 0, '0'),
            13: TrecQrel('2', '221411', 2, '0'),
            31: TrecQrel('2', '173259', 1, '0')
        })

if __name__ == '__main__':
    unittest.main()
