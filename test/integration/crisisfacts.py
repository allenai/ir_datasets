import re
import unittest
from ir_datasets.datasets.crisisfacts import CrisisFactsStreamDoc, CrisisFactsQuery
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


class TestCranfield(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('crisisfacts/2017-12-07/001', count=5920, items={
            0: CrisisFactsStreamDoc('CrisisFACTS-001-Twitter-15712-0', 'CrisisFACTS-001', 'The homie tell me meet him at a time and this foo still ain’t here smh', 'Twitter', 1512604832),
            9: CrisisFactsStreamDoc('CrisisFACTS-001-Twitter-4592-0', 'CrisisFACTS-001', 'We are next! Be Safe San Diego! Be diligent, alert and prepared. ❤️❤️❤️ https://t.co/cPjBbbm7DW', 'Twitter', 1512605075),
            5919: CrisisFactsStreamDoc('CrisisFACTS-001-Twitter-49301-0', 'CrisisFACTS-001', 'today in school. scared out of my mind. #LilacFire https://t.co/OCfjmz2nPN', 'Twitter', 1512691196),
        })

    def test_queries(self):
        self._test_queries('crisisfacts/2017-12-07/001', count=2, items={
            0: CrisisFactsQuery('CrisisFACTS-Wildfire-q001', 'What is the area does the wildfire affect?', 'Report-Factoid', 'CrisisFACTS-001', 'Lilac Wildfire 2017', '2017_12_07_lilac_wildfire.2017', 'The Lilac Fire was a fire that burned in northern San Diego County, California, United States, and the second-costliest one of multiple wildfires that erupted in Southern California in December 2017.', 'TRECIS-CTIT-H-092', 'Wildfire', 'https://en.wikipedia.org/wiki/Lilac_Fire'),
            1: CrisisFactsQuery('CrisisFACTS-Test-q001', 'TODO', None, 'CrisisFACTS-001', 'Lilac Wildfire 2017', '2017_12_07_lilac_wildfire.2017', 'The Lilac Fire was a fire that burned in northern San Diego County, California, United States, and the second-costliest one of multiple wildfires that erupted in Southern California in December 2017.', 'TRECIS-CTIT-H-092', 'Wildfire', 'https://en.wikipedia.org/wiki/Lilac_Fire'),
        })

if __name__ == '__main__':
    unittest.main()
