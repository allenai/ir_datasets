import re
import unittest
import ir_datasets
from ir_datasets.datasets.msmarco_document_v2 import MsMarcoV2Document
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMSMarcoV21Docs(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('msmarco-document-v2.1', count=10960555, items={
            0: MsMarcoV2Document('msmarco_v2.1_doc_00_0', 'http://0-60.reviews/0-60-times/', '0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews', '0-60 Times\n0-60 Times', re.compile('^0\\-60 Times \\- 0\\-60 \\| 0 to 60 Times \\& 1/4 Mile Times \\| Zero to 60 Car Reviews\n0\\-60 Times\nThere are man.{4332} biggest touted numbers for vehicles, and easier for people to relate to than horsepower and torque\\.$', flags=48)),
            9: MsMarcoV2Document('msmarco_v2.1_doc_00_110582', 'http://003.clayton.k12.ga.us/', 'Home - Morrow High School', 'Morrow High\nMorrow High', re.compile("^Home \\- Morrow High School\nMore Options\nSelect a School\nDISTRICT\nCCPS\nElementary\nAnderson Elementary\n.{4959}oks Site\nMs\\. Cavazos' Site\nMr\\. Holbrook's Site\nMs\\. Hunt's Site\nMs\\. Lamarre's Site\nMr\\. McClain's Site$", flags=48)),
            10960554: MsMarcoV2Document('msmarco_v2.1_doc_59_964287870', 'https://zzzzbov.com/blag/shortcut-to-zoom', 'Shortcut to Zoom › zzzzBov.com', 'Shortcut to Zoom\nShortcut to Zoom\nBatch File\nShortcut\nTrying it out\n', re.compile('^Shortcut to Zoom › zzzzBov\\.com\n07 \\- Apr \\- 2020\nShortcut to Zoom\nI use Chrome on Windows as my primar.{2297}hat adding even a few of these to my start menu will help reduce just a bit more friction in my day\\.$', flags=48)),
        })

    def test_queries(self):
        self._test_queries('msmarco-document-v2.1/trec-rag-2024', count=301, items={
            0: GenericQuery('2024-145979', 'what is vicarious trauma and how can it be coped with?'),
            9: GenericQuery('2024-158743', 'what was happening in germany and netherlands in the 1840s'),
            300: GenericQuery('2024-21669', 'do abortions kill more black people than other weapons'),
        })


if __name__ == '__main__':
    unittest.main()
