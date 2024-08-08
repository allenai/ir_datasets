import re
import unittest
import ir_datasets
from ir_datasets.datasets.msmarco_document_v2_1_segmented import MsMarcoV21SegmentedDocument
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMSMarcoV21DocsSegmented(DatasetIntegrationTest):
    def test_ms_marco_docs_iter_full(self):
        self._test_docs('msmarco-document-v2.1/segmented', count=5371, items={
            0: MsMarcoV21SegmentedDocument(
                doc_id='msmarco_v2.1_doc_42_0#0_0',
                title='How to Use Flip Tool in GIMP',
                url='https://www.guidingtech.com/use-flip-tool-gimp/',
                headings=re.compile('^How to Use Flip Tool in\\nGIMP\\n\\nHow to Use Flip Tool in GIMP.*'),
                segment=re.compile('^How to Use Flip Tool in GIMP\\nHow to Use Flip Tool in GIMP\\nMehvish\\n06 Sep 2019.*'),
                start_char=0,
                end_char=800
            ),
           19: MsMarcoV21SegmentedDocument(
                doc_id='msmarco_v2.1_doc_42_6080#2_22424',
                title='How to Setup and Use FTP Server on Android',
                url='https://www.guidingtech.com/use-ftp-server-file-transfer-android/',
                headings=re.compile('How to Set\\u00adup and Use\\nFTP\nServ\\u00ader on Android\.*'),
                segment=re.compile('^Also Read: Best Alternatives to Google Apps\\nIn this post.*'),
                start_char=1032,
                end_char=1959
            ),
            5370: MsMarcoV21SegmentedDocument(
                doc_id='msmarco_v2.1_doc_42_3400697#6_9024928',
                title='Can Guinea Pigs Eat Leaves? - Guinea Pig Tube',
                url='https://www.guineapigtube.com/can-guinea-pigs-eat-leaves/',
                headings=re.compile('^Can Guinea Pigs Eat Leaves\?\\nCan Guinea Pigs Eat Leaves\?.*'),
                segment=re.compile('^They protect the body from free radical damage. The free radicals cause many health problems and also cause premature aging in guinea pigs.*'),
                start_char=2954,
                end_char=3767,
            ),
        })

    def test_fast_ms_marco_docs_store(self):
        docs_store = ir_datasets.load('msmarco-document-v2.1/segmented').docs_store()

        doc = docs_store.get('msmarco_v2.1_doc_02_968#0_1561')
        self.assertEqual('msmarco_v2.1_doc_02_968#0_1561', doc.doc_id)

        doc = docs_store.get('msmarco_v2.1_doc_03_0#3_5523')
        self.assertEqual('msmarco_v2.1_doc_03_0#3_5523', doc.doc_id)

    def test_fast_docs_store_on_non_existing_documents(self):
        docs_store = ir_datasets.load('msmarco-document-v2.1/segmented').docs_store()

        with self.assertRaises(Exception) as context:
            doc = docs_store.get('msmarco_v2.1_doc_02_968#0_156')

        self.assertTrue('Expecting value: line 1 column 1' in str(context.exception))

    def test_fast_ms_marco_docs_iter(self):
        # faster alternative to above
        docs_iter = ir_datasets.load('msmarco-document-v2.1/segmented').docs_iter()
        first_doc = docs_iter.__next__()
        second_doc = docs_iter.__next__()

        self.assertEqual('msmarco_v2.1_doc_42_0#0_0', first_doc.doc_id)
        self.assertEqual('msmarco_v2.1_doc_42_0#1_1311', second_doc.doc_id)

    def test_fast_docs_count(self):
        expected = 113520750
        actual = ir_datasets.load('msmarco-document-v2.1/segmented').docs_count()

        self.assertEqual(expected, actual)

    def test_fast_queries(self):
        self._test_queries('msmarco-document-v2.1/trec-rag-2024', count=301, items={
            0: GenericQuery('2024-145979', 'what is vicarious trauma and how can it be coped with?'),
            9: GenericQuery('2024-158743', 'what was happening in germany and netherlands in the 1840s'),
            300: GenericQuery('2024-21669', 'do abortions kill more black people than other weapons'),
        })


if __name__ == '__main__':
    unittest.main()
