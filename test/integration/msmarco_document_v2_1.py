import re
import unittest
import ir_datasets
from ir_datasets.datasets.msmarco_document_v2_1 import MsMarcoV2Document
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMSMarcoV21Docs(DatasetIntegrationTest):
    def test_ms_marco_docs_iter_full(self):
        self._test_docs('msmarco-document-v2.1', count=5371, items={
            0: MsMarcoV2Document(
                doc_id='msmarco_v2.1_doc_12_0',
                title='Who Is Ringo Starr\'s Wife Barbara Bach and How Many Children Do They Have?',
                url='https://answersafrica.com/ringo-starrs-wife-children.html',
                headings=re.compile('.*Wife Barbara Bach.*'),
                body=re.compile('^Who Is Ringo Starr\'s Wife Barbara Bach.*')
            ),
            9: MsMarcoV2Document(
                doc_id='msmarco_v2.1_doc_12_70974',
                title='List of Robin Williams Movies and TV Shows From Best To Worst',
                url='https://answersafrica.com/robin-williams-movies-tv-shows.html',
                headings=re.compile('List of Robin Williams Movies and TV Shows.*'),
                body=re.compile('List of Robin Williams Movies and TV Shows From Best To Worst\nList of Robin Williams Movies and TV Shows From Best To Worst*')
            ),
            5370: MsMarcoV2Document(
                doc_id='msmarco_v2.1_doc_12_48692010',
                title='Warriors of Waterdeep 2.11.13 (Mod) latest',
                url='https://apkdry.com/warriors-of-waterdeep-2-3-24-mod/',
                headings=re.compile('^Warriors of Waterdeep 2.11.13 \(Mod\)\\nWarriors of Waterdeep 2.11.13 \(Mod\)\\nFeatures and Screenshots Warriors of Waterdeep Game for Android.*'),
                body=re.compile('Warriors of Waterdeep 2.11.13 \(Mod\) latest\\nWarriors of Waterdeep 2.11.13 \(Mod\)\\nby Apkdry 3 weeks ago Games.*')
            ),
        })

    def test_fast_ms_marco_docs_store(self):
        docs_store = ir_datasets.load('msmarco-document-v2.1').docs_store()

        doc = docs_store.get('msmarco_v2.1_doc_12_0')
        self.assertEqual('msmarco_v2.1_doc_12_0', doc.doc_id)

        doc = docs_store.get('msmarco_v2.1_doc_12_48692010')
        self.assertEqual('msmarco_v2.1_doc_12_48692010', doc.doc_id)

    def test_fast_docs_store_on_non_existing_documents(self):
        docs_store = ir_datasets.load('msmarco-document-v2.1').docs_store()

        with self.assertRaises(Exception) as context:
            doc = docs_store.get('msmarco_v2.1_doc_12_111')

        self.assertTrue('Expecting value: line 1 column 1' in str(context.exception))

    def test_fast_ms_marco_docs_iter(self):
        # faster alternative to above
        docs_iter = ir_datasets.load('msmarco-document-v2.1').docs_iter()
        first_doc = docs_iter.__next__()
        second_doc = docs_iter.__next__()

        self.assertEqual('msmarco_v2.1_doc_12_0', first_doc.doc_id)
        self.assertEqual('msmarco_v2.1_doc_12_5689', second_doc.doc_id)


if __name__ == '__main__':
    unittest.main()
