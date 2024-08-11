import re
import unittest
import ir_datasets
from ir_datasets.datasets.msmarco_document_v2_1_segmented import MsMarcoV21SegmentedDocument
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMSMarcoV21DocsSegmented(DatasetIntegrationTest):
    def test_docs(self):
        self._build_test_docs('msmarco-segment-v2.1')
        # self._test_docs('msmarco-segment-v2.1', count=113520750, items={
        #     0: MsMarcoV21SegmentedDocument('msmarco_v2.1_doc_42_0#0_0', 'https://www.guidingtech.com/use-flip-tool-gimp/', 'How to Use Flip Tool in GIMP', re.compile('^How to Use Flip Tool in\nGIMP\n\nHow to Use Flip Tool in GIMP\nMehvish\n1\\. Using the Built\\-In Flip Tool\nF.{85}ol\nFlip a Layer\nFlip All Layers in GIMP\nBonus Tip: Create Mirror Effect in GIMP\nMagic of the Mirror\n$', flags=48), re.compile("^How to Use Flip Tool in GIMP\nHow to Use Flip Tool in GIMP\nMehvish\n06 Sep 2019\nAt times, the powerful.{600}re is a guide on how to flip an image in GIMP\\. There are two methods to do it\\. Let's check them out\\.$", flags=48), 0, 800),
        #     9: MsMarcoV21SegmentedDocument('msmarco_v2.1_doc_42_0#9_9963', 'https://www.guidingtech.com/use-flip-tool-gimp/', 'How to Use Flip Tool in GIMP', re.compile('^How to Use Flip Tool in\nGIMP\n\nHow to Use Flip Tool in GIMP\nMehvish\n1\\. Using the Built\\-In Flip Tool\nF.{85}ol\nFlip a Layer\nFlip All Layers in GIMP\nBonus Tip: Create Mirror Effect in GIMP\nMagic of the Mirror\n$', flags=48), re.compile('^Flip a Layer\nTo do so, follow these steps: Step 1: Open the image in GIMP\\. Step 2: Click on the Laye.{309} click on the Image option present in the top bar and select Transform followed by your flip choice\\.$', flags=48), 2862, 3372),
        #     113520749: MsMarcoV21SegmentedDocument('msmarco_v2.1_doc_04_1869956217#8_3169040836', 'http://www.city-data.com/city/Sedgwick-Kansas.html', 'Sedgwick, Kansas (KS 67135) profile: population, maps, real estate, averages, homes, statistics, relocation, travel, jobs, hospitals, schools, crime, moving, houses, news, sex offenders', re.compile('^Sedgwick, Kansas\nSedgwick, Kansas\nLoading data\\.\\.\\.\nCrime rates in Sedgwick by year\nType\n2007\n2011\n201.{1539}ing System \\(NFIRS\\) incidents\nSedgwick compared to Kansas state average:\nOther pages you might like:\n$', flags=48), re.compile('^79\\.8 \\(low, U\\.S\\. average is 100\\)\nSedgwick, KS residents, houses, and apartments details\nPercentage of.{7764}house built \\- Built 1939 or earlier \\(%\\) Average household size Household density \\(households per squ$', flags=48), 2037, 10000),
        # })

    def test_queries(self):
        self._test_queries('msmarco-segment-v2.1/trec-rag-2024', count=301, items={
            0: GenericQuery('2024-145979', 'what is vicarious trauma and how can it be coped with?'),
            9: GenericQuery('2024-158743', 'what was happening in germany and netherlands in the 1840s'),
            300: GenericQuery('2024-21669', 'do abortions kill more black people than other weapons'),
        })


if __name__ == '__main__':
    unittest.main()
