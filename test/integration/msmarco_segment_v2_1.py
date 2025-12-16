import re
import unittest
import ir_datasets
from ir_datasets.datasets.msmarco_segment_v2_1 import MsMarcoV21SegmentedDoc
from ir_datasets.formats import TrecQrel, GenericQuery
from .base import DatasetIntegrationTest


_logger = ir_datasets.log.easy()


class TestMSMarcoV21DocsSegmented(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('msmarco-segment-v2.1', count=113520750, items={
            0: MsMarcoV21SegmentedDoc('msmarco_v2.1_doc_00_0#0_0', 'http://0-60.reviews/0-60-times/', '0-60 Times - 0-60 | 0 to 60 Times & 1/4 Mile Times | Zero to 60 Car Reviews', '0-60 Times\n0-60 Times', re.compile('^0\\-60 Times \\- 0\\-60 \\| 0 to 60 Times \\& 1/4 Mile Times \\| Zero to 60 Car Reviews\n0\\-60 Times\nThere are man.{1078}used as the standard in the United States, where the rest of the world prefers the 0\\-100 km version\\.$', flags=48), 0, 1278, 'msmarco_v2.1_doc_00_0', 0),
            9: MsMarcoV21SegmentedDoc('msmarco_v2.1_doc_00_4810#2_16701', 'http://0-www.worldcat.org.novacat.nova.edu/identities/lccn-n79036869/', 'Ethel Percy Andrus Gerontology Center [WorldCat Identities]', re.compile('^Ethel Percy Andrus Gerontology Center\nEthel Percy Andrus Gerontology Center\nAndrus \\(Ethel Percy\\) Ger.{409}niversity of Southern California Los Angeles, Calif Ethel Percy Andrus Gerontology Center\nLanguages\n$', flags=48), re.compile('^submitted to U\\.S\\. Department  of Health, Education, and Welfare, Public Health Service, Health Resea.{2311}e questionnaires used and the data derived from them, and how the data were collected and  analyzed\\.$', flags=48), 2265, 4776, 'msmarco_v2.1_doc_00_4810', 2),
            113520749: MsMarcoV21SegmentedDoc('msmarco_v2.1_doc_59_964287870#4_2159633396', 'https://zzzzbov.com/blag/shortcut-to-zoom', 'Shortcut to Zoom › zzzzBov.com', 'Shortcut to Zoom\nShortcut to Zoom\nBatch File\nShortcut\nTrying it out\n', re.compile('^When it asks "What would you like to name the shortcut\\?", type the name of the meeting \\(i\\.e\\. "Standu.{333}hat adding even a few of these to my start menu will help reduce just a bit more friction in my day\\.$', flags=48), 1963, 2497, 'msmarco_v2.1_doc_59_964287870', 4),
        })

    def test_2024_rag_queries(self):
        self._test_queries('msmarco-segment-v2.1/trec-rag-2024', count=301, items={
            0: GenericQuery('2024-145979', 'what is vicarious trauma and how can it be coped with?'),
            9: GenericQuery('2024-158743', 'what was happening in germany and netherlands in the 1840s'),
            300: GenericQuery('2024-21669', 'do abortions kill more black people than other weapons'),
        })

    def test_2025_rag_queries(self):
        self._test_queries('msmarco-segment-v2.1/trec-rag-2025', count=105, items={
            0: GenericQuery('464', "I want a thorough understanding of what makes up a community, including its definitions in various contexts like science and what it means to be a 'civilized community.' I'm also interested in related terms like 'grassroots organizations,' how communities set boundaries and priorities, and their roles in important areas such as preparedness and nation-building."),
            9: GenericQuery('66', "I want to understand how ethical theories like utilitarianism and Kantianism, along with professional codes, influence decision-making and leadership in organizations and professions such as social work and teaching. How do these ethical standards impact hiring, leadership styles, and personal behavior? I'm also puzzled why ethical people might still make unethical choices."),
            104: GenericQuery('838', "I'm looking to learn more about relationship dynamics, including how social media affects them and common challenges like long-distance relationships. I'm also interested in ways to improve communication and stability, the role of leisure activities, and how competency and image enhancement can help address social devaluation in relationships."),
        })

if __name__ == '__main__':
    unittest.main()
