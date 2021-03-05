import re
import unittest
from ir_datasets.formats import GenericDoc, GenericQuery, TrecQrel, GenericDocPair, GenericScoredDoc
from ir_datasets.datasets.msmarco_qna import MsMarcoQnADoc, MsMarcoQnAQuery, MsMarcoQnAEvalQuery
from .base import DatasetIntegrationTest


class TestMsMarcoQnA(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('msmarco-qna', count=9048606, items={
            0: MsMarcoQnADoc('0-0', re.compile('^The presence of communication amid scientific minds was equally important to the success of the Manh.{125}nd engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated\\.$', flags=48), 'http://www.pitt.edu/~sdb14/atombomb.html', '0', 'D59219'),
            9: MsMarcoQnADoc('9-0', re.compile("^One of the main reasons Hanford was selected as a site for the Manhattan Project's B Reactor was its.{13} the Columbia River, the largest river flowing into the Pacific Ocean from the North American coast\\.$", flags=48), 'https://www.atomicheritage.org/history/environmental-consequences', '9', 'D59228'),
            9048605: MsMarcoQnADoc('120010-0', re.compile('^Considering the cost of tuition at a place like UNT, this logic would read that the total cost – inc.{36}er year at UNT would be around \\$9,000\\. For Indiana, the total cost should be around \\$18,000\\-\\$19,000\\.$', flags=48), 'http://musicschoolcentral.com/real-cost-dollars-getting-college-music-education/', '120010', 'D59214'),
        })

    def test_queries(self):
        self._test_queries('msmarco-qna/train', count=808731, items={
            0: MsMarcoQnAQuery('1185869', ')what was the immediate impact of the success of the manhattan project?', 'DESCRIPTION', ('The immediate impact of the success of the manhattan project was the only cloud hanging over the impressive achievement of the atomic researchers and engineers is what their success truly meant; hundreds of thousands of innocent lives obliterated.',)),
            9: MsMarcoQnAQuery('410717', 'is funner a word?', 'DESCRIPTION', ('Yes, funner is a word.',)),
            808730: MsMarcoQnAQuery('461916', 'name some organisms that might live in a marine biome', 'ENTITY', ('Walrus, star fish, eel, crabs, jelly fish, and fresh and salt water fish',)),
        })
        self._test_queries('msmarco-qna/dev', count=101093, items={
            0: MsMarcoQnAQuery('1102432', '. what is a corporation?', 'DESCRIPTION', ('A corporation is a company or group of people authorized to act as a single entity and recognized as such in law.',)),
            9: MsMarcoQnAQuery('36558', 'average force of a raindrop', 'NUMERIC', ()),
            101092: MsMarcoQnAQuery('371455', 'how to offer health insurance to employees', 'DESCRIPTION', ('List the elements you want to include in a health insurance package for your employees. Investigate the financial impact of various deductibles, family coverage vs. individual coverage and other factors.',)),
        })
        self._test_queries('msmarco-qna/eval', count=101092, items={
            0: MsMarcoQnAEvalQuery('1136966', '#ffffff color code', 'ENTITY'),
            9: MsMarcoQnAEvalQuery('80665', 'can you use horse trailer for hay', 'DESCRIPTION'),
            101091: MsMarcoQnAEvalQuery('315646', 'how much does it cost to go to college online', 'NUMERIC'),
        })

    def test_qrels(self):
        self._test_qrels('msmarco-qna/train', count=8069749, items={
            0: TrecQrel('1185869', '0-0', 1, '0'),
            9: TrecQrel('1185869', '9-0', 0, '0'),
            8069748: TrecQrel('461916', '7066857-0', 0, '0'),
        })
        self._test_qrels('msmarco-qna/dev', count=1008985, items={
            0: TrecQrel('1102432', '7066858-0', 0, '0'),
            9: TrecQrel('1102432', '7066861-0', 0, '0'),
            1008984: TrecQrel('371455', '8009483-0', 0, '0'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('msmarco-qna/train', count=8069749, items={
            0: GenericScoredDoc('1185869', '0-0', 0.0),
            9: GenericScoredDoc('1185869', '9-0', -9.0),
            8069748: GenericScoredDoc('461916', '7066857-0', -9.0),
        })
        self._test_scoreddocs('msmarco-qna/dev', count=1008985, items={
            0: GenericScoredDoc('1102432', '7066858-0', 0.0),
            9: GenericScoredDoc('1102432', '7066861-0', -9.0),
            1008984: GenericScoredDoc('371455', '8009483-0', -9.0),
        })
        self._test_scoreddocs('msmarco-qna/eval', count=1008943, items={
            0: GenericScoredDoc('1136966', '7164732-0', 0.0),
            9: GenericScoredDoc('1136966', '8009488-0', -9.0),
            1008942: GenericScoredDoc('315646', '120010-0', -9.0),
        })


if __name__ == '__main__':
    unittest.main()
