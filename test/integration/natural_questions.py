import re
import unittest
from ir_datasets.formats import GenericQuery,GenericScoredDoc
from ir_datasets.datasets.natural_questions import NqPassageDoc, NqQrel
from .base import DatasetIntegrationTest


class TestNq(DatasetIntegrationTest):
    def test_docs(self):
        self._test_docs('natural-questions', count=28390850, items={
            0: NqPassageDoc('0-0', re.compile('^The trade winds are the prevailing pattern of easterly surface winds found in the tropics , within t.{470}on into the Americas and trade routes to become established across the Atlantic and Pacific oceans \\.$', flags=48), re.compile('^<P> The trade winds are the prevailing pattern of easterly surface winds found in the tropics , with.{479}to the Americas and trade routes to become established across the Atlantic and Pacific oceans \\. </P>$', flags=48), 43178, 44666, 44, 161, 'Trade winds', 'https://en.wikipedia.org//w/index.php?title=Trade_winds&amp;oldid=817251427', None),
            9: NqPassageDoc('0-9', re.compile('^During mid\\-summer in the Northern Hemisphere \\( July \\) , the westward \\- moving trade winds south of t.{818} decline in the health of coral reefs across the Caribbean and Florida , primarily since the 1970s \\.$', flags=48), re.compile('^<P> During mid\\-summer in the Northern Hemisphere \\( July \\) , the westward \\- moving trade winds south .{827}ine in the health of coral reefs across the Caribbean and Florida , primarily since the 1970s \\. </P>$', flags=48), 58657, 60294, 1304, 1488, 'Trade winds', 'https://en.wikipedia.org//w/index.php?title=Trade_winds&amp;oldid=817251427', None),
            28390849: NqPassageDoc('231694-385', 'A ball hit high and hard close to the leading edge , causing a low flight and a slight vibratory feel .', '<Dd> A ball hit high and hard close to the leading edge , causing a low flight and a slight vibratory feel . </Dd>', 143007, 143117, 13559, 13583, 'Glossary of golf', 'https://en.wikipedia.org//w/index.php?title=Glossary_of_golf&amp;oldid=830653780', '231694-384'),
        })

    def test_queries(self):
        self._test_queries('natural-questions/train', count=307373, items={
            0: GenericQuery('4549465242785278785', 'when is the last episode of season 8 of the walking dead'),
            9: GenericQuery('3542596469291219966', 'when was the first robot used in surgery'),
            307372: GenericQuery('-9055447625982456209', 'why is the dark age called the dark age'),
        })
        self._test_queries('natural-questions/dev', count=7830, items={
            0: GenericQuery('5225754983651766092', 'what purpose did seasonal monsoon winds have on trade'),
            9: GenericQuery('8467542931261548456', 'global trade during the ming dynasty of china'),
            7829: GenericQuery('6752717162503553157', 'how many goals have arsenal scored in the premier league'),
        })

    def test_qrels(self):
        self._test_qrels('natural-questions/train', count=152148, items={
            0: NqQrel('4549465242785278785', '7369-92', 1, ['March 18 , 2018'], 'NONE'),
            9: NqQrel('-3126006632503975915', '7383-23', 1, [], 'NONE'),
            152147: NqQrel('-9055447625982456209', '29568-1', 1, [], 'NONE'),
        })
        self._test_qrels('natural-questions/dev', count=7695, items={
            0: NqQrel('5225754983651766092', '0-0', 1, ['enabled European empire expansion into the Americas and trade routes to become established across the Atlantic and Pacific oceans'], 'NONE'),
            9: NqQrel('8081436745274892553', '11-4', 1, [], 'NONE'),
            7694: NqQrel('-430859680692445019', '7366-0', 1, ['1980s'], 'NONE'),
        })

    def test_scoreddocs(self):
        self._test_scoreddocs('natural-questions/train', count=40374730, items={
            0: GenericScoredDoc('4549465242785278785', '7369-0', 0.0),
            9: GenericScoredDoc('4549465242785278785', '7369-9', 0.0),
            40374729: GenericScoredDoc('-9055447625982456209', '29568-34', 0.0),
        })
        self._test_scoreddocs('natural-questions/dev', count=973480, items={
            0: GenericScoredDoc('5225754983651766092', '0-0', 0.0),
            9: GenericScoredDoc('5225754983651766092', '0-9', 0.0),
            973479: GenericScoredDoc('6752717162503553157', '7368-391', 0.0),
        })


if __name__ == '__main__':
    unittest.main()
