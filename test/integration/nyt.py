import re
import unittest
from ir_datasets.datasets.nyt import NytDoc
from ir_datasets.formats import GenericQrel, GenericQuery
from .base import DatasetIntegrationTest


class TestNyt(DatasetIntegrationTest):
    def test_nyt_docs(self):
        self._test_docs('nyt', count=1864661, items={
            0: NytDoc('8454', re.compile('^<title>MARSH \\&amp; MCLENNAN INC reports earnings for Qtr to Dec 31</title>\n<meta content="30" name=".{1089}5E0D7133FF933A05752C0A961948260" item\\-length="53" name="The New York Times" unit\\-of\\-measure="word"/>$', flags=48), 'MARSH & MCLENNAN INC reports earnings for Qtr to Dec 31', re.compile('^<p>LEAD:</p>\n<p>\\*3\\*\\*\\* COMPANY REPORTS \\*\\*</p>\n<p>\\*3\\*MARSH \\&amp; MCLENNAN INC \\(NYSE\\)</p>\n<p>Qtr to Dec.{228}</p>\n<p>Net inc</p>\n<p>243,200,000</p>\n<p>162,900 000</p>\n<p>Share earns</p>\n<p>3\\.30</p>\n<p>2\\.23</p>$', flags=48), re.compile('^<p>LEAD:</p>\n<p>\\*3\\*\\*\\* COMPANY REPORTS \\*\\*</p>\n<p>\\*3\\*MARSH \\&amp; MCLENNAN INC \\(NYSE\\)</p>\n<p>Qtr to Dec.{644}</p>\n<p>Net inc</p>\n<p>243,200,000</p>\n<p>162,900 000</p>\n<p>Share earns</p>\n<p>3\\.30</p>\n<p>2\\.23</p>$', flags=48)),
            9: NytDoc('8579', re.compile('^<title>SALINGER BIOGRAPHY IS BLOCKED</title>\n<meta content="30" name="publication_day_of_month"/>\n<m.{1900}6DC1530F933A05752C0A961948260" item\\-length="1080" name="The New York Times" unit\\-of\\-measure="word"/>$', flags=48), 'SALINGER BIOGRAPHY IS BLOCKED', "<p>LEAD: A biography of J. D. Salinger was blocked yesterday by a Federal appeals court in Manhattan that said the book unfairly used Mr. Salinger's unpublished letters.</p>", re.compile('^<p>LEAD: A biography of J\\. D\\. Salinger was blocked yesterday by a Federal appeals court in Manhattan.{6597}in his lifetime, the appeals court said he was entitled to protect his opportunity to sell them\\.</p>$', flags=48)),
            1864660: NytDoc('1854817', re.compile('^<title>STRATEGY ON IRAN STIRS NEW DEBATE AT WHITE HOUSE</title>\n<meta content="16diplo" name="slug"/.{5705}4D7153FF935A25755C0A9619C8B63" item\\-length="1373" name="The New York Times" unit\\-of\\-measure="word"/>$', flags=48), 'STRATEGY ON IRAN STIRS NEW DEBATE AT WHITE HOUSE', re.compile('^<p>A year after President Bush and Secretary of State Condoleezza Rice announced a new strategy towa.{463}, are pressing for greater consideration of military strikes against Iranian nuclear facilities\\.</p>$', flags=48), re.compile("^<p>A year after President Bush and Secretary of State Condoleezza Rice announced a new strategy towa.{8278}o left his post partly over his opposition to the administration's recent deal with North Korea\\.</p>$", flags=48)),
        })

    def test_nyt_queries(self):
        self._test_queries('nyt/train', count=1863657, items={
            0: GenericQuery('8454', 'MARSH & MCLENNAN INC reports earnings for Qtr to Dec 31'),
            9: GenericQuery('8579', 'SALINGER BIOGRAPHY IS BLOCKED'),
            1863656: GenericQuery('1854817', 'STRATEGY ON IRAN STIRS NEW DEBATE AT WHITE HOUSE'),
        })
        self._test_queries('nyt/valid', count=1004, items={
            0: GenericQuery('6461', "Why We're Forced To Be Slumlords"),
            9: GenericQuery('13148', 'NOVAMETRIX MEDICAL SYSTEMS INC reports earnings for Qtr to Dec 31'),
            1003: GenericQuery('1854529', 'The Newest Antique: Atari'),
        })

    def test_nyt_qrels(self):
        self._test_qrels('nyt/train', count=1863657, items={
            0: GenericQrel('8454', '8454', 1),
            9: GenericQrel('8579', '8579', 1),
            1863656: GenericQrel('1854817', '1854817', 1),
        })
        self._test_qrels('nyt/valid', count=1004, items={
            0: GenericQrel('6461', '6461', 1),
            9: GenericQrel('13148', '13148', 1),
            1003: GenericQrel('1854529', '1854529', 1),
        })


if __name__ == '__main__':
    unittest.main()
